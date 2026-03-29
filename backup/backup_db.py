#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作模块
Database Operations Module
"""

import asyncio
import logging
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from models.backup import BackupTask, BackupSet, BackupFile, BackupTaskStatus, BackupFileType, BackupSetStatus
from utils.datetime_utils import now, format_datetime
from backup.queued_files_optimizer import (
    mark_files_as_queued_optimized,
    verify_files_queued_optimized,
    ensure_index_exists
)

logger = logging.getLogger(__name__)


class BatchDBWriter:
    """批量数据库写入器 - 保持所有原有字段，提升写入性能"""

    def __init__(self, backup_set_db_id: int, batch_size: int = 5000, max_queue_size: int = 20000, timeout: Optional[int] = 5):
        self.backup_set_db_id = backup_set_db_id
        # 批次大小（用于统计，实际批次大小由调用者决定）
        self.batch_size = batch_size
        # 保留参数以兼容现有代码，但不再使用队列
        self.max_queue_size = max_queue_size
        self.timeout = timeout  # None 表示不使用超时检测

        # 不再使用队列，直接同步写入
        # self.file_queue = asyncio.Queue(maxsize=max_queue_size)  # 已移除队列
        self._batch_buffer = []
        self._is_running = False
        self._worker_task = None  # 不再使用后台worker
        self._stats = {
            'total_files': 0,
            'batch_count': 0,
            'total_time': 0
        }
        # Redis模式：缓存文件路径到ID的映射（仅缓存当前批次查询过的）
        self._file_path_cache = {}

    async def start(self):
        """启动批量写入器（顺序执行模式：不使用队列和worker）"""
        if self._is_running:
            return

        self._is_running = True
        # 顺序执行模式：不使用队列和worker，直接同步写入
        # self._worker_task = asyncio.create_task(self._batch_worker())  # 已移除worker
        logger.info(f"批量数据库写入器已启动（顺序执行模式，不使用队列）(batch_size={self.batch_size})")

    async def add_file(self, file_info: Dict):
        """添加文件到批量写入队列（可阻塞，只对临时性错误无限重试）"""
        if not self._is_running:
            await self.start()

        # 只对临时性错误（队列满、超时）无限重试，永久性错误立即抛出
        retry_count = 0
        while True:
            try:
                # 等待队列有空位，这里会产生背压
                # 移除超时限制，无限等待直到成功（队列满是临时性的）
                await self.file_queue.put(file_info)
                self._stats['total_files'] += 1
                return  # 成功添加，退出循环
            except (asyncio.TimeoutError, asyncio.QueueFull) as e:
                # 临时性错误：队列满或超时，无限重试
                retry_count += 1
                if retry_count % 100 == 0:  # 每100次重试记录一次日志
                    logger.info(f"批量写入队列满，重试第 {retry_count} 次: {file_info.get('path', 'unknown')[:200]}")
                # 等待一小段时间后重试
                await asyncio.sleep(0.1)
            except (ValueError, KeyError, TypeError) as e:
                # 永久性错误：文件信息格式错误，不应该重试
                logger.error(f"文件信息格式错误，无法添加到队列: {file_info.get('path', 'unknown')[:200]}, 错误: {str(e)}")
                raise  # 抛出异常，让调用者处理
            except Exception as e:
                # 其他未知错误，记录但尝试重试（可能是临时性错误）
                retry_count += 1
                error_msg = str(e).lower()
                # 检查是否是永久性错误
                permanent_error_keywords = ['not found', 'does not exist', 'permission denied', 'access denied', 'invalid path', 'invalid file']
                if any(keyword in error_msg for keyword in permanent_error_keywords):
                    logger.error(f"永久性错误，无法添加到队列: {file_info.get('path', 'unknown')[:200]}, 错误: {str(e)}")
                    raise  # 永久性错误，抛出异常
                # 可能是临时性错误，重试
                if retry_count % 100 == 0:
                    logger.info(f"批量写入队列未知错误，重试第 {retry_count} 次: {file_info.get('path', 'unknown')[:200]}, 错误: {str(e)}")
                await asyncio.sleep(0.1)
    
    async def write_batch_sync(self, file_batch: List[Dict]):
        """同步写入一个批次（顺序执行模式：扫描完写数据库，全部写入后再继续扫描）
        
        不使用队列，直接同步写入数据库，等待全部写入完成后再返回
        """
        if not file_batch:
            return
        
        # 更新统计信息
        self._stats['total_files'] += len(file_batch)
        self._stats['batch_count'] += 1
        
        # 直接调用 _process_batch，不使用队列，同步等待写入完成
        await self._process_batch(file_batch)

    async def _batch_worker(self):
        """批量写入worker"""
        from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
        from utils.datetime_utils import now
        import time

        start_time = now()
        # 优化：ES每次给5000个文件，立即处理，不等待
        # 快速非阻塞收集，一旦队列有文件就立即处理，最小化等待

        try:
            while self._is_running or not self.file_queue.empty():
                batch = []

                # 收集批次数据（优化：快速收集，不等待）
                try:
                    # 等待第一个文件（如果设置了超时则使用超时，否则无限等待）
                    if self.timeout is not None:
                        first_file = await asyncio.wait_for(
                            self.file_queue.get(), timeout=float(self.timeout)
                        )
                    else:
                        # 不使用超时检测，无限等待直到有文件
                        first_file = await self.file_queue.get()
                    batch.append(first_file)
                    self.file_queue.task_done()

                    # 快速非阻塞收集更多文件（不等待，立即处理）
                    # ES扫描器每次给5000个文件，这些文件会快速进入队列
                    # worker应该立即处理，不等待批次积累
                    while len(batch) < self.batch_size:
                        try:
                            # 尝试非阻塞获取（不等待，立即处理）
                            file_info = self.file_queue.get_nowait()
                            batch.append(file_info)
                            self.file_queue.task_done()
                        except asyncio.QueueEmpty:
                            # 队列为空，立即处理现有批次（不等待）
                            # ES扫描器会一次性添加很多文件，队列很快就会有文件
                            break

                except asyncio.TimeoutError:
                    # 超时但没有文件，检查是否应该退出
                    if not self._is_running:
                        break
                    continue

                # 处理批次（至少有一个文件）
                if batch:
                    batch_start = now()
                    await self._process_batch(batch)
                    batch_time = (now() - batch_start).total_seconds()
                    self._stats['batch_count'] += 1

                    queue_size = self.file_queue.qsize()
                    speed = len(batch) / batch_time if batch_time > 0 else float('inf')
                    # 仅保留 openGauss / 内存数据库模式的日志
                    logger.info(f"[批量写入] 批次 #{self._stats['batch_count']}: {len(batch)} 个文件，耗时 {batch_time:.2f}s，速度 {speed:.1f} 个/秒，队列剩余 {queue_size} 个")

        except Exception as e:
            logger.error(f"批量写入worker异常: {e}")
            raise
        finally:
            total_time = (now() - start_time).total_seconds()
            self._stats['total_time'] = total_time
            logger.info(f"批量写入器停止，处理了 {self._stats['total_files']} 个文件，"
                       f"完成 {self._stats['batch_count']} 个批次，总耗时 {total_time:.1f}s")

    async def _process_batch(self, file_batch: List[Dict]):
        """处理一个文件批次（仅保留 openGauss / 内存数据库）"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, get_backup_files_table_by_set_id
        from utils.datetime_utils import now
        from datetime import timezone

        if not file_batch:
            return

        # 仅保留 openGauss 分支；内存数据库不在此方法中处理持久化
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                # 准备数据分类
                insert_data = []
                update_data = []

                # 提取文件路径用于查询（去重，避免重复查询）
                file_paths = list(dict.fromkeys(f.get('path', '') for f in file_batch if f.get('path')))

                # 批量查询已存在的文件
                if file_paths:
                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    table_name = await get_backup_files_table_by_set_id(conn, self.backup_set_db_id)

                    existing_files = await conn.fetch(
                        f"""
                        SELECT id, file_path, is_copy_success
                        FROM {table_name}
                        WHERE backup_set_id = $1 AND file_path = ANY($2)
                        """,
                        self.backup_set_db_id, file_paths
                    )
                    # 注意：如果有多条相同 file_path 的记录，只保留第一条（id 最小的）
                    existing_map = {}
                    for row in existing_files:
                        file_path = row['file_path']
                        if file_path not in existing_map:
                            existing_map[file_path] = row
                        else:
                            # 如果已存在，保留 id 较小的记录（避免更新错误的记录）
                            if row['id'] < existing_map[file_path]['id']:
                                existing_map[file_path] = row
                else:
                    existing_map = {}

                # 用于跟踪本批次中已处理的路径，避免同一批次内重复插入
                processed_paths_in_batch = set()

                # 分类处理文件
                for file_info in file_batch:
                    file_path = file_info.get('path', '')
                    if not file_path:
                        continue  # 跳过空路径

                    # 检查本批次内是否已处理过该路径（避免同一批次内重复插入）
                    if file_path in processed_paths_in_batch:
                        logger.debug(f"[_process_batch] 跳过本批次内重复路径: {file_path}")
                        continue

                    if file_path in existing_map:
                        existing = existing_map[file_path]
                        if existing['is_copy_success']:
                            continue  # 跳过已成功复制的文件

                        # 准备更新参数
                        update_params = self._prepare_update_params(file_info, existing['id'])
                        update_data.append(update_params)
                        processed_paths_in_batch.add(file_path)
                    else:
                        # 准备插入参数
                        insert_params = self._prepare_insert_params(file_info)
                        insert_data.append(insert_params)
                        processed_paths_in_batch.add(file_path)

                # 执行批量操作（openGauss 多表方案下，_batch_insert/_batch_update 内部会按 backup_set_id 选择表名）
                if insert_data:
                    await self._batch_insert(conn, insert_data)
                
                if update_data:
                    await self._batch_update(conn, update_data)
        else:
            logger.info(f"当前仅支持 openGauss，跳过批量写入 {len(file_batch)} 个文件")

    async def _process_batch_redis(self, file_batch: List[Dict]):
        """处理一个文件批次（Redis版本，优化10000条记录写入速度）"""
        from config.redis_db import get_redis_client
        from backup.redis_backup_db import (
            KEY_PREFIX_BACKUP_FILE, KEY_INDEX_BACKUP_FILES,
            KEY_INDEX_BACKUP_FILE_BY_SET_ID, _get_redis_key
        )
        from backup.redis_backup_db import KEY_COUNTER_BACKUP_FILE
        import json as json_module
        from datetime import timezone
        import time

        if not file_batch:
            return

        batch_start_time = time.time()
        
        try:
            redis = await get_redis_client()
            
            # ========== 优化阶段1：快速分类文件（仅查缓存）==========
            step1_start = time.time()
            
            # 预先提取所有文件路径和构建映射（减少循环）
            batch_file_paths = []
            file_info_map = {}
            for file_info in file_batch:
                file_path = file_info.get('path', '')
                if file_path:
                    batch_file_paths.append(file_path)
                    file_info_map[file_path] = file_info
            
            # 只检查缓存，不查询Redis（第一次扫描时都是新文件，缓存为空）
            existing_file_paths = {}
            new_file_paths = []
            
            for file_path in batch_file_paths:
                if file_path in self._file_path_cache:
                    existing_file_paths[file_path] = self._file_path_cache[file_path]
                else:
                    new_file_paths.append(file_path)
            
            step1_time = time.time() - step1_start
            
            # ========== 优化阶段2：批量检查已复制文件（仅检查已存在的）==========
            skipped_paths = set()
            
            if existing_file_paths:
                step_check_start = time.time()
                check_pipe = redis.pipeline()
                check_items = []
                
                for file_path, file_id in existing_file_paths.items():
                    file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                    check_pipe.hget(file_key, 'is_copy_success')
                    check_items.append(file_path)
                
                copy_status_results = await check_pipe.execute()
                step_check_time = time.time() - step_check_start
                
                # 过滤已成功复制的文件
                for file_path, is_copy_success in zip(check_items, copy_status_results):
                    if is_copy_success in ('1', 'True', 'true'):
                        skipped_paths.add(file_path)
                        # 从待处理列表中移除
                        existing_file_paths.pop(file_path, None)
            
            # ========== 优化阶段3：预先准备基础数据（减少重复计算）==========
            current_time = datetime.now()
            current_time_tz = current_time.replace(tzinfo=timezone.utc)
            
            # 过滤出需要插入的新文件（排除已跳过的）
            actual_new_file_paths = [p for p in new_file_paths if p not in skipped_paths]
            new_file_count = len(actual_new_file_paths)
            new_file_ids = []
            
            # ========== 优化阶段4：合并ID获取和数据准备（减少循环次数）==========
            step_prep_start = time.time()
            
            # 关键优化：使用INCRBY批量获取ID范围（只需1次网络往返）
            if new_file_count > 0:
                step_id_start = time.time()
                # 使用INCRBY一次性获取ID范围（原子操作，1次网络往返）
                # 先获取起始ID
                start_id = await redis.incr(KEY_COUNTER_BACKUP_FILE)
                # 如果批量大于1，继续递增计数器获取剩余ID
                if new_file_count > 1:
                    # 使用INCRBY一次性增加 (new_file_count - 1)，因为第一个ID已经通过INCR获取
                    await redis.incrby(KEY_COUNTER_BACKUP_FILE, new_file_count - 1)
                # 生成ID列表：start_id 到 start_id + new_file_count - 1
                new_file_ids = list(range(start_id, start_id + new_file_count))
                step_id_time = time.time() - step_id_start
            else:
                step_id_time = 0
                new_file_ids = []
            
            insert_items = []
            update_operations = []
            file_id_index = 0
            set_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_SET_ID}:{self.backup_set_db_id}"
            
            # 预先准备插入数据（只处理实际需要插入的新文件）
            # 优化：使用列表推导式和预计算来减少循环开销
            for file_path in actual_new_file_paths:
                file_info = file_info_map[file_path]
                file_stat = file_info.get('file_stat')
                
                # 文件大小提取（优化：减少条件判断）
                file_size = (
                    int(file_info['size'])
                    if 'size' in file_info and file_info.get('size') is not None
                    else (file_stat.st_size if file_stat and hasattr(file_stat, 'st_size') else 0)
                )
                
                file_name = file_info.get('name') or Path(file_path).name
                metadata = file_info.get('file_metadata') or {}
                metadata.update({'scanned_at': current_time.isoformat()})
                
                file_id = new_file_ids[file_id_index]
                file_id_index += 1
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                
                # 预先计算时间字段（避免在循环中重复计算）
                created_time_str = (
                    datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc).isoformat()
                    if file_stat and hasattr(file_stat, 'st_ctime')
                    else ''
                )
                modified_time_str = (
                    datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc).isoformat()
                    if file_stat and hasattr(file_stat, 'st_mtime')
                    else ''
                )
                accessed_time_str = (
                    datetime.fromtimestamp(file_stat.st_atime, tz=timezone.utc).isoformat()
                    if file_stat and hasattr(file_stat, 'st_atime')
                    else ''
                )
                permissions_str = (
                    oct(file_stat.st_mode)[-3:]
                    if file_stat and hasattr(file_stat, 'st_mode')
                    else ''
                )
                
                insert_mapping = {
                    'backup_set_id': str(self.backup_set_db_id),
                    'file_path': file_path,
                    'file_name': file_name,
                    'file_type': 'file',
                    'file_size': str(file_size),
                    'compressed_size': '',
                    'file_permissions': permissions_str,
                    'created_time': created_time_str,
                    'modified_time': modified_time_str,
                    'accessed_time': accessed_time_str,
                    'compressed': '0',
                    'checksum': '',
                    'backup_time': current_time_tz.isoformat(),
                    'chunk_number': '',
                    'tape_block_start': '',
                    'file_metadata': json_module.dumps(metadata),
                    'is_copy_success': '0',
                    'copy_status_at': ''
                }
                
                insert_items.append((file_key, file_id, file_path, insert_mapping))
            
            # 预先准备更新数据
            for file_path, file_id in existing_file_paths.items():
                if file_path in skipped_paths:
                    continue
                
                file_info = file_info_map[file_path]
                file_stat = file_info.get('file_stat')
                
                # 文件大小提取（优化）
                file_size = (
                    int(file_info['size'])
                    if 'size' in file_info and file_info.get('size') is not None
                    else (file_stat.st_size if file_stat and hasattr(file_stat, 'st_size') else 0)
                )
                
                file_name = file_info.get('name') or Path(file_path).name
                metadata = file_info.get('file_metadata') or {}
                metadata.update({'scanned_at': current_time.isoformat()})
                
                file_key = _get_redis_key(KEY_PREFIX_BACKUP_FILE, file_id)
                
                # 时间字段预处理
                modified_time_str = (
                    datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc).isoformat()
                    if file_stat and hasattr(file_stat, 'st_mtime')
                    else ''
                )
                permissions_str = (
                    oct(file_stat.st_mode)[-3:]
                    if file_stat and hasattr(file_stat, 'st_mode')
                    else ''
                )
                
                update_mapping = {
                    'file_name': file_name,
                    'file_size': str(file_size),
                    'file_permissions': permissions_str,
                    'modified_time': modified_time_str,
                    'file_metadata': json_module.dumps(metadata),
                    'updated_at': current_time.isoformat()
                }
                
                update_operations.append((file_key, update_mapping))
            
            step_prep_time = time.time() - step_prep_start
            
            # ========== 优化阶段5：分批执行操作（避免Pipeline过大导致Redis处理慢）==========
            if insert_items or update_operations:
                step_exec_start = time.time()
                
                from backup.redis_backup_db import KEY_INDEX_BACKUP_FILES, KEY_INDEX_BACKUP_FILE_BY_PATH, KEY_INDEX_BACKUP_FILE_PENDING
                
                # 关键优化：分批执行Pipeline，避免单个Pipeline过大导致Redis处理慢
                # 每批最多5000个文件，避免Pipeline超过20000个命令（Redis处理会变慢）
                pipeline_batch_size = 5000
                total_inserted = 0
                total_updated = 0
                
                # 准备路径索引的批量更新（所有文件一起更新，减少操作数）
                path_index_key = f"{KEY_INDEX_BACKUP_FILE_BY_PATH}:{self.backup_set_db_id}"
                path_index_mapping = {}  # {file_path: file_id}
                
                # 分批处理插入操作
                for i in range(0, len(insert_items), pipeline_batch_size):
                    batch_insert_items = insert_items[i:i + pipeline_batch_size]
                    
                    # 构建Pipeline（分批执行，避免单个Pipeline过大）
                    pipe = redis.pipeline()
                    
                    # 收集文件ID和路径（用于批量更新索引）
                    file_ids_for_index = []
                    
                    for file_key, file_id, file_path, insert_mapping in batch_insert_items:
                        # 插入Hash（使用mapping参数，一次性设置多个字段）
                        pipe.hset(file_key, mapping=insert_mapping)
                        file_ids_for_index.append((str(file_id), file_path))
                        path_index_mapping[file_path] = str(file_id)
                        # 更新缓存（内存操作，不占用网络）
                        self._file_path_cache[file_path] = file_id
                    
                    # 批量添加到全局索引（一次性添加多个成员，减少操作数）
                    if file_ids_for_index:
                        file_ids_str = [fid for fid, _ in file_ids_for_index]
                        # Redis的SADD可以一次添加多个成员（但redis-py需要逐个调用，所以使用循环）
                        # 优化：先收集所有文件ID，然后一次性批量添加
                        pipe.sadd(KEY_INDEX_BACKUP_FILES, *file_ids_str)
                        pipe.sadd(set_index_key, *file_ids_str)
                        
                        # 阶段1优化：维护未压缩文件索引（Sorted Set，使用文件大小作为score）
                        # 所有新插入的文件默认都是未压缩的（is_copy_success='0'）
                        pending_index_key = f"{KEY_INDEX_BACKUP_FILE_PENDING}:{self.backup_set_db_id}"
                        pending_items = {}  # {file_id: file_size} 用于ZADD批量添加
                        for file_key, file_id, file_path, insert_mapping in batch_insert_items:
                            # 从insert_mapping获取file_size
                            file_size_str = insert_mapping.get('file_size', '0')
                            try:
                                file_size = int(file_size_str) if file_size_str else 0
                                if file_size > 0:  # 只添加有效的文件大小
                                    pending_items[str(file_id)] = file_size
                            except (ValueError, TypeError):
                                pass
                        if pending_items:
                            pipe.zadd(pending_index_key, pending_items)
                    
                    # 执行当前批次的Pipeline
                    await pipe.execute()
                    total_inserted += len(batch_insert_items)
                
                # 批量更新路径索引（所有文件一起更新，大幅减少操作数）
                if path_index_mapping:
                    # 使用HMSET批量设置路径索引（一次性设置多个字段）
                    await redis.hset(path_index_key, mapping=path_index_mapping)
                
                # 分批处理更新操作
                for i in range(0, len(update_operations), pipeline_batch_size):
                    batch_update_ops = update_operations[i:i + pipeline_batch_size]
                    
                    pipe = redis.pipeline()
                    for file_key, update_mapping in batch_update_ops:
                        pipe.hset(file_key, mapping=update_mapping)
                    
                    await pipe.execute()
                    total_updated += len(batch_update_ops)
                
                step_exec_time = time.time() - step_exec_start
                
                # 性能日志（优化：只记录关键信息）
                total_time = time.time() - batch_start_time
                avg_speed = len(file_batch) / total_time if total_time > 0 else float('inf')
                
                # 只记录关键信息（INFO级别）
                if total_time > 0.5 or len(file_batch) >= 1000:  # 耗时超过0.5秒或批次大于1000时记录
                    logger.info(
                        f"[Redis批量写入] 批次: {len(file_batch)} 个文件，"
                        f"插入={total_inserted}，更新={total_updated}，"
                        f"总耗时={total_time*1000:.1f}ms，速度={avg_speed:.0f} 个/秒"
                    )
                else:
                    logger.debug(
                        f"[Redis批量写入] 批次: {len(file_batch)} 个文件，"
                        f"插入={total_inserted}，更新={total_updated}，"
                        f"总耗时={total_time*1000:.1f}ms，速度={avg_speed:.0f} 个/秒"
                    )
        except Exception as e:
            logger.error(f"[Redis模式] 批量处理文件失败: {str(e)}", exc_info=True)

    def _build_file_record_fields(self, file_info: Dict) -> Dict[str, Any]:
        """根据扫描器数据构建与 backup_files 表一致的字段"""

        def ensure_datetime(value, fallback=None):
            if isinstance(value, datetime):
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return fallback

        file_path = file_info.get('path', '')
        file_stat = file_info.get('file_stat')
        file_name = file_info.get('name') or Path(file_path).name

        directory_path = None
        if file_path:
            parent = Path(file_path).parent
            anchor = Path(file_path).anchor
            parent_str = str(parent)
            if parent_str and parent_str != anchor:
                directory_path = parent_str

        display_name = file_info.get('display_name') or file_name

        file_type = (file_info.get('file_type') or '').lower()
        if file_type not in {'file', 'directory', 'symlink'}:
            if file_info.get('is_file', True):
                file_type = 'file'
            elif file_info.get('is_dir', False):
                file_type = 'directory'
            elif file_info.get('is_symlink', False):
                file_type = 'symlink'
            else:
                file_type = 'file'

        file_size = file_info.get('size')
        if file_size is None and file_stat and hasattr(file_stat, 'st_size'):
            file_size = file_stat.st_size
        file_size = int(file_size or 0)

        compressed_size = file_info.get('compressed_size')
        file_permissions = file_info.get('permissions')
        if not file_permissions and file_stat and hasattr(file_stat, 'st_mode'):
            file_permissions = oct(file_stat.st_mode)[-3:]

        file_owner = file_info.get('file_owner')
        file_group = file_info.get('file_group')

        created_time = ensure_datetime(
            file_info.get('created_time'),
            datetime.fromtimestamp(file_stat.st_ctime, tz=timezone.utc) if file_stat and hasattr(file_stat, 'st_ctime') else datetime.now(timezone.utc)
        )
        modified_time = ensure_datetime(
            file_info.get('modified_time'),
            datetime.fromtimestamp(file_stat.st_mtime, tz=timezone.utc) if file_stat and hasattr(file_stat, 'st_mtime') else created_time
        )
        accessed_time = ensure_datetime(
            file_info.get('accessed_time'),
            datetime.fromtimestamp(file_stat.st_atime, tz=timezone.utc) if file_stat and hasattr(file_stat, 'st_atime') else modified_time
        )

        tape_block_start = file_info.get('tape_block_start')
        tape_block_count = file_info.get('tape_block_count')
        compressed = bool(file_info.get('compressed', False))
        encrypted = bool(file_info.get('encrypted', False))
        checksum = file_info.get('checksum')
        is_copy_success = bool(file_info.get('is_copy_success', False))
        copy_status_at = ensure_datetime(file_info.get('copy_status_at'))
        backup_time = ensure_datetime(file_info.get('backup_time'), datetime.now(timezone.utc))
        chunk_number = file_info.get('chunk_number')
        version = file_info.get('version', 1)

        metadata_input = file_info.get('file_metadata')
        # 确保 file_metadata 始终是有效的 JSON 字符串
        if isinstance(metadata_input, str) and metadata_input.strip():
            # 验证是否为有效的 JSON 字符串
            try:
                json.loads(metadata_input)  # 验证 JSON 格式
                file_metadata = metadata_input
            except (json.JSONDecodeError, TypeError):
                # 如果不是有效的 JSON，重新构建
                file_metadata = json.dumps({'scanned_at': datetime.now(timezone.utc).isoformat(), 'scanner_source': 'batch_db_writer'})
        elif isinstance(metadata_input, dict):
            # 如果是字典，转换为 JSON 字符串
            metadata = metadata_input.copy()
            metadata.setdefault('scanned_at', datetime.now(timezone.utc).isoformat())
            metadata.setdefault('scanner_source', 'batch_db_writer')
            file_metadata = json.dumps(metadata)
        else:
            # 其他情况（None、数字、布尔值等），创建默认的 JSON 字符串
            file_metadata = json.dumps({'scanned_at': datetime.now(timezone.utc).isoformat(), 'scanner_source': 'batch_db_writer'})
        
        # 最终检查：确保 file_metadata 是字符串类型（不是 None 或其他类型）
        if not isinstance(file_metadata, str):
            file_metadata = json.dumps({'scanned_at': datetime.now(timezone.utc).isoformat(), 'scanner_source': 'batch_db_writer'})

        tags_input = file_info.get('tags')
        if isinstance(tags_input, str) and tags_input.strip():
            # 验证是否为有效的 JSON 字符串
            try:
                json.loads(tags_input)  # 验证 JSON 格式
                tags = tags_input
            except (json.JSONDecodeError, TypeError):
                # 如果不是有效的 JSON，重新构建
                tags = json.dumps({'status': 'scanned'})
        else:
            tags_data = {'status': 'scanned'}
            if isinstance(tags_input, dict):
                tags_data.update(tags_input)
            tags = json.dumps(tags_data)
        
        # 确保 tags 是字符串类型（不是 None）
        if not tags or not isinstance(tags, str):
            tags = json.dumps({'status': 'scanned'})

        return {
            'backup_set_id': self.backup_set_db_id,
            'file_path': file_path,
            'file_name': file_name,
            'directory_path': directory_path,
            'display_name': display_name,
            'file_type': file_type,
            'file_size': file_size,
            'compressed_size': compressed_size,
            'file_permissions': file_permissions,
            'file_owner': file_owner,
            'file_group': file_group,
            'created_time': created_time,
            'modified_time': modified_time,
            'accessed_time': accessed_time,
            'tape_block_start': tape_block_start,
            'tape_block_count': tape_block_count,
            'compressed': compressed,
            'encrypted': encrypted,
            'checksum': checksum,
            'is_copy_success': is_copy_success,
            'copy_status_at': copy_status_at,
            'backup_time': backup_time,
            'chunk_number': chunk_number,
            'version': version,
            'file_metadata': file_metadata,
            'tags': tags
        }

    def _prepare_insert_params(self, file_info: Dict) -> tuple:
        """准备插入参数（保持与内存数据库同步字段一致）"""
        fields = self._build_file_record_fields(file_info)

        return (
            fields['backup_set_id'],
            fields['file_path'],
            fields['file_name'],
            fields['directory_path'],
            fields['display_name'],
            fields['file_type'],
            fields['file_size'],
            fields['compressed_size'],
            fields['file_permissions'],
            fields['file_owner'],
            fields['file_group'],
            fields['created_time'],
            fields['modified_time'],
            fields['accessed_time'],
            fields['tape_block_start'],
            fields['tape_block_count'],
            fields['compressed'],
            fields['encrypted'],
            fields['checksum'],
            fields['is_copy_success'],
            fields['copy_status_at'],
            fields['backup_time'],
            fields['chunk_number'],
            fields['version'],
            fields['file_metadata'],
            fields['tags']
        )

    def _prepare_update_params(self, file_info: Dict, existing_id: int) -> tuple:
        """准备更新参数"""
        fields = self._build_file_record_fields(file_info)

        return (
            existing_id,
            fields['file_name'],
            fields['directory_path'],
            fields['display_name'],
            fields['file_type'],
            fields['file_size'],
            fields['compressed_size'],
            fields['file_permissions'],
            fields['file_owner'],
            fields['file_group'],
            fields['created_time'],
            fields['modified_time'],
            fields['accessed_time'],
            fields['tape_block_start'],
            fields['tape_block_count'],
            fields['compressed'],
            fields['encrypted'],
            fields['checksum'],
            fields['is_copy_success'],
            fields['copy_status_at'],
            fields['backup_time'],
            fields['chunk_number'],
            fields['version'],
            fields['file_metadata'],
            fields['tags']
        )

    async def _batch_insert(self, conn, insert_data: List[tuple]):
        """批量插入文件记录"""
        import asyncio
        from utils.scheduler.db_utils import get_backup_files_table_by_set_id
        from utils.scheduler.db_utils import is_opengauss, get_backup_files_table_by_set_id

        # openGauss模式下需要手动管理事务
        if is_opengauss():
            # 在openGauss模式下，确保事务正确提交
            max_retries = 3
            retry_count = 0
            insert_success = False

            # 多表方案：根据批次中的 backup_set_id 决定物理表名
            if not insert_data:
                return
            sample_backup_set_id = insert_data[0][0]
            table_name = await get_backup_files_table_by_set_id(conn, sample_backup_set_id)

            while retry_count < max_retries and not insert_success:
                try:
                    # 开始新事务
                    await conn.execute("BEGIN")

                    # 执行批量插入
                    await conn.executemany(
                        f"""
                        INSERT INTO {table_name} (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5,
                            $6::backupfiletype, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15,
                            $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, CAST($25 AS jsonb), CAST($26 AS jsonb),
                            NOW(), NOW()
                        )
                        """,
                        insert_data
                    )

                    # 显式提交事务
                    await conn.commit()

                    # 验证事务提交状态
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    transaction_status = actual_conn.info.transaction_status

                    if transaction_status == 0:  # IDLE: 事务成功提交
                        insert_success = True
                        logger.info(f"[批量插入] ✅ openGauss模式下批量插入事务提交成功：{len(insert_data)} 个文件")
                    else:
                        # 事务状态异常
                        logger.warning(
                            f"[批量插入] ⚠️ 批量插入事务状态异常: {transaction_status}，"
                            f"尝试回滚后重试 (重试 {retry_count + 1}/{max_retries})"
                        )
                        try:
                            await conn.rollback()
                        except Exception as rollback_error:
                            logger.error(f"[批量插入] 回滚失败: {rollback_error}")

                        retry_count += 1
                        if retry_count < max_retries:
                            # 等待一段时间后重试
                            await asyncio.sleep(0.5 * retry_count)
                        else:
                            raise Exception(f"批量插入事务提交失败，达到最大重试次数 {max_retries}")

                except Exception as insert_error:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(
                            f"[批量插入] ❌ 批量插入失败，达到最大重试次数 {max_retries}: {str(insert_error)}",
                            exc_info=True
                        )
                        raise

                    logger.warning(
                        f"[批量插入] ⚠️ 批量插入失败，重试 {retry_count}/{max_retries}: {str(insert_error)}"
                    )

                    # 尝试回滚可能的事务
                    try:
                        await conn.rollback()
                    except Exception:
                        pass

                    # 等待一段时间后重试
                    await asyncio.sleep(1.0 * retry_count)
        else:
            # 非openGauss模式（SQLite、Redis等）保持原有逻辑
            await conn.executemany(
                """
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6::backupfiletype, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15,
                    $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, CAST($25 AS jsonb), CAST($26 AS jsonb),
                    NOW(), NOW()
                )
                """,
                insert_data
            )

        logger.debug(f"批量插入 {len(insert_data)} 个文件记录")

    async def _batch_update(self, conn, update_data: List[tuple]):
        """批量更新文件记录"""
        import asyncio
        from utils.scheduler.db_utils import is_opengauss, get_backup_files_table_by_set_id

        # openGauss模式下需要手动管理事务
        if is_opengauss():
            # 多表方案：根据批次中的 backup_set_id 决定物理表名
            if not update_data:
                return
            sample_backup_set_id = update_data[0][0]
            table_name = await get_backup_files_table_by_set_id(conn, sample_backup_set_id)
            # 在openGauss模式下，确保事务正确提交
            max_retries = 3
            retry_count = 0
            update_success = False

            while retry_count < max_retries and not update_success:
                try:
                    # 开始新事务
                    await conn.execute("BEGIN")

                    # 执行批量更新
                    await conn.executemany(
                        f"""
                        UPDATE {table_name}
                        SET file_name = $2,
                            directory_path = $3,
                            display_name = $4,
                            file_type = $5::backupfiletype,
                            file_size = $6,
                            compressed_size = $7,
                            file_permissions = $8,
                            file_owner = $9,
                            file_group = $10,
                            created_time = $11,
                            modified_time = $12,
                            accessed_time = $13,
                            tape_block_start = $14,
                            tape_block_count = $15,
                            compressed = $16,
                            encrypted = $17,
                            checksum = $18,
                            is_copy_success = $19,
                            copy_status_at = $20,
                            backup_time = $21,
                            chunk_number = $22,
                            version = $23,
                            file_metadata = CAST($24 AS jsonb),
                            tags = CAST($25 AS jsonb),
                            updated_at = NOW()
                        WHERE id = $1
                        """,
                        update_data
                    )

                    # 显式提交事务
                    await conn.commit()

                    # 验证事务提交状态
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    transaction_status = actual_conn.info.transaction_status

                    if transaction_status == 0:  # IDLE: 事务成功提交
                        update_success = True
                        logger.info(f"[批量更新] ✅ openGauss模式下批量更新事务提交成功：{len(update_data)} 个文件")
                    else:
                        # 事务状态异常
                        logger.warning(
                            f"[批量更新] ⚠️ 批量更新事务状态异常: {transaction_status}，"
                            f"尝试回滚后重试 (重试 {retry_count + 1}/{max_retries})"
                        )
                        try:
                            await conn.rollback()
                        except Exception as rollback_error:
                            logger.error(f"[批量更新] 回滚失败: {rollback_error}")

                        retry_count += 1
                        if retry_count < max_retries:
                            # 等待一段时间后重试
                            await asyncio.sleep(0.5 * retry_count)
                        else:
                            raise Exception(f"批量更新事务提交失败，达到最大重试次数 {max_retries}")

                except Exception as update_error:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(
                            f"[批量更新] ❌ 批量更新失败，达到最大重试次数 {max_retries}: {str(update_error)}",
                            exc_info=True
                        )
                        raise

                    logger.warning(
                        f"[批量更新] ⚠️ 批量更新失败，重试 {retry_count}/{max_retries}: {str(update_error)}"
                    )

                    # 尝试回滚可能的事务
                    try:
                        await conn.rollback()
                    except Exception:
                        pass

                    # 等待一段时间后重试
                    await asyncio.sleep(1.0 * retry_count)
        else:
            # 非openGauss模式（SQLite、Redis等）保持原有逻辑
            await conn.executemany(
                """
                UPDATE backup_files
                SET file_name = $2,
                    directory_path = $3,
                    display_name = $4,
                    file_type = $5::backupfiletype,
                    file_size = $6,
                    compressed_size = $7,
                    file_permissions = $8,
                    file_owner = $9,
                    file_group = $10,
                    created_time = $11,
                    modified_time = $12,
                    accessed_time = $13,
                    tape_block_start = $14,
                    tape_block_count = $15,
                    compressed = $16,
                    encrypted = $17,
                    checksum = $18,
                    is_copy_success = $19,
                    copy_status_at = $20,
                    backup_time = $21,
                    chunk_number = $22,
                    version = $23,
                    file_metadata = CAST($24 AS jsonb),
                    tags = CAST($25 AS jsonb),
                    updated_at = NOW()
                WHERE id = $1
                """,
                update_data
            )

        logger.debug(f"批量更新 {len(update_data)} 个文件记录")

    async def stop(self):
        """停止批量写入器"""
        self._is_running = False

        if self._worker_task:
            # 等待worker完成
            await self._worker_task
            self._worker_task = None

        logger.info("批量数据库写入器已停止")

    async def flush(self):
        """强制刷新剩余文件，等待所有文件写入完成"""
        import time
        # 等待队列清空
        max_wait_time = 300  # 最大等待时间5分钟
        wait_start = time.time()
        while not self.file_queue.empty():
            if time.time() - wait_start > max_wait_time:
                logger.info(f"批量写入器flush超时（{max_wait_time}秒），队列中仍有 {self.file_queue.qsize()} 个文件")
                break
            await asyncio.sleep(0.1)
        
        # 等待最后一个批次完成（给worker时间处理最后一个批次）
        if self._worker_task:
            # 等待最多1秒，确保最后一个批次被处理
            for _ in range(10):
                if self.file_queue.empty():
                    await asyncio.sleep(0.1)  # 再等100ms确保处理完成
                    break
                await asyncio.sleep(0.1)

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return self._stats.copy()


def _parse_enum(enum_cls, value, default=None):
    if value is None:
        return default
    try:
        if isinstance(value, enum_cls):
            return value
        return enum_cls(value) if isinstance(value, str) else enum_cls(value.value)  # type: ignore[arg-type]
    except Exception:
        return default


class BackupDB:
    """备份数据库操作类"""
    
    def __init__(self):
        """初始化数据库操作类"""
        self._last_operation_status: Dict[int, str] = {}

    def _log_operation_stage_event(self, backup_task: Optional[BackupTask], operation_status: str):
        """记录关键阶段日志（即使全局日志级别较高也能看到）
        
        注意：只记录阶段名称，不包含进度信息（如 "15401/21961 个文件 (70.1%)"）
        """
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                return

            normalized = (operation_status or '').strip()
            if not normalized:
                return

            # 提取阶段名称，移除进度信息（如 "15401/21961 个文件 (70.1%)"）
            # 只保留方括号内的阶段名称部分
            import re
            # 匹配 "[阶段名称...]" 或 "[阶段名称...] 其他内容"
            stage_match = re.match(r'^(\[[^\]]+\])', normalized)
            if stage_match:
                stage_only = stage_match.group(1)  # 只取 "[阶段名称...]"
            else:
                stage_only = normalized  # 如果没有方括号，使用原字符串
            
            task_id = backup_task.id
            previous = self._last_operation_status.get(task_id)
            if previous == stage_only:
                return

            self._last_operation_status[task_id] = stage_only
            task_name = getattr(backup_task, 'task_name', '') or ''
            stage_label = stage_only.strip('[]') or stage_only
            logger.info(f"[关键阶段] 任务 {task_name or task_id}: {stage_label}")

            if any(keyword in stage_label for keyword in ("完成", "成功", "失败", "终止", "结束")):
                self._last_operation_status.pop(task_id, None)
        except Exception:
            logger.debug("记录关键阶段日志失败", exc_info=True)
    
    async def create_backup_set(self, backup_task: BackupTask, tape) -> BackupSet:
        """创建备份集
        
        Args:
            backup_task: 备份任务对象
            tape: 磁带对象
            
        Returns:
            BackupSet: 备份集对象
        """
        try:
            # 生成备份集ID
            backup_group = format_datetime(now(), '%Y-%m')
            set_id = f"{backup_group}_{backup_task.id:06d}"
            backup_time = datetime.now()
            retention_until = backup_time + timedelta(days=backup_task.retention_days)
            
            # 使用原生 openGauss SQL，避免 SQLAlchemy 版本解析（仅保留 openGauss）
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    try:
                        # 准备 source_info JSON
                        source_info_json = json.dumps({'paths': backup_task.source_paths}) if backup_task.source_paths else None
                        
                        # 插入备份集
                        backup_set_id = await conn.fetchval(
                            """
                            INSERT INTO backup_sets 
                            (set_id, set_name, backup_group, status, backup_task_id, tape_id,
                             backup_type, backup_time, source_info, retention_until, auto_delete,
                             created_at, updated_at)
                            VALUES ($1, $2, $3, $4::backupsetstatus, $5, $6, $7::backuptasktype, $8, $9::jsonb, $10, $11, $12, $13)
                            RETURNING id
                            """,
                            set_id,
                            f"{backup_task.task_name}_{set_id}",
                            backup_group,
                            BackupSetStatus.ACTIVE.value,  # 'active'
                            backup_task.id,
                            tape.tape_id,
                            backup_task.task_type.value,  # BackupTaskType enum value
                            backup_time,
                            source_info_json,
                            retention_until,
                            True,  # auto_delete
                            backup_time,  # created_at
                            backup_time   # updated_at
                        )
                        
                        # 显式提交事务（psycopg3 需要显式提交，确保其他连接能看到新创建的备份集）
                        await conn.commit()
                        
                        # 验证事务提交状态
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status == 0:  # IDLE: 事务成功提交
                                logger.debug(f"create_backup_set: 事务已提交（backup_set_id={backup_set_id}, set_id={set_id}）")
                            elif transaction_status == 1:  # INTRANS: 事务未提交
                                logger.warning(f"create_backup_set: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                                await actual_conn.rollback()
                                raise Exception("事务提交失败")
                            elif transaction_status == 3:  # INERROR: 错误状态
                                logger.error(f"create_backup_set: ❌ 连接处于错误状态，回滚事务")
                                await actual_conn.rollback()
                                raise Exception("连接处于错误状态")
                        
                        # 查询插入的记录
                        row = await conn.fetchrow(
                            """
                            SELECT id, set_id, set_name, backup_group, status, backup_task_id, tape_id,
                                   backup_type, backup_time, source_info, retention_until, created_at, updated_at
                            FROM backup_sets
                            WHERE set_id = $1
                            """,
                            set_id
                        )
                    except Exception as db_error:
                        # 异常时显式回滚，避免长事务锁表
                        logger.error(f"create_backup_set: 数据库操作失败: {str(db_error)}", exc_info=True)
                        try:
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            if hasattr(actual_conn, 'info'):
                                transaction_status = actual_conn.info.transaction_status
                                if transaction_status in (1, 3):  # INTRANS or INERROR
                                    await actual_conn.rollback()
                                    logger.debug(f"create_backup_set: 异常时事务已回滚（set_id={set_id}）")
                        except Exception as rollback_err:
                            logger.warning(f"create_backup_set: 回滚事务失败: {str(rollback_err)}")
                        raise  # 重新抛出异常
                    
                    if row:
                        # 创建 BackupSet 对象（用于返回）
                        backup_set = BackupSet(
                            id=row['id'],
                            set_id=row['set_id'],
                            set_name=row['set_name'],
                            backup_group=row['backup_group'],
                            status=row['status'],
                            backup_task_id=row['backup_task_id'],
                            tape_id=row['tape_id'],
                            backup_type=backup_task.task_type,
                            backup_time=row['backup_time'],
                            source_info={'paths': backup_task.source_paths},
                            retention_until=row['retention_until']
                        )
                    else:
                        raise RuntimeError(f"备份集插入后查询失败: {set_id}")
            else:
                # SQLite 版本
                from backup.sqlite_backup_db import create_backup_set_sqlite
                backup_set = await create_backup_set_sqlite(backup_task, tape)

            backup_task.backup_set_id = set_id
            logger.info(f"创建备份集: {set_id}")

            return backup_set

        except Exception as e:
            logger.error(f"创建备份集失败: {str(e)}")
            raise
    
    async def finalize_backup_set(self, backup_set: BackupSet, file_count: int, total_size: int):
        """完成备份集
        
        Args:
            backup_set: 备份集对象
            file_count: 文件数量
            total_size: 总大小
        """
        try:
            backup_set.total_files = file_count
            backup_set.total_bytes = total_size
            backup_set.compressed_bytes = total_size
            backup_set.compression_ratio = total_size / backup_set.total_bytes if backup_set.total_bytes > 0 else 1.0
            backup_set.chunk_count = 1  # 简化处理

            # 保存更新 - 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
            
            if is_redis():
                # Redis 版本
                from backup.redis_backup_db import finalize_backup_set_redis
                await finalize_backup_set_redis(backup_set, file_count, total_size)
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    try:
                        await conn.execute(
                            """
                            UPDATE backup_sets
                            SET total_files = $1,
                                total_bytes = $2,
                                compressed_bytes = $3,
                                compression_ratio = $4,
                                chunk_count = $5,
                                updated_at = $6
                            WHERE set_id = $7
                            """,
                            file_count,
                            total_size,
                            total_size,
                            backup_set.compression_ratio,
                            backup_set.chunk_count,
                            datetime.now(),
                            backup_set.set_id
                        )
                        
                        # 显式提交事务（psycopg3 binary protocol 需要显式提交）
                        await conn.commit()
                        
                        # 验证事务提交状态
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status == 0:  # IDLE: 事务成功提交
                                logger.debug(f"finalize_backup_set: 事务已提交（set_id={backup_set.set_id}）")
                            elif transaction_status == 1:  # INTRANS: 事务未提交
                                logger.warning(f"finalize_backup_set: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                                await actual_conn.rollback()
                                raise Exception("事务提交失败")
                            elif transaction_status == 3:  # INERROR: 错误状态
                                logger.error(f"finalize_backup_set: ❌ 连接处于错误状态，回滚事务")
                                await actual_conn.rollback()
                                raise Exception("连接处于错误状态")
                    except Exception as db_error:
                        # 异常时显式回滚，避免长事务锁表
                        logger.error(f"finalize_backup_set: 数据库操作失败: {str(db_error)}", exc_info=True)
                        try:
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            if hasattr(actual_conn, 'info'):
                                transaction_status = actual_conn.info.transaction_status
                                if transaction_status in (1, 3):  # INTRANS or INERROR
                                    await actual_conn.rollback()
                                    logger.debug(f"finalize_backup_set: 异常时事务已回滚（set_id={backup_set.set_id}）")
                        except Exception as rollback_err:
                            logger.warning(f"finalize_backup_set: 回滚事务失败: {str(rollback_err)}")
                        raise  # 重新抛出异常
            else:
                # SQLite 版本：使用原生 SQL
                from backup.sqlite_backup_db import finalize_backup_set_sqlite
                await finalize_backup_set_sqlite(backup_set, file_count, total_size)

            logger.info(f"备份集完成: {backup_set.set_id}")

        except Exception as e:
            logger.error(f"完成备份集失败: {str(e)}")
    
    async def save_backup_files_to_db(
        self, 
        file_group: List[Dict], 
        backup_set: BackupSet, 
        compressed_file: Dict, 
        tape_file_path: str, 
        chunk_number: int
    ):
        """保存备份文件信息到数据库（便于恢复）
        
        注意：如果数据库保存失败，不会中断备份流程，只会记录警告日志。
        
        Args:
            file_group: 文件组列表
            backup_set: 备份集对象
            compressed_file: 压缩文件信息字典
            tape_file_path: 磁带文件路径
            chunk_number: 块编号
        """
        # 修复：空列表检查，避免执行无意义的SQL
        if not file_group or len(file_group) == 0:
            logger.debug("save_backup_files_to_db: file_group为空，跳过保存文件信息到数据库")
            return
        
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if not is_opengauss():
                logger.info("非openGauss数据库，跳过保存备份文件信息")
                return
            
            # 使用连接池
            async with get_opengauss_connection() as conn:
                backup_set_db_id = None  # 用于异常处理
                try:
                    # 获取备份集的数据库ID
                    backup_set_row = await conn.fetchrow(
                        """
                        SELECT id FROM backup_sets WHERE set_id = $1
                        """,
                        backup_set.set_id
                    )
                    
                    if not backup_set_row:
                        logger.info(f"找不到备份集: {backup_set.set_id}，跳过保存文件信息")
                        return
                    
                    backup_set_db_id = backup_set_row['id']
                    backup_time = datetime.now()
                    
                    # 在线程池中批量处理文件信息，避免阻塞事件循环
                    loop = asyncio.get_event_loop()
                    
                    def _process_file_info(file_info):
                        """在线程中处理单个文件信息"""
                        try:
                            file_path = Path(file_info['path'])
                            
                            # 确定文件类型（使用枚举值）
                            if file_info.get('is_dir', False):
                                file_type = BackupFileType.DIRECTORY.value
                            elif file_info.get('is_symlink', False):
                                if hasattr(BackupFileType, 'SYMLINK'):
                                    file_type = BackupFileType.SYMLINK.value
                                else:
                                    file_type = BackupFileType.FILE.value
                            else:
                                file_type = BackupFileType.FILE.value
                            
                            # 获取文件元数据（同步操作，在线程中执行）
                            try:
                                file_stat = file_path.stat() if file_path.exists() else None
                            except (PermissionError, OSError, FileNotFoundError, IOError) as stat_error:
                                logger.debug(f"无法获取文件统计信息: {file_path} (错误: {str(stat_error)})")
                                file_stat = None
                            except Exception as stat_error:
                                logger.info(f"获取文件统计信息失败: {file_path} (错误: {str(stat_error)})")
                                file_stat = None
                            
                            # 跳过单个文件的校验和计算（文件已压缩，压缩包本身有校验和，避免阻塞）
                            file_checksum = None
                            
                            # 获取文件权限（Windows上可能不可用）
                            file_permissions = None
                            if file_stat:
                                try:
                                    file_permissions = oct(file_stat.st_mode)[-3:]
                                except:
                                    pass
                            
                            return {
                                'file_path': str(file_path),
                                'file_name': file_path.name,
                                'file_type': file_type,
                                'file_size': file_info.get('size', 0),
                                'file_stat': file_stat,
                                'file_permissions': file_permissions,
                                'file_checksum': file_checksum
                            }
                        except Exception as process_error:
                            logger.info(f"处理文件信息失败: {file_info.get('path', 'unknown')} (错误: {str(process_error)})")
                            return None
                    
                    # 批量处理文件信息（在线程池中执行）
                    processed_files = await asyncio.gather(*[
                        loop.run_in_executor(None, _process_file_info, file_info)
                        for file_info in file_group
                    ], return_exceptions=True)
                    
                    # 过滤掉处理失败的文件（返回None或异常）
                    valid_processed_files = [f for f in processed_files if f is not None and not isinstance(f, Exception)]
                    
                    if len(valid_processed_files) < len(file_group):
                        failed_count = len(file_group) - len(valid_processed_files)
                        logger.info(f"⚠️ 处理文件信息时，{failed_count} 个文件失败，继续保存其他文件")
                    
                    # 批量插入文件记录（使用事务）
                    success_count = 0
                    failed_count = 0
                    await self._mark_files_as_copied(
                        conn=conn,
                        backup_set_db_id=backup_set_db_id,
                        processed_files=valid_processed_files,
                        compressed_file=compressed_file,
                        tape_file_path=tape_file_path,
                        chunk_number=chunk_number,
                        backup_time=backup_time
                    )
                    
                    # 修复2: 显式提交事务（openGauss模式需要，避免长事务锁表）
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status == 1:  # INTRANS: 在事务中但未提交
                            try:
                                await actual_conn.commit()
                                logger.debug(f"save_backup_files_to_db: 事务已提交（backup_set_id={backup_set_db_id}）")
                            except Exception as commit_err:
                                logger.error(f"save_backup_files_to_db: 提交事务失败: {str(commit_err)}", exc_info=True)
                                raise
                        elif transaction_status == 0:  # IDLE: 不在事务中，可能已自动提交
                            logger.debug(f"save_backup_files_to_db: 事务已自动提交或不在事务中（backup_set_id={backup_set_db_id}）")
                        elif transaction_status == 3:  # INERROR: 错误状态
                            logger.error(f"save_backup_files_to_db: 连接处于错误状态（backup_set_id={backup_set_db_id}）")
                            raise Exception("连接处于错误状态")
                        
                except Exception as db_conn_error:
                    # 修复3: 异常时显式回滚，避免长事务锁表
                    logger.error(f"save_backup_files_to_db: 数据库操作失败: {str(db_conn_error)}", exc_info=True)
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                try:
                                    await actual_conn.rollback()
                                    logger.debug(f"save_backup_files_to_db: 异常时事务已回滚（backup_set_id={backup_set_db_id if backup_set_db_id is not None else 'unknown'}）")
                                except Exception as rollback_err:
                                    logger.warning(f"save_backup_files_to_db: 回滚事务失败: {str(rollback_err)}")
                    except Exception as rollback_check_err:
                        logger.warning(f"save_backup_files_to_db: 检查事务状态失败: {str(rollback_check_err)}")
                    # 数据库错误不影响备份流程，继续执行（不抛出异常）
                    
        except Exception as e:
            logger.info(f"⚠️ 保存备份文件信息到数据库失败: {str(e)}，但备份流程继续")
            import traceback
            logger.debug(f"错误堆栈:\n{traceback.format_exc()}")
            # 不抛出异常，因为文件已经写入磁带，数据库记录失败不应该影响备份流程

    async def mark_files_as_queued(
        self,
        backup_set: BackupSet,
        file_groups: List[List[Dict]]
    ):
        """标记文件组为已入队（仅设置 is_copy_success = TRUE，不更新压缩信息，仅支持 openGauss / 内存数据库）"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        
        # 修复2：空列表检查，避免执行无意义的SQL
        if not file_groups or len(file_groups) == 0:
            logger.debug("mark_files_as_queued: file_groups为空，跳过标记文件为已入队")
            return
        
        # 检查是否所有文件组都为空
        total_files = sum(len(group) for group in file_groups if group)
        if total_files == 0:
            logger.debug("mark_files_as_queued: 所有文件组都为空，跳过标记文件为已入队")
            return
        
        backup_set_db_id = getattr(backup_set, 'id', None)
        if not backup_set_db_id:
            logger.info(f"[mark_files_as_queued] ❌ 无法获取 backup_set.id，跳过文件状态更新")
            return
        
        # 收集所有文件路径（不去重，文件组预取器返回多少就标记多少）
        all_file_paths = []
        empty_path_count = 0
        
        for file_group in file_groups:
            for file_info in file_group:
                file_path = file_info.get('file_path') or file_info.get('path')
                if not file_path:
                    empty_path_count += 1
                    continue
                all_file_paths.append(file_path)
        
        total_files_in_groups = sum(len(group) for group in file_groups if group)
        
        if not all_file_paths:
            logger.info(f"[mark_files_as_queued] ❌ 没有有效的文件路径，跳过文件状态更新")
            return
        
        logger.info(
            f"[mark_files_as_queued] 开始标记 {len(all_file_paths)} 个文件为已入队（is_copy_success = TRUE，openGauss），"
            f"文件组条目总数={total_files_in_groups}，空路径数={empty_path_count}"
        )
        
        if is_opengauss():
            # openGauss 模式
            total_updated = 0  # SQL 实际更新的数据库行数（可能包含重复路径的记录）
            try:
                async with get_opengauss_connection() as conn:
                    total_updated = await self._mark_files_as_queued_opengauss(conn, backup_set_db_id, all_file_paths)
                    # 二次校验：确认 is_copy_success 已成功设置为 TRUE
                    try:
                        await self._verify_and_retry_files_queued_opengauss(conn, backup_set_db_id, all_file_paths)
                    except Exception as verify_err:
                        # 校验失败不影响主流程，但需要明确日志提示
                        logger.warning(
                            f"[mark_files_as_queued] ⚠️ 标记完成后校验 is_copy_success 状态失败：{verify_err}",
                            exc_info=True,
                        )
                # 说明：total_updated 是 SQL 实际更新的数据库行数（可能包含重复路径的记录）
                # len(all_file_paths) 是传入的唯一路径数
                # SQL 使用 file_path = ANY($2) 会更新所有匹配路径的记录（包括重复路径的记录）
                logger.info(
                    f"[mark_files_as_queued] ✅ openGauss模式："
                    f"SQL实际更新行数={total_updated}，"
                    f"传入唯一路径数={len(all_file_paths)}，"
                    f"所有匹配路径的记录的 is_copy_success 已设置为 TRUE"
                )
            except Exception as e:
                logger.error(f"[mark_files_as_queued] ❌ openGauss模式更新失败: {str(e)}", exc_info=True)
        else:
            logger.info("[mark_files_as_queued] 当前仅支持 openGauss，跳过数据库持久化更新")
    
    async def _mark_files_as_queued_opengauss(self, conn, backup_set_db_id: int, file_paths: List[str]) -> int:
        """openGauss模式：仅设置 is_copy_success = TRUE（使用优化版本：临时表+JOIN）
        
        Args:
            conn: 数据库连接
            backup_set_db_id: 备份集ID
            file_paths: 需要标记为已入队的文件路径列表（可以包含重复路径）
        
        Returns:
            int: SQL 实际更新的数据库行数
        """
        from utils.scheduler.db_utils import get_backup_files_table_by_set_id

        if not file_paths:
            return 0

        # 过滤掉空路径，其余路径全部参与更新（不在这里做去重，文件组预取器返回多少就标记多少）
        effective_paths = [p for p in file_paths if p]
        if not effective_paths:
            return 0

        table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

        # 使用优化版本：临时表 + JOIN 更新方式
        logger.info(
            f"[mark_files_as_queued] 使用优化版本（临时表+JOIN）："
            f"传入路径总数={len(file_paths)}，"
            f"有效路径数={len(effective_paths)}，"
            f"表名={table_name}"
        )
        
        total_updated = await mark_files_as_queued_optimized(
            conn=conn,
            table_name=table_name,
            backup_set_db_id=backup_set_db_id,
            file_paths=effective_paths,
            batch_size=10000,  # 优化：批次大小从1000增加到10000
            commit_interval=1  # 临时表方式一次性更新，只需提交一次
        )
        
        # 返回实际更新的数据库行数（可能包含重复路径的记录）
        return total_updated

    async def _verify_and_retry_files_queued_opengauss(
        self,
        conn,
        backup_set_db_id: int,
        file_paths: List[str],
    ) -> None:
        """
        校验并重试：确认指定文件的 is_copy_success 已设置为 TRUE（使用优化版本：快速检查）

        逻辑：
        1. 使用快速检查（LIMIT 1）替代全量 COUNT，提升性能
        2. 如果存在未标记成功的记录，重新调用一次 _mark_files_as_queued_opengauss
        3. 再次检查，仍然不一致则输出错误日志，交由后续流程或人工排查
        """
        from utils.scheduler.db_utils import get_backup_files_table_by_set_id

        if not file_paths:
            return

        # 不再去重，直接使用传入的路径数组进行校验（去重对 COUNT 结果无影响，但会让日志更难理解）
        effective_paths = [p for p in file_paths if p]
        if not effective_paths:
            return

        table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

        # 使用优化版本：快速检查（LIMIT 1）替代全量 COUNT
        # 对于大数据量自动使用采样校验，提升性能
        all_verified = await verify_files_queued_optimized(
            conn=conn,
            table_name=table_name,
            backup_set_db_id=backup_set_db_id,
            file_paths=effective_paths,
            sample_size=None,  # None 表示自动选择（大数据量采样1000条）
        )

        if all_verified:
            logger.info(
                f"[mark_files_as_queued] ✅ 校验通过：backup_set_id={backup_set_db_id}, "
                f"校验路径数={len(effective_paths)}，所有 is_copy_success 均已为 TRUE"
            )
            return

        logger.warning(
            f"[mark_files_as_queued] ⚠️ 校验发现仍有未标记的记录，"
            f"backup_set_id={backup_set_db_id}，准备重试一次批量更新"
        )

        # 发现未标记成功的记录后，重试一次批量更新
        try:
            await self._mark_files_as_queued_opengauss(conn, backup_set_db_id, effective_paths)
        except Exception as retry_err:
            logger.error(
                f"[mark_files_as_queued] ❌ 重试设置 is_copy_success 失败：{retry_err}",
                exc_info=True,
            )
            return

        # 重试后再次校验
        all_verified_after_retry = await verify_files_queued_optimized(
            conn=conn,
            table_name=table_name,
            backup_set_db_id=backup_set_db_id,
            file_paths=effective_paths,
        )
        
        if all_verified_after_retry:
            logger.info(
                f"[mark_files_as_queued] ✅ 重试后校验通过：backup_set_id={backup_set_db_id}, "
                f"校验路径数={len(effective_paths)}，所有 is_copy_success 均已为 TRUE"
            )
        else:
            logger.error(
                f"[mark_files_as_queued] ❌ 重试后仍有未标记的记录 "
                f"（backup_set_id={backup_set_db_id}，校验路径数={len(effective_paths)}）。"
                f"建议检查数据库连接/事务状态或手动校验这些文件记录。"
            )
    
    async def _temp_table_exists(self, conn, table_name: str) -> bool:
        """检查临时表是否存在"""
        try:
            result = await conn.fetchrow(
                """
                SELECT 1 FROM pg_temp.information_schema.tables 
                WHERE table_name = $1
                """,
                table_name
            )
            return result is not None
        except Exception:
            # 如果查询失败，假设表不存在
            return False

    async def mark_files_as_copied(
        self,
        backup_set: BackupSet,
        file_group: List[Dict],
        compressed_file: Dict,
        tape_file_path: str,
        chunk_number: int
    ):
        """标记文件为复制成功"""
        # 修复2：空列表检查，避免执行无意义的SQL
        if not file_group or len(file_group) == 0:
            logger.debug("mark_files_as_copied: file_group为空，跳过标记文件为复制成功")
            return
        
        logger.info(f"[mark_files_as_copied] 开始标记文件为复制成功: backup_set={backup_set}, file_group数量={len(file_group)}, chunk_number={chunk_number}")
        
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        
        backup_set_db_id = getattr(backup_set, 'id', None)
        
        if not is_opengauss():
            logger.info("[mark_files_as_copied] 当前仅支持 openGauss，跳过数据库持久化更新")
            return

        # openGauss 版本
        async with get_opengauss_connection() as conn:
            try:
                # 如果 backup_set_db_id 不存在，先查询（复用同一个连接，避免连接泄漏）
                if not backup_set_db_id:
                    row = await conn.fetchrow(
                        "SELECT id FROM backup_sets WHERE set_id = $1",
                        backup_set.set_id
                    )
                    if not row:
                        logger.info(f"找不到备份集: {backup_set.set_id}，无法标记文件成功")
                        return
                    backup_set_db_id = row['id']
                
                await self._mark_files_as_copied(
                    conn=conn,
                    backup_set_db_id=backup_set_db_id,
                    processed_files=file_group,
                    compressed_file=compressed_file,
                    tape_file_path=tape_file_path,
                    chunk_number=chunk_number,
                    backup_time=datetime.now()
                )
                
                # 显式提交事务
                await conn.commit()
                
                # 验证事务提交状态
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'info'):
                    transaction_status = actual_conn.info.transaction_status
                    if transaction_status == 0:  # IDLE: 事务成功提交
                        logger.debug(f"mark_files_as_copied: 事务已提交（backup_set_db_id={backup_set_db_id}）")
                    elif transaction_status == 1:  # INTRANS: 事务未提交
                        logger.warning(f"mark_files_as_copied: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                        await actual_conn.rollback()
                        raise Exception("事务提交失败")
                    elif transaction_status == 3:  # INERROR: 错误状态
                        logger.error(f"mark_files_as_copied: ❌ 连接处于错误状态，回滚事务")
                        await actual_conn.rollback()
                        raise Exception("连接处于错误状态")
                        
            except Exception as e:
                # 异常时显式回滚，避免长事务锁表
                logger.error(f"mark_files_as_copied: 数据库操作失败: {str(e)}", exc_info=True)
                try:
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug(f"mark_files_as_copied: 异常时事务已回滚（backup_set_db_id={backup_set_db_id if backup_set_db_id is not None else 'unknown'}）")
                except Exception as rollback_err:
                    logger.warning(f"mark_files_as_copied: 回滚事务失败: {str(rollback_err)}")
                raise  # 重新抛出异常

    async def get_backup_set_by_set_id(self, set_id: str) -> Optional[BackupSet]:
        """根据 set_id 获取备份集"""
        from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
        if not set_id:
            return None
        
        if is_redis():
            # Redis 版本
            from backup.redis_backup_db import get_backup_set_by_set_id_redis
            return await get_backup_set_by_set_id_redis(set_id)
        elif is_opengauss():
            async with get_opengauss_connection() as conn:
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT id, set_id, set_name, backup_group, status, backup_task_id,
                               tape_id, backup_type, backup_time, total_files, total_bytes,
                               compressed_bytes, compression_ratio, chunk_count, created_at,
                               updated_at
                        FROM backup_sets
                        WHERE set_id = $1
                        """,
                        set_id
                    )
                except Exception as e:
                    # 异常时回滚，确保连接处于干净状态
                    logger.error(f"get_backup_set_by_set_id: 查询失败: {str(e)}", exc_info=True)
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug(f"get_backup_set_by_set_id: 异常时事务已回滚（set_id={set_id}）")
                    except Exception as rollback_err:
                        logger.warning(f"get_backup_set_by_set_id: 回滚事务失败: {str(rollback_err)}")
                    raise  # 重新抛出异常
        else:
            # SQLite 版本
            from backup.sqlite_backup_db import get_backup_set_by_set_id_sqlite
            return await get_backup_set_by_set_id_sqlite(set_id)
        
        if not row:
            return None
        
        backup_set_obj = BackupSet(
            set_id=row['set_id'],
            set_name=row['set_name'],
            backup_group=row['backup_group'],
            backup_type=_parse_enum(BackupTaskType, row.get('backup_type'), BackupTaskType.FULL),
            backup_time=row['backup_time'],
            total_files=row['total_files'],
            total_bytes=row['total_bytes'],
            compressed_bytes=row['compressed_bytes'],
            compression_ratio=row['compression_ratio'],
            chunk_count=row['chunk_count']
        )
        backup_set_obj.id = row['id']
        backup_set_obj.status = _parse_enum(BackupSetStatus, row.get('status'), BackupSetStatus.ACTIVE)
        backup_set_obj.backup_task_id = row['backup_task_id']
        backup_set_obj.tape_id = row['tape_id']
        backup_set_obj.created_at = row['created_at']
        backup_set_obj.updated_at = row['updated_at']
        return backup_set_obj

    async def clear_backup_files_for_set(self, backup_set_db_id: int):
        """清理指定备份集的文件记录"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, get_backup_files_table_by_set_id
        if not is_opengauss():
            return
        try:
            async with get_opengauss_connection() as conn:
                try:
                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                    await conn.execute(
                        f"DELETE FROM {table_name} WHERE backup_set_id = $1",
                        backup_set_db_id
                    )
                    
                    # 显式提交事务
                    await conn.commit()
                    
                    # 验证事务提交状态
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status == 0:  # IDLE: 事务成功提交
                            logger.debug(f"clear_backup_files_for_set: 事务已提交（backup_set_db_id={backup_set_db_id}, 表={table_name}）")
                        elif transaction_status == 1:  # INTRANS: 事务未提交
                            logger.warning(f"clear_backup_files_for_set: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                            await actual_conn.rollback()
                            raise Exception("事务提交失败")
                        elif transaction_status == 3:  # INERROR: 错误状态
                            logger.error(f"clear_backup_files_for_set: ❌ 连接处于错误状态，回滚事务")
                            await actual_conn.rollback()
                            raise Exception("连接处于错误状态")
                except Exception as db_error:
                    # 异常时显式回滚，避免长事务锁表
                    logger.error(f"clear_backup_files_for_set: 数据库操作失败: {str(db_error)}", exc_info=True)
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug(f"clear_backup_files_for_set: 异常时事务已回滚（backup_set_db_id={backup_set_db_id}）")
                    except Exception as rollback_err:
                        logger.warning(f"clear_backup_files_for_set: 回滚事务失败: {str(rollback_err)}")
                    raise  # 重新抛出异常
        except Exception as e:
            error_msg = str(e)
            # 如果表不存在，记录警告但不报错（可能是数据库未初始化）
            if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                logger.info(
                    f"清理备份集 {backup_set_db_id} 的文件记录时表不存在，跳过清理（可能是数据库未初始化）: {error_msg}"
                )
            else:
                # 其他错误记录错误日志但不抛出异常，避免影响扫描流程
                logger.error(
                    f"清理备份集 {backup_set_db_id} 的文件记录失败: {error_msg}",
                    exc_info=True
                )

    async def upsert_scanned_file_record(self, backup_set_db_id: int, file_info: Dict):
        """保存扫描阶段的文件/目录信息"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return
        
        file_path = file_info.get('path') or file_info.get('file_path')
        if not file_path:
            return

        directory_path = str(Path(file_path).parent) if Path(file_path).parent else None
        display_name = file_info.get('name') or file_info.get('file_name') or Path(file_path).name
        file_size = file_info.get('size') or file_info.get('file_size') or 0
        file_permissions = file_info.get('permissions') or file_info.get('file_permissions')
        modified_time = file_info.get('modified_time')
        accessed_time = file_info.get('accessed_time')
        created_time = file_info.get('created_time') or modified_time

        if isinstance(modified_time, str):
            modified_time = datetime.fromisoformat(modified_time)
        if isinstance(accessed_time, str):
            accessed_time = datetime.fromisoformat(accessed_time)
        if isinstance(created_time, str):
            created_time = datetime.fromisoformat(created_time)

        if file_info.get('is_dir'):
            file_type = BackupFileType.DIRECTORY.value
        elif file_info.get('is_symlink'):
            file_type = BackupFileType.SYMLINK.value if hasattr(BackupFileType, 'SYMLINK') else BackupFileType.FILE.value
        else:
            file_type = BackupFileType.FILE.value

        async with get_opengauss_connection() as conn:
            try:
                # 多表方案：根据 backup_set_db_id 决定物理表名
                from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                existing = await conn.fetchrow(
                    f"""
                    SELECT id, is_copy_success 
                    FROM {table_name}
                    WHERE backup_set_id = $1 AND file_path = $2
                    """,
                    backup_set_db_id,
                    file_path
                )

                metadata = file_info.get('file_metadata') or {}
                metadata.update({'scanned_at': datetime.now().isoformat()})

                if existing and existing['is_copy_success']:
                    # 已复制成功的文件不覆盖
                    return

                if existing:
                    await conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET file_name = $3,
                            directory_path = $4,
                            display_name = $5,
                            file_type = $6::backupfiletype,
                            file_size = $7,
                            file_permissions = $8,
                            created_time = $9,
                            modified_time = $10,
                            accessed_time = $11,
                            file_metadata = $12::jsonb
                        WHERE id = $13
                        """,
                        backup_set_db_id,
                        file_path,
                        display_name,
                        directory_path,
                        display_name,
                        file_type,
                        file_size,
                        file_permissions,
                        created_time,
                        modified_time,
                        accessed_time,
                        json.dumps(metadata),
                        existing['id']
                    )
                else:
                    await conn.execute(
                        f"""
                        INSERT INTO {table_name} (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions,
                            created_time, modified_time, accessed_time, compressed,
                            checksum, backup_time, chunk_number, tape_block_start,
                            file_metadata, is_copy_success, copy_status_at
                        ) VALUES (
                            $1, $2, $3, $4, $5,
                            $6::backupfiletype, $7, $8, $9,
                            $10, $11, $12, $13,
                            $14, $15, $16, $17,
                            $18::jsonb, FALSE, NULL
                        )
                        """,
                        backup_set_db_id,
                        file_path,
                        display_name,
                        directory_path,
                        display_name,
                        file_type,
                        file_size,
                        0,
                        file_permissions,
                        created_time,
                        modified_time,
                        accessed_time,
                        False,
                        None,
                        datetime.now(),
                        0,
                        0,
                        json.dumps(metadata)
                    )
                
                # 显式提交事务
                await conn.commit()
                
                # 验证事务提交状态
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'info'):
                    transaction_status = actual_conn.info.transaction_status
                    if transaction_status == 0:  # IDLE: 事务成功提交
                        logger.debug(f"upsert_scanned_file_record: 事务已提交（backup_set_db_id={backup_set_db_id}, file_path={file_path}）")
                    elif transaction_status == 1:  # INTRANS: 事务未提交
                        logger.warning(f"upsert_scanned_file_record: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                        await actual_conn.rollback()
                        raise Exception("事务提交失败")
                    elif transaction_status == 3:  # INERROR: 错误状态
                        logger.error(f"upsert_scanned_file_record: ❌ 连接处于错误状态，回滚事务")
                        await actual_conn.rollback()
                        raise Exception("连接处于错误状态")
            except Exception as db_error:
                # 异常时显式回滚，避免长事务锁表
                logger.error(f"upsert_scanned_file_record: 数据库操作失败: {str(db_error)}", exc_info=True)
                try:
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug(f"upsert_scanned_file_record: 异常时事务已回滚（backup_set_db_id={backup_set_db_id}, file_path={file_path}）")
                except Exception as rollback_err:
                    logger.warning(f"upsert_scanned_file_record: 回滚事务失败: {str(rollback_err)}")
                raise  # 重新抛出异常

    async def fetch_pending_backup_files(self, backup_set_db_id: int, limit: int = 500) -> List[Dict]:
        """获取待复制的文件列表（保留以兼容旧代码，但不再使用）"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if not is_opengauss():
            return []
        
        async with get_opengauss_connection() as conn:
            # 多表方案：根据 backup_set_db_id 决定物理表名
            table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

            rows = await conn.fetch(
                f"""
                SELECT id, file_path, file_name, directory_path, display_name, file_type,
                       file_size, file_permissions, modified_time, accessed_time
                FROM {table_name}
                WHERE backup_set_id = $1
                  AND (is_copy_success IS DISTINCT FROM TRUE)
                  AND file_type = 'file'::backupfiletype
                ORDER BY id
                LIMIT $2
                """,
                backup_set_db_id,
                limit
            )
        
        pending_files = []
        for row in rows:
            file_type = row['file_type']
            pending_files.append({
                'id': row['id'],
                'path': row['file_path'],
                'file_path': row['file_path'],
                'name': row['file_name'],
                'file_name': row['file_name'],
                'directory_path': row['directory_path'],
                'display_name': row['display_name'],
                'size': row['file_size'] or 0,
                'permissions': row['file_permissions'],
                'modified_time': row['modified_time'],
                'accessed_time': row['accessed_time'],
                'is_dir': str(file_type).lower() == 'directory',
                'is_file': str(file_type).lower() == 'file',
                'is_symlink': str(file_type).lower() == 'symlink'
            })
        return pending_files
    
    async def fetch_pending_files_grouped_by_size(
        self,
        backup_set_db_id: int,
        max_file_size: int,
        backup_task_id: int = None,
        should_wait_if_small: bool = True,
        start_from_id: int = 0
    ) -> List[List[Dict]]:
        """
        新策略：从数据库检索所有未压缩文件，构建压缩组

        策略说明：
        1. 每次检索所有 is_copy_success = FALSE 的文件
        2. 累积文件直到达到 max_file_size 阈值
        3. 超过阈值的文件跳过（保持 FALSE 状态，下次仍可检索）
        4. 如果一轮达不到阈值，最多重试6次后强制压缩
        5. 超大文件（超过容差上限）单独成组

        Args:
            backup_set_db_id: 备份集数据库ID
            max_file_size: 单个文件组的最大大小（字节）
            backup_task_id: 夀份任务ID，用于获取重试计数
            should_wait_if_small: 是否在组大小不足时等待

        Returns:
            List[List[Dict]]: 包含一个文件组的列表，空列表表示等待或无文件
        """
        from utils.scheduler.db_utils import (
            is_opengauss,
            get_opengauss_connection,
            get_backup_files_table_by_set_id,
        )
        from backup.utils import format_bytes
        from config.settings import get_settings

        if not is_opengauss():
            logger.info("[fetch_pending_files_grouped_by_size] 当前仅支持 openGauss，返回空结果")
            return []

        # 获取重试计数（如果没有backup_task_id则使用0）
        retry_count = 0
        max_retries = 6
        if backup_task_id:
            # 从备份引擎获取重试计数（需要通过外部传递或数据库存储）
            # 这里简化处理，通过should_wait_if_small判断是否应该继续等待
            retry_count = 0 if should_wait_if_small else max_retries

        # 标记检索开始（仅在第一次调用时标记，且扫描未完成时）
        if backup_task_id:
            current_scan_status = await self.get_scan_status(backup_task_id)
            # 如果扫描已经完成（completed），不应该再设置为 retrieving
            # 只有在扫描未完成（pending, running, None）时才设置为 retrieving
            if current_scan_status not in ('retrieving', 'completed'):
                logger.info(f"[openGauss优化] 标记检索状态为开始检索（backup_task_id={backup_task_id}，当前状态={current_scan_status}）")
                await self.update_scan_status(backup_task_id, 'retrieving')
        
        # openGauss 优化：分批检索文件，避免一次性检索所有未压缩文件
        # 使用原生 openGauss SQL，每次检索一定数量的文件（分批处理）
        # 动态计算批次大小：根据最大文件大小（GB）* 6 * 1000
        # 假设平均文件大小约 1MB，这样可以确保一次查询能获取足够的文件
        # 例如：6GB * 6 * 1000 = 36000，12GB * 6 * 1000 = 72000
        max_file_size_gb = max_file_size / (1024 * 1024 * 1024)  # 转换为GB
        batch_size = int(max_file_size_gb * 0.5 * 1000)
        # 设置合理的上下限：最小5000，最大50000
        batch_size = max(3000, min(batch_size, 50000))
        logger.info(
            f"[openGauss优化] 动态计算批次大小: {batch_size} (基于 max_file_size={max_file_size_gb:.1f}GB), "
            f"start_from_id={start_from_id}"
        )
        
        all_files = []  # 累积的文件列表
        current_group_size = 0  # 当前组的大小
        seen_paths = {}  # 用于去重：file_path -> (file_info, id)，只保留 id 最小的记录
        total_duplicate_count = 0  # 总重复路径数量（跨所有批次）
        total_files_processed = 0  # 本次累计处理的文件数（去重后）
        
        # 新策略参数：简化版本
        tolerance = max_file_size * 0.05  # 5% 容差（用于计算最小目标大小）
        min_group_size = max_file_size - tolerance  # 最小目标大小（例如：6GB - 0.3GB = 5.7GB）
        max_group_size = max_file_size + tolerance  # 容差上限（例如：6GB + 0.3GB = 6.3GB，保留变量但不作为判断条件）
        # 注意：文件组大小只要 > min_group_size 就返回，不再判断max_group_size上限
        # 最小文件组大小限制：即使扫描已完成，如果文件组太小也不应该返回
        # 设置为 max_file_size 的 1% 或 100MB，取较大值
        min_acceptable_group_size = max(max_file_size * 0.01, 100 * 1024 * 1024)  # 1% 或 100MB
        
        # 注意：整个方法使用同一个连接，确保连接复用
        # 第一次查询和后续循环都使用同一个conn对象，避免连接泄漏
        async with get_opengauss_connection() as conn:
            # 多表方案：根据 backup_set_db_id 决定物理表名，避免直接访问基础表 backup_files
            table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

            # 关键修复：先查询当前备份集中第一个未压缩文件的ID
            # 如果有多个备份集，需要确保从当前备份集的第一个文件开始
            # 使用索引优化：利用 idx_backup_files_set_copy_type_id 索引，使用 ORDER BY id LIMIT 1 比 MIN(id) 更高效
            # 使用子查询和 COALESCE 避免 openGauss 缓冲区错误，更兼容
            # 参考 tasks_delete.py 的处理方式，使用 asyncio.wait_for 和错误处理
            try:
                await conn.execute("SET LOCAL statement_timeout = '30s'")
                first_pending_file_row = await asyncio.wait_for(
                    conn.fetchrow(
                        f"""
                        SELECT COALESCE(MIN(id), 0)::BIGINT as min_id
                        FROM {table_name}
                        WHERE backup_set_id = $1::INTEGER
                          AND (is_copy_success IS DISTINCT FROM TRUE)
                          AND file_type = 'file'::backupfiletype
                        """,
                        backup_set_db_id
                    ),
                    timeout=30.0
                )
                # 安全地获取 min_id，处理 NULL 值
                if first_pending_file_row and first_pending_file_row.get('min_id') is not None:
                    first_pending_id = int(first_pending_file_row['min_id'])
                    # 如果 COALESCE 返回 0，说明没有文件，设置为 None
                    if first_pending_id == 0:
                        first_pending_id = None
                else:
                    first_pending_id = None
            except Exception as e:
                error_msg = str(e)
                # 修复1：异常时显式回滚，避免长事务锁表
                try:
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug(f"[openGauss优化] 查询第一个待处理文件ID异常时事务已回滚")
                except Exception as rollback_err:
                    logger.warning(f"[openGauss优化] 回滚事务失败: {str(rollback_err)}")
                
                # 如果表不存在，返回空结果
                if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(e).__name__):
                    logger.info(
                        f"[openGauss优化] backup_files 表不存在，返回空结果（可能是数据库未初始化）: {error_msg}"
                    )
                    return ([], start_from_id)
                # 其他错误也记录警告并返回空结果
                logger.info(f"[openGauss优化] 查询第一个待处理文件ID时出错: {error_msg}，返回空结果")
                return ([], start_from_id)
            
            if first_pending_id is None:
                logger.info(f"[openGauss优化] 当前备份集 {backup_set_db_id} 没有未压缩文件")
                return ([], start_from_id)
            
            logger.info(
                f"[openGauss优化] 当前备份集 {backup_set_db_id} 第一个未压缩文件ID: {first_pending_id}, "
                f"传入的 start_from_id={start_from_id}"
            )
            
            # 确定查询起始ID：
            # 1. 如果 start_from_id 大于第一个未压缩文件ID，说明可能跳过了某些文件，应该从第一个文件开始
            # 2. 如果 start_from_id 小于第一个未压缩文件ID - 1，说明可能跳过了某些文件，应该从第一个文件开始
            # 3. 正常情况：start_from_id 在 [first_pending_id - 1, first_pending_id] 范围内，从 start_from_id 开始查询
            # 注意：start_from_id = first_pending_id - 1 是正常的（查询条件 id > start_from_id 能包含第一个文件）
            if start_from_id > first_pending_id:
                # start_from_id 大于第一个未压缩文件ID，说明可能跳过了某些文件，应该从第一个文件开始
                logger.info(
                    f"[openGauss优化] ⚠️ start_from_id ({start_from_id}) 大于第一个未压缩文件ID ({first_pending_id})，"
                    f"可能存在ID更小的未压缩文件，从第一个文件ID开始查询"
                )
                last_processed_id = first_pending_id - 1  # 从第一个文件开始（id > first_pending_id - 1 即 id >= first_pending_id）
            elif start_from_id > 0 and start_from_id < first_pending_id - 1:
                # start_from_id 小于第一个未压缩文件ID - 1，说明可能跳过了某些文件，应该从第一个文件开始
                logger.info(
                    f"[openGauss优化] ⚠️ start_from_id ({start_from_id}) 小于第一个未压缩文件ID - 1 ({first_pending_id - 1})，"
                    f"可能存在ID更小的未压缩文件，从第一个文件ID开始查询"
                )
                last_processed_id = first_pending_id - 1  # 从第一个文件开始（id > first_pending_id - 1 即 id >= first_pending_id）
            elif start_from_id >= first_pending_id - 1:
                # 正常情况：从 start_from_id 开始查询（包括 start_from_id = first_pending_id - 1 的情况）
                last_processed_id = start_from_id
            else:
                # start_from_id = 0，从第一个文件开始
                last_processed_id = first_pending_id - 1  # 从第一个文件开始
            
            logger.info(
                f"[openGauss优化] 确定查询起始ID: last_processed_id={last_processed_id} "
                f"(first_pending_id={first_pending_id}, start_from_id={start_from_id})"
            )
            # 循环检索文件，直到累积到足够的文件或没有更多文件
            should_stop = False  # 是否应该停止检索
            query_count = 0  # 查询次数计数器
            while not should_stop:
                query_count += 1
                # 使用原生 openGauss SQL 分批检索（使用 LIMIT 和 WHERE id > last_processed_id）
                # 减少日志输出：只在每10次查询或第一次查询时输出
                if query_count == 1 or query_count % 10 == 0:
                    logger.debug(
                        f"[openGauss优化] 执行查询 #{query_count}: backup_set_id={backup_set_db_id}, "
                        f"id > {last_processed_id}, LIMIT {batch_size}"
                    )
                else:
                    logger.debug(
                        f"[openGauss优化] 执行查询 #{query_count}: id > {last_processed_id}, LIMIT {batch_size}"
                    )
                
                # 先查询一下总共有多少未压缩文件（用于调试）
                # 添加超时和错误处理，避免缓冲区错误导致整个函数失败
                total_pending = 0
                pending_after_id = 0
                try:
                    # 只在每10次查询或第一次查询时执行统计查询（减少开销）
                    if query_count == 1 or query_count % 10 == 0:
                        total_pending_count_row = await asyncio.wait_for(
                            conn.fetchrow(
                                f"""
                                SELECT COUNT(*)::BIGINT as count
                                FROM {table_name}
                                WHERE backup_set_id = $1::INTEGER
                                  AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                                  AND file_type = 'file'::backupfiletype
                                """,
                                backup_set_db_id
                            ),
                            timeout=10.0
                        )
                        total_pending = total_pending_count_row['count'] if total_pending_count_row else 0
                        
                        # 查询 id > last_processed_id 的未压缩文件数量
                        pending_after_id_row = await asyncio.wait_for(
                            conn.fetchrow(
                                f"""
                                SELECT COUNT(*)::BIGINT as count
                                FROM {table_name}
                                WHERE backup_set_id = $1::INTEGER
                                  AND (is_copy_success IS DISTINCT FROM TRUE)
                                  AND file_type = 'file'::backupfiletype
                                  AND id > $2::BIGINT
                                """,
                                backup_set_db_id,
                                last_processed_id
                            ),
                            timeout=10.0
                        )
                        pending_after_id = pending_after_id_row['count'] if pending_after_id_row else 0
                        
                        logger.debug(
                            f"[openGauss优化] 未压缩文件统计（查询 #{query_count}）: 总计={total_pending}, "
                            f"id > {last_processed_id} 的数量={pending_after_id}, "
                            f"本次文件累计={total_files_processed}"
                        )
                except Exception as e:
                    error_msg = str(e)
                    # 如果表不存在，返回空结果
                    if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(e).__name__):
                        logger.info(
                            f"[openGauss优化] backup_files 表不存在，返回空结果（可能是数据库未初始化）: {error_msg}"
                        )
                        return ([], start_from_id)
                    # 调试查询失败不影响主流程，只记录警告
                    error_type = type(e).__name__
                    
                    # 根据错误类型提供更详细的说明
                    if isinstance(e, AssertionError) and "insufficient data in buffer" in error_msg:
                        reason = "缓冲区错误（可能是数据库负载高或连接问题）"
                    elif isinstance(e, asyncio.TimeoutError):
                        reason = "查询超时（10秒内未完成，可能是数据库负载高）"
                    elif isinstance(e, (ValueError, TypeError, KeyError)):
                        reason = "数据解析错误（可能是数据库返回格式异常）"
                    else:
                        reason = "未知错误"
                    
                    logger.info(
                        f"[openGauss优化] 统计查询失败（不影响主流程）: {error_type}: {error_msg}，"
                        f"原因: {reason}，继续执行主查询"
                    )
                    logger.debug(
                        f"[openGauss优化] 统计查询失败详情: backup_set_id={backup_set_db_id}, "
                        f"last_processed_id={last_processed_id}",
                        exc_info=True
                    )
                
                # 使用显式类型转换避免 openGauss 缓冲区错误
                # 无限重试直到成功，确保备份任务不会因临时错误而中断
                # 已取消所有延迟控制，立即重试
                current_batch_size = batch_size
                min_batch_size = 50  # 最小批次大小（从100降低到50，避免缓冲区错误）
                retry_count = 0
                rows = None
                duplicate_count_in_batch = 0  # 用于统计本批次重复路径数量
                while True:  # 无限循环直到成功
                    try:
                        # 如果批次大小已经是50且重试次数很多，尝试进一步减小批次
                        # 这可能是某些记录数据量过大导致的
                        if current_batch_size == 50 and retry_count > 100:
                            # 尝试更小的批次：10条
                            if retry_count % 100 == 0:  # 每100次重试尝试一次更小的批次
                                current_batch_size = 10
                                logger.info(
                                    f"[openGauss优化] 批次大小50持续失败（第 {retry_count} 次重试），"
                                    f"尝试更小批次：{current_batch_size}，"
                                    f"可能是某些文件记录数据量过大（路径很长等）"
                                )
                        elif current_batch_size == 10 and retry_count > 200:
                            # 如果10条还是失败，尝试1条
                            if retry_count % 200 == 0:  # 每200次重试尝试一次单条查询
                                current_batch_size = 1
                                logger.info(
                                    f"[openGauss优化] 批次大小10持续失败（第 {retry_count} 次重试），"
                                    f"尝试单条查询：{current_batch_size}，"
                                    f"将逐条处理以避免缓冲区错误"
                                )
                        # 如果之前失败，尝试减小批次大小
                        if retry_count > 0:
                            # 批次大小减小策略：
                            # 1. 如果批次 > min_batch_size，每次减半
                            # 2. 如果批次已经是 min_batch_size，不再减小（避免无限减小）
                            if current_batch_size > min_batch_size:
                                current_batch_size = max(min_batch_size, current_batch_size // 2)
                                logger.info(
                                    f"[openGauss优化] 重试查询（第 {retry_count} 次），"
                                    f"减小批次大小至 {current_batch_size}"
                                )
                            
                            # 每10次重试记录一次详细信息
                            if retry_count % 10 == 0:
                                logger.info(
                                    f"[openGauss优化] 持续重试中（第 {retry_count} 次），"
                                    f"批次大小={current_batch_size}"
                                )
                            
                            # 每50次重试记录一次警告（批次很小但持续失败）
                            if retry_count % 50 == 0 and current_batch_size <= 50:
                                logger.info(
                                    f"[openGauss优化] ⚠️ 批次大小已降至 {current_batch_size} 但持续失败（第 {retry_count} 次重试），"
                                    f"可能原因：\n"
                                    f"  1. 某些文件记录的数据量过大（路径很长、文件名很长等）\n"
                                    f"  2. 数据库连接不稳定或网络传输问题\n"
                                    f"  3. asyncpg 驱动缓冲区限制\n"
                                    f"处理策略：\n"
                                    f"  - 批次大小={current_batch_size}（已是最小值或接近最小值）\n"
                                    f"  - 将继续无限重试直到成功\n"
                                    f"  - 如果持续失败，将在重试 {100 - (retry_count % 100)} 次后尝试更小批次"
                                )
                        
                        # 设置查询并行度以优化大批量查询性能
                        try:
                            settings = get_settings()
                            query_dop = getattr(settings, 'DB_QUERY_DOP', 16)
                            await conn.execute(f"SET LOCAL query_dop = {query_dop};")
                        except Exception as e:
                            # 设置失败不影响查询，继续执行
                            logger.debug(f"设置 query_dop 失败（可能不是 openGauss）: {e}")
                        
                        # 根据批次大小动态设置查询超时时间
                        # 批次越大，查询时间越长，需要更长的超时时间
                        # 估算：每1000条记录约需1-2秒，加上安全余量
                        estimated_query_time = max(30.0, (current_batch_size / 1000) * 2.0)  # 至少30秒，每1000条2秒
                        query_timeout = min(estimated_query_time, 300.0)  # 最多5分钟
                        
                        try:
                            # 设置数据库层面的查询超时
                            await conn.execute(f"SET LOCAL statement_timeout = '{int(query_timeout)}s'")
                        except Exception as e:
                            logger.debug(f"设置 statement_timeout 失败: {e}")
                        
                        # 优化：简化查询字段，减少数据传输量，避免缓冲区错误
                        # 只查询必要的字段，减少单行数据大小
                        # 使用 asyncio.wait_for 包装查询，设置超时保护
                        rows = await asyncio.wait_for(
                            conn.fetch(
                                f"""
                                SELECT 
                                    id,
                                    file_path,
                                    file_name,
                                    directory_path,
                                    display_name,
                                    file_type,
                                    file_size,
                                    file_permissions,
                                    modified_time,
                                    accessed_time
                                FROM {table_name}
                                WHERE backup_set_id = $1
                                  -- 查询 is_copy_success = FALSE OR is_copy_success IS NULL 的记录（未标记为 TRUE 的记录）
                                  -- 这些记录将在成组后通过 mark_files_as_queued 设置为 is_copy_success = TRUE
                                  AND (is_copy_success IS DISTINCT FROM TRUE)
                                  AND file_type = 'file'
                                  AND id > $2
                                ORDER BY id
                                LIMIT $3
                                """,
                                backup_set_db_id,
                                last_processed_id,
                                current_batch_size
                            ),
                            timeout=query_timeout  # 使用动态计算的超时时间
                        )
                        # 查询成功，退出循环
                        if retry_count > 0:
                            logger.info(
                                f"[openGauss优化] 查询成功（经过 {retry_count} 次重试），"
                                f"最终批次大小={current_batch_size}，返回 {len(rows)} 行"
                            )
                        break
                        
                    except AssertionError as e:
                        # 缓冲区错误，可能是批次太大或连接问题
                        error_msg = str(e)
                        if "insufficient data in buffer" in error_msg:
                            retry_count += 1
                            
                            logger.info(
                                f"[openGauss优化] 缓冲区错误（第 {retry_count} 次重试）: {e}, "
                                f"当前批次大小={current_batch_size}，将无限重试直到成功"
                            )
                            
                            # BufferError优化：智能减小批次大小策略
                            # 批次很大时快速减小，批次中等时慢速减小，批次小时不减小
                            if current_batch_size > 10000:
                                # 批次很大（>10000）：快速减小（每次减半）
                                new_batch_size = max(min_batch_size, current_batch_size // 2)
                            elif current_batch_size > 1000:
                                # 批次中等（1000-10000）：慢速减小（每次减少30%）
                                new_batch_size = max(min_batch_size, int(current_batch_size * 0.7))
                            else:
                                # 批次已经很小（<1000）：不减小
                                new_batch_size = current_batch_size
                            
                            if new_batch_size < current_batch_size:
                                current_batch_size = new_batch_size
                                logger.info(
                                    f"[openGauss优化] 批次大小已减小至 {current_batch_size} 以避免缓冲区错误"
                                )
                            
                            # 立即重试，不等待
                            continue  # 继续重试
                        else:
                            # 其他 AssertionError，也继续重试（可能是临时错误）
                            retry_count += 1
                            logger.info(
                                f"[openGauss优化] 查询错误（第 {retry_count} 次重试）: {e}, "
                                f"将无限重试直到成功"
                            )
                            # 立即重试，不等待
                            continue  # 继续重试
                            
                    except Exception as e:
                        error_msg = str(e)
                        error_type = type(e).__name__
                        # 如果表不存在，直接返回空结果，不重试
                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in error_type:
                            logger.info(
                                f"[openGauss优化] backup_files 表不存在，返回空结果（可能是数据库未初始化）: {error_msg}"
                            )
                            return ([], start_from_id)
                        
                        # 其他异常继续重试逻辑
                        if isinstance(e, asyncio.TimeoutError):
                            # 超时错误，继续重试
                            retry_count += 1
                            logger.info(
                                f"[openGauss优化] 查询超时（第 {retry_count} 次重试）: {e}, "
                            f"将无限重试直到成功"
                        )
                        # 立即重试，不等待
                        continue  # 继续重试
                    
                    except Exception as e:
                        # 其他异常，也继续重试（可能是临时错误）
                        error_msg = str(e)
                        error_type = type(e).__name__
                        
                        # 如果表不存在，直接返回空结果，不重试
                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in error_type:
                            logger.info(
                                f"[openGauss优化] backup_files 表不存在，返回空结果（可能是数据库未初始化）: {error_msg}"
                            )
                            return ([], start_from_id)
                        
                        retry_count += 1
                        
                        # 检查是否是BufferError（asyncpg可能抛出BufferError而不是AssertionError）
                        is_buffer_error = (
                            error_type == 'BufferError' or 
                            'unexpected trailing' in error_msg.lower() or
                            'buffer' in error_msg.lower()
                        )
                        
                        if is_buffer_error:
                            # BufferError特殊处理：智能减小批次大小策略
                            if current_batch_size > 10000:
                                # 批次很大：快速减小（每次减半）
                                new_batch_size = max(min_batch_size, current_batch_size // 2)
                            elif current_batch_size > 1000:
                                # 批次中等：慢速减小（每次减少30%）
                                new_batch_size = max(min_batch_size, int(current_batch_size * 0.7))
                            else:
                                # 批次已经很小：不减小
                                new_batch_size = current_batch_size
                            
                            if new_batch_size < current_batch_size:
                                current_batch_size = new_batch_size
                                logger.info(
                                    f"[openGauss优化] BufferError：批次大小已减小至 {current_batch_size}"
                                )
                        
                        # 检查是否是 BufferError 相关错误
                        is_buffer_error = (
                            error_type == 'BufferError' or 
                            'unexpected trailing' in str(e).lower() or
                            'buffer' in str(e).lower()
                        )
                        
                        if is_buffer_error:
                            logger.info(
                                f"[openGauss优化] 查询异常（第 {retry_count} 次重试）: {error_type}: {e}\n"
                                f"说明: 这是数据库缓冲区错误，通常由以下原因导致：\n"
                                f"  1. 查询结果数据量过大，超出缓冲区容量\n"
                                f"  2. 网络传输过程中数据包不完整\n"
                                f"  3. 数据库连接不稳定\n"
                                f"处理: 已自动减小批次大小至 {current_batch_size}，将无限重试直到成功"
                            )
                        else:
                            logger.info(
                                f"[openGauss优化] 查询异常（第 {retry_count} 次重试）: {error_type}: {e}, "
                                f"将无限重试直到成功"
                            )
                        # 立即重试，不等待
                        continue  # 继续重试
                # 减少日志输出：只在每10次查询或第一次查询时输出详细信息
                if query_count == 1 or query_count % 10 == 0:
                    logger.debug(
                        f"[openGauss优化] 查询结果 #{query_count}: 返回 {len(rows)} 行, "
                        f"last_processed_id={last_processed_id}, "
                        f"实际批次大小={current_batch_size}, "
                        f"第一行ID={rows[0]['id'] if rows else 'N/A'}, "
                        f"最后一行ID={rows[-1]['id'] if rows else 'N/A'}"
                    )
                
                # 记录重复路径统计（在处理完 rows 后）
                if duplicate_count_in_batch > 0:
                    total_duplicate_count += duplicate_count_in_batch
                    logger.warning(
                        f"[openGauss优化] ⚠️ 本批次发现 {duplicate_count_in_batch} 条重复路径记录，"
                        f"已去重（只保留 id 最小的记录），累计重复 {total_duplicate_count} 条"
                    )
                
                if not rows:
                    # 没有更多文件了，检查是否有异常情况
                    # 先查询一下总共有多少未压缩文件（用于调试）
                    total_pending = 0
                    pending_after_id = 0
                    try:
                        total_pending_count_row = await asyncio.wait_for(
                            conn.fetchrow(
                                f"""
                                SELECT COUNT(*)::BIGINT as count
                                FROM {table_name}
                                WHERE backup_set_id = $1::INTEGER
                                  AND (is_copy_success IS DISTINCT FROM TRUE)
                                  AND file_type = 'file'::backupfiletype
                                """,
                                backup_set_db_id
                            ),
                            timeout=10.0
                        )
                        total_pending = total_pending_count_row['count'] if total_pending_count_row else 0
                        
                        # 查询 id > last_processed_id 的未压缩文件数量
                        pending_after_id_row = await asyncio.wait_for(
                            conn.fetchrow(
                                f"""
                                SELECT COUNT(*)::BIGINT as count
                                FROM {table_name}
                                WHERE backup_set_id = $1::INTEGER
                                  AND (is_copy_success = FALSE OR is_copy_success IS NULL)
                                  AND file_type = 'file'::backupfiletype
                                  AND id > $2::BIGINT
                                """,
                                backup_set_db_id,
                                last_processed_id
                            ),
                            timeout=10.0
                        )
                        pending_after_id = pending_after_id_row['count'] if pending_after_id_row else 0
                    except Exception:
                        pass  # 忽略统计查询错误
                    
                    # 如果有未压缩文件但 id > last_processed_id 的没有，说明可能有ID更小的未压缩文件
                    if total_pending > 0 and pending_after_id == 0:
                        # 检查扫描状态：只有扫描完成时才进行全库检索
                        scan_status = await self.get_scan_status(backup_task_id) if backup_task_id else None
                        
                        if scan_status == 'completed':
                            logger.info(
                                f"[openGauss优化] ⚠️ 检测到异常：总共有 {total_pending} 个未压缩文件，"
                                f"但 id > {last_processed_id} 的没有，扫描已完成，进行全库检索（不限制ID范围）"
                            )
                            # 扫描已完成，进行全库检索，不限制 id > last_processed_id
                            should_stop = True  # 退出当前循环
                            # 标记需要进行全库检索
                            need_full_search = True
                            break
                        else:
                            logger.info(
                                f"[openGauss优化] ⚠️ 检测到异常：总共有 {total_pending} 个未压缩文件，"
                                f"但 id > {last_processed_id} 的没有，可能存在ID更小的未压缩文件，"
                                f"或者新文件还在内存数据库中未同步。扫描状态={scan_status}，重置查询起点，从第一个未压缩文件开始查询"
                            )
                            # 重新查询第一个未压缩文件ID
                            try:
                                first_pending_id_row = await asyncio.wait_for(
                                    conn.fetchrow(
                                        f"""
                                        SELECT MIN(id)::BIGINT as min_id
                                        FROM {table_name}
                                        WHERE backup_set_id = $1::INTEGER
                                          AND (is_copy_success IS DISTINCT FROM TRUE)
                                          AND file_type = 'file'::backupfiletype
                                        """,
                                        backup_set_db_id
                                    ),
                                    timeout=10.0
                                )
                                first_pending_id = first_pending_id_row['min_id'] if first_pending_id_row and first_pending_id_row['min_id'] else None
                                
                                if first_pending_id:
                                    logger.info(
                                        f"[openGauss优化] 重置查询起点：从第一个未压缩文件ID {first_pending_id} 开始查询"
                                    )
                                    # 重置 last_processed_id，从第一个未压缩文件开始查询
                                    last_processed_id = first_pending_id - 1
                                    # 继续循环，重新查询（不返回空列表）
                                    continue
                                else:
                                    # 如果查询不到第一个文件ID，但扫描未完成，返回空列表等待
                                    logger.info(
                                        f"[openGauss优化] 无法查询到第一个未压缩文件ID，但扫描未完成（状态={scan_status}），"
                                        f"返回空列表等待更多文件同步"
                                    )
                                    should_stop = True
                                    break
                            except Exception as e:
                                logger.error(
                                    f"[openGauss优化] 查询第一个未压缩文件ID时出错: {e}，"
                                    f"扫描状态={scan_status}，返回空列表等待"
                                )
                                should_stop = True
                                break
                    else:
                        # 正常情况：没有更多文件了
                        break
                
                # 转换为文件信息字典并累积（去重：相同 file_path 只保留 id 最小的记录）
                # 重置本批次重复计数（每次查询新批次时重置）
                duplicate_count_in_batch = 0
                for row in rows:
                    file_type = row['file_type']
                    file_path = row['file_path']
                    file_info = {
                        'id': row['id'],
                        'path': file_path,
                        'file_path': file_path,
                        'name': row['file_name'],
                        'file_name': row['file_name'],
                        'directory_path': row['directory_path'],
                        'display_name': row['display_name'],
                        'size': row['file_size'] or 0,
                        'permissions': row['file_permissions'],
                        'modified_time': row['modified_time'],
                        'accessed_time': row['accessed_time'],
                        'is_dir': str(file_type).lower() == 'directory',
                        'is_file': str(file_type).lower() == 'file',
                        'is_symlink': str(file_type).lower() == 'symlink'
                    }
                    
                    # 去重逻辑：相同 file_path 只保留 id 最小的记录
                    should_skip = False
                    if file_path in seen_paths:
                        existing_id, existing_info = seen_paths[file_path]
                        if row['id'] < existing_id:
                            # 当前记录的 id 更小，需要替换已保存的记录
                            duplicate_count_in_batch += 1
                            # 如果旧记录已经在 all_files 中，需要移除并更新大小
                            if existing_info in all_files:
                                all_files.remove(existing_info)
                                current_group_size -= existing_info.get('size', 0)
                            # 保存新记录
                            seen_paths[file_path] = (row['id'], file_info)
                        else:
                            # 当前记录的 id 更大，跳过（保留已保存的记录）
                            duplicate_count_in_batch += 1
                            should_skip = True
                    else:
                        # 首次遇到该路径，保存
                        seen_paths[file_path] = (row['id'], file_info)
                    
                    # 如果是重复路径且已跳过，不进行后续处理
                    if should_skip:
                        continue
                    
                    file_size = file_info['size']
                    
                    # 第一原则：超大文件处理（最高优先级）
                    # 如果单个文件 > max_file_size - 5%，立即独立成组，放弃当前已累积的所有文件
                    if file_size > min_group_size:
                        # 如果当前组已有文件，丢弃当前组（前面加的文件保持 is_copy_success = FALSE，下次还可以成组压缩）
                        if all_files:
                            discarded_count = len(all_files)
                            discarded_size = current_group_size
                            first_discarded_id = all_files[0].get('id')
                            # 下一次查询需要从第一个被丢弃的文件重新开始（id > resume_id）
                            resume_id = (first_discarded_id - 1) if first_discarded_id else (row['id'] - 1)
                            
                            logger.info(
                                f"[第一原则] 超大文件独立成组：文件 {format_bytes(file_size)} > {format_bytes(min_group_size)}，"
                                f"丢弃已累积的 {discarded_count} 个文件（总大小 {format_bytes(discarded_size)}），"
                                f"超大文件优先单独处理，被丢弃的文件保持 is_copy_success = FALSE，下次重新累积"
                            )
                            # 丢弃当前组，只返回超大文件单独成组，last_processed_id 从丢弃文件前一个ID算起
                            total_files_processed += 1  # 累计处理的文件数（超大文件）
                            return ([[file_info]], resume_id)
                        
                        # 超大文件单独成组
                        total_files_processed += 1  # 累计处理的文件数（超大文件）
                        logger.info(
                            f"[第一原则] 超大文件单独成组：{format_bytes(file_size)} "
                            f"(超过阈值 {format_bytes(min_group_size)} = MAX_FILE_SIZE - 5%)"
                        )
                        return ([[file_info]], row['id'])
                    
                    # 第二原则：文件组累积处理（简化版：大于min_group_size就返回，不再判断max_group_size上限）
                    new_group_size = current_group_size + file_size

                    # 如果新组大小 <= min_group_size，继续累积文件
                    if new_group_size <= min_group_size:
                        # 加入当前组
                        all_files.append(file_info)
                        current_group_size = new_group_size
                        last_processed_id = row['id']
                        total_files_processed += 1  # 累计处理的文件数
                        # 继续处理下一个文件
                        continue
                    
                    # 如果新组大小 > min_group_size，加入文件并返回（不再判断max_group_size上限，不再跳过文件）
                    if new_group_size > min_group_size:
                        # 加入文件并返回
                        all_files.append(file_info)
                        current_group_size = new_group_size
                        last_processed_id = row['id']
                        total_files_processed += 1  # 累计处理的文件数
                        logger.info(
                            f"[openGauss优化] 文件组大小超过阈值：{format_bytes(new_group_size)} > {format_bytes(min_group_size)}，返回文件组"
                        )
                        should_stop = True
                        break  # 跳出内层循环（处理 rows 的循环）
                
                # 关键修复：无论是否提前break，都要将 last_processed_id 更新为查询返回的最后一个文件ID
                # 这样可以确保下次查询不会跳过任何文件（即使有些文件被跳过处理，也要确保查询范围覆盖所有文件）
                if rows:
                    # 更新 last_processed_id 为查询返回的最后一个文件ID（不是实际处理的最后一个）
                    last_row_id = rows[-1]['id']
                    if last_row_id > last_processed_id:
                        logger.debug(
                            f"[openGauss优化] 批次处理完成，更新 last_processed_id: {last_processed_id} -> {last_row_id} "
                            f"（查询返回的最后一行ID，确保不跳过任何文件）"
                        )
                        last_processed_id = last_row_id
                
                # 如果检索到的文件数少于当前批次大小，说明没有更多文件了
                if len(rows) < current_batch_size:
                    break
                
                # 如果已经达到容差范围，跳出外层循环
                if should_stop:
                    break
        
        # 如果已经有文件组了，直接返回，不需要全库检索
        if all_files:
            # 文件已在检索时处理并分组，all_files 就是当前组
            current_group = all_files
            
            logger.info(
                f"[openGauss优化] 检索到 {len(current_group)} 个未压缩文件，"
                f"总大小 {format_bytes(current_group_size)}，"
                f"阈值：{format_bytes(min_group_size)}（文件组大小 > {format_bytes(min_group_size)} 时返回），"
                f"重试次数：{retry_count}/{max_retries}，"
                f"执行查询次数：{query_count} 次（每次查询 {batch_size} 条）"
                + (f"，去重：发现并过滤了 {total_duplicate_count} 条重复路径记录" if total_duplicate_count > 0 else "")
            )
            
            # 检查文件组是否为空
            if not current_group:
                logger.info("[openGauss优化] 没有待压缩文件")
                return ([], last_processed_id if last_processed_id > start_from_id else start_from_id)
            
            # 计算大小比例
            size_ratio = current_group_size / max_file_size if max_file_size > 0 else 0
            scan_status = await self.get_scan_status(backup_task_id) if backup_task_id else None
            
            # 如果文件组大小超过阈值，直接返回
            if current_group_size > min_group_size:
                logger.info(
                    f"[openGauss优化] 文件组大小超过阈值：{format_bytes(current_group_size)} > {format_bytes(min_group_size)}，返回文件组"
                )
                # 注意：不要在这里标记扫描完成
                # 扫描完成应该由扫描任务自己标记，而不是在检索文件时标记
                # 检索到文件组不等于扫描完成，文件可能还在扫描中
                # 返回文件组和最后处理的文件ID
                return ([current_group], last_processed_id)
            
            # 关键修复：扫描完成后，无论文件组大小如何，只要有文件就必须返回，不能丢弃
            if scan_status == 'completed' and current_group and len(current_group) > 0:
                # 扫描已完成，强制返回剩余文件组（不能丢弃）
                logger.info(
                    f"[openGauss优化] ✅ 扫描已完成，强制返回剩余文件组："
                    f"文件组大小 {format_bytes(current_group_size)} "
                    f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，"
                    f"文件数: {len(current_group)} 个，确保所有文件都被压缩"
                )
                return ([current_group], last_processed_id)
            
            # 文件组大小不足且扫描未完成，继续等待（返回空列表，等待更多文件）
            logger.info(
                f"[openGauss优化] 文件组大小不足：{format_bytes(current_group_size)} < {format_bytes(min_group_size)}，"
                f"扫描状态：{scan_status}，等待更多文件..."
            )
            # 关键修复：已累积的文件不能丢弃！如果已检索到文件但大小不够，应该从第一个累积文件的ID-1开始查询
            if current_group and len(current_group) > 0:
                first_file_id = current_group[0].get('id')
                if first_file_id:
                    resume_id = first_file_id - 1
                    logger.info(
                        f"[openGauss优化] 已检索到 {len(current_group)} 个文件但大小不够，"
                        f"第一个文件ID: {first_file_id}，下次查询将从 id > {resume_id} 开始（确保不丢弃未成组文件）"
                    )
                    return ([], resume_id)
            return ([], last_processed_id)
        
        # 注意：已禁用全库检索，避免对900万记录进行全库查询（性能问题）
        # 异常检测逻辑已在循环内的 if not rows 块中处理，通过重置查询起点（MIN(id)）来继续查询
        else:
            logger.info(
                f"[openGauss优化] 没有检索到任何文件，返回空列表，"
                f"start_from_id={start_from_id}, last_processed_id={last_processed_id}"
            )
            # 关键修复：即使没有检索到文件，也要返回 last_processed_id（如果有效），避免丢失进度
            return_id = last_processed_id if last_processed_id > start_from_id else start_from_id
            return ([], return_id)

        # 文件已在检索时处理并分组，all_files 就是当前组
        current_group = all_files
        skipped_files = []  # openGauss优化：跳过的文件已在检索时处理，这里保留变量以兼容日志

        logger.info(
            f"[openGauss优化] 检索到 {len(current_group)} 个未压缩文件，"
            f"总大小 {format_bytes(current_group_size)}，"
            f"阈值：{format_bytes(min_group_size)}（文件组大小 > {format_bytes(min_group_size)} 时返回），"
            f"重试次数：{retry_count}/{max_retries}"
            + (f"，去重：发现并过滤了 {total_duplicate_count} 条重复路径记录" if total_duplicate_count > 0 else "")
        )

        # 处理最终的文件组
        if not current_group:
            logger.info("[openGauss优化] 没有待压缩文件")
            return ([], last_processed_id if last_processed_id > start_from_id else start_from_id)

        # 检查当前组大小是否在容差范围内
        size_ratio = current_group_size / max_file_size if max_file_size > 0 else 0
        scan_status = await self.get_scan_status(backup_task_id) if backup_task_id else None

        if current_group_size < min_group_size and scan_status != 'completed' and retry_count < max_retries:
            # 组大小低于容差下限且扫描未完成，继续等待
            logger.info(
                f"[openGauss优化] 文件组大小低于容差下限：{format_bytes(current_group_size)} "
                f"(需要 ≥ {format_bytes(min_group_size)} = {size_ratio*100:.1f}% of 目标)，"
                f"扫描状态：{scan_status}，等待更多文件...（重试 {retry_count}/{max_retries}）"
            )
            # 关键修复：已累积的文件不能丢弃！如果已检索到文件但大小不够，应该从第一个累积文件的ID-1开始查询
            # 这样下次查询 id > (first_file_id - 1) 就能包含所有已累积的文件，确保它们不会被丢弃
            # 返回格式：(file_groups, last_processed_id)
            if current_group and len(current_group) > 0:
                # 获取第一个累积文件的ID
                first_file_id = current_group[0].get('id')
                if first_file_id:
                    # 返回第一个文件的ID - 1，确保下次查询能包含所有已累积的文件
                    resume_id = first_file_id - 1
                    logger.info(
                        f"[openGauss优化] 已检索到 {len(current_group)} 个文件但大小不够，"
                        f"第一个文件ID: {first_file_id}，最后处理的文件ID: {last_processed_id}，"
                        f"下次查询将从 id > {resume_id} 开始（确保包含所有已累积的文件，不丢弃未成组文件）"
                    )
                    # 返回空列表和第一个文件的ID-1，确保已累积的文件不会被丢弃
                    return ([], resume_id)
                else:
                    # 如果无法获取第一个文件的ID，使用 last_processed_id
                    logger.warning(
                        f"[openGauss优化] 无法获取第一个累积文件的ID，使用 last_processed_id: {last_processed_id}"
                    )
                    return ([], last_processed_id)
            # 如果没有检索到文件，返回传入的 start_from_id 或 last_processed_id（取较大值）
            return_id = last_processed_id if last_processed_id > start_from_id else start_from_id
            logger.info(
                f"[openGauss优化] 文件组为空或 last_processed_id 无效，"
                f"返回 start_from_id={start_from_id}, last_processed_id={last_processed_id}, "
                f"最终返回ID={return_id}"
            )
            return ([], return_id)
        
        # 如果达到重试上限或扫描已完成，即使大小不足也返回当前文件组
        if current_group_size < min_group_size:
            # 关键修复：扫描完成后，无论文件组大小如何，只要有文件就必须返回，不能丢弃
            if scan_status == 'completed' and current_group and len(current_group) > 0:
                # 扫描已完成，强制返回剩余文件组（不能丢弃）
                logger.info(
                    f"[openGauss优化] ✅ 扫描已完成，强制返回剩余文件组："
                    f"文件组大小 {format_bytes(current_group_size)} "
                    f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，"
                    f"文件数: {len(current_group)} 个，确保所有文件都被压缩"
                )
                return ([current_group], last_processed_id)
            
            # 检查文件组大小是否太小，扫描未完成时继续等待
            if current_group_size < min_acceptable_group_size:
                reason = '扫描已完成' if scan_status == 'completed' else '达到重试上限'
                logger.warning(
                    f"[openGauss优化] ❌ 文件组大小过小，拒绝返回：文件组大小 {format_bytes(current_group_size)} "
                    f"< 最小可接受大小 {format_bytes(min_acceptable_group_size)} "
                    f"(< {format_bytes(min_group_size)} = 95% of 目标)，"
                    f"原因：{reason}，返回空列表，等待更多文件累积"
                )
                # 关键修复：已累积的文件不能丢弃！如果已检索到文件但大小过小，应该从第一个累积文件的ID-1开始查询
                if current_group and len(current_group) > 0:
                    first_file_id = current_group[0].get('id')
                    if first_file_id:
                        resume_id = first_file_id - 1
                        logger.info(
                            f"[openGauss优化] 已检索到 {len(current_group)} 个文件但大小过小，"
                            f"第一个文件ID: {first_file_id}，下次查询将从 id > {resume_id} 开始（确保不丢弃未成组文件）"
                        )
                        return ([], resume_id)
                # 返回空列表，等待更多文件累积
                return ([], last_processed_id)
            
            reason = '扫描已完成' if scan_status == 'completed' else '达到重试上限'
            logger.info(
                f"[openGauss优化] ⚠️ 强制返回文件组：文件组大小 {format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)} 但 ≥ {format_bytes(min_acceptable_group_size)})，"
                f"原因：{reason}，返回文件组（{len(current_group)} 个文件）"
            )
            # 标记检索完成（如果返回了文件组，说明检索完成）
            if backup_task_id and current_group:
                current_scan_status = await self.get_scan_status(backup_task_id)
                if current_scan_status == 'retrieving':
                    logger.info(f"[openGauss优化] 标记检索状态为完成（backup_task_id={backup_task_id}）")
                    await self.update_scan_status(backup_task_id, 'completed')
            return ([current_group], last_processed_id)

        # 文件组大小 >= min_group_size，返回文件组
        if current_group_size >= min_group_size:
            logger.info(
                f"[openGauss优化] 文件组大小超过阈值：{format_bytes(current_group_size)} >= {format_bytes(min_group_size)}，返回文件组"
            )
            # 标记检索完成（如果返回了文件组，说明检索完成）
            if backup_task_id and current_group:
                current_scan_status = await self.get_scan_status(backup_task_id)
                if current_scan_status == 'retrieving':
                    logger.info(f"[openGauss优化] 标记检索状态为完成（backup_task_id={backup_task_id}）")
                    await self.update_scan_status(backup_task_id, 'completed')
            # 返回文件组和最后处理的文件ID
            return ([current_group], last_processed_id)
        else:
            # 强制压缩情况：组大小不够，需要进行全库搜索防止遗漏
            reason = '扫描已完成' if scan_status == 'completed' else '达到重试上限'
            logger.info(
                f"[openGauss优化] 强制压缩：文件组大小 {format_bytes(current_group_size)} "
                f"({size_ratio*100:.1f}% of 目标，< {format_bytes(min_group_size)})，"
                f"原因：{reason}，将进行全库搜索防止遗漏"
            )
            
            # 当不能凑够容量时，进行本备份集全库搜索，防止遗漏
            # 超时时间按1000万记录设置（估算：1000万记录 * 0.1秒/万记录 = 1000秒，约16.7分钟）
            full_search_timeout = 7200.0  # 7200秒超时（按1000万记录计算）
            logger.info(f"[openGauss优化] 开始全库搜索本备份集所有未压缩文件（超时时间：{full_search_timeout}秒，按1000万记录设置）...")
            
            try:
                async with get_opengauss_connection() as conn:
                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                    # 设置查询超时（使用原生 openGauss SQL）
                    await conn.execute(f"SET LOCAL statement_timeout = '{int(full_search_timeout)}s'")
                    
                    # 全库搜索：检索本备份集所有未压缩文件（使用原生 openGauss SQL）
                    all_pending_rows = await asyncio.wait_for(
                        conn.fetch(
                            f"""
                            SELECT id, file_path, file_name, directory_path, display_name, file_type,
                                   file_size, file_permissions, modified_time, accessed_time
                            FROM {table_name}
                            WHERE backup_set_id = $1
                              AND (is_copy_success IS DISTINCT FROM TRUE)
                              AND file_type = 'file'::backupfiletype
                            ORDER BY id
                            """,
                            backup_set_db_id
                        ),
                        timeout=full_search_timeout
                    )
                    
                    logger.info(f"[openGauss优化] 全库搜索完成，找到 {len(all_pending_rows)} 个未压缩文件")
                    
                    # 如果全库搜索找到更多文件，使用全库搜索结果重新构建文件组
                    if all_pending_rows and len(all_pending_rows) > len(all_files):
                        logger.info(
                            f"[openGauss优化] 全库搜索发现更多文件："
                            f"分批检索找到 {len(all_files)} 个，全库搜索找到 {len(all_pending_rows)} 个，"
                            f"将使用全库搜索结果重新构建文件组"
                        )
                        
                        # 重新构建文件组（使用全库搜索结果）
                        all_files = []
                        current_group_size = 0
                        skipped_files = []
                        
                        for row in all_pending_rows:
                            file_type = row['file_type']
                            file_info = {
                                'id': row['id'],
                                'path': row['file_path'],
                                'file_path': row['file_path'],
                                'name': row['file_name'],
                                'file_name': row['file_name'],
                                'directory_path': row['directory_path'],
                                'display_name': row['display_name'],
                                'size': row['file_size'] or 0,
                                'permissions': row['file_permissions'],
                                'modified_time': row['modified_time'],
                                'accessed_time': row['accessed_time'],
                                'is_dir': str(file_type).lower() == 'directory',
                                'is_file': str(file_type).lower() == 'file',
                                'is_symlink': str(file_type).lower() == 'symlink'
                            }
                            
                            file_size = file_info['size']
                            
                            # 第一原则：超大文件单独成组
                            if file_size > min_group_size:
                                if all_files:
                                    # 已累积的文件不能丢弃！返回已累积的文件组，超大文件下次处理
                                    first_file_id = all_files[0].get('id')
                                    resume_id = (first_file_id - 1) if first_file_id else (file_info['id'] - 1)
                                    logger.info(
                                        f"[openGauss优化] 全库搜索发现超大文件，但已累积 {len(all_files)} 个文件，"
                                        f"总大小 {format_bytes(current_group_size)}，"
                                        f"返回已累积的文件组（不丢弃未成组文件），超大文件下次处理"
                                    )
                                    # 返回已累积的文件组，确保不丢弃未成组文件
                                    return ([all_files], resume_id)
                                
                                # 超大文件单独成组
                                logger.info(
                                    f"[openGauss优化] 全库搜索发现超大文件单独成组：{format_bytes(file_size)} "
                                    f"(超过阈值 {format_bytes(min_group_size)})"
                                )
                                return ([[file_info]], file_info['id'])
                            
                            # 第二原则：计算新组大小，大于阈值就返回
                            new_group_size = current_group_size + file_size
                            
                            # 如果新组大小 <= 阈值，加入当前组
                            if new_group_size <= min_group_size:
                                all_files.append(file_info)
                                current_group_size = new_group_size
                                continue
                            
                            # 如果新组大小 > 阈值，加入并返回（不再判断上限）
                            all_files.append(file_info)
                            current_group_size = new_group_size
                            logger.info(
                                f"[openGauss优化] 全库搜索文件组大小超过阈值：{format_bytes(new_group_size)} > {format_bytes(min_group_size)}，返回文件组"
                            )
                            # 更新 current_group 并跳出循环
                            current_group = all_files
                            break
                        
                        # 如果没有在循环中返回，更新 current_group
                        if current_group != all_files:
                            current_group = all_files
                            logger.info(
                                f"[openGauss优化] 全库搜索重新构建文件组完成：{len(current_group)} 个文件，"
                                f"总大小 {format_bytes(current_group_size)}"
                            )
                    elif len(all_pending_rows) == len(all_files):
                        logger.info(
                            f"[openGauss优化] 全库搜索确认：分批检索和全库搜索找到的文件数量一致（{len(all_files)} 个），"
                            f"无遗漏"
                        )
                    else:
                        logger.info(
                            f"[openGauss优化] 全库搜索发现文件数量异常："
                            f"分批检索找到 {len(all_files)} 个，全库搜索找到 {len(all_pending_rows)} 个"
                        )
                        
            except asyncio.TimeoutError:
                logger.error(
                    f"[openGauss优化] 全库搜索超时（{full_search_timeout}秒），"
                    f"将使用分批检索的结果（{len(all_files)} 个文件）"
                )
            except Exception as full_search_error:
                logger.error(
                    f"[openGauss优化] 全库搜索失败：{str(full_search_error)}，"
                    f"将使用分批检索的结果（{len(all_files)} 个文件）",
                    exc_info=True
                )

        # 返回当前文件组（新策略：每次只返回一个组）
        # 计算最后处理的文件ID（文件组中最后一个文件的ID）
        final_last_processed_id = current_group[-1]['id'] if current_group else last_processed_id
        
        # 标记检索完成（如果返回了文件组，说明检索完成）
        if backup_task_id and current_group:
            current_scan_status = await self.get_scan_status(backup_task_id)
            if current_scan_status == 'retrieving':
                logger.info(f"[openGauss优化] 标记检索状态为完成（backup_task_id={backup_task_id}）")
                await self.update_scan_status(backup_task_id, 'completed')
        
        return ([current_group], final_last_processed_id)

    async def get_scan_status(self, backup_task_id: int) -> Optional[str]:
        """获取扫描状态"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                try:
                    row = await conn.fetchrow(
                        "SELECT scan_status FROM backup_tasks WHERE id = $1",
                        backup_task_id
                    )
                    return row['scan_status'] if row else None
                except Exception as e:
                    # 异常时回滚，确保连接处于干净状态
                    logger.error(f"get_scan_status: 查询失败: {str(e)}", exc_info=True)
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug(f"get_scan_status: 异常时事务已回滚（backup_task_id={backup_task_id}）")
                    except Exception as rollback_err:
                        logger.warning(f"get_scan_status: 回滚事务失败: {str(rollback_err)}")
                    raise  # 重新抛出异常
        else:
            logger.info("[get_scan_status] 当前仅支持 openGauss，返回 None")
            return None

    async def update_scan_status(self, backup_task_id: int, status: str):
        """更新扫描状态"""
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        if is_opengauss():
            current_time = datetime.now()
            async with get_opengauss_connection() as conn:
                try:
                    if status == 'completed':
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET scan_status = $1,
                                scan_completed_at = $2,
                                updated_at = $3
                            WHERE id = $4
                            """,
                            status,
                            current_time,
                            current_time,
                            backup_task_id
                        )
                    else:
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET scan_status = $1,
                                updated_at = $2
                            WHERE id = $3
                            """,
                            status,
                            current_time,
                            backup_task_id
                        )
                    
                    # 显式提交事务，确保状态更新对其他线程可见
                    await conn.commit()
                    
                    # 验证事务提交状态
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status == 0:  # IDLE: 事务成功提交
                            logger.debug(f"update_scan_status: 事务已提交（backup_task_id={backup_task_id}, status={status}）")
                        elif transaction_status == 1:  # INTRANS: 事务未提交
                            logger.warning(f"update_scan_status: ⚠️ 事务未提交，状态={transaction_status}，尝试回滚")
                            await actual_conn.rollback()
                            raise Exception("事务提交失败")
                        elif transaction_status == 3:  # INERROR: 错误状态
                            logger.error(f"update_scan_status: ❌ 连接处于错误状态，回滚事务")
                            await actual_conn.rollback()
                            raise Exception("连接处于错误状态")
                except Exception as db_error:
                    # 异常时显式回滚，避免长事务锁表
                    logger.error(f"update_scan_status: 数据库操作失败: {str(db_error)}", exc_info=True)
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug(f"update_scan_status: 异常时事务已回滚（backup_task_id={backup_task_id}）")
                    except Exception as rollback_err:
                        logger.warning(f"update_scan_status: 回滚事务失败: {str(rollback_err)}")
                    raise  # 重新抛出异常
        else:
            logger.info("[update_scan_status] 当前仅支持 openGauss，跳过数据库更新")

    async def _mark_files_as_copied(
        self,
        conn,
        backup_set_db_id: int,
        processed_files: List[Dict],
        compressed_file: Dict,
        tape_file_path: str,
        chunk_number: int,
        backup_time: datetime
    ):
        """Mark files as copied in the database (使用批量更新优化性能)"""
        # 空列表检查：避免执行不必要的 SQL 查询
        if not processed_files or len(processed_files) == 0:
            logger.debug("[_mark_files_as_copied] processed_files 为空，跳过数据库操作")
            return
        
        success_count = 0
        failed_count = 0
        per_file_compressed_size = compressed_file.get('compressed_size', 0)
        per_file_compressed_size = per_file_compressed_size // len(processed_files) if processed_files else 0
        is_compressed = compressed_file.get('compression_enabled', True)
        checksum = compressed_file.get('checksum')
        copy_time = datetime.now()

        # 批量查询已存在的文件
        file_paths = [f.get('file_path') for f in processed_files if f.get('file_path')]
        if not file_paths:
            logger.info(f"[mark_files_as_copied] 没有有效的文件路径，processed_files 数量: {len(processed_files)}")
            if processed_files:
                # 调试：打印第一个文件的结构
                first_file = processed_files[0]
                logger.info(f"[mark_files_as_copied] 第一个文件的结构: {list(first_file.keys())}")
            return
        
        logger.info(f"[mark_files_as_copied] 准备更新 {len(file_paths)} 个文件的 is_copy_success 状态")
        
        # 分批查询已存在的文件，避免单次查询过多文件导致超时
        # openGauss 批次大小：1000 个文件一批（减小批次大小以降低查询超时和缓冲区错误风险）
        batch_size = 1000
        existing_map = {}
        import asyncio
        
        if len(file_paths) > batch_size:
            logger.info(f"[mark_files_as_copied] 文件数量较多（{len(file_paths)} 个），将分批查询（每批 {batch_size} 个）")
            for i in range(0, len(file_paths), batch_size):
                batch_paths = file_paths[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(file_paths) + batch_size - 1) // batch_size
                
                # openGauss 重试机制：最多重试 3 次
                max_retries = 3
                retry_count = 0
                batch_success = False
                
                while retry_count < max_retries and not batch_success:
                    try:
                        # 每次查询前重置超时设置（使用 SQL statement_timeout，确保每次查询独立计时）
                        # 设置查询超时为 180 秒（3分钟），给查询足够的执行时间
                        if retry_count == 0:
                            logger.info(f"[mark_files_as_copied] 开始查询批次 {batch_num}/{total_batches}，包含 {len(batch_paths)} 个文件...")
                        else:
                            logger.info(f"[mark_files_as_copied] 批次 {batch_num}/{total_batches} 重试查询（第 {retry_count + 1}/{max_retries} 次）...")
                        
                        await conn.execute("SET LOCAL statement_timeout = '180s'")
                        logger.debug(f"[mark_files_as_copied] 已设置 statement_timeout，开始执行查询...")
                        
                        # 添加超时设置，防止查询阻塞（180秒超时）
                        # 使用显式类型转换，避免 openGauss 缓冲区错误
                        # 多表方案：根据 backup_set_db_id 决定物理表名
                        from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                        table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                        batch_existing = await asyncio.wait_for(
                            conn.fetch(
                                f"""
                                SELECT 
                                    id::INTEGER as id,
                                    file_path::TEXT as file_path,
                                    is_copy_success::BOOLEAN as is_copy_success
                                FROM {table_name}
                                WHERE backup_set_id = $1 AND file_path = ANY($2)
                                """,
                                backup_set_db_id, batch_paths
                            ),
                            timeout=180.0  # 180秒超时
                        )
                        logger.info(f"[mark_files_as_copied] 批次 {batch_num}/{total_batches} 查询完成，找到 {len(batch_existing)} 个已存在文件")
                        for row in batch_existing:
                            existing_map[row['file_path']] = row
                        logger.debug(f"[mark_files_as_copied] 已查询批次 {batch_num}/{total_batches}，找到 {len(batch_existing)} 个已存在文件")
                        batch_success = True
                    except asyncio.TimeoutError:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.error(f"[mark_files_as_copied] 批次 {batch_num}/{total_batches} 查询超时（180秒），已重试 {max_retries} 次，跳过该批次")
                            break
                        else:
                            logger.info(f"[mark_files_as_copied] 批次 {batch_num}/{total_batches} 查询超时，将在 {1.0 * retry_count} 秒后重试...")
                            await asyncio.sleep(1.0 * retry_count)  # 递增延迟重试
                    except (AssertionError, ConnectionError, OSError) as buffer_error:
                        # openGauss 缓冲区错误或连接错误，进行重试
                        retry_count += 1
                        error_type = type(buffer_error).__name__
                        error_msg = str(buffer_error) if buffer_error else "未知错误"
                        if retry_count >= max_retries:
                            logger.error(
                                f"[mark_files_as_copied] 批次查询失败（批次 {batch_num}/{total_batches}）: "
                                f"错误类型: {error_type}, 错误信息: {error_msg}，已重试 {max_retries} 次，跳过该批次",
                                exc_info=True
                            )
                            break
                        else:
                            logger.info(
                                f"[mark_files_as_copied] 批次 {batch_num}/{total_batches} 查询失败（{error_type}），"
                                f"将在 {1.0 * retry_count} 秒后重试（第 {retry_count + 1}/{max_retries} 次）..."
                            )
                            await asyncio.sleep(1.0 * retry_count)  # 递增延迟重试
                    except Exception as batch_error:
                        # 其他错误，记录但不重试（可能是数据问题）
                        error_type = type(batch_error).__name__
                        error_msg = str(batch_error) if batch_error else "未知错误"
                        logger.error(
                            f"[mark_files_as_copied] 批次查询失败（批次 {batch_num}/{total_batches}）: "
                            f"错误类型: {error_type}, 错误信息: {error_msg}，跳过该批次",
                            exc_info=True
                        )
                        break
        else:
            # 文件数量较少，直接查询（openGauss 添加重试机制）
            max_retries = 3
            retry_count = 0
            query_success = False
            
            while retry_count < max_retries and not query_success:
                try:
                    # 每次查询前重置超时设置（使用 SQL statement_timeout，确保每次查询独立计时）
                    if retry_count == 0:
                        logger.info(f"[mark_files_as_copied] 开始查询已存在文件，文件数={len(file_paths)}")
                    else:
                        logger.info(f"[mark_files_as_copied] 重试查询已存在文件（第 {retry_count + 1}/{max_retries} 次）...")
                    
                    await conn.execute("SET LOCAL statement_timeout = '300s'")
                    
                    # 添加超时设置，防止查询阻塞（300秒超时，增加超时时间以处理大数据量）
                    # 使用显式类型转换，避免 openGauss 缓冲区错误
                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                    existing_files = await asyncio.wait_for(
                        conn.fetch(
                            f"""
                            SELECT 
                                id::INTEGER as id,
                                file_path::TEXT as file_path,
                                is_copy_success::BOOLEAN as is_copy_success
                            FROM {table_name}
                            WHERE backup_set_id = $1 AND file_path = ANY($2)
                            """,
                            backup_set_db_id, file_paths
                        ),
                        timeout=300.0  # 300秒超时（增加超时时间）
                    )
                    existing_map = {row['file_path']: row for row in existing_files}
                    query_success = True
                except asyncio.TimeoutError:
                    retry_count += 1
                    if retry_count >= max_retries:
                        logger.error(f"[mark_files_as_copied] 查询已存在文件超时（300秒），文件数={len(file_paths)}，已重试 {max_retries} 次，将跳过查询，直接进行更新")
                        # 查询超时，清空 existing_map，让所有文件都走更新流程（通过 file_path 匹配）
                        existing_map = {}
                        break
                    else:
                        logger.info(f"[mark_files_as_copied] 查询超时，将在 {1.0 * retry_count} 秒后重试...")
                        await asyncio.sleep(1.0 * retry_count)  # 递增延迟重试
                except (AssertionError, ConnectionError, OSError) as buffer_error:
                    # openGauss 缓冲区错误或连接错误，进行重试
                    retry_count += 1
                    error_type = type(buffer_error).__name__
                    error_msg = str(buffer_error) if buffer_error else "未知错误"
                    if retry_count >= max_retries:
                        logger.error(
                            f"[mark_files_as_copied] 查询已存在文件失败（{error_type}）: {error_msg}，"
                            f"已重试 {max_retries} 次，将跳过查询，直接进行更新",
                            exc_info=True
                        )
                        # 查询失败，清空 existing_map，让所有文件都走更新流程
                        existing_map = {}
                        break
                    else:
                        logger.info(
                            f"[mark_files_as_copied] 查询失败（{error_type}），"
                            f"将在 {1.0 * retry_count} 秒后重试（第 {retry_count + 1}/{max_retries} 次）..."
                        )
                        await asyncio.sleep(1.0 * retry_count)  # 递增延迟重试
                except Exception as query_error:
                    # 其他错误，记录但不重试（可能是数据问题）
                    logger.error(
                        f"[mark_files_as_copied] 查询已存在文件失败: {query_error}，将跳过查询，直接进行更新",
                        exc_info=True
                    )
                    # 查询失败，清空 existing_map，让所有文件都走更新流程
                    existing_map = {}
                    break
        
        # 准备批量更新和插入的数据
        update_data = []
        insert_data = []
        
        # 如果查询失败或超时，existing_map 为空，需要通过 file_path 直接更新
        use_file_path_update = len(existing_map) == 0 and len(processed_files) > 0
        
        for processed_file in processed_files:
            try:
                file_path = processed_file.get('file_path')
                if not file_path:
                    continue
                
                file_stat = processed_file.get('file_stat')
                metadata = processed_file.get('file_metadata') or {}
                metadata.update({
                    'tape_file_path': tape_file_path,
                    'chunk_number': chunk_number,
                    'original_path': file_path
                })
                
                # 确保 metadata_json 是有效的 JSON 字符串
                try:
                    metadata_json = json.dumps(metadata) if metadata else '{}'
                    # 验证 JSON 格式
                    json.loads(metadata_json)
                except (TypeError, ValueError) as json_error:
                    logger.info(f"[mark_files_as_copied] metadata JSON 序列化失败: {json_error}, 使用空对象")
                    metadata_json = '{}'
                
                if file_path in existing_map:
                    # 批量更新（通过 id）
                    file_id = existing_map[file_path]['id']
                    update_data.append((
                        file_id,
                        per_file_compressed_size,
                        is_compressed,
                        checksum,
                        backup_time,
                        chunk_number,
                        0,
                        metadata_json,
                        copy_time
                    ))
                elif use_file_path_update:
                    # 查询失败时，通过 file_path 更新（需要 file_path 作为参数）
                    update_data.append((
                        file_path,  # 使用 file_path 而不是 id
                        per_file_compressed_size,
                        is_compressed,
                        checksum,
                        backup_time,
                        chunk_number,
                        0,
                        metadata_json,
                        copy_time
                    ))
                else:
                    # 批量插入
                    insert_data.append((
                        backup_set_db_id,
                        file_path,
                        processed_file.get('file_name', Path(file_path).name),
                        processed_file.get('file_type', BackupFileType.FILE.value),
                        processed_file.get('file_size', 0),
                        per_file_compressed_size,
                        processed_file.get('file_permissions'),
                        datetime.fromtimestamp(file_stat.st_ctime) if file_stat else None,
                        datetime.fromtimestamp(file_stat.st_mtime) if file_stat else None,
                        datetime.fromtimestamp(file_stat.st_atime) if file_stat else None,
                        is_compressed,
                        checksum,
                        backup_time,
                        chunk_number,
                        0,
                        metadata_json,
                        copy_time
                    ))
            except Exception as e:
                failed_count += 1
                logger.info(
                    f"[mark_files_as_copied] Failed to prepare {processed_file.get('file_path', 'unknown')}: {e}"
                )
                continue
                    
        # 执行批量更新（分批处理，避免单次操作过多数据）
        if update_data:
            update_batch_size = 2000  # 减小批次大小以降低超时风险
            if len(update_data) > update_batch_size:
                logger.info(f"[mark_files_as_copied] 更新数据较多（{len(update_data)} 条），将分批更新（每批 {update_batch_size} 条）")
                for i in range(0, len(update_data), update_batch_size):
                    batch_update = update_data[i:i + update_batch_size]
                    try:
                        # 每次更新前重置超时设置（使用 SQL statement_timeout，确保每次更新独立计时）
                        await conn.execute("SET LOCAL statement_timeout = '300s'")
                        
                        # 添加超时设置，防止更新阻塞（300秒超时，增加超时时间）
                        import asyncio
                        # 根据是否使用 file_path 更新选择不同的 SQL
                        if use_file_path_update and len(batch_update) > 0 and isinstance(batch_update[0][0], str):
                            # 通过 file_path 更新（查询失败时的备用方案）
                            from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                            table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                            await asyncio.wait_for(
                                conn.executemany(
                                    f"""
                                    UPDATE {table_name}
                                    SET compressed_size = $2,
                                        compressed = $3,
                                        checksum = $4,
                                        backup_time = $5,
                                        chunk_number = $6,
                                        tape_block_start = $7,
                                        file_metadata = CAST($8 AS jsonb),
                                        is_copy_success = TRUE,
                                        copy_status_at = $9
                                    WHERE backup_set_id = $10 AND file_path = $1
                                    """,
                                    # 参数顺序必须按照 SQL 中占位符出现的顺序：$2, $3, $4, $5, $6, $7, $8, $9, $10, $1
                                    [(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], backup_set_db_id, row[0]) for row in batch_update]
                                ),
                                timeout=300.0  # 300秒超时
                            )
                        else:
                            # 通过 id 更新（正常情况）
                            from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                            table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                            await asyncio.wait_for(
                                conn.executemany(
                                    f"""
                                    UPDATE {table_name}
                                    SET compressed_size = $2,
                                        compressed = $3,
                                        checksum = $4,
                                        backup_time = $5,
                                        chunk_number = $6,
                                        tape_block_start = $7,
                                        file_metadata = CAST($8 AS jsonb),
                                        is_copy_success = TRUE,
                                        copy_status_at = $9
                                    WHERE id = $1
                                    """,
                                    # 参数顺序必须按照 SQL 中占位符出现的顺序：$2, $3, $4, $5, $6, $7, $8, $9, $1
                                    [(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[0]) for row in batch_update]
                                ),
                                timeout=300.0  # 300秒超时（增加超时时间）
                            )
                        success_count += len(batch_update)
                        logger.debug(f"[mark_files_as_copied] 已更新批次 {i // update_batch_size + 1}/{(len(update_data) + update_batch_size - 1) // update_batch_size}，{len(batch_update)} 个文件")
                    except asyncio.TimeoutError:
                        failed_count += len(batch_update)
                        logger.error(f"[mark_files_as_copied] 批次更新超时（批次 {i // update_batch_size + 1}，180秒），跳过该批次")
                    except Exception as batch_update_error:
                        failed_count += len(batch_update)
                        logger.error(f"[mark_files_as_copied] 批次更新失败（批次 {i // update_batch_size + 1}）: {batch_update_error}")
            else:
                try:
                    # 每次更新前重置超时设置（使用 SQL statement_timeout，确保每次更新独立计时）
                    await conn.execute("SET LOCAL statement_timeout = '180s'")
                    
                    # 添加超时设置，防止更新阻塞（180秒超时）
                    import asyncio
                    # 根据是否使用 file_path 更新选择不同的 SQL
                    if use_file_path_update and len(update_data) > 0 and isinstance(update_data[0][0], str):
                        # 通过 file_path 更新（查询失败时的备用方案）
                        from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                        table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                        await asyncio.wait_for(
                            conn.executemany(
                                f"""
                                UPDATE {table_name}
                                SET compressed_size = $2,
                                    compressed = $3,
                                    checksum = $4,
                                    backup_time = $5,
                                    chunk_number = $6,
                                    tape_block_start = $7,
                                    file_metadata = CAST($8 AS jsonb),
                                    is_copy_success = TRUE,
                                    copy_status_at = $9
                                WHERE backup_set_id = $10 AND file_path = $1
                                """,
                                [(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], backup_set_db_id) for row in update_data]
                            ),
                            timeout=300.0  # 300秒超时
                        )
                    else:
                        # 通过 id 更新（正常情况）
                        from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                        table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                        await asyncio.wait_for(
                            conn.executemany(
                                f"""
                                UPDATE {table_name}
                                SET compressed_size = $2,
                                    compressed = $3,
                                    checksum = $4,
                                    backup_time = $5,
                                    chunk_number = $6,
                                    tape_block_start = $7,
                                    file_metadata = CAST($8 AS jsonb),
                                    is_copy_success = TRUE,
                                    copy_status_at = $9
                                WHERE id = $1
                                """,
                                # 参数顺序必须按照 SQL 中占位符出现的顺序：$2, $3, $4, $5, $6, $7, $8, $9, $1
                                [(row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[0]) for row in update_data]
                            ),
                            timeout=300.0  # 300秒超时（增加超时时间）
                        )
                    success_count += len(update_data)
                    logger.debug(f"[mark_files_as_copied] 批量更新 {len(update_data)} 个文件")
                except asyncio.TimeoutError:
                    failed_count += len(update_data)
                    logger.error(f"[mark_files_as_copied] 批量更新超时（180秒），跳过")
                except Exception as update_error:
                    failed_count += len(update_data)
                    logger.error(f"[mark_files_as_copied] 批量更新失败: {update_error}")
        
        # 执行批量插入（分批处理，避免单次操作过多数据）
        if insert_data:
            insert_batch_size = 5000
            if len(insert_data) > insert_batch_size:
                logger.info(f"[mark_files_as_copied] 插入数据较多（{len(insert_data)} 条），将分批插入（每批 {insert_batch_size} 条）")
                for i in range(0, len(insert_data), insert_batch_size):
                    batch_insert = insert_data[i:i + insert_batch_size]
                    try:
                        # 每次插入前重置超时设置（使用 SQL statement_timeout，确保每次插入独立计时）
                        await conn.execute("SET LOCAL statement_timeout = '180s'")
                        
                        # 添加超时设置，防止插入阻塞（180秒超时）
                        import asyncio
                        await asyncio.wait_for(
                            conn.executemany(
                                """
                                INSERT INTO backup_files (
                                    backup_set_id, file_path, file_name, file_type, file_size,
                                    compressed_size, file_permissions, created_time, modified_time,
                                    accessed_time, compressed, checksum, backup_time, chunk_number,
                                    tape_block_start, file_metadata, is_copy_success, copy_status_at
                                ) VALUES (
                                    $1, $2, $3, $4::backupfiletype, $5,
                                    $6, $7, $8, $9,
                                    $10, $11, $12, $13, $14,
                                    $15, $16::jsonb, TRUE, $17
                                )
                                """,
                                batch_insert
                            ),
                            timeout=180.0  # 180秒超时
                        )
                        success_count += len(batch_insert)
                        logger.debug(f"[mark_files_as_copied] 已插入批次 {i // insert_batch_size + 1}/{(len(insert_data) + insert_batch_size - 1) // insert_batch_size}，{len(batch_insert)} 个文件")
                    except asyncio.TimeoutError:
                        failed_count += len(batch_insert)
                        logger.error(f"[mark_files_as_copied] 批次插入超时（批次 {i // insert_batch_size + 1}，180秒），跳过该批次")
                    except Exception as batch_insert_error:
                        failed_count += len(batch_insert)
                        logger.error(f"[mark_files_as_copied] 批次插入失败（批次 {i // insert_batch_size + 1}）: {batch_insert_error}")
            else:
                try:
                    # 每次插入前重置超时设置（使用 SQL statement_timeout，确保每次插入独立计时）
                    await conn.execute("SET LOCAL statement_timeout = '180s'")
                    
                    # 添加超时设置，防止插入阻塞（180秒超时）
                    import asyncio
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    # insert_data 中每条的第一个元素是 backup_set_id
                    sample_backup_set_id = insert_data[0][0]
                    table_name = await get_backup_files_table_by_set_id(conn, sample_backup_set_id)
                    await asyncio.wait_for(
                        conn.executemany(
                            f"""
                            INSERT INTO {table_name} (
                                backup_set_id, file_path, file_name, file_type, file_size,
                                compressed_size, file_permissions, created_time, modified_time,
                                accessed_time, compressed, checksum, backup_time, chunk_number,
                                tape_block_start, file_metadata, is_copy_success, copy_status_at
                            ) VALUES (
                                $1, $2, $3, $4::backupfiletype, $5,
                                $6, $7, $8, $9,
                                $10, $11, $12, $13, $14,
                                $15, $16::jsonb, TRUE, $17
                            )
                            """,
                            insert_data
                        ),
                        timeout=180.0  # 180秒超时
                    )
                    success_count += len(insert_data)
                    logger.debug(f"[mark_files_as_copied] 批量插入 {len(insert_data)} 个文件")
                except asyncio.TimeoutError:
                    failed_count += len(insert_data)
                    logger.error(f"[mark_files_as_copied] 批量插入超时（180秒），跳过")
                except Exception as insert_error:
                    failed_count += len(insert_data)
                    logger.error(f"[mark_files_as_copied] 批量插入失败: {insert_error}")
        
        if success_count > 0:
            logger.info(
                f"[mark_files_as_copied] 成功更新 {success_count} 个文件的 is_copy_success=TRUE（批量操作：更新 {len(update_data)} 个，插入 {len(insert_data)} 个）"
            )
        if failed_count > 0:
            logger.info(
                f"[mark_files_as_copied] {failed_count} 个文件更新失败，继续备份流程"
            )
        if success_count == 0 and failed_count == 0:
            logger.info(f"[mark_files_as_copied] 没有文件被更新（update_data={len(update_data)}, insert_data={len(insert_data)}）")
    
    async def update_scan_progress(
        self, 
        backup_task: BackupTask, 
        scanned_count: int, 
        valid_count: int, 
        operation_status: str = None
    ):
        """更新扫描进度到数据库
        
        Args:
            backup_task: 备份任务对象
            scanned_count: 已处理文件数（processed_files）
            valid_count: 压缩包数量（total_files），仅在压缩/写入阶段使用；扫描阶段传入的值会被忽略，使用 backup_task.total_files
            operation_status: 操作状态（如"[扫描文件中...]"、"[压缩文件中...]"等）
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            normalized_status = operation_status.strip() if isinstance(operation_status, str) else operation_status
            if normalized_status:
                # 注意：不再调用 _log_operation_stage_event，避免进度更新时重复记录关键阶段日志
                # 关键阶段日志应该只在阶段开始时调用一次（如压缩循环开始时）
                operation_status = normalized_status
            
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 重要：不再更新 total_files 字段（压缩包数量）
                    # total_files 字段由独立的后台扫描任务 _scan_for_progress_update 负责更新（总文件数）
                    # 压缩包数量存储在 result_summary.estimated_archive_count 中
                    
                    if operation_status:
                        # 先获取当前description、total_files、total_bytes和result_summary
                        row = await conn.fetchrow(
                            "SELECT description, total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        current_desc = row['description'] if row and row['description'] else ''
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = row['total_files'] if row and row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变
                        total_bytes_from_db = row['total_bytes'] if row and row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = row['result_summary'] if row and row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 移除所有操作状态标记（保留格式化状态）
                        # 移除所有 [操作状态...] 格式的标记，但保留 [格式化中]
                        if '[格式化中]' in current_desc:
                            # 保留格式化状态，移除其他操作状态
                            cleaned_desc = re.sub(r'\[(?!格式化中)[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        else:
                            # 移除所有操作状态标记
                            cleaned_desc = re.sub(r'\[[^\]]+\.\.\.\]', '', current_desc)
                            cleaned_desc = cleaned_desc.replace('  ', ' ').strip()
                            new_desc = cleaned_desc + ' ' + operation_status if cleaned_desc else operation_status
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', None)
                        if compressed_bytes is None:
                            compressed_bytes = 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', None)
                        if processed_bytes is None:
                            processed_bytes = 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        logger.debug(f"[update_scan_progress] 更新任务 {backup_task.id} 的进度：processed_files={scanned_count}, processed_bytes={processed_bytes}, compressed_bytes={compressed_bytes}")
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::jsonb,
                                description = $7,
                                updated_at = $8
                            WHERE id = $9
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 压缩包数量
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            new_desc,
                            datetime.now(),
                            backup_task.id
                        )
                        logger.debug(f"[update_scan_progress] 数据库更新完成：任务 {backup_task.id} 的 processed_files 和 processed_bytes 已更新")

                        # psycopg3 binary protocol 需要显式提交事务
                        # 使用连接池的 commit() 方法，而不是直接操作底层连接
                        try:
                            await conn.commit()
                            logger.info(f"[压缩进度] 任务 {backup_task.id} 扫描进度更新已提交: {new_desc}")
                        except Exception as commit_err:
                            logger.error(f"提交扫描进度更新事务失败: {commit_err}")
                            # 尝试回滚
                            try:
                                await conn.rollback()
                            except Exception as rollback_err:
                                logger.error(f"回滚扫描进度更新事务失败: {rollback_err}")

                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
                    else:
                        # 没有操作状态，只更新进度和字节数
                        # 从数据库读取当前的 total_files、total_bytes 和 result_summary，保持不变
                        current_row = await conn.fetchrow(
                            "SELECT total_files, total_bytes, result_summary FROM backup_tasks WHERE id = $1",
                            backup_task.id
                        )
                        # 从数据库读取当前的 total_files 值（总文件数），保持不变
                        total_files_value = current_row['total_files'] if current_row and current_row['total_files'] else 0
                        # 从数据库读取当前的 total_bytes 值（总字节数），保持不变（不更新）
                        total_bytes_from_db = current_row['total_bytes'] if current_row and current_row['total_bytes'] else 0
                        # 保持数据库中的 result_summary 不变（由后台扫描任务更新）
                        result_summary = current_row['result_summary'] if current_row and current_row['result_summary'] else {}
                        if isinstance(result_summary, str):
                            try:
                                result_summary = json.loads(result_summary)
                            except json.JSONDecodeError:
                                result_summary = {}
                        elif not isinstance(result_summary, dict):
                            result_summary = {}
                        
                        # 获取compressed_bytes和processed_bytes用于更新
                        compressed_bytes = getattr(backup_task, 'compressed_bytes', None)
                        if compressed_bytes is None:
                            compressed_bytes = 0
                        processed_bytes = getattr(backup_task, 'processed_bytes', None)
                        if processed_bytes is None:
                            processed_bytes = 0
                        
                        # 重要：不从 backup_task 对象读取 total_bytes 和 total_bytes_actual
                        # 这些字段由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        # 压缩流程只更新 processed_files 和 processed_bytes，不更新总文件数和总字节数
                        
                        # 只更新 result_summary 中的 estimated_archive_count（如果备份任务对象中有）
                        if hasattr(backup_task, 'result_summary') and backup_task.result_summary:
                            if isinstance(backup_task.result_summary, dict):
                                if 'estimated_archive_count' in backup_task.result_summary:
                                    result_summary['estimated_archive_count'] = backup_task.result_summary['estimated_archive_count']
                        
                        # 将result_summary转换为JSON字符串
                        result_summary_json = json.dumps(result_summary) if result_summary else None
                        
                        await conn.execute(
                            """
                            UPDATE backup_tasks
                            SET progress_percent = $1,
                                processed_files = $2,
                                total_files = $3,
                                processed_bytes = $4,
                                compressed_bytes = $5,
                                result_summary = $6::jsonb,
                                updated_at = $7
                            WHERE id = $8
                            """,
                            backup_task.progress_percent,
                            scanned_count,  # processed_files: 已处理文件数
                            total_files_value,  # total_files: 总文件数（从数据库读取，保持不变）
                            processed_bytes,  # processed_bytes: 已处理字节数
                            compressed_bytes,  # compressed_bytes: 压缩后字节数
                            result_summary_json,
                            datetime.now(),
                            backup_task.id
                        )

                        # psycopg3 binary protocol 需要显式提交事务
                        try:
                            await conn.commit()
                            logger.debug(f"[压缩进度] 任务 {backup_task.id} 进度更新已提交（无操作状态）")
                        except Exception as commit_err:
                            logger.error(f"提交进度更新事务失败: {commit_err}")
                            try:
                                await conn.rollback()
                            except:
                                pass
                        # 注意：total_bytes 字段不更新，由后台扫描任务负责更新
            else:
                logger.info(f"不支持的数据库类型（当前仅支持 openGauss），跳过更新扫描进度")
        except Exception as e:
            logger.info(f"更新扫描进度失败（忽略继续）: {str(e)}")
    
    async def update_task_status(self, backup_task: BackupTask, status: BackupTaskStatus):
        """更新任务状态
        
        Args:
            backup_task: 备份任务对象
            status: 任务状态
        """
        try:
            backup_task.status = status
            
            # 根据状态更新 started_at 和 completed_at
            current_time = datetime.now()
            update_fields = ['status', 'updated_at']
            update_values = [status.value, current_time]
            
            if status == BackupTaskStatus.RUNNING:
                # 运行状态：更新 started_at
                update_fields.append('started_at')
                update_values.append(current_time)
                
                # 同时更新 source_paths 和 tape_id（如果有的话），以便任务卡片正确显示
                if hasattr(backup_task, 'source_paths') and backup_task.source_paths:
                    update_fields.append('source_paths')
                    update_values.append(json.dumps(backup_task.source_paths) if isinstance(backup_task.source_paths, list) else backup_task.source_paths)
                
                if hasattr(backup_task, 'tape_id') and backup_task.tape_id:
                    update_fields.append('tape_id')
                    update_values.append(backup_task.tape_id)
            elif status in (BackupTaskStatus.COMPLETED, BackupTaskStatus.FAILED, BackupTaskStatus.CANCELLED):
                # 完成/失败/取消状态：更新 completed_at
                update_fields.append('completed_at')
                update_values.append(current_time)
            
            # 使用原生 openGauss SQL（仅支持 openGauss）
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_redis
            
            if is_redis():
                # Redis 版本
                from backup.redis_backup_db import update_task_status_redis
                await update_task_status_redis(backup_task, status)
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field in update_fields:
                        if field == 'status':
                            # 确保状态值正确：如果是枚举，使用.value；如果是字符串，直接使用
                            status_value = update_values[update_fields.index(field)]
                            if hasattr(status_value, 'value'):
                                status_value = status_value.value
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                            params.append(status_value)
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::jsonb")
                            params.append(update_values[update_fields.index(field)])
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                            params.append(update_values[update_fields.index(field)])
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    # 添加调试日志
                    logger.info(f"[更新任务状态] 任务ID: {backup_task.id}, 状态: {status.value if hasattr(status, 'value') else status}")
                    logger.debug(f"[更新任务状态] UPDATE SQL: SET {', '.join(set_clauses)} WHERE id = ${param_index}")
                    logger.debug(f"[更新任务状态] 参数: {params}")
                    
                    # psycopg3 binary protocol 需要显式提交事务
                    result = await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
                    logger.info(f"[openGauss] 更新任务状态成功: task_id={backup_task.id}, status={status.value}, 影响行数: {result}")

                    # 显式提交事务
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    try:
                        await actual_conn.commit()
                        logger.debug(f"任务 {backup_task.id} 状态更新已提交到数据库")
                    except Exception as commit_err:
                        logger.info(f"提交任务状态更新事务失败（可能已自动提交）: {commit_err}")
                        # 如果不在事务中，commit() 可能会失败，尝试回滚
                        try:
                            await actual_conn.rollback()
                        except:
                            pass

                    # 验证更新是否成功
                    verify_row = await conn.fetchrow(
                        "SELECT id, status, is_template FROM backup_tasks WHERE id = $1",
                        backup_task.id
                    )
                    if verify_row:
                        logger.info(f"[更新任务状态] 验证: 任务ID={verify_row['id']}, 状态={verify_row['status']}, is_template={verify_row['is_template']}")
                    else:
                        logger.info(f"[更新任务状态] 验证失败: 找不到任务ID={backup_task.id}")
            else:
                # SQLite 版本
                from backup.sqlite_backup_db import update_task_status_sqlite
                await update_task_status_sqlite(backup_task, status)
        except Exception as e:
            logger.error(f"更新任务状态失败: {str(e)}")
    
    async def update_task_stage_async(self, backup_task: BackupTask, stage_code: str, description: str = None):
        """更新任务的操作阶段（异步方法）

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            description: 可选的阶段描述，如果提供则同时更新description字段
        """
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                logger.info("无效的任务对象，无法更新阶段")
                return

            task_id = backup_task.id

            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection

            current_time = datetime.now()

            if is_opengauss():
                import asyncio
                max_retries = 3
                retry_count = 0
                update_success = False

                while retry_count < max_retries and not update_success:
                    try:
                        async with get_opengauss_connection() as conn:
                            # 开始新事务
                            await conn.execute("BEGIN")

                            if description:
                                # 同时更新operation_stage和description
                                await conn.execute("""
                                    UPDATE backup_tasks
                                    SET operation_stage = $1,
                                        description = $2,
                                        updated_at = $3
                                    WHERE id = $4
                                """, stage_code, description, current_time, task_id)
                            else:
                                # 只更新operation_stage
                                await conn.execute("""
                                    UPDATE backup_tasks
                                    SET operation_stage = $1,
                                        updated_at = $2
                                    WHERE id = $3
                                """, stage_code, current_time, task_id)

                            # psycopg3 binary protocol 需要显式提交事务
                            await conn.commit()

                            # 验证事务提交状态
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            transaction_status = actual_conn.info.transaction_status

                            if transaction_status == 0:  # IDLE: 事务成功提交
                                update_success = True
                                logger.debug(f"任务 {task_id} 阶段更新事务提交成功: {stage_code}")
                            else:
                                # 事务状态异常
                                logger.warning(f"任务 {task_id} 阶段更新事务状态异常: {transaction_status}，重试 {retry_count + 1}/{max_retries}")
                                retry_count += 1
                                if retry_count < max_retries:
                                    await asyncio.sleep(0.2 * retry_count)  # 短暂等待后重试

                    except Exception as update_error:
                        retry_count += 1
                        if retry_count >= max_retries:
                            logger.error(f"任务 {task_id} 阶段更新失败，达到最大重试次数 {max_retries}: {str(update_error)}")
                            raise
                        else:
                            logger.warning(f"任务 {task_id} 阶段更新失败，重试 {retry_count}/{max_retries}: {str(update_error)}")
                            await asyncio.sleep(0.3 * retry_count)

                if not update_success:
                    logger.error(f"任务 {task_id} 阶段更新最终失败: {stage_code}")
                    raise Exception(f"阶段更新失败: {stage_code}")
            else:
                logger.info("[update_task_stage_async] 当前仅支持 openGauss，跳过数据库更新")

            logger.debug(f"任务 {task_id} 阶段更新为: {stage_code}" + (f", 描述: {description}" if description else ""))

        except Exception as e:
            logger.error(f"更新任务阶段失败: {str(e)}")

    async def update_task_stage_with_description(self, backup_task: BackupTask, stage_code: str, description: str):
        """更新任务阶段和描述的便捷方法

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            description: 阶段描述
        """
        await self.update_task_stage_async(backup_task, stage_code, description)

    async def get_compressed_files_count(self, backup_set_db_id: int) -> int:
        """查询已压缩文件数（聚合所有进程的进度）
        
        注意：只统计真正压缩完成的文件（chunk_number IS NOT NULL），
        不包括预取时标记为已入队的文件（is_copy_success = TRUE 但 chunk_number IS NULL）。
        
        Args:
            backup_set_db_id: 备份集数据库ID
            
        Returns:
            已压缩文件数（is_copy_success = TRUE 且 chunk_number IS NOT NULL 的文件数）
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection

            if is_opengauss():
                # openGauss 版本：只统计真正压缩完成的文件（chunk_number IS NOT NULL）
                async with get_opengauss_connection() as conn:
                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                    row = await conn.fetchrow(
                        f"""
                        SELECT COUNT(*)::BIGINT as count
                        FROM {table_name}
                        WHERE backup_set_id = $1::INTEGER
                          AND (is_copy_success = TRUE)
                          AND chunk_number IS NOT NULL
                          AND file_type = 'file'::backupfiletype
                        """,
                        backup_set_db_id
                    )
                    return row['count'] if row else 0
            else:
                logger.info("[get_compressed_files_count] 当前仅支持 openGauss，返回 0")
                return 0
        except Exception as e:
            logger.error(f"查询已压缩文件数失败: {str(e)}", exc_info=True)
            return 0

    def update_task_stage(self, backup_task: BackupTask, stage_code: str, main_loop=None, description: str = None):
        """更新任务的操作阶段（同步方法，在线程中调用时需要使用主事件循环）

        Args:
            backup_task: 备份任务对象
            stage_code: 阶段代码（scan/compress/copy/finalize）
            main_loop: 主事件循环（如果在线程中调用，必须提供）
            description: 可选的阶段描述，如果提供则同时更新description字段
        """
        try:
            if not backup_task or not getattr(backup_task, 'id', None):
                logger.info("无效的任务对象，无法更新阶段")
                return

            import asyncio

            # 如果提供了主事件循环，使用它（从线程中调用）
            if main_loop:
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        self.update_task_stage_async(backup_task, stage_code, description),
                        main_loop
                    )
                    # 等待完成（设置超时避免阻塞）
                    future.result(timeout=5.0)
                except Exception as e:
                    logger.info(f"通过主事件循环更新任务阶段失败: {str(e)}")
                return

            # 否则，尝试在当前事件循环中执行
            try:
                loop = asyncio.get_running_loop()
                # 如果事件循环正在运行，创建任务
                loop.create_task(self.update_task_stage_async(backup_task, stage_code, description))
            except RuntimeError:
                # 没有运行的事件循环，创建新的
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(self.update_task_stage_async(backup_task, stage_code, description))
                finally:
                    loop.close()

        except Exception as e:
            logger.error(f"更新任务阶段失败: {str(e)}")
    
    async def update_task_fields(self, backup_task: BackupTask, **fields):
        """更新任务的特定字段
        
        Args:
            backup_task: 备份任务对象
            **fields: 要更新的字段（键值对）
        """
        try:
            if not fields:
                return
            
            # 更新对象属性
            for field, value in fields.items():
                if hasattr(backup_task, field):
                    setattr(backup_task, field, value)
            
            # 使用原生 openGauss SQL（仅支持 openGauss）
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 动态构建UPDATE语句
                    set_clauses = []
                    params = []
                    param_index = 1
                    
                    for field, value in fields.items():
                        if field == 'status':
                            set_clauses.append(f"status = ${param_index}::backuptaskstatus")
                        elif field == 'source_paths':
                            set_clauses.append(f"source_paths = ${param_index}::jsonb")
                        else:
                            set_clauses.append(f"{field} = ${param_index}")
                        
                        # 处理 JSON 字段
                        if field == 'source_paths' and isinstance(value, list):
                            params.append(json.dumps(value))
                        else:
                            params.append(value)
                        param_index += 1
                    
                    params.append(backup_task.id)
                    
                    await conn.execute(
                        f"""
                        UPDATE backup_tasks
                        SET {', '.join(set_clauses)}, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ${param_index}
                        """,
                        *params
                    )
            else:
                logger.info("[update_task_fields] 当前仅支持 openGauss，跳过数据库更新")
        except Exception as e:
            logger.error(f"更新任务字段失败: {str(e)}")
    
    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务状态字典，如果不存在则返回None
        """
        try:
            # 使用原生 openGauss SQL
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
            
            if is_redis():
                # Redis 版本
                from backup.redis_backup_db import get_task_status_redis
                return await get_task_status_redis(task_id)
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT id, status, progress_percent, processed_files, total_files,
                               processed_bytes, total_bytes, compressed_bytes, description,
                               source_paths, tape_device, tape_id, result_summary, started_at, completed_at
                        FROM backup_tasks
                        WHERE id = $1
                        """,
                        task_id
                    )
                    
                    if row:
                        source_paths = None
                        if row['source_paths']:
                            try:
                                if isinstance(row['source_paths'], str):
                                    source_paths = json.loads(row['source_paths'])
                                else:
                                    source_paths = row['source_paths']
                            except:
                                source_paths = None
                        
                        # 计算压缩率
                        compression_ratio = 0.0
                        if row['processed_bytes'] and row['processed_bytes'] > 0 and row['compressed_bytes']:
                            compression_ratio = float(row['compressed_bytes']) / float(row['processed_bytes'])
                        
                        # 解析result_summary获取预计的压缩包总数
                        estimated_archive_count = None
                        total_scanned_bytes = None
                        if row['result_summary']:
                            try:
                                result_summary_dict = None
                                if isinstance(row['result_summary'], str):
                                    result_summary_dict = json.loads(row['result_summary'])
                                elif isinstance(row['result_summary'], dict):
                                    result_summary_dict = row['result_summary']
                                
                                if isinstance(result_summary_dict, dict):
                                    estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                                    total_scanned_bytes = result_summary_dict.get('total_scanned_bytes')
                            except Exception:
                                pass
                        
                        return {
                            'task_id': task_id,
                            'status': row['status'],
                            'progress_percent': row['progress_percent'] or 0.0,
                            'processed_files': row['processed_files'] or 0,
                            'total_files': row['total_files'] or 0,  # 总文件数（由后台扫描任务更新）
                            'total_bytes': row['total_bytes'] or 0,  # 总字节数（由后台扫描任务更新）
                            'processed_bytes': row['processed_bytes'] or 0,
                            'compressed_bytes': row['compressed_bytes'] or 0,
                            'compression_ratio': compression_ratio,
                            'estimated_archive_count': estimated_archive_count,  # 压缩包数量（从 result_summary.estimated_archive_count 读取）
                            'description': row['description'] or '',
                            'source_paths': source_paths,
                            'tape_device': row['tape_device'],
                            'tape_id': row['tape_id'],
                            'started_at': row['started_at'],
                            'completed_at': row['completed_at']
                        }
            else:
                # SQLite 版本
                from backup.sqlite_backup_db import get_task_status_sqlite
                result = await get_task_status_sqlite(task_id)
                if result:
                    return {
                        'task_id': result['id'],
                        'status': result['status'],
                        'progress_percent': result['progress_percent'],
                        'processed_files': result['processed_files'],
                        'total_files': result['total_files'],
                        'total_bytes': result['total_bytes'],
                        'processed_bytes': result['processed_bytes'],
                        'compressed_bytes': result['compressed_bytes'],
                        'compression_ratio': result['compression_ratio'],
                        'estimated_archive_count': result['estimated_archive_count'],
                        'description': result['description'],
                        'source_paths': result['source_paths'],
                        'tape_device': result['tape_device'],
                        'tape_id': result['tape_id'],
                        'started_at': result['started_at'],
                        'completed_at': result['completed_at']
                    }
            
            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None
    
    async def get_total_files_from_db(self, task_id: int) -> int:
        """从数据库读取总文件数（由后台扫描任务更新）
        
        Args:
            task_id: 任务ID
            
        Returns:
            总文件数，如果不存在则返回0
        """
        try:
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
            
            if is_redis():
                # Redis 版本
                from config.redis_db import get_redis_client
                from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
                redis = await get_redis_client()
                task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
                task_data = await redis.hgetall(task_key)
                if task_data:
                    total_files_str = task_data.get('total_files', '0')
                    try:
                        return int(total_files_str) if total_files_str else 0
                    except (ValueError, TypeError):
                        return 0
                return 0
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        "SELECT total_files FROM backup_tasks WHERE id = $1",
                        task_id
                    )
                    if row:
                        return row['total_files'] or 0
            elif is_sqlite():
                # SQLite 版本：使用原生 SQL
                async with get_sqlite_connection() as conn:
                    cursor = await conn.execute("SELECT total_files FROM backup_tasks WHERE id = ?", (task_id,))
                    row = await cursor.fetchone()
                    return row[0] or 0 if row else 0
            return 0
        except Exception as e:
            logger.debug(f"读取总文件数失败（忽略继续）: {str(e)}")
            return 0
    
    async def update_scan_progress_only(self, backup_task: BackupTask, total_files: int, total_bytes: int):
        """仅更新扫描进度（总文件数和总字节数），不更新已处理文件数
        
        Args:
            backup_task: 备份任务对象
            total_files: 总文件数
            total_bytes: 总字节数
        """
        try:
            if not backup_task or not backup_task.id:
                return
            
            # 更新备份任务对象的统计信息
            backup_task.total_files = total_files  # total_files: 总文件数
            backup_task.total_bytes = total_bytes  # total_bytes: 总字节数
            
            # 更新数据库
            from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection
            
            if is_redis():
                # Redis 版本
                from backup.redis_backup_db import update_scan_progress_only_redis
                await update_scan_progress_only_redis(backup_task, total_files, total_bytes)
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 获取当前的 result_summary
                    row = await conn.fetchrow(
                        "SELECT result_summary FROM backup_tasks WHERE id = $1",
                        backup_task.id
                    )
                    result_summary = {}
                    if row and row['result_summary']:
                        try:
                            if isinstance(row['result_summary'], str):
                                result_summary = json.loads(row['result_summary'])
                            elif isinstance(row['result_summary'], dict):
                                result_summary = row['result_summary']
                        except Exception:
                            result_summary = {}
                    
                    # 更新 result_summary 中的总文件数和总字节数（作为备份存储）
                    result_summary['total_scanned_files'] = total_files
                    result_summary['total_scanned_bytes'] = total_bytes
                    
                    # 更新数据库：使用正确的字段存储总文件数和总字节数
                    # 使用 NUMERIC 类型处理大数值（避免 int64 溢出）
                    # 将 total_bytes 转换为字符串，然后使用 NUMERIC 类型
                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET total_files = $1,
                            total_bytes = $2::NUMERIC,
                            result_summary = $3::jsonb,
                            updated_at = $4
                        WHERE id = $5
                        """,
                        total_files,  # total_files: 总文件数
                        str(total_bytes),   # total_bytes: 总字节数（转换为字符串，使用 NUMERIC 类型）
                        json.dumps(result_summary),
                        datetime.now(),
                        backup_task.id
                    )
                    
                    # psycopg3 binary protocol 需要显式提交事务
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    try:
                        await actual_conn.commit()
                        logger.debug(f"任务 {backup_task.id} 扫描进度更新已提交到数据库: total_files={total_files}, total_bytes={total_bytes}")
                    except Exception as commit_err:
                        logger.info(f"提交扫描进度更新事务失败（可能已自动提交）: {commit_err}")
                        try:
                            await actual_conn.rollback()
                        except:
                            pass
            elif is_sqlite():
                # SQLite 版本
                from backup.sqlite_backup_db import update_scan_progress_only_sqlite
                await update_scan_progress_only_sqlite(backup_task, total_files, total_bytes)
        except Exception as e:
            logger.info(f"更新扫描进度失败（忽略继续）: {str(e)}")

