#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简洁扫描模块 - 完全按照 test_scan_direct_write.py 的方法扫描和写数据库
Simple Scanner Module - Scan and write to database using the same method as test_scan_direct_write.py

功能：
1. 扫描目录（使用 os.scandir + FileScanner）
2. 直接批量写入 openGauss 数据库（与测试程序完全一致）
3. 数据库表名通过内存获取（backup_task.backup_files_table）
4. 单线程同步模式，扫描和写入在同一循环中
"""

import asyncio
import logging
import os
import time
import threading
import queue as queue_module
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timezone

from models.backup import BackupTask, BackupSet
from backup.utils import format_bytes
from backup.file_scanner import FileScanner
from utils.scheduler.db_utils import get_opengauss_connection, is_opengauss
from utils.network_path import validate_network_path, is_unc_path
from config.settings import get_settings

logger = logging.getLogger(__name__)


from contextlib import asynccontextmanager

@asynccontextmanager
async def _conditional_db_connection(use_memory_mode):
    """条件性数据库连接：内存模式返回None，否则打开openGauss连接"""
    if use_memory_mode:
        yield None, None
    else:
        async with get_opengauss_connection() as conn:
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            yield conn, actual_conn


class SimpleScanner:
    """简洁扫描器 - 完全按照 test_scan_direct_write.py 的方法"""
    
    def __init__(self, backup_db):
        """初始化简洁扫描器
        
        Args:
            backup_db: 数据库操作对象
        """
        self.backup_db = backup_db
        self.settings = get_settings()
        self.file_scanner = FileScanner()
    
    async def scan_and_write(
        self,
        backup_task: BackupTask,
        source_paths: List[str],
        exclude_patterns: List[str],
        backup_set: BackupSet,
        restart: bool = False,
        in_memory_store=None,
    ):
        """
        简洁扫描和写入 - 完全按照 test_scan_direct_write.py 的方法

        Args:
            backup_task: 备份任务对象
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_set: 备份集对象
            restart: 是否重新扫描（清理旧数据）
            in_memory_store: 可选的 InMemoryFileStore（纯内存扫描模式，跳过数据库写入）
        """
        use_memory_mode = in_memory_store is not None
        write_verb = "已缓存" if use_memory_mode else "已写入"
        backup_set_db_id = getattr(backup_set, "id", None)
        logger.info(
            f"[简洁扫描] backup_task_id={getattr(backup_task, 'id', 'N/A')}, "
            f"backup_set_id={backup_set_db_id}, source_paths={source_paths}, exclude_patterns={exclude_patterns}, "
            f"memory_mode={use_memory_mode}"
        )

        # 初始化/清理状态：同时在内存和数据库中设置 scan_status = 'running'
        try:
            if backup_task and backup_task.id:
                backup_task.scan_status = "running"
                # 内存模式也需要更新数据库 scan_status，其他组件可能查数据库
                await self.backup_db.update_scan_status(backup_task.id, "running")
            if not use_memory_mode and restart and backup_set_db_id:
                # 清理原有 backup_files 记录，重新扫描
                await self.backup_db.clear_backup_files_for_set(backup_set_db_id)
        except Exception as e:
            logger.warning(f"[简洁扫描] 初始化扫描状态失败（忽略继续）: {e}")
        
        logger.info("[简洁扫描] 开始扫描文件（使用简洁扫描模式，单线程同步）")
        
        # 处理源路径为空的情况
        if not source_paths:
            logger.warning("[简洁扫描] source_paths 为空列表，没有文件要扫描")
            if backup_task:
                backup_task.total_files = 0
                backup_task.total_bytes = 0
            return
        
        # 统计信息（与测试程序完全一致）
        stats: Dict[str, Any] = {
            "total_scanned": 0,          # 扫描到的文件数
            "total_scanned_bytes": 0,     # 扫描到的总字节数
            "total_written": 0,          # 成功写入的文件数
            "total_bytes": 0,            # 成功写入的总字节数
            "total_written_bytes": 0,    # 成功写入的总字节数（用于日志展示，等于 total_bytes）
            "total_failed": 0,           # 写入失败的文件数
            "total_failed_bytes": 0,      # 写入失败的总字节数
            "excluded_count": 0,          # 被排除的文件数
            "excluded_dirs": 0,          # 被排除的目录数
            "excluded_bytes": 0,         # 被排除的总字节数
            "error_count": 0,            # 文件错误数
            "error_dirs": 0,             # 目录错误数
            "dirs_scanned": 0,           # 扫描到的目录数
            "dirs_skipped": 0,            # 跳过的目录数
            "symlinks_skipped": 0,       # 跳过的符号链接数
            "start_time": time.time(),
        }
        
        # 从内存获取分表名（backup_task.backup_files_table）
        # 内存模式不需要表名（压缩后才写入数据库）
        table_name = None
        if not use_memory_mode:
            if backup_task:
                table_name = getattr(backup_task, "backup_files_table", None)
                if table_name and isinstance(table_name, str) and table_name.startswith("backup_files_"):
                    logger.debug(f"[简洁扫描] 从内存获取分表名: {table_name}")
                else:
                    # 如果内存中没有，回退到查询数据库（仅一次）
                    if backup_set_db_id:
                        try:
                            from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                            async with get_opengauss_connection() as conn:
                                table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                                # 同时更新内存中的 backup_task，供后续使用
                                if backup_task and table_name:
                                    backup_task.backup_files_table = table_name
                                logger.debug(f"[简洁扫描] 从数据库获取分表名: {table_name}")
                        except Exception as e:
                            logger.warning(f"[简洁扫描] 获取分表名失败: {e}")

            if not table_name:
                logger.error("[简洁扫描] 无法获取分表名，扫描无法继续")
                return
        
        # 批次大小（与测试程序一致）
        batch_size = getattr(self.settings, "SCAN_UPDATE_INTERVAL", 10000) or 10000
        
        # === 阶段1: UNC/网络路径处理（必须在异步上下文中完成） ===
        expanded_paths = []
        for idx, source_path_str in enumerate(source_paths):
            logger.info(
                f"[简洁扫描] 扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}"
            )
            actual_path = source_path_str
            if is_unc_path(source_path_str):
                logger.info(f"[简洁扫描] 检测到 UNC 路径，尝试挂载 SMB 共享...")
                validation = await validate_network_path(source_path_str)
                if validation.get('valid') and validation.get('expanded_paths'):
                    actual_path = validation['expanded_paths'][0]
                    logger.info(f"[简洁扫描] UNC 路径已映射到本地: {source_path_str} -> {actual_path}")
                    expanded_paths.append(actual_path)
                else:
                    logger.warning(f"[简洁扫描] UNC 路径验证失败: {validation.get('message')}")
                    continue
            else:
                expanded_paths.append(actual_path)

        if not expanded_paths:
            logger.warning("[简洁扫描] 没有有效的源路径")
            return

        # === 阶段2: 启动同步扫描线程（os.scandir 在线程中运行，不阻塞事件循环） ===
        batch_queue: queue_module.Queue = queue_module.Queue()
        stop_event = threading.Event()

        scan_thread = threading.Thread(
            target=self._sync_collect_files,
            args=(expanded_paths, exclude_patterns, batch_size, batch_queue, stop_event),
            daemon=True,
            name="simple-scanner",
        )
        scan_thread.start()
        logger.info("[简洁扫描] 文件扫描线程已启动（不阻塞事件循环）")

        # 定义批次写入辅助函数（闭包延迟绑定 conn/actual_conn）
        async def _flush_batch(batch, batch_num):
            """写入一个批次的文件记录"""
            if use_memory_mode:
                return await in_memory_store.add_files(batch)
            else:
                return await self._write_batch_to_db(
                    conn, actual_conn, batch, backup_set_db_id, table_name, batch_num
                )

        # === 阶段3: 异步读取队列 + 写入批次（事件循环保持响应） ===
        loop = asyncio.get_running_loop()
        batch_number = 0

        # 打开数据库连接（仅在数据库模式下；内存模式使用空连接）
        async with _conditional_db_connection(use_memory_mode) as (conn, actual_conn):
            # === 异步队列读取 + 批次写入（事件循环保持响应） ===
            try:
                while True:
                    # 从队列读取（run_in_executor 不阻塞事件循环）
                    try:
                        item = await loop.run_in_executor(
                            None, batch_queue.get, True, 1.0
                        )
                    except queue_module.Empty:
                        if not scan_thread.is_alive() and batch_queue.empty():
                            break
                        await asyncio.sleep(0)  # 让出事件循环
                        continue

                    if item[0] == 'done':
                        # 扫描线程完成，更新最终统计
                        final_thread_stats = item[1]
                        stats['total_scanned'] = final_thread_stats.get('total_scanned', stats['total_scanned'])
                        stats['total_scanned_bytes'] = final_thread_stats.get('total_scanned_bytes', stats['total_scanned_bytes'])
                        stats['excluded_count'] = final_thread_stats.get('excluded_count', stats['excluded_count'])
                        stats['excluded_dirs'] = final_thread_stats.get('excluded_dirs', stats['excluded_dirs'])
                        stats['excluded_bytes'] = final_thread_stats.get('excluded_bytes', stats['excluded_bytes'])
                        stats['error_count'] = final_thread_stats.get('error_count', stats['error_count'])
                        stats['error_dirs'] = final_thread_stats.get('error_dirs', stats['error_dirs'])
                        stats['dirs_scanned'] = final_thread_stats.get('dirs_scanned', stats['dirs_scanned'])
                        stats['dirs_skipped'] = final_thread_stats.get('dirs_skipped', stats['dirs_skipped'])
                        stats['symlinks_skipped'] = final_thread_stats.get('symlinks_skipped', stats['symlinks_skipped'])
                        break

                    if item[0] == 'batch':
                        _, file_list, thread_stats = item

                        # 同步线程统计
                        stats['total_scanned'] = thread_stats.get('total_scanned', stats['total_scanned'])
                        stats['total_scanned_bytes'] = thread_stats.get('total_scanned_bytes', stats['total_scanned_bytes'])
                        stats['excluded_count'] = thread_stats.get('excluded_count', stats['excluded_count'])
                        stats['excluded_dirs'] = thread_stats.get('excluded_dirs', stats['excluded_dirs'])
                        stats['excluded_bytes'] = thread_stats.get('excluded_bytes', stats['excluded_bytes'])
                        stats['error_count'] = thread_stats.get('error_count', stats['error_count'])
                        stats['error_dirs'] = thread_stats.get('error_dirs', stats['error_dirs'])
                        stats['dirs_scanned'] = thread_stats.get('dirs_scanned', stats['dirs_scanned'])
                        stats['dirs_skipped'] = thread_stats.get('dirs_skipped', stats['dirs_skipped'])
                        stats['symlinks_skipped'] = thread_stats.get('symlinks_skipped', stats['symlinks_skipped'])

                        # 写入/缓存批次
                        written_count = await _flush_batch(file_list, batch_number)
                        stats['total_written'] += written_count
                        stats['total_failed'] += (len(file_list) - written_count)
                        batch_bytes = sum(f.get('size', 0) or 0 for f in file_list[:written_count])
                        stats['total_bytes'] += batch_bytes
                        stats['total_written_bytes'] = stats['total_bytes']
                        failed_bytes = sum(f.get('size', 0) or 0 for f in file_list[written_count:])
                        stats['total_failed_bytes'] += failed_bytes
                        batch_number += 1

                        if backup_task:
                            backup_task.total_files = stats["total_written"]
                            backup_task.total_bytes = stats["total_bytes"]

                        # 批次进度日志
                        elapsed = time.time() - stats['start_time']
                        files_per_sec = stats['total_written'] / elapsed if elapsed > 0 else 0
                        logger.info(
                            f"[简洁扫描] 批次 {batch_number}: {write_verb} {stats['total_written']:,} 个文件, "
                            f"总容量: {format_bytes(stats['total_bytes'])}, "
                            f"速度: {files_per_sec:.0f} 文件/秒, "
                            f"耗时: {elapsed:.1f}秒"
                        )

                        # 让出事件循环，让预取器和压缩器有机会运行
                        await asyncio.sleep(0)

            finally:
                # 通知扫描线程停止
                stop_event.set()
                scan_thread.join(timeout=5.0)

                # 排空队列中剩余的批次
                while not batch_queue.empty():
                    try:
                        item = batch_queue.get_nowait()
                        if item[0] == 'batch':
                            _, file_list, thread_stats = item
                            written_count = await _flush_batch(file_list, batch_number)
                            stats['total_written'] += written_count
                            stats['total_failed'] += (len(file_list) - written_count)
                            batch_bytes = sum(f.get('size', 0) or 0 for f in file_list[:written_count])
                            stats['total_bytes'] += batch_bytes
                            stats['total_written_bytes'] = stats['total_bytes']
                            batch_number += 1
                            if backup_task:
                                backup_task.total_files = stats["total_written"]
                                backup_task.total_bytes = stats["total_bytes"]
                        elif item[0] == 'done':
                            final_thread_stats = item[1]
                            stats['total_scanned'] = final_thread_stats.get('total_scanned', stats['total_scanned'])
                            stats['total_scanned_bytes'] = final_thread_stats.get('total_scanned_bytes', stats['total_scanned_bytes'])
                            stats['excluded_count'] = final_thread_stats.get('excluded_count', stats['excluded_count'])
                            stats['error_count'] = final_thread_stats.get('error_count', stats['error_count'])
                    except queue_module.Empty:
                        break
        
        # 扫描结束：更新内存状态，并做一次最终数据库同步
        if backup_task:
            backup_task.total_files = stats["total_written"]
            backup_task.total_bytes = stats["total_bytes"]
            # 内存模式和数据库模式都需要同步 total_files/total_bytes 到数据库
            try:
                if hasattr(self.backup_db, "update_scan_progress_only"):
                    await self.backup_db.update_scan_progress_only(
                        backup_task,
                        stats["total_written"],
                        stats["total_bytes"],
                    )
            except Exception as sync_err:
                logger.debug(
                    f"[简洁扫描] 扫描结束时同步扫描统计到数据库失败（忽略继续）: {sync_err}"
                )

            if not use_memory_mode:
                # 数据库模式：更新阶段描述
                try:
                    if hasattr(self.backup_db, "update_task_stage_with_description"):
                        summary_desc = (
                            f"[扫描完成] "
                            f"扫描 {stats['total_scanned']:,} 个文件 "
                            f"({format_bytes(stats['total_scanned_bytes'])}), "
                            f"写入 {stats['total_written']:,} 个, "
                            f"失败 {stats['total_failed']:,} 个, "
                            f"排除 {stats['excluded_count']:,} 个"
                        )
                        await self.backup_db.update_task_stage_with_description(
                            backup_task,
                            "scan",
                            summary_desc,
                        )
                except Exception as stage_err:
                    logger.debug(
                        f"[简洁扫描] 更新扫描阶段描述失败（忽略继续）: {stage_err}"
                    )
        
        elapsed = time.time() - stats["start_time"]
        files_per_sec = (
            stats["total_written"] / elapsed if elapsed > 0 else 0.0
        )
        
        # 扫描完成日志：换行输出，与其他日志有明显差异
        logger.info("=" * 80)
        logger.info("[简洁扫描] ========== 扫描完成 ==========")
        logger.info(f"  扫描到: {stats['total_scanned']:,} 个文件 ({format_bytes(stats['total_scanned_bytes'])})")
        logger.info(f"  成功{write_verb}: {stats['total_written']:,} 个文件 ({format_bytes(stats['total_written_bytes'])})")
        logger.info(f"  失败: {stats['total_failed']:,} 个文件 ({format_bytes(stats['total_failed_bytes'])})")
        logger.info(f"  排除: {stats['excluded_count']:,} 个文件")
        logger.info(f"  总耗时: {elapsed:.2f} 秒, {write_verb}速度: {files_per_sec:.0f} 文件/秒")
        logger.info("=" * 80)
        
        # 扫描全部结束后，设置 scan_status = 'completed'
        if backup_task and backup_task.id:
            try:
                backup_task.scan_status = "completed"
                if use_memory_mode:
                    await in_memory_store.set_scan_completed()
                    # 内存模式也需要更新数据库的 scan_status，backup_engine 判定任务完成时查数据库
                    await self.backup_db.update_scan_status(backup_task.id, "completed")
                else:
                    await self.backup_db.update_scan_status(backup_task.id, "completed")
            except Exception as e:
                logger.warning(f"[简洁扫描] 更新扫描状态为 completed 失败（忽略继续）: {e}")
    
    def _sync_collect_files(
        self,
        expanded_paths: List[str],
        exclude_patterns: List[str],
        batch_size: int,
        batch_queue: queue_module.Queue,
        stop_event: threading.Event,
    ) -> None:
        """纯同步文件扫描（在线程中运行，不阻塞事件循环）

        扫描源路径并将文件批次放入队列，供异步主循环消费。
        通过 stop_event 支持优雅退出。

        队列消息格式：
        - ('batch', file_list, thread_stats): 一个批次的文件 + 线程统计快照
        - ('done', final_stats): 扫描完成 + 最终统计
        """
        thread_stats = {
            "total_scanned": 0,
            "total_scanned_bytes": 0,
            "excluded_count": 0,
            "excluded_dirs": 0,
            "excluded_bytes": 0,
            "error_count": 0,
            "error_dirs": 0,
            "dirs_scanned": 0,
            "dirs_skipped": 0,
            "symlinks_skipped": 0,
        }

        current_batch = []
        start_time = time.time()
        last_progress_time = start_time
        progress_interval = 5.0
        last_log_count = 0
        log_interval_count = 10000

        def _put_batch():
            """将当前批次放入队列"""
            nonlocal current_batch
            if current_batch:
                batch_queue.put(('batch', list(current_batch), dict(thread_stats)))
                current_batch = []

        try:
            for idx, source_path_str in enumerate(expanded_paths):
                if stop_event.is_set():
                    break

                source_path = Path(source_path_str)
                if not source_path.exists():
                    logger.warning(f"[简洁扫描-线程] 路径不存在，跳过: {source_path_str}")
                    continue

                # 单个文件
                if source_path.is_file():
                    try:
                        if self.file_scanner.should_exclude_file(source_path_str, exclude_patterns):
                            thread_stats["excluded_count"] += 1
                            continue
                        # 用 os.stat 直接构造 file_info（避免 async get_file_info）
                        try:
                            stat_result = os.stat(source_path_str)
                            file_info = {
                                'path': source_path_str,
                                'name': os.path.basename(source_path_str),
                                'size': stat_result.st_size,
                                'modified_time': datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc),
                                'is_file': True,
                                'permissions': oct(stat_result.st_mode)[-3:],
                            }
                        except (OSError, PermissionError):
                            thread_stats["error_count"] += 1
                            continue

                        file_size = file_info.get('size', 0) or 0
                        thread_stats['total_scanned'] += 1
                        thread_stats['total_scanned_bytes'] += file_size
                        current_batch.append(file_info)

                        if len(current_batch) >= batch_size:
                            _put_batch()
                    except Exception as e:
                        logger.warning(f"[简洁扫描-线程] 处理文件失败: {source_path_str}, 错误: {str(e)}")
                        thread_stats["error_count"] += 1
                    continue

                # 目录
                if not source_path.is_dir():
                    continue

                if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                    thread_stats["excluded_dirs"] += 1
                    continue

                dirs_to_scan = [source_path]
                scanned_dirs = set()
                current_dir_count = 0
                current_dir_str = str(source_path.resolve())

                while dirs_to_scan:
                    if stop_event.is_set():
                        break

                    current_dir = dirs_to_scan.pop(0)
                    try:
                        current_dir_str = str(current_dir.resolve())
                    except Exception:
                        current_dir_str = str(current_dir)

                    if current_dir_str in scanned_dirs:
                        thread_stats['dirs_skipped'] += 1
                        continue
                    scanned_dirs.add(current_dir_str)
                    thread_stats['dirs_scanned'] += 1

                    if self.file_scanner.should_exclude_file(current_dir_str, exclude_patterns):
                        thread_stats['excluded_dirs'] += 1
                        continue

                    try:
                        with os.scandir(current_dir_str) as entries:
                            for entry in entries:
                                if stop_event.is_set():
                                    break
                                try:
                                    entry_path = Path(entry.path)
                                    entry_path_str = str(entry_path)

                                    if self.file_scanner.should_exclude_file(entry_path_str, exclude_patterns):
                                        thread_stats['excluded_count'] += 1
                                        if entry.is_file(follow_symlinks=False):
                                            try:
                                                thread_stats["excluded_bytes"] += entry.stat().st_size
                                            except Exception:
                                                pass
                                        continue

                                    if entry.is_dir(follow_symlinks=False):
                                        dirs_to_scan.append(entry_path)
                                        continue

                                    if entry.is_file(follow_symlinks=False):
                                        file_info = self.file_scanner.get_file_info_from_entry(entry)
                                        if file_info:
                                            file_size = file_info.get('size', 0) or 0
                                            thread_stats['total_scanned'] += 1
                                            thread_stats['total_scanned_bytes'] += file_size
                                            current_batch.append(file_info)
                                            current_dir_count += 1

                                            if len(current_batch) >= batch_size:
                                                _put_batch()

                                            # 定期进度日志
                                            current_time = time.time()
                                            if (thread_stats['total_scanned'] - last_log_count >= log_interval_count or
                                                    current_time - last_progress_time >= progress_interval):
                                                elapsed = current_time - start_time
                                                fps = thread_stats['total_scanned'] / elapsed if elapsed > 0 else 0
                                                logger.debug(
                                                    f"[简洁扫描-线程] 进度: 已扫描 {thread_stats['total_scanned']:,} 个文件, "
                                                    f"总容量: {format_bytes(thread_stats['total_scanned_bytes'])}, "
                                                    f"速度: {fps:.0f} 文件/秒, "
                                                    f"当前目录: {current_dir_str[:80]}... ({current_dir_count:,} 个文件), "
                                                    f"待扫描目录: {len(dirs_to_scan)}"
                                                )
                                                last_progress_time = current_time
                                                last_log_count = thread_stats['total_scanned']
                                        else:
                                            thread_stats['error_count'] += 1
                                        continue

                                    if entry.is_symlink():
                                        thread_stats['symlinks_skipped'] += 1
                                    else:
                                        thread_stats['error_count'] += 1

                                except (PermissionError, OSError, FileNotFoundError):
                                    thread_stats['error_count'] += 1
                                    continue
                                except Exception as e:
                                    logger.warning(
                                        f"[简洁扫描-线程] 处理条目失败: "
                                        f"{entry.path if hasattr(entry, 'path') else 'unknown'}, 错误: {str(e)}"
                                    )
                                    thread_stats['error_count'] += 1
                                    continue

                    except (PermissionError, OSError) as e:
                        logger.warning(f"[简洁扫描-线程] 无法访问目录: {current_dir_str}, 错误: {str(e)}")
                        thread_stats['error_dirs'] += 1
                        continue

            # 刷出剩余批次
            _put_batch()

        except Exception as e:
            logger.error(f"[简洁扫描-线程] 扫描线程异常: {e}", exc_info=True)
        finally:
            # 发送完成信号
            batch_queue.put(('done', dict(thread_stats)))
            elapsed = time.time() - start_time
            fps = thread_stats['total_scanned'] / elapsed if elapsed > 0 else 0
            logger.info(
                f"[简洁扫描-线程] 扫描线程完成: "
                f"扫描 {thread_stats['total_scanned']:,} 个文件 "
                f"({format_bytes(thread_stats['total_scanned_bytes'])}), "
                f"排除 {thread_stats['excluded_count']:,} 个, "
                f"耗时: {elapsed:.1f}秒, 速度: {fps:.0f} 文件/秒"
            )

    async def _write_batch_to_db(
        self,
        conn,
        actual_conn,
        file_info_batch: List[Dict],
        backup_set_id: int,
        table_name: str,
        batch_number: int
    ) -> int:
        """
        批量写入文件信息到数据库（完全使用测试程序的逻辑）
        
        注意：
        - openGauss 模式下需要显式提交事务
        - 异常时需要回滚，避免长事务锁表
        - 空列表直接返回，不执行 SQL
        
        Args:
            conn: 数据库连接
            actual_conn: 实际连接对象（用于 commit/rollback）
            file_info_batch: 文件信息批次
            backup_set_id: 备份集ID
            table_name: 表名
            batch_number: 批次编号
            
        Returns:
            成功写入的文件数
        """
        # 空列表直接返回，不执行 SQL（避免无意义的数据库操作）
        if not file_info_batch:
            return 0
        
        # 准备插入数据（完全使用测试程序的内联逻辑，避免方法调用开销）
        insert_data = []
        for file_info in file_info_batch:
            try:
                # 提取文件信息（与测试程序完全一致，内联实现）
                # 基本路径信息 - 来自文件扫描器
                file_path = file_info.get('path', '')
                # 使用 os.path.basename 替代 Path().name，性能更好
                file_name = file_info.get('name') or (os.path.basename(file_path) if file_path else '')
                
                # 目录路径：使用 os.path.dirname 替代 Path().parent，性能更好
                if file_path:
                    directory_path = os.path.dirname(file_path)
                    # 如果目录路径是根路径（如 "C:\" 或 "/"）或为空，设为 None
                    if not directory_path or directory_path == os.path.dirname(directory_path):
                        directory_path = None
                else:
                    directory_path = None
                
                # 显示名称（暂时与文件名相同）
                display_name = file_name
                
                # 文件类型 - 根据扫描器输出判断
                if file_info.get('is_file', True):
                    file_type = 'file'
                elif file_info.get('is_dir', False):
                    file_type = 'directory'
                elif file_info.get('is_symlink', False):
                    file_type = 'symlink'
                else:
                    file_type = 'file'
                
                # 文件大小 - 关键字段！直接从扫描器的size字段获取
                file_size = file_info.get('size', 0) or 0
                
                # 压缩大小（初始为None，压缩时更新）
                compressed_size = None
                
                # 文件权限 - 来自扫描器
                file_permissions = file_info.get('permissions')
                
                # 文件所有者和组（初始为None，Linux环境下可扩展）
                file_owner = None
                file_group = None
                
                # 时间戳处理 - 优先使用扫描器提供的modified_time
                modified_time = file_info.get('modified_time')
                if isinstance(modified_time, datetime):
                    modified_time = modified_time.replace(tzinfo=timezone.utc)
                else:
                    modified_time = datetime.now(timezone.utc)
                
                # 创建时间和访问时间（暂时使用修改时间作为默认值）
                created_time = modified_time
                accessed_time = modified_time
                
                # 磁带相关信息（初始为None，压缩时更新）
                tape_block_start = None
                tape_block_count = None
                compressed = False
                encrypted = False
                checksum = None
                error_message = file_info.get('error_message')
                is_copy_success = False
                copy_status_at = None
                
                # 备份时间
                backup_time = datetime.now(timezone.utc)
                
                # 其他字段
                chunk_number = None
                version = 1
                
                # 返回格式：按照openGauss INSERT语句的字段顺序（不包含created_at和updated_at，它们在SQL中使用NOW()）
                data_tuple = (
                    backup_set_id,          # backup_set_id
                    file_path,              # file_path
                    file_name,              # file_name
                    directory_path,         # directory_path
                    display_name,           # display_name
                    file_type,              # file_type
                    file_size,              # file_size
                    compressed_size,        # compressed_size
                    file_permissions,       # file_permissions
                    file_owner,             # file_owner
                    file_group,             # file_group
                    created_time,           # created_time
                    modified_time,          # modified_time
                    accessed_time,          # accessed_time
                    tape_block_start,       # tape_block_start
                    tape_block_count,       # tape_block_count
                    compressed,             # compressed
                    encrypted,              # encrypted
                    checksum,               # checksum
                    file_info.get('error_message'),  # error_message
                    is_copy_success,        # is_copy_success
                    copy_status_at,         # copy_status_at
                    backup_time,            # backup_time
                    chunk_number,          # chunk_number
                    version                 # version
                )
                insert_data.append(data_tuple)
            except Exception as e:
                logger.warning(
                    f"[简洁扫描] 准备文件数据失败: {file_info.get('path', 'unknown')[:200]}, 错误: {str(e)}"
                )
                continue
        
        # 空列表直接返回，不执行 SQL（避免无意义的数据库操作）
        if not insert_data:
            return 0
        
        # 批量插入数据库（完全使用测试程序的逻辑）
        try:
            write_start_time = time.time()
            rowcount = await conn.executemany(
                f"""
                INSERT INTO {table_name} (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, error_message, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, NOW(), NOW()
                )
                """,
                insert_data
            )
            write_duration = time.time() - write_start_time
            
            # 注意：psycopg3_compat.executemany 已经在内部调用了 commit()，不需要再次提交
            # 双重提交会导致额外的网络往返和性能开销
            
            if rowcount != len(insert_data):
                logger.warning(
                    f"[简洁扫描] 批次 {batch_number}: 部分文件未插入，期望={len(insert_data)}, 实际={rowcount}"
                )
            else:
                # 输出批次写入详情（仅在批次较大时，避免日志过多，与测试程序一致）
                if len(insert_data) >= 1000:
                    files_per_sec = len(insert_data) / write_duration if write_duration > 0 else 0
                    logger.info(
                        f"[简洁扫描] 批次 {batch_number}: 成功写入 {len(insert_data):,} 个文件, "
                        f"耗时: {write_duration:.2f}秒, "
                        f"速度: {files_per_sec:.0f} 文件/秒"
                    )
            
            return rowcount if rowcount else len(insert_data)
        
        except Exception as e:
            logger.error(f"[简洁扫描] 批次 {batch_number} 写入失败: {str(e)}", exc_info=True)
            # 异常时回滚事务，避免长事务锁表
            if hasattr(actual_conn, 'rollback'):
                try:
                    await actual_conn.rollback()
                except Exception:
                    pass
            return 0
    
    def _prepare_insert_data_for_opengauss(self, file_info: Dict, backup_set_id: int) -> tuple:
        """
        为openGauss准备插入数据（与测试程序完全一致）
        
        Args:
            file_info: 文件信息字典（来自 FileScanner）
            backup_set_id: 备份集ID
            
        Returns:
            插入数据元组
        """
        # 基本路径信息 - 来自文件扫描器
        file_path = file_info.get('path', '')
        # 使用 os.path.basename 替代 Path().name，性能更好
        file_name = file_info.get('name') or (os.path.basename(file_path) if file_path else '')
        
        # 目录路径：使用 os.path.dirname 替代 Path().parent，性能更好
        if file_path:
            directory_path = os.path.dirname(file_path)
            # 如果目录路径是根路径（如 "C:\" 或 "/"）或为空，设为 None
            if not directory_path or directory_path == os.path.dirname(directory_path):
                directory_path = None
        else:
            directory_path = None
        
        # 显示名称（暂时与文件名相同）
        display_name = file_name
        
        # 文件类型 - 根据扫描器输出判断
        if file_info.get('is_file', True):
            file_type = 'file'
        elif file_info.get('is_dir', False):
            file_type = 'directory'
        elif file_info.get('is_symlink', False):
            file_type = 'symlink'
        else:
            file_type = 'file'
        
        # 文件大小 - 关键字段！直接从扫描器的size字段获取
        file_size = file_info.get('size', 0) or 0
        
        # 压缩大小（初始为None，压缩时更新）
        compressed_size = None
        
        # 文件权限 - 来自扫描器
        file_permissions = file_info.get('permissions')
        
        # 文件所有者和组（初始为None，Linux环境下可扩展）
        file_owner = None
        file_group = None
        
        # 时间戳处理 - 优先使用扫描器提供的modified_time
        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)
        
        # 创建时间和访问时间（暂时使用修改时间作为默认值）
        created_time = modified_time
        accessed_time = modified_time
        
        # 磁带相关信息（初始为None，压缩时更新）
        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None
        
        # 备份时间
        backup_time = datetime.now(timezone.utc)
        
        # 其他字段
        chunk_number = None
        version = 1
        
        # 返回格式：按照openGauss INSERT语句的字段顺序（不包含created_at和updated_at，它们在SQL中使用NOW()）
        # 注意：file_metadata 和 tags 字段已从扫描阶段删除，压缩阶段会更新 file_metadata
        return (
            backup_set_id,     # backup_set_id
            file_path,         # file_path
            file_name,         # file_name
            directory_path,    # directory_path
            display_name,      # display_name
            file_type,         # file_type
            file_size,         # file_size
            compressed_size,   # compressed_size
            file_permissions,  # file_permissions
            file_owner,        # file_owner
            file_group,        # file_group
            created_time,      # created_time
            modified_time,     # modified_time
            accessed_time,      # accessed_time
            tape_block_start,  # tape_block_start
            tape_block_count,  # tape_block_count
            compressed,        # compressed
            encrypted,         # encrypted
            checksum,          # checksum
            is_copy_success,   # is_copy_success
            copy_status_at,    # copy_status_at
            backup_time,       # backup_time
            chunk_number,      # chunk_number
            version            # version
        )
