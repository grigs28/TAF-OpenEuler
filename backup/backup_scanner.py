#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
后台扫描任务模块
Background Scanner Module

独立的后台扫描任务，专门更新卡片中的总文件数和总字节数
"""

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone

from models.backup import BackupTask, BackupSet
from backup.utils import format_bytes
from config.settings import get_settings

logger = logging.getLogger(__name__)


class BackupScanner:
    """后台扫描任务类"""
    
    def __init__(self, file_scanner, backup_db):
        """初始化后台扫描任务
        
        Args:
            file_scanner: 文件扫描器对象
            backup_db: 数据库操作对象
        """
        self.file_scanner = file_scanner
        self.backup_db = backup_db
        self.settings = get_settings()
    
    async def scan_for_progress_update(
        self,
        backup_task: BackupTask,
        source_paths: List[str],
        exclude_patterns: List[str],
        backup_set: BackupSet,
        restart: bool = False,
        in_memory_store=None,
    ):
        """独立的后台扫描任务，专门更新卡片中的总文件数和总字节数
        
        这个任务：
        1. 独立扫描目录，统计文件数和字节数
        2. 定期（每100个文件）更新数据库中的 total_files 和 total_bytes
        3. 扫描完成后任务退出
        4. 不影响压缩流程
        5. 支持 KeyboardInterrupt 和 CancelledError，能够被正确取消
        
        Args:
            backup_task: 备份任务对象
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
        """
        # openGauss 模式下：根据 ENABLE_SIMPLE_SCAN 配置选择扫描方式
        # 优先级：1. 备份任务的 enable_simple_scan（备份策略级别） 2. 系统配置 ENABLE_SIMPLE_SCAN（默认 True）
        # 不再使用内存数据库/复杂队列，简化为：扫描 → 批量写入 backup_files → 更新任务状态
        try:
            from utils.scheduler.db_utils import is_opengauss
            if is_opengauss():
                # 优先从备份任务对象获取简洁扫描配置（备份策略级别）
                enable_simple_scan = getattr(backup_task, "enable_simple_scan", None)
                
                # 如果备份任务中没有配置，则使用系统配置
                if enable_simple_scan is None:
                    enable_simple_scan = getattr(self.settings, "ENABLE_SIMPLE_SCAN", True)
                    logger.debug(f"[后台扫描] 备份任务未配置 enable_simple_scan，使用系统配置 ENABLE_SIMPLE_SCAN={enable_simple_scan}")
                else:
                    logger.debug(f"[后台扫描] 使用备份任务配置 enable_simple_scan={enable_simple_scan}")
                
                if enable_simple_scan:
                    # 使用简洁扫描模块（完全按照 memory_db_writer.py 的方法）
                    logger.info(f"[后台扫描] 检测到 openGauss 模式，启用简洁扫描（enable_simple_scan=True）")
                    from backup.simple_scanner import SimpleScanner
                    simple_scanner = SimpleScanner(self.backup_db)
                    await simple_scanner.scan_and_write(
                        backup_task=backup_task,
                        source_paths=source_paths,
                        exclude_patterns=exclude_patterns,
                        backup_set=backup_set,
                        restart=restart,
                        in_memory_store=in_memory_store,
                    )
                    return
                else:
                    # 使用原扫描方式
                    logger.info(f"[后台扫描] 检测到 openGauss 模式，使用原扫描方式（enable_simple_scan=False）")
                    await self._scan_opengauss_direct_scandir(
                        backup_task=backup_task,
                        source_paths=source_paths,
                        exclude_patterns=exclude_patterns,
                        backup_set=backup_set,
                        restart=restart,
                    )
                    return
        except Exception as e:
            # 检测数据库类型失败时，回退到原有逻辑，避免影响其他数据库
            logger.warning(f"[后台扫描] 检测 openGauss 模式失败，回退到原有扫描逻辑: {e}")

        backup_set_db_id = getattr(backup_set, 'id', None)
        logger.info(
            f"[后台扫描] 获取 backup_set_db_id: {backup_set_db_id}, "
            f"backup_set.id={backup_set.id if hasattr(backup_set, 'id') else 'N/A'}, "
            f"backup_set.set_id={getattr(backup_set, 'set_id', 'N/A')}"
        )

        try:
            if backup_task and backup_task.id:
                await self.backup_db.update_scan_status(backup_task.id, 'running')
            if restart and backup_set_db_id:
                await self.backup_db.clear_backup_files_for_set(backup_set_db_id)
            
            # 记录关键阶段：扫描文件开始
            self.backup_db._log_operation_stage_event(backup_task, "[扫描文件中...]")
            # 更新operation_stage和description
            await self.backup_db.update_task_stage_with_description(
                backup_task,
                "scan",
                "[扫描文件中] 正在扫描源文件系统..."
            )
            
            logger.info("========== 后台扫描任务启动：专门更新卡片中的总文件数和总字节数 ==========")
            logger.info(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            logger.info(f"源路径列表: {source_paths}")
            logger.info(f"排除规则: {exclude_patterns}")
            
            # 确保 source_paths 不为 None
            if source_paths is None:
                logger.warning("后台扫描任务：source_paths 为 None，使用空列表")
                source_paths = []
            
            if not source_paths:
                logger.warning("后台扫描任务：source_paths 为空列表，没有文件要扫描")
                # 即使没有源路径，也要更新数据库（设置为0）
                await self.backup_db.update_scan_progress_only(backup_task, 0, 0)
                logger.info("========== 后台扫描任务完成：没有源路径 ==========")
                return
            
            # 使用流式扫描模式：一边扫描一边通过队列提交文件信息，由专门的后台worker写入 backup_files，并按间隔更新统计
            # 非流式分支仅用于兼容旧逻辑（只统计、不写入文件列表）
            use_streaming = True
            
            # 初始化统计变量
            total_files = 0
            total_bytes = 0
            # 使用全局settings统一读取配置（仅在开始时读取一次）
            settings = get_settings()
            # 从配置读取扫描进度更新间隔（.env 中可通过 SCAN_UPDATE_INTERVAL 覆盖）
            update_interval = settings.SCAN_UPDATE_INTERVAL
            # 进度日志时间间隔（秒），控制"后台扫描任务：已扫描 N 个文件..."的输出频率
            log_interval_seconds = getattr(settings, 'SCAN_LOG_INTERVAL_SECONDS', 60)
            # 统计用时间基准（用于计算扫描速度）
            scan_start_time = time.time()
            last_log_time = scan_start_time
            last_log_files = 0
            
            # 检查扫描方法配置（实时读取最新配置）
            scan_method = getattr(settings, 'SCAN_METHOD', 'default').lower()
            use_es_scanner = (scan_method == 'es')
            
            if use_es_scanner:
                # 使用ES扫描器（实时读取最新配置）
                es_scanner_initialized = False
                try:
                    from backup.es_scanner import ESScanner
                    es_exe_path = getattr(settings, 'ES_EXE_PATH', r'E:\app\TAF\ITDT\ES\es.exe')
                    es_scanner = ESScanner(es_exe_path=es_exe_path)
                    # 检查ES工具是否可用（在开始扫描前检查）
                    if not es_scanner._check_es_tool():
                        raise FileNotFoundError(f"ES工具不存在或不可用: {es_exe_path}")
                    es_scanner_initialized = True
                    logger.info(f"========== 使用ES扫描器进行后台扫描 ==========")
                    logger.info(f"ES工具路径: {es_exe_path}")
                    
                    # 使用批量数据库写入器提升写入性能
                    if backup_set_db_id:
                        # 检查数据库类型，Redis不使用SQLite内存数据库
                        use_memory_db = getattr(settings, 'USE_MEMORY_DB', True)

                        if use_memory_db:
                            sync_batch_size = getattr(settings, 'MEMORY_DB_SYNC_BATCH_SIZE', 3000)
                            sync_interval = getattr(settings, 'MEMORY_DB_SYNC_INTERVAL', 30)
                            max_memory_files = getattr(settings, 'MEMORY_DB_MAX_FILES', 5000000)
                            checkpoint_interval = getattr(settings, 'MEMORY_DB_CHECKPOINT_INTERVAL', 300)
                            checkpoint_retention_hours = getattr(settings, 'MEMORY_DB_CHECKPOINT_RETENTION_HOURS', 24)
                            enable_checkpoint = getattr(settings, 'USE_CHECKPOINT', False)

                            from backup.memory_db_writer import MemoryDBWriter
                            memory_writer = MemoryDBWriter(
                                backup_set_db_id=backup_set_db_id,
                                sync_batch_size=sync_batch_size,
                                sync_interval=sync_interval,
                                max_memory_files=max_memory_files,
                                checkpoint_interval=checkpoint_interval,
                                checkpoint_retention_hours=checkpoint_retention_hours,
                                enable_checkpoint=enable_checkpoint
                            )
                            await memory_writer.initialize()
                            logger.info(
                                f"内存数据库写入器已启动 (backup_set_db_id={backup_set_db_id}, "
                                f"sync_batch={sync_batch_size}, interval={sync_interval}s)"
                            )
                        else:
                            # 回退到批量写入器（不使用内存数据库时）（实时读取最新配置）
                            # 不使用内存数据库时，批次大小由 SCAN_UPDATE_INTERVAL 控制
                            batch_size = update_interval  # 使用扫描进度更新间隔作为批次大小
                            max_queue_size = getattr(settings, 'DB_QUEUE_MAX_SIZE', 50000)

                            from backup.backup_db import BatchDBWriter
                            batch_writer = BatchDBWriter(
                                backup_set_db_id=backup_set_db_id,
                                batch_size=batch_size,
                                max_queue_size=max_queue_size,
                                timeout=None  # 不使用超时检测
                            )
                            await batch_writer.start()  # 启动批量写入器（顺序执行模式，不使用队列）
                            
                            logger.info(f"批量写入器已启动（顺序执行模式）(batch_size={batch_size})")
                    
                    # 使用ES扫描器进行流式扫描
                    # ES扫描器直接顺序写内存数据库：ES扫描器传过来多少写多少，一批次全部写入内存数据库
                    # 批次数量使用.env的后台扫描进度更新间隔（文件数），ES扫描器传递过来的数据不丢弃
                    
                    async for file_batch in es_scanner.scan_files_streaming(
                        source_paths,
                        exclude_patterns,
                        backup_task,
                        log_context="[后台扫描-ES]"
                    ):
                        # ES扫描器返回的每个批次，直接全部写入内存数据库
                        if not file_batch:
                            continue
                        
                        # 统计当前批次
                        batch_size = len(file_batch)
                        for file_info in file_batch:
                            file_size = file_info.get('size', 0) or 0
                            total_files += 1
                            total_bytes += file_size
                        
                        # 直接写入内存数据库（不累积，不丢弃）
                        if backup_set_db_id:
                            try:
                                if use_memory_db and 'memory_writer' in locals():
                                    # 批量写入内存数据库（使用批量插入优化性能）
                                    logger.debug(f"[后台扫描-ES] 开始批量写入 {batch_size} 个文件到内存数据库（批次大小由SCAN_UPDATE_INTERVAL控制）...")
                                    try:
                                        await memory_writer.add_files_batch(file_batch)
                                        logger.debug(f"[后台扫描-ES] ✅ 已成功批量写入 {batch_size} 个文件到内存数据库")
                                    except Exception as batch_error:
                                        logger.error(f"[后台扫描-ES] ❌ 批量写入失败: {str(batch_error)}，尝试逐个添加（不丢弃数据）", exc_info=True)
                                        # 回退到逐个添加（容错处理，不丢弃数据）
                                        success_count = 0
                                        failed_count = 0
                                        failed_files = []
                                        for file_info in file_batch:
                                            try:
                                                await memory_writer.add_file(file_info)
                                                success_count += 1
                                            except Exception as file_error:
                                                failed_count += 1
                                                file_path = file_info.get('path', 'unknown')
                                                failed_files.append((file_path, str(file_error)))
                                                logger.warning(f"[后台扫描-ES] 添加文件到内存数据库失败: {file_path[:200]}, 错误: {str(file_error)}")
                                        
                                        if success_count > 0:
                                            logger.debug(f"[后台扫描-ES] ✅ 已成功提交 {success_count} 个文件到内存数据库（逐个添加）")
                                        if failed_count > 0:
                                            logger.warning(f"[后台扫描-ES] ⚠️ {failed_count} 个文件添加失败，但数据未丢弃，将在下次同步时重试")
                                            # 记录失败的文件信息，但不中断扫描
                                            for file_path, error_msg in failed_files[:5]:  # 只记录前5个
                                                logger.debug(f"[后台扫描-ES] 失败文件: {file_path[:200]}, 错误: {error_msg}")
                                else:
                                    # 不使用内存数据库时，直接同步写入数据库
                                    try:
                                        logger.debug(f"[后台扫描-ES] 开始同步写入批次到数据库: {batch_size} 个文件")
                                        await batch_writer.write_batch_sync(file_batch)
                                        logger.debug(f"[后台扫描-ES] ✅ 批次已全部写入数据库: {batch_size} 个文件")
                                    except Exception as batch_error:
                                        # 批量写入失败，记录错误但不中断扫描
                                        logger.error(f"[后台扫描-ES] ❌ 批量写入失败: {str(batch_error)}", exc_info=True)
                                        # 继续处理下一批次，不中断扫描
                            except Exception as e:
                                # 其他错误，记录但不中断扫描，不丢弃数据
                                logger.error(f"[后台扫描-ES] ❌ 批量写入文件失败: {e}，数据未丢弃，将在下次同步时重试", exc_info=True)
                                # 继续处理下一批次，不中断扫描
                        
                        # 更新数据库中的统计字段
                        logger.debug(f"[后台扫描-ES] 更新扫描进度: 已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                        await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                        # 再根据时间间隔决定是否输出一条统计日志
                        now = time.time()
                        elapsed_since_last_log = now - last_log_time
                        if elapsed_since_last_log >= log_interval_seconds:
                            elapsed_total = max(now - scan_start_time, 0.001)
                            elapsed_window = max(elapsed_since_last_log, 0.001)
                            files_total_rate = total_files / elapsed_total
                            files_window = total_files - last_log_files
                            files_window_rate = files_window / elapsed_window if files_window > 0 else 0.0
                            logger.info(
                                f"后台扫描任务（ES）：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                                f"平均速度 {files_total_rate:.1f} 个文件/秒，"
                                f"最近 {files_window} 个文件用时 {elapsed_window:.1f} 秒，速度 {files_window_rate:.1f} 个文件/秒"
                            )
                            last_log_time = now
                            last_log_files = total_files
                    
                    # ES扫描完成，不需要处理剩余文件（每个批次都已直接写入）
                    
                    # ES扫描完成，继续后续处理（与原有逻辑相同）
                    # 等待写入器完成所有文件写入
                    if backup_set_db_id:
                        try:
                            if use_memory_db and 'memory_writer' in locals():
                                # 停止内存数据库写入器（会自动完成最终同步）
                                stats = memory_writer.get_stats()
                                sync_status = await memory_writer.get_sync_status()

                                logger.info(f"内存数据库统计: 处理 {stats['total_files']} 个文件，"
                                           f"已同步 {stats['synced_files']} 个文件，同步进度 {stats['sync_progress']:.1f}%")

                                logger.info(f"同步状态: 总计 {sync_status['total_files']}, "
                                           f"已同步 {sync_status['synced_files']}, "
                                           f"待同步 {sync_status['pending_files']}, "
                                           f"错误 {sync_status['error_files']}")

                                await memory_writer.stop()

                            elif 'batch_writer' in locals():
                                # 停止批量写入器
                                stats = batch_writer.get_stats()
                                logger.info(f"批量写入统计: 处理 {stats['total_files']} 个文件，"
                                           f"完成 {stats['batch_count']} 个批次，耗时 {stats['total_time']:.1f}s")

                                await batch_writer.stop()

                        except Exception as e:
                            logger.error(f"后台扫描任务：停止写入器时出错: {str(e)}", exc_info=True)
                    
                    # 扫描完成后做最后一次进度更新
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    # 结束时再输出一次总平均速度
                    end_time = time.time()
                    elapsed_total = max(end_time - scan_start_time, 0.001)
                    files_total_rate = total_files / elapsed_total
                    logger.info(
                        f"后台扫描任务（ES）完成，共扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                        f"平均速度 {files_total_rate:.1f} 个文件/秒"
                    )
                    
                    # 记录关键阶段：扫描完成
                    self.backup_db._log_operation_stage_event(backup_task, f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                    # 更新operation_stage和description
                    await self.backup_db.update_task_stage_with_description(
                        backup_task,
                        "scan",
                        f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}"
                    )
                    
                    if backup_task and backup_task.id:
                        await self.backup_db.update_scan_status(backup_task.id, 'completed')
                    return
                    
                except FileNotFoundError as e:
                    # ES工具不存在或不可用，回退到默认扫描
                    logger.warning(f"ES扫描器初始化失败: {str(e)}，回退到默认扫描方法")
                    use_es_scanner = False
                    # 如果已经初始化了数据库写入器，需要清理
                    if es_scanner_initialized and backup_set_db_id:
                        try:
                            if 'memory_writer' in locals():
                                await memory_writer.stop()
                            elif 'batch_writer' in locals():
                                await batch_writer.stop()
                        except Exception as cleanup_err:
                            logger.warning(f"清理ES扫描器的数据库写入器时出错: {cleanup_err}")
                except Exception as e:
                    # ES扫描出错，回退到默认扫描
                    logger.error(f"ES扫描出错: {str(e)}，回退到默认扫描方法", exc_info=True)
                    use_es_scanner = False
                    # 如果已经初始化了数据库写入器，需要清理
                    if es_scanner_initialized and backup_set_db_id:
                        try:
                            if 'memory_writer' in locals():
                                await memory_writer.stop()
                            elif 'batch_writer' in locals():
                                await batch_writer.stop()
                        except Exception as cleanup_err:
                            logger.warning(f"清理ES扫描器的数据库写入器时出错: {cleanup_err}")
            
            if use_streaming and not use_es_scanner:
                # 重置统计变量（如果ES扫描失败回退到这里）
                # 使用全局settings获取最新配置
                settings = get_settings()
                total_files = 0
                total_bytes = 0
                update_interval = settings.SCAN_UPDATE_INTERVAL
                log_interval_seconds = getattr(settings, 'SCAN_LOG_INTERVAL_SECONDS', 60)
                scan_start_time = time.time()
                last_log_time = scan_start_time
                last_log_files = 0

                # 使用批量数据库写入器提升写入性能

                if backup_set_db_id:
                    # 检查是否使用内存数据库写入器
                    use_memory_db = getattr(settings, 'USE_MEMORY_DB', True)

                    if use_memory_db:
                        sync_batch_size = getattr(settings, 'MEMORY_DB_SYNC_BATCH_SIZE', 5000)
                        sync_interval = getattr(settings, 'MEMORY_DB_SYNC_INTERVAL', 30)
                        max_memory_files = getattr(settings, 'MEMORY_DB_MAX_FILES', 100000)
                        checkpoint_interval = getattr(settings, 'MEMORY_DB_CHECKPOINT_INTERVAL', 300)
                        checkpoint_retention_hours = getattr(settings, 'MEMORY_DB_CHECKPOINT_RETENTION_HOURS', 24)
                        enable_checkpoint = getattr(settings, 'USE_CHECKPOINT', False)

                        from backup.memory_db_writer import MemoryDBWriter
                        memory_writer = MemoryDBWriter(
                            backup_set_db_id=backup_set_db_id,
                            sync_batch_size=sync_batch_size,
                            sync_interval=sync_interval,
                            max_memory_files=max_memory_files,
                            checkpoint_interval=checkpoint_interval,
                            checkpoint_retention_hours=checkpoint_retention_hours,
                            enable_checkpoint=enable_checkpoint
                        )
                        await memory_writer.initialize()
                        logger.info(f"内存数据库写入器已启动 (sync_batch={sync_batch_size}, interval={sync_interval}s)")
                    else:
                        # 回退到批量写入器
                        batch_size = getattr(settings, 'DB_BATCH_SIZE', 1000)
                        max_queue_size = getattr(settings, 'DB_QUEUE_MAX_SIZE', 5000)

                        from backup.backup_db import BatchDBWriter
                        batch_writer = BatchDBWriter(
                            backup_set_db_id=backup_set_db_id,
                            batch_size=batch_size,
                            max_queue_size=max_queue_size
                        )
                        await batch_writer.start()
                        logger.info(f"批量写入器已启动（顺序执行模式）(batch_size={batch_size})")
                
                # 文件扫描顺序写入内存数据库：使用SCAN_UPDATE_INTERVAL作为批次大小
                # 扫描器返回的每个批次大小 = SCAN_UPDATE_INTERVAL，直接写入内存数据库，无需再次累积
                file_buffer = []  # 用于累积不满一个批次的文件
                
                # 初始化 source_path_str，用于日志输出（如果源路径列表不为空，使用第一个路径）
                source_path_str = source_paths[0] if source_paths else "N/A"
                current_scanning_dir = source_path_str  # 当前正在扫描的目录
                
                async for file_batch in self.file_scanner.scan_source_files_streaming(
                    source_paths,
                    exclude_patterns,
                    backup_task,
                    batch_size=update_interval,  # 使用SCAN_UPDATE_INTERVAL作为批次大小
                    log_context="[后台扫描]"
                ):
                    # 从文件批次中提取当前正在扫描的目录（用于日志显示）
                    if file_batch:
                        # 从第一个文件的路径中提取当前目录
                        first_file_path = file_batch[0].get('path', '')
                        if first_file_path:
                            try:
                                from pathlib import Path
                                file_path_obj = Path(first_file_path)
                                # 获取文件的父目录
                                parent_dir = str(file_path_obj.parent)
                                # 如果父目录在源路径列表中，使用父目录；否则使用源路径
                                if any(parent_dir.startswith(src) or src.startswith(parent_dir) for src in source_paths):
                                    current_scanning_dir = parent_dir
                                else:
                                    # 找到包含该文件的源路径
                                    for src_path in source_paths:
                                        if first_file_path.startswith(src_path):
                                            current_scanning_dir = src_path
                                            break
                            except Exception:
                                # 如果提取失败，保持使用源路径
                                pass
                    
                    # 扫描器返回的批次大小 = SCAN_UPDATE_INTERVAL，直接写入内存数据库
                    # 重要：只在写入成功后才统计，避免统计的文件数 > 实际写入的文件数
                    if file_batch:
                        if backup_set_db_id:
                            try:
                                if use_memory_db and 'memory_writer' in locals():
                                    # 直接批量写入内存数据库（批次大小由SCAN_UPDATE_INTERVAL控制）
                                    try:
                                        await memory_writer.add_files_batch(file_batch)
                                        # 写入成功后才统计
                                        for file_info in file_batch:
                                            file_size = file_info.get('size', 0) or 0
                                            total_files += 1
                                            total_bytes += file_size
                                        logger.debug(f"[后台扫描] ✅ 已成功批量写入 {len(file_batch)} 个文件到内存数据库（批次大小由SCAN_UPDATE_INTERVAL控制）")
                                    except Exception as batch_error:
                                        logger.error(f"[后台扫描] ❌ 批量提交失败: {str(batch_error)}，尝试逐个添加", exc_info=True)
                                        # 回退到逐个添加（容错处理）
                                        success_count = 0
                                        failed_count = 0
                                        for buffered_file in file_batch:
                                            try:
                                                await memory_writer.add_file(buffered_file)
                                                # 写入成功后才统计
                                                file_size = buffered_file.get('size', 0) or 0
                                                total_files += 1
                                                total_bytes += file_size
                                                success_count += 1
                                            except Exception as file_error:
                                                failed_count += 1
                                                file_path = buffered_file.get('path', 'unknown')
                                                logger.warning(f"[后台扫描] 添加文件到内存数据库失败: {file_path[:200]}, 错误: {str(file_error)}")
                                        if failed_count > 0:
                                            logger.warning(f"[后台扫描] ⚠️ {failed_count} 个文件添加失败，已跳过（未计入统计）")
                                else:
                                    # 顺序执行模式：直接同步写入数据库，等待全部写入完成后再继续扫描
                                    try:
                                        logger.debug(f"[后台扫描] 开始同步写入批次到数据库: {len(file_batch)} 个文件")
                                        await batch_writer.write_batch_sync(file_batch)
                                        logger.debug(f"[后台扫描] ✅ 批次已全部写入数据库: {len(file_batch)} 个文件")
                                    except Exception as batch_error:
                                        # 批量写入失败，记录错误但不中断扫描
                                        logger.error(f"[后台扫描] ❌ 批量写入失败: {str(batch_error)}", exc_info=True)
                                        # 继续处理下一批次，不中断扫描
                            except Exception as e:
                                # 其他错误，记录但保留文件在缓冲区，继续重试
                                logger.error(f"批量写入文件失败: {e}，保留 {len(file_batch)} 个文件在缓冲区继续重试", exc_info=True)
                                # 如果写入失败，将文件添加到file_buffer以便后续重试
                                file_buffer.extend(file_batch)
                                continue
                        
                        # 所有文件都成功添加后，更新统计信息
                            if backup_set_db_id:
                                try:
                                    if use_memory_db and 'memory_writer' in locals():
                                        # 批量写入内存数据库（使用批量插入优化性能）
                                        try:
                                            await memory_writer.add_files_batch(file_buffer)
                                            # 写入成功后才统计
                                            for buffered_file in file_buffer:
                                                file_size = buffered_file.get('size', 0) or 0
                                                total_files += 1
                                                total_bytes += file_size
                                        except Exception as batch_error:
                                            logger.error(f"[后台扫描] ❌ 批量提交失败: {str(batch_error)}，尝试逐个添加", exc_info=True)
                                            # 回退到逐个添加（容错处理）
                                            success_count = 0
                                            failed_count = 0
                                            for buffered_file in file_buffer:
                                                try:
                                                    await memory_writer.add_file(buffered_file)
                                                    # 写入成功后才统计
                                                    file_size = buffered_file.get('size', 0) or 0
                                                    total_files += 1
                                                    total_bytes += file_size
                                                    success_count += 1
                                                except Exception as file_error:
                                                    failed_count += 1
                                                    file_path = buffered_file.get('path', 'unknown')
                                                    logger.warning(f"[后台扫描] 添加文件到内存数据库失败: {file_path[:200]}, 错误: {str(file_error)}")
                                            if failed_count > 0:
                                                logger.warning(f"[后台扫描] ⚠️ {failed_count} 个文件添加失败，已跳过（未计入统计）")
                                    else:
                                        # 顺序执行模式：直接同步写入数据库，等待全部写入完成后再继续扫描
                                        if file_buffer:
                                            try:
                                                logger.debug(f"[后台扫描] 开始同步写入剩余文件到数据库: {len(file_buffer)} 个文件")
                                                await batch_writer.write_batch_sync(file_buffer)
                                                logger.debug(f"[后台扫描] ✅ 剩余文件已全部写入数据库: {len(file_buffer)} 个文件")
                                            except Exception as batch_error:
                                                # 批量写入失败，记录错误但不中断扫描
                                                logger.error(f"[后台扫描] ❌ 批量写入剩余文件失败: {str(batch_error)}", exc_info=True)
                                                # 继续处理，不中断扫描
                                except Exception as e:
                                    # 其他错误，记录但保留文件在缓冲区，继续重试
                                    logger.error(f"批量写入文件失败: {e}，保留 {len(file_buffer)} 个文件在缓冲区继续重试", exc_info=True)
                                    # 不清空缓冲区，下次循环会重试
                                    continue
                            
                            # 所有文件都成功添加，清空缓冲区
                            file_buffer.clear()
                            
                            # 更新数据库中的统计字段
                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                            # 再根据时间间隔决定是否输出一条统计日志
                            now = time.time()
                            elapsed_since_last_log = now - last_log_time
                            if elapsed_since_last_log >= log_interval_seconds:
                                elapsed_total = max(now - scan_start_time, 0.001)
                                elapsed_window = max(elapsed_since_last_log, 0.001)
                                files_total_rate = total_files / elapsed_total
                                files_window = total_files - last_log_files
                                files_window_rate = files_window / elapsed_window if files_window > 0 else 0.0
                                
                                # 获取扫描方式标识
                                scan_type_info = ""
                                try:
                                    from utils.scheduler.db_utils import is_opengauss
                                    scan_method = getattr(self.settings, 'SCAN_METHOD', 'default').lower()
                                    use_multithread = getattr(self.settings, 'USE_SCAN_MULTITHREAD', True)
                                    scan_threads = getattr(self.settings, 'SCAN_THREADS', 4)
                                    
                                    use_concurrent = (
                                        is_opengauss() and 
                                        scan_method == 'default' and 
                                        use_multithread and 
                                        scan_threads > 1
                                    )
                                    use_sequential = (
                                        is_opengauss() and 
                                        scan_method == 'default' and 
                                        not use_multithread
                                    )
                                    
                                    if use_concurrent:
                                        scan_type_info = f"[多线程扫描-{scan_threads}线程] "
                                    elif use_sequential:
                                        scan_type_info = "[顺序扫描] "
                                except Exception:
                                    pass
                                
                                logger.info(
                                    f"{scan_type_info}后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                                    f"平均速度 {files_total_rate:.1f} 个文件/秒，"
                                    f"最近 {files_window} 个文件用时 {elapsed_window:.1f} 秒，✅速度 {files_window_rate:.1f} 个文件/秒，"
                                    f"当前目录: {current_scanning_dir}"
                                )
                                last_log_time = now
                                last_log_files = total_files
                
                # 扫描完成，提交缓冲区中剩余的文件（如果有）
                if file_buffer and backup_set_db_id:
                    try:
                        if use_memory_db and 'memory_writer' in locals():
                            # 批量写入内存数据库（使用批量插入优化性能）
                            try:
                                await memory_writer.add_files_batch(file_buffer)
                            except Exception as batch_error:
                                logger.error(f"[后台扫描] ❌ 批量提交剩余文件失败: {str(batch_error)}，尝试逐个添加", exc_info=True)
                                # 回退到逐个添加（容错处理）
                                for buffered_file in file_buffer:
                                    try:
                                        await memory_writer.add_file(buffered_file)
                                    except Exception as file_error:
                                        file_path = buffered_file.get('path', 'unknown')
                                        logger.warning(f"[后台扫描] 添加剩余文件到内存数据库失败: {file_path[:200]}, 错误: {str(file_error)}")
                        else:
                            # 批量写入批量写入器
                            # 顺序执行模式：直接同步写入数据库，等待全部写入完成后再继续扫描
                            try:
                                logger.debug(f"[后台扫描] 开始同步写入剩余文件到数据库: {len(file_buffer)} 个文件")
                                await batch_writer.write_batch_sync(file_buffer)
                                logger.debug(f"[后台扫描] ✅ 剩余文件已全部写入数据库: {len(file_buffer)} 个文件")
                            except Exception as batch_error:
                                # 批量写入失败，记录错误但不中断扫描
                                logger.error(f"[后台扫描] ❌ 批量写入剩余文件失败: {str(batch_error)}", exc_info=True)
                                # 继续处理，不中断扫描
                    except Exception as e:
                        logger.error(f"批量写入剩余文件失败: {e}，将保留文件继续重试", exc_info=True)
                        # 不清空缓冲区，让下次循环继续重试
                        # file_buffer.clear()  # 注释掉，不丢弃文件
                    else:
                        # 只有成功时才清空缓冲区
                        file_buffer.clear()

                # 等待写入器完成所有文件写入
                if backup_set_db_id:
                    try:
                        if use_memory_db and 'memory_writer' in locals():
                            # 停止内存数据库写入器（会自动完成最终同步）
                            stats = memory_writer.get_stats()
                            sync_status = await memory_writer.get_sync_status()

                            logger.info(f"内存数据库统计: 处理 {stats['total_files']} 个文件，"
                                       f"已同步 {stats['synced_files']} 个文件，同步进度 {stats['sync_progress']:.1f}%")

                            logger.info(f"同步状态: 总计 {sync_status['total_files']}, "
                                       f"已同步 {sync_status['synced_files']}, "
                                       f"待同步 {sync_status['pending_files']}, "
                                       f"错误 {sync_status['error_files']}")

                            await memory_writer.stop()

                        elif 'batch_writer' in locals():
                            # 停止批量写入器
                            stats = batch_writer.get_stats()
                            logger.info(f"批量写入统计: 处理 {stats['total_files']} 个文件，"
                                       f"完成 {stats['batch_count']} 个批次，耗时 {stats['total_time']:.1f}s")

                            await batch_writer.stop()

                    except Exception as e:
                        logger.error(f"后台扫描任务：停止写入器时出错: {str(e)}", exc_info=True)
                
                # 扫描完成后做最后一次进度更新
                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                # 结束时再输出一次总平均速度
                end_time = time.time()
                elapsed_total = max(end_time - scan_start_time, 0.001)
                files_total_rate = total_files / elapsed_total
                logger.info(
                    f"后台扫描任务完成，共扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}，"
                    f"平均速度 {files_total_rate:.1f} 个文件/秒"
                )
                
                # 记录关键阶段：扫描完成
                self.backup_db._log_operation_stage_event(backup_task, f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                # 更新operation_stage和description
                await self.backup_db.update_task_stage_with_description(
                    backup_task,
                    "scan",
                    f"[扫描完成] 共 {total_files} 个文件，总大小 {format_bytes(total_bytes)}"
                )
                
                if backup_task and backup_task.id:
                    await self.backup_db.update_scan_status(backup_task.id, 'completed')
                return
            
            total_files = 0  # 总文件数
            total_bytes = 0  # 总字节数
            # 使用全局settings获取最新配置
            settings = get_settings()
            # 从配置读取扫描进度更新间隔（.env 中可通过 SCAN_UPDATE_INTERVAL 覆盖）
            update_interval = settings.SCAN_UPDATE_INTERVAL
            
            # 处理网络路径（UNC路径）
            from utils.network_path import is_unc_path, normalize_unc_path
            
            for source_path_str in source_paths:
                logger.info(f"后台扫描任务：扫描源路径 {source_path_str}")
                
                # 处理 UNC 网络路径
                if is_unc_path(source_path_str):
                    normalized_path = normalize_unc_path(source_path_str)
                    source_path = Path(normalized_path)
                    logger.info(f"后台扫描任务：检测到 UNC 路径，规范化后: {normalized_path}")
                else:
                    source_path = Path(source_path_str)
                
                if not source_path.exists():
                    logger.warning(f"后台扫描任务：源路径不存在，跳过: {source_path_str}")
                    continue
                
                try:
                    if source_path.is_file():
                        # 单个文件
                        try:
                            # 检查是否应该排除
                            if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                                continue
                            
                            # 获取文件信息
                            file_info = await self.file_scanner.get_file_info(source_path)
                            if file_info:
                                if backup_set_db_id:
                                    await self.backup_db.upsert_scanned_file_record(backup_set_db_id, file_info)
                                total_files += 1
                                total_bytes += file_info['size']
                                
                                # 每100个文件更新一次数据库
                                if total_files % update_interval == 0:
                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                    logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
                            logger.warning(f"后台扫描任务：跳过无法访问的文件: {source_path_str} (错误: {str(e)})")
                            continue
                        except Exception as e:
                            logger.warning(f"后台扫描任务：跳过出错的文件: {source_path_str} (错误: {str(e)})")
                            continue
                    
                    elif source_path.is_dir():
                        # 目录：递归扫描
                        logger.info(f"后台扫描任务：扫描目录 {source_path_str}")
                        
                        # 检查目录本身是否应该排除
                        if self.file_scanner.should_exclude_file(str(source_path), exclude_patterns):
                            logger.info(f"后台扫描任务：目录匹配排除规则，跳过整个目录: {source_path_str}")
                            continue
                        
                        # 使用队列来传递扫描结果（每100个文件一批）
                        scan_queue = asyncio.Queue(maxsize=0)  # 无限制队列
                        
                        # 重要：获取当前事件循环（主事件循环），在启动后台线程之前
                        # 因为后台线程中无法使用 get_running_loop()
                        main_loop = asyncio.get_running_loop()
                        
                        # 创建停止标志（用于响应 Ctrl+C）
                        import threading
                        stop_event = threading.Event()
                        
                        def sync_scan_worker():
                            """在线程池中执行同步遍历，统计文件数和字节数，根据目录数量智能匹配批次阈值（10、25、50、100个文件）提交一次，或者每20分钟强制提交一次"""
                            batch_files = 0  # 当前批次的文件数
                            batch_bytes = 0  # 当前批次的字节数
                            last_log_count = 0  # 上次输出日志的文件数
                            last_log_time = None  # 上次输出日志的时间
                            last_batch_submit_time = None  # 上次提交批次的时间
                            last_progress_log_time = None  # 上次输出进度日志的时间
                            last_dir_log_time = None  # 上次输出目录日志的时间
                            current_dir = None  # 当前正在扫描的目录
                            scan_failed = False  # 扫描是否失败
                            scan_error = None  # 扫描错误信息
                            file_count = 0  # 文件计数（在finally中使用）
                            dir_count = 0  # 目录计数
                            permission_error_count = 0  # 权限错误计数
                            BATCH_FORCE_INTERVAL = 1200.0  # 强制提交批次的时间间隔（秒）- 20分钟
                            PROGRESS_LOG_INTERVAL = 60.0  # 进度日志输出间隔（秒）- 1分钟
                            DIR_LOG_INTERVAL = 120.0  # 目录日志输出间隔（秒）- 2分钟
                            
                            def get_batch_threshold(dir_count: int) -> int:
                                """根据目录数量智能匹配批次阈值
                                
                                使用 settings.SCAN_BATCH_SIZE 作为基础值，根据目录数量进行比例调整
                                
                                Args:
                                    dir_count: 待扫描目录数量
                                
                                Returns:
                                    int: 批次阈值（文件数）
                                """
                                # 使用全局settings获取最新配置
                                settings = get_settings()
                                # 使用 settings.SCAN_BATCH_SIZE 作为基础值
                                base_batch_size = settings.SCAN_BATCH_SIZE
                                
                                if dir_count >= 50000:
                                    # 目录数量很多（>50000），使用基础值的约1.7%（300/180000）
                                    return int(base_batch_size * 0.0017)
                                elif dir_count >= 10000:
                                    # 目录数量较多（10000-50000），使用基础值的约0.28%（500/180000）
                                    return int(base_batch_size * 0.0028)
                                elif dir_count >= 1000:
                                    # 目录数量中等（1000-10000），使用基础值的约0.44%（800/180000）
                                    return int(base_batch_size * 0.0044)
                                else:
                                    # 目录数量少（<1000），使用基础值的约0.56%（1000/180000）
                                    return int(base_batch_size * 0.0056)
                            
                            # 使用全局settings获取最新配置
                            settings = get_settings()
                            # 获取批次字节数阈值（与压缩扫描使用相同的参数）
                            batch_bytes_threshold = settings.SCAN_BATCH_SIZE_BYTES
                            MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                            
                            def truncate_path(path_str: str, max_len: int = MAX_PATH_DISPLAY) -> str:
                                """截断路径以便在日志中显示"""
                                if len(path_str) <= max_len:
                                    return path_str
                                # 保留开头和结尾
                                prefix_len = max_len - 50
                                return f"{path_str[:prefix_len]}...{path_str[-(max_len-prefix_len-3):]}"
                            
                            def format_path_for_log(path_str: str) -> str:
                                """格式化路径以便在日志中显示"""
                                try:
                                    return truncate_path(path_str)
                                except Exception:
                                    return str(path_str)[:MAX_PATH_DISPLAY]
                            
                            try:
                                logger.info(f"后台扫描任务：开始遍历目录 {source_path_str}（线程中）")
                                file_count = 0
                                dir_count = 0
                                start_time = time.time()
                                last_log_time = start_time
                                last_batch_submit_time = start_time
                                last_progress_log_time = start_time
                                last_dir_log_time = start_time
                                
                                # 文件信息批次（用于写入数据库）
                                file_info_batch = []  # 存储文件信息字典列表
                                
                                # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                                # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                                # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                                dirs_to_scan = [source_path]  # 待扫描的目录队列
                                scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                                
                                # 对于大量目录，调整日志频率
                                LARGE_DIR_THRESHOLD = 10000  # 超过1万个目录时，使用更频繁的日志
                                is_large_dir_structure = False
                                
                                try:
                                    while dirs_to_scan:
                                        # 检查停止标志（响应 Ctrl+C）
                                        if stop_event.is_set():
                                            logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                            scan_failed = True
                                            scan_error = "用户中断（Ctrl+C）"
                                            break
                                        
                                        try:
                                            current_scan_dir = dirs_to_scan.pop(0)
                                            
                                            # 避免重复扫描
                                            try:
                                                current_scan_dir_str = str(current_scan_dir.resolve())
                                            except Exception:
                                                current_scan_dir_str = str(current_scan_dir)
                                            
                                            if current_scan_dir_str in scanned_dirs:
                                                continue
                                            
                                            scanned_dirs.add(current_scan_dir_str)
                                            current_dir = current_scan_dir_str
                                            dir_count += 1
                                            
                                            # 检测是否为大型目录结构（超过1万个目录）
                                            if dir_count >= LARGE_DIR_THRESHOLD and not is_large_dir_structure:
                                                is_large_dir_structure = True
                                                # 根据待扫描目录数量获取批次阈值
                                                current_batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                logger.info(f"后台扫描任务：检测到大型目录结构（已扫描 {dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}），批次阈值: {current_batch_threshold}，将使用更频繁的进度日志（目录: {source_path_str}）")
                                            
                                            # 每2分钟输出一次当前扫描的目录（大型目录结构时每30秒输出一次）
                                            current_time = time.time()
                                            elapsed_since_dir_log = current_time - last_dir_log_time
                                            dir_log_interval = 30.0 if is_large_dir_structure else DIR_LOG_INTERVAL
                                            if current_dir and elapsed_since_dir_log >= dir_log_interval:
                                                logger.info(f"后台扫描任务：正在扫描目录 {format_path_for_log(current_dir)}，已扫描 {dir_count} 个目录，{file_count} 个文件，待扫描目录: {len(dirs_to_scan)}（目录: {source_path_str}）")
                                                last_dir_log_time = current_time
                                            
                                            # 检查停止标志（在扫描目录前检查）
                                            if stop_event.is_set():
                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                scan_failed = True
                                                scan_error = "用户中断（Ctrl+C）"
                                                break
                                            
                                            try:
                                                # 使用 os.scandir() 扫描当前目录
                                                with os.scandir(current_scan_dir_str) as entries:
                                                    for entry in entries:
                                                        # 检查停止标志（在每次迭代时检查，提高响应速度）
                                                        if stop_event.is_set():
                                                            logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                            scan_failed = True
                                                            scan_error = "用户中断（Ctrl+C）"
                                                            break
                                                        try:
                                                            # 获取路径
                                                            try:
                                                                entry_path = Path(entry.path)
                                                                current_path_str = str(entry_path)
                                                            except Exception as path_str_err:
                                                                # 路径字符串化失败（可能是编码问题）
                                                                logger.warning(f"后台扫描任务：路径字符串化失败: {str(path_str_err)}")
                                                                continue
                                                            
                                                            # 检查是否应该排除
                                                            if current_path_str and self.file_scanner.should_exclude_file(current_path_str, exclude_patterns):
                                                                continue
                                                            
                                                            # 处理目录和文件
                                                            try:
                                                                if entry.is_dir(follow_symlinks=False):
                                                                    # 目录：添加到待扫描队列
                                                                    dirs_to_scan.append(entry_path)
                                                                elif entry.is_file(follow_symlinks=False):
                                                                    # 检查停止标志（在处理文件前检查）
                                                                    if stop_event.is_set():
                                                                        logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                        scan_failed = True
                                                                        scan_error = "用户中断（Ctrl+C）"
                                                                        break
                                                                    
                                                                    # 文件：获取文件信息并统计
                                                                    try:
                                                                        # 使用 file_scanner 的方法获取文件信息（与流式扫描一致）
                                                                        file_info = self.file_scanner.get_file_info_from_entry(entry)
                                                                        if not file_info:
                                                                            # 无法获取文件信息，跳过
                                                                            continue
                                                                        
                                                                        file_size = file_info.get('size', 0) or 0
                                                                        
                                                                        # 添加到文件信息批次（用于写入数据库）
                                                                        file_info_batch.append(file_info)
                                                                        
                                                                        # 在线程中统计（本地变量，线程安全）
                                                                        batch_files += 1
                                                                        batch_bytes += file_size
                                                                        file_count += 1
                                                                        
                                                                        current_time = time.time()
                                                                        
                                                                        # 每处理1000个文件检查一次停止标志（提高响应速度）
                                                                        if file_count % 1000 == 0:
                                                                            if stop_event.is_set():
                                                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                                scan_failed = True
                                                                                scan_error = "用户中断（Ctrl+C）"
                                                                                break
                                                                        
                                                                        # 检查是否需要强制提交批次（即使没有达到阈值，也要定期提交）
                                                                        elapsed_since_last_batch = current_time - last_batch_submit_time
                                                                        # 根据待扫描目录数量智能匹配批次阈值
                                                                        batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                                        
                                                                        if batch_files > 0 and elapsed_since_last_batch >= BATCH_FORCE_INTERVAL:
                                                                            # 强制提交当前批次（即使没有达到阈值，每20分钟强制提交一次）
                                                                            try:
                                                                                # 提交文件信息批次和统计信息
                                                                                batch_file_infos = file_info_batch.copy()
                                                                                file_info_batch.clear()
                                                                                asyncio.run_coroutine_threadsafe(
                                                                                    scan_queue.put((batch_files, batch_bytes, batch_file_infos)),
                                                                                    main_loop
                                                                                )
                                                                                current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                                logger.info(f"后台扫描任务：强制提交批次到队列（超过{BATCH_FORCE_INTERVAL}秒），文件数={batch_files}, 字节数={format_bytes(batch_bytes)}，累计已扫描={file_count}个文件，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（目录: {source_path_str}）")
                                                                            except Exception as e:
                                                                                logger.error(f"后台扫描任务：强制提交批次失败: {str(e)}", exc_info=True)
                                                                            batch_files = 0
                                                                            batch_bytes = 0
                                                                            last_batch_submit_time = current_time
                                                                        # 根据目录数量智能匹配批次阈值提交批次（正常提交）
                                                                        # 检查文件数或字节数是否达到阈值（与压缩扫描使用相同的逻辑）
                                                                        elif batch_files >= batch_threshold or batch_bytes >= batch_bytes_threshold:
                                                                            try:
                                                                                # 提交文件信息批次和统计信息
                                                                                batch_file_infos = file_info_batch.copy()
                                                                                file_info_batch.clear()
                                                                                # 使用主事件循环（在启动线程前获取的）
                                                                                asyncio.run_coroutine_threadsafe(
                                                                                    scan_queue.put((batch_files, batch_bytes, batch_file_infos)),
                                                                                    main_loop
                                                                                )
                                                                                current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                                logger.info(f"后台扫描任务：已提交批次到队列，文件数={batch_files}, 字节数={format_bytes(batch_bytes)}，累计已扫描={file_count}个文件，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（目录: {source_path_str}）")
                                                                            except Exception as e:
                                                                                logger.error(f"后台扫描任务：提交批次失败: {str(e)}", exc_info=True)
                                                                                # 提交批次失败不算扫描失败，继续扫描
                                                                            batch_files = 0
                                                                            batch_bytes = 0
                                                                            last_batch_submit_time = current_time
                                                                        
                                                                        # 每10000个文件或每1分钟输出一次进度日志（大型目录结构时每30秒输出一次）
                                                                        elapsed_since_last_progress = current_time - last_progress_log_time
                                                                        progress_log_interval = 30.0 if is_large_dir_structure else PROGRESS_LOG_INTERVAL
                                                                        if file_count - last_log_count >= 10000 or elapsed_since_last_progress >= progress_log_interval:
                                                                            # 检查停止标志（在输出日志时也检查，提高响应速度）
                                                                            if stop_event.is_set():
                                                                                logger.warning(f"后台扫描任务：检测到停止信号，中止扫描（目录: {source_path_str}），已扫描 {file_count} 个文件，{dir_count} 个目录")
                                                                                scan_failed = True
                                                                                scan_error = "用户中断（Ctrl+C）"
                                                                                break
                                                                            
                                                                            elapsed = current_time - last_log_time if last_log_time else 0
                                                                            rate = (file_count - last_log_count) / elapsed_since_last_progress if elapsed_since_last_progress > 0 else 0
                                                                            current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                            logger.info(f"后台扫描任务：正在扫描目录 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前批次 {batch_files} 个文件 {format_bytes(batch_bytes)}，耗时 {elapsed:.1f} 秒，速度 {rate:.0f} 文件/秒，距上次提交 {elapsed_since_last_batch:.1f} 秒，当前目录: {current_dir_display}，权限错误: {permission_error_count}（线程运行中）")
                                                                            last_log_count = file_count
                                                                            last_log_time = current_time
                                                                            last_progress_log_time = current_time
                                                                            
                                                                    except (PermissionError, OSError) as file_err:
                                                                        # 权限错误：记录详细路径信息
                                                                        permission_error_count += 1
                                                                        try:
                                                                            file_path_display = format_path_for_log(current_path_str)
                                                                            if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                                logger.warning(f"后台扫描任务：权限错误（文件 #{permission_error_count}）: {file_path_display}，错误: {str(file_err)}")
                                                                        except Exception:
                                                                            logger.warning(f"后台扫描任务：权限错误（文件 #{permission_error_count}）: 无法获取路径，错误: {str(file_err)}")
                                                                        continue
                                                                    except (FileNotFoundError, IOError) as file_err:
                                                                        # 文件不存在或IO错误：跳过
                                                                        continue
                                                                    except Exception as file_err:
                                                                        # 记录文件错误但继续扫描
                                                                        try:
                                                                            file_path_display = format_path_for_log(current_path_str)
                                                                            logger.warning(f"后台扫描任务：跳过文件错误 {file_path_display}: {str(file_err)}")
                                                                        except Exception:
                                                                            logger.warning(f"后台扫描任务：跳过文件错误: {str(file_err)}")
                                                                        continue
                                                            except (OSError, PermissionError) as entry_err:
                                                                # 无法判断类型，尝试作为文件处理
                                                                try:
                                                                    if entry_path.is_file():
                                                                        # 使用 entry.stat() 而不是 entry_path.stat()，因为 entry 已经缓存了 stat 信息
                                                                        stat = entry.stat(follow_symlinks=False)
                                                                        file_size = stat.st_size
                                                                        batch_files += 1
                                                                        batch_bytes += file_size
                                                                        file_count += 1
                                                                    elif entry_path.is_dir():
                                                                        dirs_to_scan.append(entry_path)
                                                                except Exception:
                                                                    permission_error_count += 1
                                                                    if permission_error_count <= 20:
                                                                        logger.warning(f"后台扫描任务：无法访问路径: {format_path_for_log(current_path_str)}，错误: {str(entry_err)}")
                                                                    continue
                                                        except (PermissionError, OSError) as entry_err:
                                                            # 路径权限错误：记录详细路径信息
                                                            permission_error_count += 1
                                                            try:
                                                                path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                path_display = format_path_for_log(path_str)
                                                                if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                    logger.warning(f"后台扫描任务：路径权限错误（路径 #{permission_error_count}）: {path_display}，错误: {str(entry_err)}")
                                                            except Exception:
                                                                logger.warning(f"后台扫描任务：路径权限错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                            continue
                                                        except (FileNotFoundError, IOError) as entry_err:
                                                            # 路径不存在或IO错误：跳过
                                                            continue
                                                        except Exception as entry_err:
                                                            # 记录路径错误但继续扫描
                                                            try:
                                                                path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                path_display = format_path_for_log(path_str)
                                                                logger.warning(f"后台扫描任务：跳过路径错误 {path_display}: {str(entry_err)}")
                                                            except Exception:
                                                                logger.warning(f"后台扫描任务：跳过路径错误: {str(entry_err)}")
                                                            continue
                                            except (PermissionError, OSError) as scan_dir_err:
                                                # 目录权限错误：记录并跳过该目录
                                                permission_error_count += 1
                                                try:
                                                    path_display = format_path_for_log(current_scan_dir_str)
                                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                                        logger.warning(f"后台扫描任务：目录权限错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                                except Exception:
                                                    logger.warning(f"后台扫描任务：目录权限错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                                continue
                                            except (FileNotFoundError, IOError) as scan_dir_err:
                                                # 目录不存在或IO错误：跳过
                                                continue
                                            except Exception as scan_dir_err:
                                                # 记录目录错误但继续扫描
                                                try:
                                                    path_display = format_path_for_log(current_scan_dir_str)
                                                    logger.warning(f"后台扫描任务：跳过目录错误 {path_display}: {str(scan_dir_err)}")
                                                except Exception:
                                                    logger.warning(f"后台扫描任务：跳过目录错误: {str(scan_dir_err)}")
                                                continue
                                        except KeyboardInterrupt:
                                            logger.warning(f"后台扫描任务：遍历目录被中断 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录")
                                            # 设置停止标志，通知其他部分停止
                                            stop_event.set()
                                            scan_failed = True
                                            scan_error = "用户中断（KeyboardInterrupt）"
                                            break
                                        except Exception as dir_scan_err:
                                            # 目录扫描错误：记录但继续
                                            logger.warning(f"后台扫描任务：目录扫描错误: {str(dir_scan_err)}")
                                            continue
                                    
                                    # 如果还有待扫描的目录，记录警告
                                    if dirs_to_scan and not scan_failed:
                                        logger.warning(f"后台扫描任务：还有 {len(dirs_to_scan)} 个目录待扫描，但主循环已退出（目录: {source_path_str}）")
                                    
                                    total_time = time.time() - start_time
                                    logger.info(f"后台扫描任务：目录遍历完成 {source_path_str}，共扫描 {file_count} 个文件，{dir_count} 个目录，权限错误: {permission_error_count} 个，总耗时 {total_time:.1f} 秒")
                                except KeyboardInterrupt:
                                    logger.warning(f"后台扫描任务：遍历目录被中断 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录")
                                    scan_failed = True
                                    scan_error = "用户中断（KeyboardInterrupt）"
                                    raise
                                except Exception as scan_err:
                                    # 扫描过程出错（可能是文件系统错误、内存不足等）
                                    scan_failed = True
                                    scan_error = str(scan_err)
                                    logger.error(f"后台扫描任务：扫描目录失败 {source_path_str}，已扫描 {file_count} 个文件，{dir_count} 个目录: {scan_error}", exc_info=True)
                                    # 不 raise，继续执行 finally 块提交已扫描的批次
                                
                            except KeyboardInterrupt:
                                logger.warning(f"后台扫描任务：线程被中断 {source_path_str}，已扫描 {file_count} 个文件")
                                scan_failed = True
                                scan_error = "用户中断（KeyboardInterrupt）"
                                # 设置停止标志
                                stop_event.set()
                                raise
                            except Exception as e:
                                # 记录所有其他异常，包括完整的堆栈跟踪
                                scan_failed = True
                                scan_error = str(e)
                                logger.error(f"后台扫描任务：遍历目录失败 {source_path_str}，已扫描 {file_count} 个文件: {scan_error}", exc_info=True)
                            finally:
                                # 重要：无论是否异常，都确保发送信号到队列，让主循环知道线程已退出
                                try:
                                    # 提交剩余的批次
                                    if batch_files > 0:
                                        try:
                                            # 提交文件信息批次和统计信息
                                            batch_file_infos = file_info_batch.copy()
                                            file_info_batch.clear()
                                            # 使用主事件循环（在启动线程前获取的）
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put((batch_files, batch_bytes, batch_file_infos)),
                                                main_loop
                                            )
                                            logger.info(f"后台扫描任务：提交剩余批次到队列，文件数={batch_files}, 字节数={format_bytes(batch_bytes)}（目录: {source_path_str}）")
                                        except Exception as e:
                                            logger.warning(f"后台扫描任务：提交剩余批次失败: {str(e)}")
                                    
                                    # 发送完成/退出信号
                                    # 如果扫描失败，发送错误信号；否则发送完成信号
                                    if scan_failed:
                                        # 发送错误信号：使用特殊标记 ('ERROR', error_message, file_count)
                                        try:
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put(('ERROR', scan_error, file_count)),
                                                main_loop
                                            )
                                            logger.warning(f"后台扫描任务：发送错误信号到队列（目录: {source_path_str}），错误: {scan_error}，已扫描: {file_count} 个文件")
                                        except Exception as e:
                                            logger.error(f"后台扫描任务：发送错误信号失败: {str(e)}", exc_info=True)
                                    else:
                                        # 发送完成信号：None 表示正常完成
                                        try:
                                            asyncio.run_coroutine_threadsafe(
                                                scan_queue.put(None),  # None 表示正常完成
                                                main_loop
                                            )
                                            logger.info(f"后台扫描任务：发送完成信号到队列（目录: {source_path_str}），共扫描 {file_count} 个文件")
                                        except Exception as e:
                                            logger.error(f"后台扫描任务：发送完成信号失败: {str(e)}", exc_info=True)
                                except Exception as finally_err:
                                    # finally 块中的异常不应该阻止信号发送，但需要记录
                                    logger.error(f"后台扫描任务：finally 块中发生异常（目录: {source_path_str}）: {str(finally_err)}", exc_info=True)
                                    # 尝试最后一次发送信号（使用最简单的方式）
                                    try:
                                        asyncio.run_coroutine_threadsafe(
                                            scan_queue.put(('ERROR', f"finally块异常: {str(finally_err)}", file_count)),
                                            main_loop
                                        )
                                    except Exception:
                                        pass
                        
                        # 启动扫描任务
                        logger.info(f"后台扫描任务：启动后台线程扫描目录 {source_path_str}")
                        scan_task = asyncio.create_task(asyncio.to_thread(sync_scan_worker))
                        scan_start_time = time.time()  # 记录扫描开始时间
                        last_heartbeat_time = time.time()  # 上次心跳时间
                        
                        # 从队列中获取批次并更新统计
                        logger.info(f"后台扫描任务：开始从队列获取批次并更新统计（目录: {source_path_str}）")
                        batch_received_count = 0
                        timeout_count = 0  # 超时计数
                        last_timeout_log_time = None  # 上次超时日志时间
                        try:
                            while True:
                                try:
                                    # 检查任务是否被取消（Ctrl+C）
                                    try:
                                        current_task = asyncio.current_task()
                                        if current_task and current_task.cancelled():
                                            logger.warning("后台扫描任务：检测到任务已被取消（Ctrl+C）")
                                            # 设置停止标志，通知后台线程停止
                                            stop_event.set()
                                            # 取消后台扫描任务
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            break
                                    except RuntimeError:
                                        # 如果没有当前任务，可能已经被取消
                                        logger.warning("后台扫描任务：检测到任务可能已被取消")
                                        # 设置停止标志，通知后台线程停止
                                        stop_event.set()
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        break
                                    
                                    # 检查停止标志（在主循环中也要检查）
                                    if stop_event.is_set():
                                        logger.warning("后台扫描任务：检测到停止信号，退出主循环")
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        break
                                    
                                    # 等待批次或完成信号（带超时，避免无限等待）
                                    try:
                                        item = await asyncio.wait_for(scan_queue.get(), timeout=1200.0)  # 超时时间：20分钟（1200秒）
                                        timeout_count = 0  # 收到数据，重置超时计数
                                    except asyncio.CancelledError:
                                        # 任务被取消（Ctrl+C）
                                        logger.warning("后台扫描任务：任务被取消（CancelledError）")
                                        # 设置停止标志，通知后台线程停止
                                        stop_event.set()
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        raise
                                    except asyncio.TimeoutError:
                                        timeout_count += 1
                                        # 超时检查扫描任务是否完成
                                        if scan_task.done():
                                            logger.info(f"后台扫描任务：扫描任务已完成，获取队列中剩余的批次（目录: {source_path_str}）")
                                            # 尝试获取队列中剩余的所有批次
                                            remaining_count = 0
                                            while not scan_queue.empty():
                                                try:
                                                    item = scan_queue.get_nowait()
                                                    if item is None:
                                                        break
                                                    # 处理批次（可能是旧格式或新格式）
                                                    if isinstance(item, tuple) and len(item) == 3:
                                                        batch_files, batch_bytes, file_info_batch = item
                                                        # 写入文件信息（如果有）
                                                        if file_info_batch and backup_set_db_id and use_memory_db and 'memory_writer' in locals():
                                                            try:
                                                                await memory_writer.add_files_batch(file_info_batch)
                                                            except Exception:
                                                                pass  # 忽略写入错误，只统计
                                                    else:
                                                        batch_files, batch_bytes = item[0], item[1]
                                                    total_files += batch_files
                                                    total_bytes += batch_bytes
                                                    remaining_count += 1
                                                    # 更新数据库
                                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                    logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                                except asyncio.QueueEmpty:
                                                    break
                                            if remaining_count > 0:
                                                logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                            break
                                        else:
                                            # 如果扫描任务还在运行，继续等待（输出调试信息）
                                            # 每20分钟输出一次日志，避免日志过多，同时确保知道任务还在运行
                                            current_time = time.time()
                                            elapsed_since_start = current_time - scan_start_time
                                            elapsed_since_last_heartbeat = current_time - last_heartbeat_time
                                            
                                            # 检查任务是否真的在运行（如果任务已完成但没有发送完成信号，可能是异常退出）
                                            if scan_task.done():
                                                # 任务已完成，但可能没有发送完成信号（异常退出）
                                                logger.warning(f"后台扫描任务：扫描任务已完成但未发送完成信号（目录: {source_path_str}），已接收批次: {batch_received_count}，耗时: {elapsed_since_start:.1f} 秒")
                                                # 尝试获取任务异常
                                                try:
                                                    exception = scan_task.exception()
                                                    if exception:
                                                        logger.error(f"后台扫描任务：扫描任务异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                                                except Exception:
                                                    pass
                                                # 获取队列中剩余的所有批次
                                                remaining_count = 0
                                                while not scan_queue.empty():
                                                    try:
                                                        item = scan_queue.get_nowait()
                                                        if item is None:
                                                            continue
                                                        # 处理批次（可能是旧格式或新格式）
                                                        if isinstance(item, tuple) and len(item) == 3:
                                                            batch_files, batch_bytes, file_info_batch = item
                                                            # 写入文件信息（如果有）
                                                            if file_info_batch and backup_set_db_id and use_memory_db and 'memory_writer' in locals():
                                                                try:
                                                                    await memory_writer.add_files_batch(file_info_batch)
                                                                except Exception:
                                                                    pass  # 忽略写入错误，只统计
                                                        else:
                                                            batch_files, batch_bytes = item[0], item[1]
                                                        total_files += batch_files
                                                        total_bytes += batch_bytes
                                                        remaining_count += 1
                                                        await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                        logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                                    except asyncio.QueueEmpty:
                                                        break
                                                if remaining_count > 0:
                                                    logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                                break
                                            
                                            # 每20分钟输出一次心跳日志（与超时时间一致）
                                            if last_timeout_log_time is None or current_time - last_timeout_log_time >= 1200.0:
                                                logger.info(f"后台扫描任务：等待批次中...（目录: {source_path_str}，已接收批次: {batch_received_count}，超时次数: {timeout_count}，已运行: {elapsed_since_start:.1f} 秒，扫描任务状态: 运行中）")
                                                last_timeout_log_time = current_time
                                                last_heartbeat_time = current_time
                                            
                                            # 如果超过30分钟没有收到批次，输出警告（可能线程卡住了）
                                            if elapsed_since_last_heartbeat > 1800.0:
                                                logger.warning(f"后台扫描任务：超过30分钟没有收到批次（目录: {source_path_str}），已接收批次: {batch_received_count}，耗时: {elapsed_since_start:.1f} 秒")
                                                last_heartbeat_time = current_time
                                            
                                            continue
                                    
                                    # 成功获取 item，检查是否是完成信号或错误信号（只有在成功获取 item 时才执行）
                                    if item is None:
                                        # 正常完成信号
                                        logger.info(f"后台扫描任务：收到完成信号，获取队列中剩余的批次（目录: {source_path_str}）")
                                        # 尝试获取队列中剩余的所有批次
                                        remaining_count = 0
                                        while not scan_queue.empty():
                                            try:
                                                batch_item = scan_queue.get_nowait()
                                                if batch_item is None:
                                                    continue
                                                # 检查是否是错误信号
                                                if isinstance(batch_item, tuple) and len(batch_item) >= 2 and batch_item[0] == 'ERROR':
                                                    error_msg, error_file_count = batch_item[1], batch_item[2] if len(batch_item) > 2 else 0
                                                    logger.error(f"后台扫描任务：队列中发现错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                                    break
                                                # 处理批次（可能是旧格式或新格式）
                                                if isinstance(batch_item, tuple) and len(batch_item) == 3:
                                                    batch_files, batch_bytes, file_info_batch = batch_item
                                                    # 写入文件信息（如果有）
                                                    if file_info_batch and backup_set_db_id and use_memory_db and 'memory_writer' in locals():
                                                        try:
                                                            await memory_writer.add_files_batch(file_info_batch)
                                                        except Exception:
                                                            pass  # 忽略写入错误，只统计
                                                else:
                                                    batch_files, batch_bytes = batch_item[0], batch_item[1]
                                                total_files += batch_files
                                                total_bytes += batch_bytes
                                                remaining_count += 1
                                                # 更新数据库
                                                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                            except asyncio.QueueEmpty:
                                                break
                                        if remaining_count > 0:
                                            logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                        break
                                    elif isinstance(item, tuple) and len(item) >= 2 and item[0] == 'ERROR':
                                        # 错误信号：('ERROR', error_message, file_count)
                                        error_msg = item[1]
                                        error_file_count = item[2] if len(item) > 2 else total_files
                                        logger.error(f"后台扫描任务：收到错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                        # 更新数据库（使用已扫描的文件数和字节数）
                                        if total_files > 0:
                                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                            logger.info(f"后台扫描任务：已更新数据库，累计 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}）")
                                        # 获取队列中剩余的所有批次（如果有）
                                        remaining_count = 0
                                        while not scan_queue.empty():
                                            try:
                                                batch_item = scan_queue.get_nowait()
                                                if batch_item is None or (isinstance(batch_item, tuple) and len(batch_item) >= 2 and batch_item[0] == 'ERROR'):
                                                    continue
                                                # 处理批次（可能是旧格式或新格式）
                                                if isinstance(batch_item, tuple) and len(batch_item) == 3:
                                                    batch_files, batch_bytes, file_info_batch = batch_item
                                                    # 写入文件信息（如果有）
                                                    if file_info_batch and backup_set_db_id and use_memory_db and 'memory_writer' in locals():
                                                        try:
                                                            await memory_writer.add_files_batch(file_info_batch)
                                                        except Exception:
                                                            pass  # 忽略写入错误，只统计
                                                else:
                                                    batch_files, batch_bytes = batch_item[0], batch_item[1]
                                                total_files += batch_files
                                                total_bytes += batch_bytes
                                                remaining_count += 1
                                                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，剩余批次: {remaining_count}）")
                                            except asyncio.QueueEmpty:
                                                break
                                        if remaining_count > 0:
                                            logger.info(f"后台扫描任务：已处理 {remaining_count} 个剩余批次（目录: {source_path_str}）")
                                        # 错误信号也意味着扫描任务结束
                                        break
                                    else:
                                        # 处理正常批次
                                        try:
                                            # 检查是否是新的格式（包含文件信息批次）
                                            if isinstance(item, tuple) and len(item) == 3:
                                                batch_files, batch_bytes, file_info_batch = item
                                            else:
                                                # 旧格式（只有统计信息，没有文件信息）
                                                batch_files, batch_bytes = item
                                                file_info_batch = []
                                            
                                            # 写入文件信息到数据库（如果有）
                                            if file_info_batch and backup_set_db_id:
                                                try:
                                                    if use_memory_db and 'memory_writer' in locals():
                                                        # 写入内存数据库
                                                        try:
                                                            await memory_writer.add_files_batch(file_info_batch)
                                                            logger.debug(f"[后台扫描] ✅ 已成功批量写入 {len(file_info_batch)} 个文件到内存数据库")
                                                        except Exception as batch_error:
                                                            logger.error(f"[后台扫描] ❌ 批量写入失败: {str(batch_error)}，尝试逐个添加", exc_info=True)
                                                            # 回退到逐个添加
                                                            success_count = 0
                                                            failed_count = 0
                                                            for file_info in file_info_batch:
                                                                try:
                                                                    await memory_writer.add_file(file_info)
                                                                    success_count += 1
                                                                except Exception as file_error:
                                                                    failed_count += 1
                                                                    logger.warning(f"[后台扫描] 添加文件到内存数据库失败: {file_info.get('path', 'unknown')[:200]}, 错误: {str(file_error)}")
                                                            if failed_count > 0:
                                                                logger.warning(f"[后台扫描] ⚠️ {failed_count} 个文件添加失败，已跳过（未计入统计）")
                                                            # 只统计成功写入的文件
                                                            batch_files = success_count
                                                            batch_bytes = sum(f.get('size', 0) or 0 for f in file_info_batch[:success_count])
                                                    else:
                                                        # 顺序执行模式：直接同步写入数据库
                                                        try:
                                                            await batch_writer.write_batch_sync(file_info_batch)
                                                            logger.debug(f"[后台扫描] ✅ 批次已全部写入数据库: {len(file_info_batch)} 个文件")
                                                        except Exception as batch_error:
                                                            logger.error(f"[后台扫描] ❌ 批量写入失败: {str(batch_error)}", exc_info=True)
                                                            # 只统计成功写入的文件（这里无法知道具体成功数，使用原值）
                                                except Exception as e:
                                                    logger.error(f"[后台扫描] 写入文件信息失败: {str(e)}", exc_info=True)
                                                    # 写入失败，不统计这些文件
                                                    batch_files = 0
                                                    batch_bytes = 0
                                            
                                            # 只在写入成功后才统计
                                            total_files += batch_files
                                            total_bytes += batch_bytes
                                            batch_received_count += 1
                                            
                                            # 更新数据库
                                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                            logger.info(f"后台扫描任务：已扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)}（目录: {source_path_str}，批次: {batch_received_count}）")
                                        except (ValueError, TypeError) as batch_err:
                                            # 批次数据格式错误（可能是未知的信号类型）
                                            logger.error(f"后台扫描任务：批次数据格式错误（目录: {source_path_str}）: {str(batch_err)}, 数据: {item}")
                                            # 检查是否是错误信号（但没有正确识别）
                                            if isinstance(item, tuple) and len(item) >= 2 and item[0] == 'ERROR':
                                                error_msg = item[1]
                                                error_file_count = item[2] if len(item) > 2 else total_files
                                                logger.error(f"后台扫描任务：检测到错误信号（目录: {source_path_str}），错误: {error_msg}，已扫描: {error_file_count} 个文件")
                                                if total_files > 0:
                                                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                                                break
                                            # 检查扫描任务是否已完成
                                            if scan_task.done():
                                                logger.info(f"后台扫描任务：扫描任务已完成，退出循环（目录: {source_path_str}）")
                                                break
                                            continue
                                        
                                except asyncio.CancelledError:
                                    # 任务被取消（Ctrl+C）
                                    logger.warning("后台扫描任务：任务被取消（CancelledError）")
                                    if not scan_task.done():
                                        scan_task.cancel()
                                    raise
                                except Exception as e:
                                    logger.error(f"后台扫描任务：处理批次失败（目录: {source_path_str}）: {str(e)}", exc_info=True)
                                    # 检查扫描任务状态
                                    if scan_task.done():
                                        logger.info(f"后台扫描任务：扫描任务已完成，退出循环（目录: {source_path_str}）")
                                        # 尝试获取任务异常
                                        try:
                                            exception = scan_task.exception()
                                            if exception:
                                                logger.error(f"后台扫描任务：扫描任务异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                                        except Exception:
                                            pass
                                        break
                                    # 如果任务还在运行，继续等待
                                    continue
                                    
                        except KeyboardInterrupt:
                            # 用户中断（Ctrl+C）
                            logger.warning("后台扫描任务：用户中断（KeyboardInterrupt）")
                            # 设置停止标志，通知后台线程停止
                            stop_event.set()
                            if not scan_task.done():
                                scan_task.cancel()
                            raise
                        
                        # 确保扫描任务完成
                        try:
                            await scan_task
                            logger.info(f"后台扫描任务：扫描任务正常完成（目录: {source_path_str}）")
                        except Exception as task_err:
                            logger.error(f"后台扫描任务：扫描任务异常退出（目录: {source_path_str}）: {str(task_err)}", exc_info=True)
                        
                        # 检查任务是否有异常
                        if scan_task.done():
                            try:
                                exception = scan_task.exception()
                                if exception:
                                    logger.error(f"后台扫描任务：扫描任务内部异常（目录: {source_path_str}）: {str(exception)}", exc_info=True)
                            except Exception:
                                pass
                        
                        # 扫描完一个目录后，最后更新一次数据库
                        if total_files > 0:
                            await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                            logger.info(f"后台扫描任务：目录扫描完成 {source_path_str}，累计 {total_files} 个文件，总大小 {format_bytes(total_bytes)}")
                        else:
                            logger.warning(f"后台扫描任务：目录扫描完成但没有扫描到文件（目录: {source_path_str}）")
                            
                except Exception as e:
                    logger.error(f"后台扫描任务：处理源路径失败 {source_path_str}: {str(e)}")
                    continue
            
            # 扫描完成，最后一次更新数据库
            if total_files > 0:
                await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                logger.info(f"========== 后台扫描任务完成：共扫描 {total_files} 个文件，总大小 {format_bytes(total_bytes)} ==========")
            else:
                logger.info("========== 后台扫描任务完成：未扫描到文件 ==========")
        
        except KeyboardInterrupt:
            # 用户按 Ctrl+C 中止任务
            logger.warning("========== 后台扫描任务被用户中止（Ctrl+C） ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            # 即使被中止，也更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            # 重新抛出 KeyboardInterrupt，让上层处理
            raise
            
        except asyncio.CancelledError:
            # 任务被取消
            logger.warning("========== 后台扫描任务被取消 ==========")
            logger.warning(f"任务ID: {backup_task.id if backup_task else 'N/A'}")
            # 即使被取消，也更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            # 重新抛出 CancelledError，让上层处理
            raise
        
        except Exception as e:
            logger.error(f"后台扫描任务失败: {str(e)}")
            import traceback
            logger.error(f"错误堆栈:\n{traceback.format_exc()}")
            # 即使失败，也尝试更新已扫描的文件数和字节数
            if 'total_files' in locals() and total_files > 0:
                try:
                    await self.backup_db.update_scan_progress_only(backup_task, total_files, total_bytes)
                    logger.info(f"后台扫描任务：已更新已扫描的文件数 {total_files} 和总大小 {format_bytes(total_bytes)}")
                except Exception as update_error:
                    logger.error(f"更新扫描进度失败: {str(update_error)}")
            if backup_task and backup_task.id:
                await self.backup_db.update_scan_status(backup_task.id, 'failed')


    async def _scan_opengauss_direct_scandir(
        self,
        backup_task: BackupTask,
        source_paths: List[str],
        exclude_patterns: List[str],
        backup_set: BackupSet,
        restart: bool = False,
    ):
        """
        openGauss 模式下的简化扫描任务：
        - 直接使用 os.scandir + FileScanner 扫描文件系统
        - 参考 tests/test_scan_direct_write.py 的实现
        - 按批次直接批量写入 openGauss 的 backup_files 表（不使用内存数据库）
        - 定期更新 backup_task.total_files / total_bytes 和任务阶段描述
        - 作为后台任务运行，不阻塞压缩流程，自己使用短事务，避免长事务锁表
        """
        from utils.scheduler.db_utils import get_opengauss_connection

        backup_set_db_id = getattr(backup_set, "id", None)
        logger.info(
            f"[后台扫描-openGauss直写] backup_task_id={getattr(backup_task, 'id', 'N/A')}, "
            f"backup_set_id={backup_set_db_id}, source_paths={source_paths}, exclude_patterns={exclude_patterns}"
        )

        # 初始化/清理状态：同时在内存和数据库中设置 scan_status = 'running'
        try:
            if backup_task and backup_task.id:
                backup_task.scan_status = "running"
                await self.backup_db.update_scan_status(backup_task.id, "running")
            if restart and backup_set_db_id:
                # 清理原有 backup_files 记录，重新扫描
                await self.backup_db.clear_backup_files_for_set(backup_set_db_id)
        except Exception as e:
            logger.warning(f"[后台扫描-openGauss直写] 初始化扫描状态失败（忽略继续）: {e}")

        # 记录关键阶段：扫描文件开始（仅日志，不再写入数据库的 operation_stage/description）
        logger.info("[后台扫描-openGauss直写] 开始扫描文件（不再通过数据库传递阶段和进度，仅使用内存状态）")

        # 处理源路径为空的情况
        if not source_paths:
            logger.warning("[后台扫描-openGauss直写] source_paths 为空列表，没有文件要扫描")
            # 不再通过数据库更新进度/状态，直接更新内存中的任务对象
            if backup_task:
                backup_task.total_files = 0
                backup_task.total_bytes = 0
            return

        # 统计信息（与 tests/test_scan_direct_write.py 保持一致，并增加目录统计）
        stats: Dict[str, Any] = {
            "total_scanned": 0,  # 扫描到的文件数（尝试写入）
            "total_written": 0,  # 成功写入 backup_files 的文件数
            "total_failed": 0,  # 写入失败/准备数据失败
            "total_bytes": 0,  # 成功写入的总字节数
            "excluded_count": 0,  # 被排除的文件数
            "excluded_dirs": 0,  # 被排除的目录数
            "error_count": 0,  # 文件错误数
            "error_dirs": 0,  # 目录错误数
            "dirs_scanned": 0,  # 扫描到的目录数
            "dirs_skipped": 0,  # 跳过的目录数（重复等）
            "symlinks_skipped": 0,  # 跳过的符号链接数
            "start_time": time.time(),
        }

        # 优化：从内存获取分表名（backup_task.backup_files_table），避免查询数据库
        table_name = None
        if backup_task:
            table_name = getattr(backup_task, "backup_files_table", None)
            if table_name and isinstance(table_name, str) and table_name.startswith("backup_files_"):
                logger.debug(f"[后台扫描-openGauss直写] 从内存获取分表名: {table_name}")
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
                            logger.debug(f"[后台扫描-openGauss直写] 从数据库获取分表名: {table_name}")
                    except Exception as e:
                        logger.warning(f"[后台扫描-openGauss直写] 获取分表名失败: {e}")

        if not table_name:
            logger.error("[后台扫描-openGauss直写] 无法获取分表名，扫描无法继续")
            return

        # 批次相关：沿用 SCAN_UPDATE_INTERVAL 作为批次大小
        batch_size = getattr(self.settings, "SCAN_UPDATE_INTERVAL", 1000) or 1000
        current_batch: List[Dict[str, Any]] = []
        batch_number = 0

        # 进度输出相关（仅日志，不再写入数据库）
        last_progress_time = time.time()
        progress_interval = getattr(self.settings, "SCAN_LOG_INTERVAL_SECONDS", 60) or 60
        last_log_count = 0
        log_interval_count = 10000

        # 优化：打开数据库连接后持续复用，不关闭（提高速度）
        scan_conn = None
        try:
            # 手动打开连接，不使用 context manager（避免自动关闭）
            conn_context = get_opengauss_connection()
            scan_conn = await conn_context.__aenter__()
            logger.debug("[后台扫描-openGauss直写] 已打开数据库连接，将在整个扫描过程中复用")

            async def flush_batch(current_dir_str: str = ""):
                """将当前批次写入 backup_files_* 分表，并更新统计（按实际写入成功的文件数统计）
                
                Args:
                    current_dir_str: 当前正在处理的目录路径，仅用于日志输出，便于排查性能问题
                """
                nonlocal current_batch, batch_number, stats, last_progress_time, last_log_count, scan_conn
                if not current_batch or not backup_set_db_id or not table_name or not scan_conn:
                    current_batch = []
                    return

                insert_data = []
                for file_info in current_batch:
                    try:
                        data_tuple = self._prepare_insert_data_for_backup_files(file_info, backup_set_db_id)
                        insert_data.append(data_tuple)
                    except Exception as e:
                        file_path = file_info.get("path", "unknown")
                        logger.warning(
                            f"[后台扫描-openGauss直写] 准备插入数据失败: {file_path[:200]}, 错误: {e}"
                        )
                        stats["total_failed"] += 1

                if not insert_data:
                    current_batch = []
                    return

                # 批量插入 backup_files_*（使用复用的连接，短事务，避免长事务锁表）
                try:
                    await scan_conn.executemany(
                        f"""
                        INSERT INTO {table_name} (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, NOW(), NOW()
                        )
                        """,
                        insert_data,
                    )

                    actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                    try:
                        await actual_conn.commit()
                    except Exception as commit_err:
                        # 提交失败时尝试回滚，避免长事务锁表
                        logger.warning(
                            f"[后台扫描-openGauss直写] 提交批次事务失败（可能已自动提交）: {commit_err}"
                        )
                        try:
                            await actual_conn.rollback()
                        except Exception:
                            pass
                        # 提交失败，这批文件未写入，计入失败统计
                        stats["total_failed"] += len(insert_data)
                        current_batch = []
                        return

                    # 只有成功提交后才统计（按实际写入成功的文件数统计，确保统计正确）
                    written = len(insert_data)
                    stats["total_written"] += written
                    # 统计写入的总字节数（只统计成功写入的文件）
                    stats["total_bytes"] += sum(
                        (fi.get("size", 0) or 0) for fi in current_batch[:written]
                    )
                    batch_number += 1

                    # 更新内存中的任务对象统计信息（供 UI 使用，基于实际写入成功的文件数）
                    if backup_task:
                        backup_task.total_files = stats["total_written"]
                        backup_task.total_bytes = stats["total_bytes"]

                    # 进度日志（减少日志频率，提高速度）
                    now_ts = time.time()
                    elapsed = now_ts - stats["start_time"]
                    files_per_sec = stats["total_written"] / elapsed if elapsed > 0 else 0
                    # 只在批次较大或达到日志间隔时输出，减少日志开销
                    if written >= 5000 or (now_ts - last_progress_time >= progress_interval):
                        current_dir_display = current_dir_str[:80] + "..." if current_dir_str and len(current_dir_str) > 80 else (current_dir_str or "")
                        logger.info(
                            f"[后台扫描-openGauss直写] 批次 {batch_number}: 已写入 {stats['total_written']:,} 个文件, "
                            f"总容量: {format_bytes(stats['total_bytes'])}, 速度: {files_per_sec:.0f} 文件/秒"
                            + (f", 当前目录: {current_dir_display}" if current_dir_display else "")
                        )
                        last_progress_time = now_ts

                    # 定期将扫描统计同步到数据库，用于前端卡片显示 total_files / total_bytes
                    try:
                        if backup_task and hasattr(self.backup_db, "update_scan_progress_only"):
                            # 每 progress_interval 秒同步一次，避免过于频繁
                            if now_ts - last_progress_time >= progress_interval:
                                await self.backup_db.update_scan_progress_only(
                                    backup_task,
                                    stats["total_written"],
                                    stats["total_bytes"],
                                )
                                last_progress_time = now_ts
                    except Exception as sync_err:
                        # 同步失败仅记录调试日志，不影响扫描主流程
                        logger.debug(
                            f"[后台扫描-openGauss直写] 同步扫描统计到数据库失败（忽略继续）: {sync_err}"
                        )

                    last_log_count = stats["total_written"]  # 使用 total_written 而不是 total_scanned
                except Exception as e:
                    logger.error(
                        f"[后台扫描-openGauss直写] 批次写入 backup_files 失败: {e}", exc_info=True
                    )
                    # 异常时回滚，避免长事务锁表
                    try:
                        actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                        if hasattr(actual_conn, "rollback"):
                            await actual_conn.rollback()
                    except Exception:
                        pass
                    # 写入失败，这批文件未写入，计入失败统计（确保不丢文件：失败会被记录）
                    stats["total_failed"] += len(insert_data)

                finally:
                    current_batch = []

            # 辅助函数：查询数据库中的总文件数和总容量（使用复用的连接）
            async def get_db_stats(backup_set_db_id: int) -> tuple:
                """查询数据库中该备份集的总文件数和总容量
                
                使用复用的 scan_conn，避免新建连接
                
                Returns:
                    (db_total_files, db_total_bytes): 数据库中的总文件数和总字节数
                """
                if not scan_conn or not table_name:
                    return (0, 0)
                try:
                    row = await scan_conn.fetchrow(
                        f"""
                        SELECT 
                            COUNT(*)::BIGINT as total_files,
                            COALESCE(SUM(file_size), 0)::BIGINT as total_bytes
                        FROM {table_name}
                        WHERE backup_set_id = $1::INTEGER
                          AND file_type = 'file'::backupfiletype
                        """,
                        backup_set_db_id
                    )
                    # openGauss 模式下需要显式提交事务（fetchrow 后）
                    actual_conn = scan_conn._conn if hasattr(scan_conn, '_conn') else scan_conn
                    if hasattr(actual_conn, 'commit'):
                        try:
                            await actual_conn.commit()
                        except Exception:
                            pass  # 可能不在事务中
                    
                    if row:
                        return (row.get("total_files", 0) or 0, row.get("total_bytes", 0) or 0)
                except Exception as e:
                    logger.debug(f"[后台扫描-openGauss直写] 查询数据库统计失败: {e}")
                return (0, 0)

            # 主扫描循环（os.scandir + FileScanner）
            for idx, source_path_str in enumerate(source_paths):
                source_path = Path(source_path_str)
                logger.info(
                    f"[后台扫描-openGauss直写] 扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}"
                )

                if not source_path.exists():
                    logger.warning(
                        f"[后台扫描-openGauss直写] 源路径不存在，跳过: {source_path_str}"
                    )
                    continue

                # 单个文件
                if source_path.is_file():
                    try:
                        if self.file_scanner.should_exclude_file(
                            str(source_path), exclude_patterns
                        ):
                            stats["excluded_count"] += 1
                            continue
                        file_info = await self.file_scanner.get_file_info(source_path)
                        if file_info:
                            current_batch.append(file_info)
                            stats["total_scanned"] += 1
                            if len(current_batch) >= batch_size:
                                await flush_batch(str(source_path))
                    except Exception as e:
                        logger.warning(
                            f"[后台扫描-openGauss直写] 处理单个文件失败: {source_path_str}, 错误: {e}"
                        )
                        stats["error_count"] += 1

                    # 单个文件处理完成，直接继续下一个源路径（不做额外数据库统计，以提高性能）
                    continue

                # 目录：使用 os.scandir 递归扫描
                if source_path.is_dir():
                    logger.info(
                        f"[后台扫描-openGauss直写] 扫描目录: {source_path_str}（os.scandir 模式）"
                    )

                    # 检查目录本身是否被排除
                    if self.file_scanner.should_exclude_file(
                        str(source_path), exclude_patterns
                    ):
                        logger.info(
                            f"[后台扫描-openGauss直写] 目录匹配排除规则，跳过整个目录: {source_path_str}"
                        )
                        stats["excluded_dirs"] += 1
                        continue

                    dirs_to_scan = [source_path]
                    scanned_dirs = set()

                    try:
                        while dirs_to_scan:
                            current_dir = dirs_to_scan.pop(0)
                            current_dir_str = str(current_dir.resolve())

                            if current_dir_str in scanned_dirs:
                                stats["dirs_skipped"] += 1
                                continue
                            scanned_dirs.add(current_dir_str)
                            stats["dirs_scanned"] += 1

                            # 检查目录排除
                            if self.file_scanner.should_exclude_file(
                                current_dir_str, exclude_patterns
                            ):
                                stats["excluded_dirs"] += 1
                                continue

                            try:
                                with os.scandir(current_dir_str) as entries:
                                    for entry in entries:
                                        try:
                                            entry_path = Path(entry.path)
                                            entry_path_str = str(entry_path)

                                            if self.file_scanner.should_exclude_file(
                                                entry_path_str, exclude_patterns
                                            ):
                                                stats["excluded_count"] += 1
                                                continue

                                            # 目录：加入队列
                                            if entry.is_dir(follow_symlinks=False):
                                                dirs_to_scan.append(entry_path)
                                                continue

                                            # 文件：获取文件信息
                                            if entry.is_file(follow_symlinks=False):
                                                file_info = self.file_scanner.get_file_info_from_entry(
                                                    entry
                                                )
                                                if file_info:
                                                    current_batch.append(file_info)
                                                    stats["total_scanned"] += 1

                                                    if len(current_batch) >= batch_size:
                                                        await flush_batch(current_dir_str)
                                                else:
                                                    stats["error_count"] += 1
                                                continue

                                            # 其他情况（符号链接等）简单跳过并计数
                                            if entry.is_symlink():
                                                stats["symlinks_skipped"] += 1
                                            else:
                                                stats["error_count"] += 1
                                        except (PermissionError, OSError, FileNotFoundError):
                                            stats["error_count"] += 1
                                            continue
                            except (PermissionError, OSError) as e:
                                logger.warning(
                                    f"[后台扫描-openGauss直写] 无法访问目录: {current_dir_str}, 错误: {e}"
                                )
                                stats["error_dirs"] += 1
                                continue
                    except Exception as e:
                        logger.warning(
                            f"[后台扫描-openGauss直写] 扫描目录失败: {source_path_str}, 错误: {e}"
                        )
                        stats["error_count"] += 1

                    # 目录扫描完成后，直接继续下一个源路径（不做额外数据库统计，以提高性能）
                    continue

            # 写入最后一个批次
            if current_batch:
                logger.info(
                    f"[后台扫描-openGauss直写] 写入最后批次 ({len(current_batch)} 个文件)..."
                )
                # 此处无法精确知道最后批次对应的目录，传空字符串只输出汇总信息
                await flush_batch("")

            # 扫描结束：更新内存状态，并做一次最终数据库同步（用于前端展示总文件数/总字节数）
            if backup_task:
                backup_task.total_files = stats["total_written"]
                backup_task.total_bytes = stats["total_bytes"]
                try:
                    if hasattr(self.backup_db, "update_scan_progress_only"):
                        await self.backup_db.update_scan_progress_only(
                            backup_task,
                            stats["total_written"],
                            stats["total_bytes"],
                        )
                except Exception as sync_err:
                    logger.debug(
                        f"[后台扫描-openGauss直写] 扫描结束时同步扫描统计到数据库失败（忽略继续）: {sync_err}"
                    )

            elapsed = time.time() - stats["start_time"]
            files_per_sec = (
                stats["total_written"] / elapsed if elapsed > 0 else 0.0
            )
            logger.info(
                f"[后台扫描-openGauss直写] 扫描完成：成功写入 {stats['total_written']:,} 个文件，"
                f"总大小 {format_bytes(stats['total_bytes'])}, 平均速度 {files_per_sec:.1f} 文件/秒, "
                f"失败 {stats['total_failed']:,} 个, 排除 {stats['excluded_count']:,} 个"
            )

            # 扫描全部结束后，在内存和数据库中设置 scan_status = 'completed'
            if backup_task and backup_task.id:
                try:
                    backup_task.scan_status = "completed"
                    await self.backup_db.update_scan_status(backup_task.id, "completed")
                except Exception as e:
                    logger.warning(f"[后台扫描-openGauss直写] 更新扫描状态为 completed 失败（忽略继续）: {e}")

            # 扫描全部结束后，统一做一次真实对账（内存统计 vs 数据库统计，确保统计正确、不丢文件）
            if backup_set_db_id:
                try:
                    db_total_files, db_total_bytes = await get_db_stats(backup_set_db_id)
                    memory_files = stats["total_written"]
                    memory_bytes = stats["total_bytes"]
                    
                    # 对账：计算差异
                    file_diff = memory_files - db_total_files
                    bytes_diff = memory_bytes - db_total_bytes
                    
                    if file_diff == 0 and bytes_diff == 0:
                        logger.info(
                            f"[后台扫描-openGauss直写] ✅ 对账一致：内存统计={memory_files:,} 文件/{format_bytes(memory_bytes)}, "
                            f"数据库统计={db_total_files:,} 文件/{format_bytes(db_total_bytes)}"
                        )
                    else:
                        logger.warning(
                            f"[后台扫描-openGauss直写] ⚠️ 对账差异：内存统计={memory_files:,} 文件/{format_bytes(memory_bytes)}, "
                            f"数据库统计={db_total_files:,} 文件/{format_bytes(db_total_bytes)}, "
                            f"差异={file_diff:,} 文件/{format_bytes(bytes_diff)}。"
                            f"可能原因：部分批次写入失败但未重试，或数据库事务未完全提交"
                        )
                except Exception as e:
                    logger.warning(
                        f"[后台扫描-openGauss直写] 扫描结束时查询数据库对账失败: {e}"
                    )
        finally:
            # 关闭数据库连接（整个扫描过程结束）
            if scan_conn:
                try:
                    # 需要保存 conn_context 以便在 finally 中关闭
                    # 但由于 conn_context 在 try 块内，我们需要用另一种方式
                    actual_conn = scan_conn._conn if hasattr(scan_conn, "_conn") else scan_conn
                    if hasattr(actual_conn, "close"):
                        await actual_conn.close()
                    logger.debug("[后台扫描-openGauss直写] 已关闭数据库连接")
                except Exception as e:
                    logger.warning(f"[后台扫描-openGauss直写] 关闭数据库连接失败: {e}")

    def _prepare_insert_data_for_backup_files(
        self, file_info: Dict[str, Any], backup_set_id: int
    ) -> tuple:
        """
        为 openGauss 的 backup_files 表准备插入数据元组。
        逻辑参考 MemoryDBWriter._prepare_insert_data_for_opengauss，保持字段一致性。
        """
        file_path = file_info.get("path", "")
        file_name = file_info.get("name") or Path(file_path).name

        # 目录路径
        directory_path = (
            str(Path(file_path).parent)
            if file_path and Path(file_path).parent != Path(file_path).anchor
            else None
        )

        display_name = file_name

        # 文件类型
        if file_info.get("is_file", True):
            file_type = "file"
        elif file_info.get("is_dir", False):
            file_type = "directory"
        elif file_info.get("is_symlink", False):
            file_type = "symlink"
        else:
            file_type = "file"

        file_size = file_info.get("size", 0) or 0
        compressed_size = None
        file_permissions = file_info.get("permissions")
        file_owner = None
        file_group = None

        # 时间戳处理
        modified_time = file_info.get("modified_time")
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)

        created_time = modified_time
        accessed_time = modified_time

        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None

        backup_time = datetime.now(timezone.utc)
        chunk_number = None
        version = 1

        # 注意：file_metadata 和 tags 字段已从扫描阶段删除，压缩阶段会更新 file_metadata

        return (
            backup_set_id,
            file_path,
            file_name,
            directory_path,
            display_name,
            file_type,
            file_size,
            compressed_size,
            file_permissions,
            file_owner,
            file_group,
            created_time,
            modified_time,
            accessed_time,
            tape_block_start,
            tape_block_count,
            compressed,
            encrypted,
            checksum,
            is_copy_success,
            copy_status_at,
            backup_time,
            chunk_number,
            version,
        )