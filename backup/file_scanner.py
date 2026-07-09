#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件扫描模块
File Scanner Module
"""

import logging
import fnmatch
import asyncio
import os
import time
import threading
import queue
from collections import deque  # 使用deque优化队列操作性能（O(1)复杂度）
from concurrent.futures import ThreadPoolExecutor, as_completed  # 并发目录扫描
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, AsyncGenerator, Callable, Awaitable, Tuple

logger = logging.getLogger(__name__)


class FileScanner:
    """文件扫描器"""
    
    def __init__(self, settings=None, update_progress_callback: Optional[Callable] = None):
        """初始化文件扫描器
        
        Args:
            settings: 系统设置对象
            update_progress_callback: 更新进度回调函数 (backup_task, scanned_count, valid_count, operation_status)
        """
        self.settings = settings
        self.update_progress_callback = update_progress_callback
    
    def get_file_info_from_entry(self, entry) -> Optional[Dict]:
        """从 os.scandir 的 DirEntry 对象直接获取文件信息（一次性获取路径和大小，避免额外调用）
        
        如果遇到权限错误、访问错误等，返回None，调用者应该跳过该文件。
        
        Args:
            entry: os.scandir 返回的 DirEntry 对象
            
        Returns:
            文件信息字典，如果无法访问则返回None
        """
        try:
            # 一次性获取路径和 stat 信息（使用 entry.stat() 更高效，可能利用 scandir 缓存）
            entry_path_str = entry.path
            stat = entry.stat()
            
            # 从路径中提取文件名
            try:
                from pathlib import Path
                file_name = Path(entry_path_str).name
            except Exception:
                # 如果 Path 解析失败，尝试直接提取
                file_name = entry_path_str.split('\\')[-1].split('/')[-1]
            
            return {
                'path': entry_path_str,
                'name': file_name,
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_file': entry.is_file(follow_symlinks=False),
                'is_dir': entry.is_dir(follow_symlinks=False),
                'is_symlink': entry.is_symlink()
            }
        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
            # 权限错误、访问错误等，返回带错误信息的最小记录
            logger.warning(f"跳过无法访问的文件: {entry.path if hasattr(entry, 'path') else 'unknown'} (错误: {str(e)})")
            entry_path_str = entry.path if hasattr(entry, 'path') else 'unknown'
            try:
                file_name = Path(entry_path_str).name
            except Exception:
                file_name = entry_path_str.split('\\')[-1].split('/')[-1]
            return {
                'path': entry_path_str,
                'name': file_name,
                'size': 0,
                'modified_time': datetime.now(),
                'permissions': '',
                'is_file': True,
                'is_dir': False,
                'is_symlink': False,
                'error_message': f'扫描错误: {str(e)}'
            }
        except Exception as e:
            # 其他错误，也返回带错误信息的最小记录
            logger.warning(f"获取文件信息失败 {entry.path if hasattr(entry, 'path') else 'unknown'}: {str(e)}")
            entry_path_str = entry.path if hasattr(entry, 'path') else 'unknown'
            try:
                file_name = Path(entry_path_str).name
            except Exception:
                file_name = entry_path_str.split('\\')[-1].split('/')[-1]
            return {
                'path': entry_path_str,
                'name': file_name,
                'size': 0,
                'modified_time': datetime.now(),
                'permissions': '',
                'is_file': True,
                'is_dir': False,
                'is_symlink': False,
                'error_message': f'扫描错误: {str(e)}'
            }
    
    async def get_file_info(self, file_path: Path) -> Optional[Dict]:
        """获取文件信息（兼容旧接口，用于非 scandir 场景）
        
        如果遇到权限错误、访问错误等，返回None，调用者应该跳过该文件。
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件信息字典，如果无法访问则返回None
        """
        try:
            stat = file_path.stat()
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified_time': datetime.fromtimestamp(stat.st_mtime),
                'permissions': oct(stat.st_mode)[-3:],
                'is_file': file_path.is_file(),
                'is_dir': file_path.is_dir(),
                'is_symlink': file_path.is_symlink()
            }
        except (PermissionError, OSError, FileNotFoundError, IOError) as e:
            # 权限错误、访问错误等，返回带错误信息的最小记录
            logger.warning(f"跳过无法访问的文件: {file_path} (错误: {str(e)})")
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': 0,
                'modified_time': datetime.now(),
                'permissions': '',
                'is_file': True,
                'is_dir': False,
                'is_symlink': False,
                'error_message': f'扫描错误: {str(e)}'
            }
        except Exception as e:
            # 其他错误，也返回带错误信息的最小记录
            logger.warning(f"获取文件信息失败 {file_path}: {str(e)}")
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': 0,
                'modified_time': datetime.now(),
                'permissions': '',
                'is_file': True,
                'is_dir': False,
                'is_symlink': False,
                'error_message': f'扫描错误: {str(e)}'
            }
    
    def should_exclude_file(self, file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件或目录是否应该被排除

        排除规则支持三种匹配方式：
        1. 完整路径匹配: fnmatch 匹配整个路径
        2. 路径段名称匹配: 对路径中每个目录/文件名进行匹配
           例如模式 "System Volume Information" 会匹配路径中任何位置的该目录名
        3. 父目录匹配: 如果父目录被排除，其下所有文件也被排除

        Args:
            file_path: 文件或目录路径
            exclude_patterns: 排除模式列表（从计划任务 action_config 获取）

        Returns:
            bool: 如果文件/目录应该被排除返回 True
        """
        if not exclude_patterns:
            return False

        # 将路径标准化（统一使用正斜杠）
        normalized_path = file_path.replace('\\', '/')
        path_parts = [p for p in normalized_path.split('/') if p]

        # 1. 检查完整路径是否匹配排除规则
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if fnmatch.fnmatch(normalized_path, normalized_pattern):
                return True

        # 2. 检查路径中每个段（目录名/文件名）是否匹配排除规则
        # 这样 "System Volume Information" 可以匹配任何位置的同名目录
        for part in path_parts:
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', '/')
                # 去掉模式中的路径前缀，只保留最后一段用于名称匹配
                pattern_name = normalized_pattern.rsplit('/', 1)[-1]
                if fnmatch.fnmatch(part, pattern_name):
                    return True

        # 3. 检查父目录路径是否匹配排除规则（含通配符）
        for i in range(len(path_parts)):
            parent_path = '/'.join(path_parts[:i+1])
            if not parent_path:
                continue

            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', '/')
                if fnmatch.fnmatch(parent_path, normalized_pattern):
                    return True
                if fnmatch.fnmatch(parent_path + '/*', normalized_pattern):
                    return True

        return False
    
    async def scan_source_files_streaming(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None,
        batch_size: int = 100,
        log_context: Optional[str] = None
    ) -> AsyncGenerator[List[Dict], None]:
        """流式扫描源文件（异步生成器，分批返回文件）
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79\yz - 指定共享路径
        - 自动处理 UNC 路径的文件和目录扫描
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            batch_size: 每批返回的文件数
            log_context: 日志上下文前缀（用于区分调用场景）
            
        Yields:
            List[Dict]: 每批文件列表
        """
        if not source_paths:
            logger.warning("源路径列表为空")
            return
        
        context_prefix = f"{log_context} " if log_context else ""
        
        # 不再估算总文件数，直接使用后台扫描任务提供的 total_files
        # 后台扫描任务会独立扫描并更新数据库中的 total_files 和 total_bytes
        # 这样可以避免重复扫描，特别是对于包含大量目录的场景（如40.5万个目录）
        
        total_scanned = 0
        total_scanned_size = 0  # 所有扫描到的文件总字节数
        current_batch = []
        total_valid_files = 0  # 累计的有效文件总数
        
        # 预先判断扫描方式（用于日志前缀）
        scan_type_info = ""
        try:
            from utils.scheduler.db_utils import is_opengauss
            from config.settings import get_settings
            settings = get_settings()
            scan_method = getattr(settings, 'SCAN_METHOD', 'default').lower()
            use_multithread = getattr(settings, 'USE_SCAN_MULTITHREAD', True)
            scan_threads = getattr(settings, 'SCAN_THREADS', 4)
            
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
                scan_type_info = f"[多线程扫描-{scan_threads}线程]"
            elif use_sequential:
                scan_type_info = "[顺序扫描]"
        except Exception:
            pass
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"{scan_type_info} 扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            
            # 处理 UNC 网络路径
            from utils.network_path import is_unc_path, normalize_unc_path
            if is_unc_path(source_path_str):
                # UNC 路径需要使用规范化后的路径
                normalized_path = normalize_unc_path(source_path_str)
                source_path = Path(normalized_path)
                logger.debug(f"检测到 UNC 路径，规范化后: {normalized_path}")
            else:
                source_path = Path(source_path_str)
            
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                current_batch.append(file_info)
                                total_valid_files += 1  # 累计有效文件数
                                total_scanned_size += file_info['size']
                                # 注意：不再在这里更新 total_bytes_actual
                                # 这些统计由独立的后台扫描任务 _scan_for_progress_update 负责更新
                        total_scanned += 1
                        
                        # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                        # 后台扫描任务会独立扫描并更新数据库中的 total_files，这里只需要更新已扫描的文件数
                        if backup_task and self.update_progress_callback:
                            # 从数据库读取最新的 total_files（由后台扫描任务更新）
                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                # 使用后台扫描任务提供的 total_files 计算进度
                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                backup_task.progress_percent = scan_progress
                            else:
                                # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                backup_task.progress_percent = 0.0
                            await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                        
                        # 达到批次大小，yield当前批次
                        if len(current_batch) >= batch_size:
                            yield current_batch
                            current_batch = []
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []  # 记录出错的路径
                    
                    try:
                        # 检查是否使用并发扫描（openGauss模式且SCAN_THREADS > 1）
                        from utils.scheduler.db_utils import is_opengauss
                        from config.settings import get_settings
                        settings = get_settings()  # 获取最新配置
                        # 检查是否使用多线程扫描
                        # 条件：1) openGauss模式 2) 扫描方法为default 3) 启用多线程选项 4) 线程数>1
                        scan_method = getattr(settings, 'SCAN_METHOD', 'default').lower()
                        use_multithread = getattr(settings, 'USE_SCAN_MULTITHREAD', True)
                        scan_threads = getattr(settings, 'SCAN_THREADS', 4)
                        
                        use_concurrent_scan = (
                            is_opengauss() and 
                            scan_method == 'default' and 
                            use_multithread and 
                            scan_threads > 1
                        )
                        
                        if use_concurrent_scan:
                            # 使用并发目录扫描（openGauss模式）
                            scan_threads = getattr(settings, 'SCAN_THREADS', 4)
                            logger.info(f"{log_context or ''} [多线程扫描] 启用并发目录扫描 (线程数: {scan_threads}, ConcurrentDirScanner)")
                            
                            # 异步生成器遍历目录（使用并发扫描）
                            async def async_rglob_generator(path: Path):
                                """异步递归遍历目录生成器（使用并发扫描）"""
                                from backup.concurrent_dir_scanner import ConcurrentDirScanner
                                
                                # 使用无限制队列来缓冲路径（处理海量文件）
                                path_queue = asyncio.Queue(maxsize=0)  # maxsize=0 表示无限制
                                stop_signal = object()  # 停止信号
                                scan_done = False
                                total_paths_scanned = 0  # 总路径数（用于统计）
                                
                                # 获取主事件循环
                                try:
                                    main_loop = asyncio.get_running_loop()
                                except RuntimeError:
                                    main_loop = None
                                
                                # 创建并发扫描器
                                scanner = ConcurrentDirScanner(
                                    max_workers=scan_threads,
                                    context_prefix=context_prefix or "[并发扫描]"
                                )
                                
                                # 在线程池中启动并发扫描任务
                                def concurrent_scan_worker():
                                    """在线程池中执行并发扫描"""
                                    nonlocal scan_done, total_paths_scanned
                                    try:
                                        # 开始并发扫描
                                        # 注意：使用batch_size作为批次阈值（由SCAN_UPDATE_INTERVAL控制）
                                        total_paths_scanned = scanner.scan_directory_tree(
                                            root_path=path,
                                            path_queue=path_queue,  # asyncio.Queue（异步队列）
                                            main_loop=main_loop,
                                            batch_threshold=batch_size,  # 使用传入的batch_size（SCAN_UPDATE_INTERVAL）
                                            batch_force_interval=1200.0,  # 强制提交间隔（20分钟）
                                            log_interval=60.0  # 日志输出间隔（1分钟）
                                        )
                                        
                                        scan_done = True
                                        
                                        # 发送停止信号
                                        if main_loop:
                                            future = asyncio.run_coroutine_threadsafe(
                                                path_queue.put(stop_signal),
                                                main_loop
                                            )
                                            future.result(timeout=10.0)
                                        else:
                                            path_queue.put(stop_signal)
                                    
                                    except KeyboardInterrupt:
                                        logger.warning(f"{context_prefix or ''} 并发扫描被中断")
                                        scanner.scan_cancelled = True
                                        if main_loop:
                                            try:
                                                future = asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(('CANCELLED', None, total_paths_scanned)),
                                                    main_loop
                                                )
                                                future.result(timeout=10.0)
                                            except Exception:
                                                pass
                                    except Exception as e:
                                        logger.error(f"{context_prefix or ''} 并发扫描出错: {str(e)}", exc_info=True)
                                        if main_loop:
                                            try:
                                                future = asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(('ERROR', str(e), total_paths_scanned)),
                                                    main_loop
                                                )
                                                future.result(timeout=10.0)
                                            except Exception:
                                                pass
                                
                                # 在线程池中启动并发扫描任务
                                scan_task = asyncio.create_task(
                                    asyncio.to_thread(concurrent_scan_worker)
                                )
                                
                                # 从队列中逐步获取路径并yield
                                total_paths_count = 0
                                try:
                                    while True:
                                        try:
                                            batch = await asyncio.wait_for(path_queue.get(), timeout=5.0)
                                            
                                            if batch == stop_signal:
                                                break
                                            elif isinstance(batch, tuple) and len(batch) == 3:
                                                # 错误信号
                                                signal_type, error_info, paths_count = batch
                                                if signal_type == 'CANCELLED':
                                                    logger.warning(f"{context_prefix or ''} 并发扫描被取消")
                                                    break
                                                elif signal_type == 'ERROR':
                                                    # 记录错误，但不中断扫描
                                                    # 单个目录的错误已经在扫描器内部被处理了（跳过该目录）
                                                    logger.error(
                                                        f"{context_prefix or ''} 并发扫描出错: {error_info}，"
                                                        f"已扫描 {paths_count} 个路径。"
                                                        f"注意：单个目录的错误应该已经在扫描器内部被处理，这里不应该导致整个源路径被跳过。"
                                                    )
                                                    # 不中断，继续处理已扫描的路径
                                                    total_paths_count = paths_count
                                                    # 继续循环，等待更多路径或停止信号
                                                    continue
                                                total_paths_count = paths_count
                                                break
                                            else:
                                                # 文件批次
                                                if batch:
                                                    total_paths_count += len(batch)
                                                    yield batch
                                        except asyncio.TimeoutError:
                                            # 超时，检查是否完成
                                            if scan_done:
                                                break
                                            continue
                                finally:
                                    # 确保任务完成
                                    if not scan_task.done():
                                        scan_task.cancel()
                                        try:
                                            await scan_task
                                        except asyncio.CancelledError:
                                            pass
                            
                            # 使用并发扫描生成器
                            async for file_info_batch in async_rglob_generator(source_path):
                                # file_info_batch 已经是文件信息字典批次（List[Dict]），已经在扫描时获取了文件信息
                                if not file_info_batch:
                                    continue
                                
                                # 过滤排除的文件
                                file_batch = []
                                for file_info in file_info_batch:
                                    try:
                                        if file_info and not self.should_exclude_file(file_info.get('path', ''), exclude_patterns):
                                            file_batch.append(file_info)
                                            scanned_count += 1
                                    except Exception as file_error:
                                        error_count += 1
                                        continue
                                
                                # 返回文件批次
                                if file_batch:
                                    yield file_batch
                            
                            continue  # 跳过原有的顺序扫描代码
                        
                        # 检查是否使用新的顺序扫描器（openGauss模式 + 不启用多线程）
                        use_sequential_scanner = (
                            is_opengauss() and 
                            scan_method == 'default' and 
                            not use_multithread
                        )
                        
                        if use_sequential_scanner:
                            # 使用新的顺序扫描器（SequentialDirScanner）
                            logger.info(f"{log_context or ''} [顺序扫描] 启用顺序目录扫描（os.scandir优化版，SequentialDirScanner）")
                            
                            from backup.sequential_dir_scanner import SequentialDirScanner
                            
                            # 创建排除检查函数
                            def exclude_check(path_str: str) -> bool:
                                return self.should_exclude_file(path_str, exclude_patterns)
                            
                            # 异步生成器遍历目录（使用顺序扫描器）
                            async def async_sequential_generator(path: Path):
                                """异步顺序扫描生成器"""
                                # 获取主事件循环（在启动线程前获取）
                                try:
                                    main_loop = asyncio.get_running_loop()
                                except RuntimeError:
                                    main_loop = None
                                
                                path_queue = asyncio.Queue(maxsize=0)
                                current_batch = []
                                
                                def file_callback(file_path: Path):
                                    """文件回调：添加到批次"""
                                    nonlocal current_batch
                                    current_batch.append(file_path)
                                    
                                    # 批次达到阈值，放入队列
                                    if len(current_batch) >= batch_size and main_loop:
                                        # 重试机制：最多重试 6 次，每次间隔 3 分钟
                                        max_retries = 6
                                        retry_interval = 180.0  # 3 分钟
                                        retry_count = 0
                                        success = False
                                        
                                        while retry_count < max_retries and not success:
                                            try:
                                                # 记录放入队列前的状态
                                                queue_size_before = path_queue.qsize() if hasattr(path_queue, 'qsize') else 'N/A'
                                                
                                                future = asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(current_batch.copy()),
                                                    main_loop
                                                )
                                                # 超时时间改为 5 分钟（300秒）
                                                future.result(timeout=300.0)
                                                current_batch.clear()
                                                success = True
                                                
                                                if retry_count > 0:
                                                    logger.info(
                                                        f"{context_prefix or ''} 顺序扫描：放入队列成功（重试 {retry_count} 次后），"
                                                        f"批次大小: {len(current_batch)}"
                                                    )
                                            except asyncio.TimeoutError:
                                                retry_count += 1
                                                queue_size_after = path_queue.qsize() if hasattr(path_queue, 'qsize') else 'N/A'
                                                
                                                if retry_count < max_retries:
                                                    logger.warning(
                                                        f"{context_prefix or ''} 顺序扫描：放入队列超时（5分钟），"
                                                        f"批次大小: {len(current_batch)}，"
                                                        f"队列大小: {queue_size_before} -> {queue_size_after}，"
                                                        f"重试 {retry_count}/{max_retries}，"
                                                        f"等待 {retry_interval} 秒后重试..."
                                                    )
                                                    # 等待 3 分钟后重试
                                                    import time
                                                    time.sleep(retry_interval)
                                                else:
                                                    logger.error(
                                                        f"{context_prefix or ''} 顺序扫描：放入队列超时（5分钟），"
                                                        f"已重试 {max_retries} 次，批次大小: {len(current_batch)}，"
                                                        f"队列大小: {queue_size_before} -> {queue_size_after}。"
                                                        f"批次将保留，等待扫描结束时处理。"
                                                    )
                                                    # 最后一次重试失败，不清空批次，等待扫描结束时处理
                                            except Exception as e:
                                                retry_count += 1
                                                queue_size_after = path_queue.qsize() if hasattr(path_queue, 'qsize') else 'N/A'
                                                
                                                if retry_count < max_retries:
                                                    logger.warning(
                                                        f"{context_prefix or ''} 顺序扫描：放入队列失败: {str(e)}，"
                                                        f"批次大小: {len(current_batch)}，"
                                                        f"队列大小: {queue_size_after}，"
                                                        f"重试 {retry_count}/{max_retries}，"
                                                        f"等待 {retry_interval} 秒后重试..."
                                                    )
                                                    # 等待 3 分钟后重试
                                                    import time
                                                    time.sleep(retry_interval)
                                                else:
                                                    logger.error(
                                                        f"{context_prefix or ''} 顺序扫描：放入队列失败: {str(e)}，"
                                                        f"已重试 {max_retries} 次，批次大小: {len(current_batch)}，"
                                                        f"队列大小: {queue_size_after}。"
                                                        f"批次将保留，等待扫描结束时处理。"
                                                    )
                                                    # 最后一次重试失败，不清空批次，等待扫描结束时处理
                                
                                # 在线程池中执行顺序扫描
                                def sequential_scan_worker():
                                    """在线程池中执行顺序扫描"""
                                    try:
                                        scanner = SequentialDirScanner(
                                            context_prefix=context_prefix or "[顺序扫描]"
                                        )
                                        scanner.scan_directory_tree(
                                            root_path=path,
                                            exclude_check_func=exclude_check,
                                            file_callback=file_callback
                                        )
                                        
                                        # 处理剩余批次（包括之前放入队列失败的批次）
                                        if current_batch and main_loop:
                                            retry_count = 0
                                            max_retries = 6  # 重试次数改为 6 次
                                            retry_interval = 180.0  # 间隔 3 分钟
                                            success = False
                                            
                                            while retry_count < max_retries and current_batch and not success:
                                                try:
                                                    asyncio.run_coroutine_threadsafe(
                                                        path_queue.put(current_batch.copy()),
                                                        main_loop
                                                    ).result(timeout=300.0)  # 超时时间改为 5 分钟
                                                    logger.info(
                                                        f"{context_prefix or ''} 顺序扫描：剩余批次已成功放入队列，"
                                                        f"批次大小: {len(current_batch)}"
                                                        + (f"（重试 {retry_count} 次后）" if retry_count > 0 else "")
                                                    )
                                                    current_batch.clear()
                                                    success = True
                                                    break  # 成功，退出重试循环
                                                except asyncio.TimeoutError:
                                                    retry_count += 1
                                                    if retry_count < max_retries:
                                                        logger.warning(
                                                            f"{context_prefix or ''} 顺序扫描：剩余批次放入队列超时（5分钟），"
                                                            f"重试 {retry_count}/{max_retries}，批次大小: {len(current_batch)}，"
                                                            f"等待 {retry_interval} 秒后重试..."
                                                        )
                                                        # 等待 3 分钟后重试
                                                        import time
                                                        time.sleep(retry_interval)
                                                    else:
                                                        logger.error(
                                                            f"{context_prefix or ''} 顺序扫描：剩余批次放入队列失败（已重试 {max_retries} 次），"
                                                            f"批次大小: {len(current_batch)}，这些文件将丢失！"
                                                        )
                                                        # 最后一次重试失败，清空批次避免内存泄漏
                                                        # 但会丢失这些文件
                                                        current_batch.clear()
                                                except Exception as e:
                                                    retry_count += 1
                                                    if retry_count < max_retries:
                                                        logger.warning(
                                                            f"{context_prefix or ''} 顺序扫描：剩余批次放入队列失败: {str(e)}，"
                                                            f"重试 {retry_count}/{max_retries}，批次大小: {len(current_batch)}，"
                                                            f"等待 {retry_interval} 秒后重试..."
                                                        )
                                                        import time
                                                        time.sleep(retry_interval)
                                                    else:
                                                        logger.error(
                                                            f"{context_prefix or ''} 顺序扫描：剩余批次放入队列失败（已重试 {max_retries} 次）: {str(e)}，"
                                                            f"批次大小: {len(current_batch)}，这些文件将丢失！"
                                                        )
                                                        current_batch.clear()
                                        
                                        # 发送停止信号
                                        if main_loop:
                                            try:
                                                asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(None),
                                                    main_loop
                                                ).result(timeout=10.0)
                                            except Exception:
                                                pass
                                    except Exception as e:
                                        # 顺序扫描出错，记录但不抛出异常
                                        # 单个目录的错误已经在 SequentialDirScanner 内部被处理了
                                        logger.error(
                                            f"{context_prefix or ''} 顺序扫描出错: {str(e)}，"
                                            f"已扫描的文件可能不完整，但不会导致整个源路径被跳过。",
                                            exc_info=True
                                        )
                                        # 不抛出异常，让扫描继续
                                
                                # 启动扫描任务
                                scan_task = asyncio.create_task(
                                    asyncio.to_thread(sequential_scan_worker)
                                )
                                
                                # 从队列中获取批次
                                # 对于大型目录（50万+子目录），使用智能等待策略：
                                # 1. 如果扫描任务还在运行，无限等待（不设置超时）
                                # 2. 定期检查任务状态，避免真的阻塞
                                # 3. 使用较长的检查间隔（30秒），减少检查频率
                                last_status_check = time.time()
                                status_check_interval = 30.0  # 每30秒检查一次任务状态
                                
                                try:
                                    while True:
                                        # 检查扫描任务状态
                                        current_time = time.time()
                                        if current_time - last_status_check >= status_check_interval:
                                            last_status_check = current_time
                                            if scan_task.done():
                                                # 扫描任务已完成，尝试获取剩余数据后退出
                                                logger.debug(f"{context_prefix or ''} 顺序扫描：扫描任务已完成，获取剩余数据后退出")
                                                # 尝试获取队列中剩余的所有数据
                                                try:
                                                    while True:
                                                        try:
                                                            batch = await asyncio.wait_for(path_queue.get_nowait(), timeout=0.1)
                                                            if batch is None:
                                                                break
                                                            if batch:
                                                                yield batch
                                                        except asyncio.QueueEmpty:
                                                            break
                                                        except asyncio.TimeoutError:
                                                            break
                                                except Exception:
                                                    pass
                                                break
                                        
                                        # 如果任务还在运行，使用较长的超时时间等待队列数据
                                        # 对于大型目录，可能需要很长时间，使用30分钟超时
                                        try:
                                            batch = await asyncio.wait_for(path_queue.get(), timeout=1800.0)  # 30分钟超时
                                            if batch is None:
                                                # 收到停止信号
                                                break
                                            if batch:
                                                yield batch
                                        except asyncio.TimeoutError:
                                            # 30分钟超时，检查任务状态
                                            if scan_task.done():
                                                logger.debug(f"{context_prefix or ''} 顺序扫描：队列获取超时（30分钟），扫描任务已完成，退出循环")
                                                break
                                            else:
                                                # 任务仍在运行，可能是目录非常大，继续等待
                                                logger.info(
                                                    f"{context_prefix or ''} 顺序扫描：队列获取超时（30分钟），"
                                                    f"扫描任务仍在运行，可能目录非常大（50万+子目录），继续等待..."
                                                )
                                                continue
                                finally:
                                    if not scan_task.done():
                                        scan_task.cancel()
                                        try:
                                            await scan_task
                                        except asyncio.CancelledError:
                                            pass
                            
                            # 使用顺序扫描生成器
                            async for file_path_batch in async_sequential_generator(source_path):
                                if not file_path_batch:
                                    continue
                                
                                # 将Path对象转换为文件信息
                                file_batch = []
                                for file_path_item in file_path_batch:
                                    try:
                                        file_info = await self.get_file_info(file_path_item)
                                        if file_info:
                                            file_batch.append(file_info)
                                            scanned_count += 1
                                    except Exception as file_error:
                                        error_count += 1
                                        continue
                                
                                # 返回文件批次
                                if file_batch:
                                    yield file_batch
                            
                            continue  # 跳过原有的顺序扫描代码
                        
                        # 原有的顺序扫描代码（非openGauss模式或SCAN_THREADS=1）
                        # 异步生成器遍历目录（逐步yield，避免一次性加载所有路径）
                        async def async_rglob_generator(path: Path):
                            """异步递归遍历目录生成器（逐步yield路径，避免阻塞）"""
                            # 使用无限制队列来缓冲路径（处理海量文件）
                            path_queue = asyncio.Queue(maxsize=0)  # maxsize=0 表示无限制
                            stop_signal = object()  # 停止信号
                            scan_done = False
                            total_paths_scanned = 0  # 总路径数（用于统计）
                            
                            def sync_rglob_worker():
                                """在线程池中执行同步遍历，将路径放入队列
                                
                                职责：专注于遍历整个目录树，根据目录数量智能匹配批次阈值（10、25、50、100个路径）提交一次，或者每20分钟强制提交一次，中间不停，直到扫描完成
                                """
                                nonlocal scan_done, total_paths_scanned
                                import time
                                
                                # 防错机制配置
                                BATCH_FORCE_INTERVAL = 1200.0  # 强制提交批次的时间间隔（秒）- 20分钟
                                PROGRESS_LOG_INTERVAL = 60.0  # 进度日志输出间隔（秒）- 1分钟
                                DIR_LOG_INTERVAL = 120.0  # 目录日志输出间隔（秒）- 2分钟
                                MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                                
                                def get_batch_threshold(dir_count: int) -> int:
                                    """根据目录数量智能匹配批次阈值
                                    
                                    Args:
                                        dir_count: 待扫描目录数量
                                    
                                    Returns:
                                        int: 批次阈值（路径数）
                                    """
                                    if dir_count >= 50000:
                                        # 目录数量很多（>50000），使用10个路径作为批次阈值
                                        return 300
                                    elif dir_count >= 10000:
                                        # 目录数量较多（10000-50000），使用25个路径作为批次阈值
                                        return 500
                                    elif dir_count >= 1000:
                                        # 目录数量中等（1000-10000），使用50个路径作为批次阈值
                                        return 800
                                    else:
                                        # 目录数量少（<1000），使用100个路径作为批次阈值
                                        return 1000
                                
                                # 统计变量
                                dir_count = 0  # 目录计数
                                permission_error_count = 0  # 权限错误计数
                                last_batch_submit_time = None  # 上次提交批次的时间
                                last_progress_log_time = None  # 上次输出进度日志的时间
                                last_dir_log_time = None  # 上次输出目录日志的时间
                                last_log_count = 0  # 上次输出日志的路径数
                                current_dir = None  # 当前正在扫描的目录
                                
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
                                
                                # 用于检测取消的标志（在主线程中设置）
                                scan_cancelled = False
                                scan_error_info = None
                                
                                try:
                                    start_time = time.time()
                                    last_batch_submit_time = start_time
                                    last_progress_log_time = start_time
                                    last_dir_log_time = start_time
                                    
                                    # 获取主事件循环（在启动线程前获取）
                                    try:
                                        main_loop = asyncio.get_running_loop()
                                    except RuntimeError:
                                        main_loop = None
                                    
                                    # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                                    # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                                    batch = []
                                    # 性能优化：使用deque替代list，popleft()是O(1)操作，比pop(0)的O(n)快得多
                                    dirs_to_scan = deque([path])  # 待扫描的目录队列（使用deque提升性能）
                                    scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                                    # 优化：缓存路径字符串，避免重复解析
                                    dir_path_cache = {}  # {Path对象: 字符串路径} 缓存
                                    
                                    # 对于大量目录，调整日志频率
                                    LARGE_DIR_THRESHOLD = 10000  # 超过1万个目录时，使用更频繁的日志
                                    is_large_dir_structure = False
                                    
                                    try:
                                        # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                                        # 性能优化：使用deque.popleft()替代list.pop(0)，O(1)复杂度
                                        while dirs_to_scan:
                                            try:
                                                # 性能优化：deque.popleft()是O(1)操作，比list.pop(0)的O(n)快得多
                                                current_scan_dir = dirs_to_scan.popleft()
                                                
                                                # 性能优化：减少路径解析开销，使用缓存避免重复resolve
                                                if current_scan_dir in dir_path_cache:
                                                    current_scan_dir_str = dir_path_cache[current_scan_dir]
                                                else:
                                                    # 只在第一次访问时解析路径
                                                    try:
                                                        # 如果已经是字符串，直接使用；否则解析为绝对路径
                                                        if isinstance(current_scan_dir, str):
                                                            current_scan_dir_str = current_scan_dir
                                                        else:
                                                            current_scan_dir_str = str(current_scan_dir.resolve())
                                                        # 缓存解析结果
                                                        if not isinstance(current_scan_dir, str):
                                                            dir_path_cache[current_scan_dir] = current_scan_dir_str
                                                    except Exception:
                                                        # 解析失败，使用字符串表示
                                                        current_scan_dir_str = str(current_scan_dir)
                                                        if not isinstance(current_scan_dir, str):
                                                            dir_path_cache[current_scan_dir] = current_scan_dir_str
                                                
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
                                                    logger.info(f"{context_prefix}流式扫描：检测到大型目录结构（已扫描 {dir_count} 个目录，待扫描目录: {len(dirs_to_scan)}），批次阈值: {current_batch_threshold}，将使用更频繁的进度日志（源路径: {path}）")
                                                
                                                # 每2分钟输出一次当前扫描的目录（大型目录结构时每30秒输出一次）
                                                current_time = time.time()
                                                elapsed_since_dir_log = current_time - last_dir_log_time
                                                dir_log_interval = 30.0 if is_large_dir_structure else DIR_LOG_INTERVAL
                                                if current_dir and elapsed_since_dir_log >= dir_log_interval:
                                                    logger.info(f"{context_prefix}流式扫描：正在扫描目录 {format_path_for_log(current_dir)}，已扫描 {dir_count} 个目录，{total_paths_scanned} 个路径（源路径: {path}）")
                                                    last_dir_log_time = current_time
                                                
                                                try:
                                                    # 使用 os.scandir() 扫描当前目录
                                                    # 捕获所有错误（权限错误、IO错误等），记录日志后跳过
                                                    try:
                                                        entries = os.scandir(current_scan_dir_str)
                                                    except (PermissionError, OSError, FileNotFoundError, IOError) as scandir_err:
                                                        # 目录无法打开（权限不足、不存在等）：记录并跳过
                                                        permission_error_count += 1
                                                        try:
                                                            path_display = format_path_for_log(current_scan_dir_str)
                                                            if permission_error_count <= 20:
                                                                logger.warning(f"{context_prefix}流式扫描：无法打开目录（目录 #{permission_error_count}）: {path_display}，错误: {str(scandir_err)}")
                                                        except Exception:
                                                            logger.warning(f"{context_prefix}流式扫描：无法打开目录（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scandir_err)}")
                                                        continue
                                                    except Exception as scandir_err:
                                                        # 其他错误：记录并跳过
                                                        permission_error_count += 1
                                                        try:
                                                            path_display = format_path_for_log(current_scan_dir_str)
                                                            logger.warning(f"{context_prefix}流式扫描：扫描目录时出错（目录 #{permission_error_count}）: {path_display}，错误: {str(scandir_err)}")
                                                        except Exception:
                                                            logger.warning(f"{context_prefix}流式扫描：扫描目录时出错（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scandir_err)}")
                                                        continue
                                                    
                                                    with entries:
                                                        for entry in entries:
                                                            try:
                                                                # 检查是否被取消
                                                                if scan_cancelled:
                                                                    break
                                                                
                                                                # 优化：直接使用 entry.path 和 entry.stat()，不转换为 Path
                                                                entry_path_str = entry.path
                                                                
                                                                # 处理目录和文件
                                                                try:
                                                                    if entry.is_dir(follow_symlinks=False):
                                                                        # 目录：添加到待扫描队列（需要 Path 对象用于队列）
                                                                        dirs_to_scan.append(Path(entry_path_str))
                                                                    elif entry.is_file(follow_symlinks=False):
                                                                        # 文件：直接使用 entry.stat() 一次性获取文件信息，避免额外调用
                                                                        try:
                                                                            file_info = self.get_file_info_from_entry(entry)
                                                                            if file_info:
                                                                                # 将文件信息添加到批次（而不是只添加路径）
                                                                                batch.append(file_info)
                                                                                total_paths_scanned += 1
                                                                            else:
                                                                                # 文件信息获取失败，跳过
                                                                                permission_error_count += 1
                                                                                if permission_error_count <= 20:
                                                                                    logger.debug(f"{context_prefix}流式扫描：无法获取文件信息: {format_path_for_log(entry_path_str)}")
                                                                        except Exception as file_info_err:
                                                                            permission_error_count += 1
                                                                            if permission_error_count <= 20:
                                                                                logger.debug(f"{context_prefix}流式扫描：获取文件信息失败: {format_path_for_log(entry_path_str)}，错误: {str(file_info_err)}")
                                                                            continue
                                                                except (OSError, PermissionError) as entry_err:
                                                                    # 无法判断类型，尝试作为文件处理
                                                                    try:
                                                                        if entry.is_file(follow_symlinks=False):
                                                                            # 优化：直接使用 entry.stat() 一次性获取文件信息
                                                                            try:
                                                                                file_info = self.get_file_info_from_entry(entry)
                                                                                if file_info:
                                                                                    batch.append(file_info)
                                                                                    total_paths_scanned += 1
                                                                            except Exception:
                                                                                pass
                                                                        elif entry.is_dir(follow_symlinks=False):
                                                                            dirs_to_scan.append(Path(entry_path_str))
                                                                    except Exception:
                                                                        permission_error_count += 1
                                                                        if permission_error_count <= 20:
                                                                            logger.debug(f"{context_prefix}流式扫描：无法访问路径: {format_path_for_log(entry_path_str)}，错误: {str(entry_err)}")
                                                                        continue
                                                                
                                                                # 检查是否需要强制提交批次（即使没有达到阈值，也要定期提交）
                                                                current_time = time.time()
                                                                elapsed_since_last_batch = current_time - last_batch_submit_time
                                                                # 根据待扫描目录数量智能匹配批次阈值
                                                                batch_threshold = get_batch_threshold(len(dirs_to_scan))
                                                                
                                                                if len(batch) > 0 and elapsed_since_last_batch >= BATCH_FORCE_INTERVAL:
                                                                    # 强制提交当前批次（即使没有达到阈值，每20分钟强制提交一次）
                                                                    try:
                                                                        if main_loop:
                                                                            future = asyncio.run_coroutine_threadsafe(
                                                                                path_queue.put(batch.copy()),
                                                                                main_loop
                                                                            )
                                                                            future.result(timeout=10.0)
                                                                        else:
                                                                            asyncio.run(path_queue.put(batch.copy()))
                                                                        current_dir_display = format_path_for_log(current_dir) if current_dir else "未知"
                                                                        logger.info(f"{context_prefix}流式扫描：强制提交批次到队列（超过{BATCH_FORCE_INTERVAL}秒），路径数={len(batch)}，累计已扫描={total_paths_scanned}个路径，{dir_count}个目录，待扫描目录: {len(dirs_to_scan)}，批次阈值: {batch_threshold}，当前目录: {current_dir_display}（源路径: {path}）")
                                                                    except Exception as e:
                                                                        logger.error(f"{context_prefix}流式扫描：强制提交批次失败: {str(e)}", exc_info=True)
                                                                    batch = []
                                                                    last_batch_submit_time = current_time
                                                                
                                                                # 根据目录数量智能匹配批次阈值提交批次（正常提交）
                                                                elif len(batch) >= batch_threshold:
                                                                    try:
                                                                        if main_loop:
                                                                            future = asyncio.run_coroutine_threadsafe(
                                                                                path_queue.put(batch.copy()),
                                                                                main_loop
                                                                            )
                                                                            future.result(timeout=10.0)
                                                                        else:
                                                                            asyncio.run(path_queue.put(batch.copy()))
                                                                    except Exception as e:
                                                                        logger.warning(f"{context_prefix}流式扫描：放入路径队列失败: {str(e)}")
                                                                    batch = []
                                                                    last_batch_submit_time = current_time
                                                                
                                                                # 每10000个路径或每1分钟输出一次进度日志（大型目录结构时每30秒输出一次）
                                                                # 该进度日志仅用于调试，当前已停用（详见 backup_scanner 中的统计日志）
                                                            except (PermissionError, OSError, FileNotFoundError, IOError) as entry_err:
                                                                # 路径权限错误、不存在或IO错误：记录详细路径信息并跳过
                                                                permission_error_count += 1
                                                                try:
                                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                    path_display = format_path_for_log(path_str)
                                                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                                                        logger.warning(f"{context_prefix}流式扫描：路径错误（路径 #{permission_error_count}）: {path_display}，错误: {str(entry_err)}")
                                                                except Exception:
                                                                    if permission_error_count <= 20:
                                                                        logger.warning(f"{context_prefix}流式扫描：路径错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                                continue
                                                            except Exception as entry_err:
                                                                # 记录路径错误但继续扫描（不中止）
                                                                permission_error_count += 1
                                                                try:
                                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                                    path_display = format_path_for_log(path_str)
                                                                    if permission_error_count <= 20:
                                                                        logger.warning(f"{context_prefix}流式扫描：跳过路径错误（路径 #{permission_error_count}）: {path_display}，错误: {str(entry_err)}")
                                                                except Exception:
                                                                    if permission_error_count <= 20:
                                                                        logger.warning(f"{context_prefix}流式扫描：跳过路径错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                                continue
                                                except (PermissionError, OSError, FileNotFoundError, IOError) as scan_dir_err:
                                                    # 目录权限错误、不存在或IO错误：记录并跳过该目录（不中止）
                                                    permission_error_count += 1
                                                    try:
                                                        path_display = format_path_for_log(current_scan_dir_str)
                                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                                            logger.warning(f"{context_prefix}流式扫描：目录错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                                    except Exception:
                                                        if permission_error_count <= 20:
                                                            logger.warning(f"{context_prefix}流式扫描：目录错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                                    continue
                                                except Exception as scan_dir_err:
                                                    # 记录目录错误但继续扫描（不中止）
                                                    permission_error_count += 1
                                                    try:
                                                        path_display = format_path_for_log(current_scan_dir_str)
                                                        if permission_error_count <= 20:
                                                            logger.warning(f"{context_prefix}流式扫描：跳过目录错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                                    except Exception:
                                                        if permission_error_count <= 20:
                                                            logger.warning(f"{context_prefix}流式扫描：跳过目录错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                                    continue
                                                
                                                # 检查是否被取消
                                                if scan_cancelled:
                                                    break
                                                    
                                            except KeyboardInterrupt:
                                                # 在主线程中捕获 KeyboardInterrupt 时，会通过任务取消来通知
                                                # 这里记录但不抛出，继续执行 finally 块
                                                scan_cancelled = True
                                                scan_error_info = "用户中断（KeyboardInterrupt）"
                                                logger.warning(f"{context_prefix}流式扫描：遍历目录被中断 {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录")
                                                break
                                            except Exception as dir_scan_err:
                                                # 目录扫描错误：记录但继续（不中止）
                                                permission_error_count += 1
                                                try:
                                                    path_display = format_path_for_log(current_scan_dir_str) if current_scan_dir_str else "未知目录"
                                                    if permission_error_count <= 20:
                                                        logger.warning(f"{context_prefix}流式扫描：目录扫描错误（目录 #{permission_error_count}）: {path_display}，错误: {str(dir_scan_err)}")
                                                except Exception:
                                                    if permission_error_count <= 20:
                                                        logger.warning(f"{context_prefix}流式扫描：目录扫描错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(dir_scan_err)}")
                                                continue
                                        
                                        # 如果还有待扫描的目录，记录警告
                                        if dirs_to_scan and not scan_cancelled:
                                            logger.warning(f"{context_prefix}流式扫描：还有 {len(dirs_to_scan)} 个目录待扫描，但主循环已退出（源路径: {path}）")
                                            
                                    except KeyboardInterrupt:
                                        # 在主线程中捕获 KeyboardInterrupt 时，会通过任务取消来通知
                                        # 这里记录但不抛出，继续执行 finally 块
                                        scan_cancelled = True
                                        scan_error_info = "用户中断（KeyboardInterrupt）"
                                        logger.warning(f"{context_prefix}流式扫描：遍历目录被中断 {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录")
                                    except Exception as scan_err:
                                        # 扫描过程出错：记录但继续（不中止）
                                        scan_error_info = str(scan_err)
                                        permission_error_count += 1
                                        try:
                                            path_display = format_path_for_log(path)
                                            logger.warning(f"{context_prefix}流式扫描：扫描目录失败（目录 #{permission_error_count}）: {path_display}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，错误: {scan_error_info}")
                                        except Exception:
                                            logger.warning(f"{context_prefix}流式扫描：扫描目录失败（目录 #{permission_error_count}）: {path}，已扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，错误: {scan_error_info}")
                                        # 继续执行，不中止
                                    
                                    # 放入剩余的路径
                                    if batch:
                                        try:
                                            if main_loop:
                                                future = asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(batch),
                                                    main_loop
                                                )
                                                future.result(timeout=10.0)
                                            else:
                                                asyncio.run(path_queue.put(batch))
                                        except Exception:
                                            pass
                                    
                                    total_time = time.time() - start_time
                                    if scan_cancelled:
                                        logger.warning(f"{context_prefix}流式扫描：目录遍历被中断 {path}，共扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，权限错误: {permission_error_count} 个，总耗时 {total_time:.1f} 秒")
                                    else:
                                        logger.info(f"{context_prefix}流式扫描：目录树遍历完成，共扫描 {total_paths_scanned} 个路径，{dir_count} 个目录，权限错误: {permission_error_count} 个，总耗时 {total_time:.1f} 秒")
                                except KeyboardInterrupt:
                                    # 线程级别的 KeyboardInterrupt（很少发生，因为主线程会先捕获）
                                    scan_cancelled = True
                                    scan_error_info = "用户中断（KeyboardInterrupt）"
                                    logger.warning(f"{context_prefix}流式扫描：线程被中断 {path}，已扫描 {total_paths_scanned} 个路径")
                                except Exception as e:
                                    scan_error_info = str(e)
                                    logger.error(f"{context_prefix}流式扫描：遍历目录失败 {path}: {str(e)}", exc_info=True)
                                finally:
                                    # 重要：无论是否异常，都确保发送停止信号到队列，让主循环知道线程已退出
                                    scan_done = True
                                    try:
                                        # 发送停止信号（包含总路径数和取消标志）
                                        try:
                                            if main_loop:
                                                # 如果被取消，发送特殊的停止信号
                                                if scan_cancelled:
                                                    # 发送取消信号
                                                    future = asyncio.run_coroutine_threadsafe(
                                                        path_queue.put(('CANCELLED', scan_error_info, total_paths_scanned)),
                                                        main_loop
                                                    )
                                                    future.result(timeout=10.0)
                                                    logger.warning(f"{context_prefix}流式扫描：发送取消信号到队列（源路径: {path}），错误: {scan_error_info}，已扫描: {total_paths_scanned} 个路径")
                                                else:
                                                    # 正常完成信号
                                                    future = asyncio.run_coroutine_threadsafe(
                                                        path_queue.put((stop_signal, total_paths_scanned)),
                                                        main_loop
                                                    )
                                                    future.result(timeout=10.0)
                                            else:
                                                if scan_cancelled:
                                                    asyncio.run(path_queue.put(('CANCELLED', scan_error_info, total_paths_scanned)))
                                                else:
                                                    asyncio.run(path_queue.put((stop_signal, total_paths_scanned)))
                                        except Exception as signal_err:
                                            logger.error(f"{context_prefix}流式扫描：发送停止信号失败: {str(signal_err)}", exc_info=True)
                                    except Exception as finally_err:
                                        # finally 块中的异常不应该阻止信号发送，但需要记录
                                        logger.error(f"{context_prefix}流式扫描：finally 块中发生异常（源路径: {path}）: {str(finally_err)}", exc_info=True)
                                        # 尝试最后一次发送信号（使用最简单的方式）
                                        try:
                                            if main_loop:
                                                asyncio.run_coroutine_threadsafe(
                                                    path_queue.put(('CANCELLED', f"finally块异常: {str(finally_err)}", total_paths_scanned)),
                                                    main_loop
                                                )
                                        except Exception:
                                            pass
                            
                            # 在线程池中启动遍历任务
                            scan_task = asyncio.create_task(
                                asyncio.to_thread(sync_rglob_worker)
                            )
                            
                            # 从队列中逐步获取路径并yield
                            total_paths_count = 0  # 统计总路径数
                            try:
                                while True:
                                    try:
                                        # 检查任务是否被取消（Ctrl+C）
                                        try:
                                            current_task = asyncio.current_task()
                                            if current_task and current_task.cancelled():
                                                logger.warning(f"{context_prefix}流式扫描循环：检测到任务已被取消（Ctrl+C）")
                                                # 取消后台扫描任务
                                                if not scan_task.done():
                                                    scan_task.cancel()
                                                break
                                        except RuntimeError:
                                            # 如果没有当前任务，可能已经被取消
                                            logger.warning(f"{context_prefix}流式扫描循环：检测到任务可能已被取消")
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            break
                                        
                                        # 等待路径或停止信号（带超时，避免无限等待）- 超时时间：20分钟（1200秒）
                                        try:
                                            item = await asyncio.wait_for(path_queue.get(), timeout=1200.0)
                                        except asyncio.TimeoutError:
                                            # 超时检查扫描任务是否完成
                                            if scan_task.done() or scan_done:
                                                # 尝试获取队列中剩余的所有路径
                                                while not path_queue.empty():
                                                    try:
                                                        item = path_queue.get_nowait()
                                                        # 检查是否是停止信号或取消信号
                                                        if isinstance(item, tuple):
                                                            if item[0] is stop_signal:
                                                                total_paths_count = item[1] if len(item) > 1 else total_paths_count
                                                                break
                                                            elif item[0] == 'CANCELLED':
                                                                # 取消信号
                                                                error_msg = item[1] if len(item) > 1 else "用户中断"
                                                                scanned_count = item[2] if len(item) > 2 else total_paths_count
                                                                logger.warning(f"{context_prefix}流式扫描：收到取消信号，错误: {error_msg}，已扫描: {scanned_count} 个路径")
                                                                break
                                                        elif item is stop_signal:
                                                            break
                                                        # item 是一个文件信息字典列表（批次），已经在扫描时获取了文件信息
                                                        for file_info in item:
                                                            yield file_info
                                                            total_paths_count += 1
                                                    except asyncio.QueueEmpty:
                                                        break
                                                break
                                            # 如果扫描任务还在运行，继续等待
                                            continue
                                        except asyncio.CancelledError:
                                            # 任务被取消（Ctrl+C）
                                            logger.warning(f"{context_prefix}流式扫描循环：任务被取消（CancelledError）")
                                            if not scan_task.done():
                                                scan_task.cancel()
                                            raise
                                        
                                        # 检查是否是停止信号或取消信号（可能是元组格式：(stop_signal, total_paths) 或 ('CANCELLED', error_msg, count)）
                                        if isinstance(item, tuple):
                                            if item[0] is stop_signal:
                                                total_paths_count = item[1] if len(item) > 1 else total_paths_count
                                                break
                                            elif item[0] == 'CANCELLED':
                                                # 取消信号
                                                error_msg = item[1] if len(item) > 1 else "用户中断"
                                                scanned_count = item[2] if len(item) > 2 else total_paths_count
                                                logger.warning(f"{context_prefix}流式扫描：收到取消信号，错误: {error_msg}，已扫描: {scanned_count} 个路径")
                                                # 取消后台扫描任务
                                                if not scan_task.done():
                                                    scan_task.cancel()
                                                break
                                        elif item is stop_signal:
                                            break
                                        
                                        # item 是一个文件信息字典列表（批次），已经在扫描时获取了文件信息
                                        for file_info in item:
                                            yield file_info
                                            total_paths_count += 1
                                            
                                    except asyncio.CancelledError:
                                        # 任务被取消（Ctrl+C）
                                        logger.warning(f"{context_prefix}流式扫描循环：任务被取消（CancelledError）")
                                        if not scan_task.done():
                                            scan_task.cancel()
                                        raise
                                    except Exception as e:
                                        logger.error(f"{context_prefix}流式扫描：处理批次失败: {str(e)}", exc_info=True)
                                        # 检查扫描任务状态
                                        if scan_task.done():
                                            break
                                        continue
                            except asyncio.CancelledError:
                                # 任务被取消（Ctrl+C）
                                logger.warning(f"{context_prefix}流式扫描：任务被取消")
                                if not scan_task.done():
                                    scan_task.cancel()
                                raise
                            except KeyboardInterrupt:
                                # 用户中断
                                logger.warning(f"{context_prefix}流式扫描：用户中断（KeyboardInterrupt）")
                                if not scan_task.done():
                                    scan_task.cancel()
                                raise
                            finally:
                                # 确保扫描任务完成
                                if not scan_task.done():
                                    scan_task.cancel()
                                    try:
                                        await scan_task
                                    except (asyncio.CancelledError, Exception):
                                        pass
                            
                            # 返回总路径数（用于统计）
                            logger.info(f"目录遍历完成，共处理 {total_paths_count} 个路径")
                        
                        # 防错机制配置（与后台扫描任务一致）
                        MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                        MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                        path_too_long_count = 0  # 路径过长计数
                        permission_error_count = 0  # 权限错误计数
                        
                        def truncate_path(path_str: str, max_len: int = MAX_PATH_DISPLAY) -> str:
                            """截断路径以便在日志中显示"""
                            if len(path_str) <= max_len:
                                return path_str
                            # 保留开头和结尾
                            prefix_len = max_len - 50
                            return f"{path_str[:prefix_len]}...{path_str[-(max_len-prefix_len-3):]}"
                        
                        def format_path_for_log(path_str: str) -> str:
                            """格式化路径以便在日志中显示，考虑长度限制"""
                            try:
                                # 检查路径长度
                                path_len = len(path_str)
                                if path_len > MAX_PATH_LENGTH:
                                    return f"{truncate_path(path_str)} (路径长度: {path_len} 字符，超过Windows限制 {MAX_PATH_LENGTH} 字符)"
                                return truncate_path(path_str)
                            except Exception:
                                return str(path_str)[:MAX_PATH_DISPLAY]
                        
                        # 使用异步生成器逐步遍历目录
                        # 优化：async_rglob_generator 现在直接返回文件信息字典（已在扫描时获取）
                        async for file_info in async_rglob_generator(source_path):
                            try:
                                # 文件信息已经在扫描时获取，直接使用
                                if not file_info:
                                    continue
                                
                                path_str = file_info.get('path', '')
                                
                                # 路径长度检查
                                if path_str:
                                    path_len = len(path_str)
                                    if path_len > MAX_PATH_LENGTH:
                                        path_too_long_count += 1
                                        if path_too_long_count <= 10:  # 只记录前10个路径过长的情况
                                            logger.warning(f"压缩扫描：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(path_str)}")
                                        continue
                                
                                # 检查文件路径是否匹配排除规则
                                if path_str and self.should_exclude_file(path_str, exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                # 确保是文件（虽然扫描时已经过滤，但再次确认）
                                if file_info.get('is_file', True):
                                    scanned_count += 1
                                    total_scanned += 1
                                    
                                    # 每扫描10000个文件输出一次进度（避免日志过多）
                                    if scanned_count % 10000 == 0:
                                        logger.info(f"{context_prefix}压缩扫描：已扫描 {scanned_count} 个文件（当前源路径，仅为当前源路径的文件数），找到 {total_valid_files} 个有效文件（当前批次: {len(current_batch)} 个）...")
                                    
                                    # 每扫描50个文件更新一次进度（使用后台扫描任务提供的 total_files）
                                    if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                        # 从数据库读取最新的 total_files（由后台扫描任务更新）
                                        if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                            # 使用后台扫描任务提供的 total_files 计算进度
                                            scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                            backup_task.progress_percent = scan_progress
                                        else:
                                            # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                            backup_task.progress_percent = 0.0
                                        await self.update_progress_callback(backup_task, total_scanned, len(current_batch), "[扫描文件中...]")
                                    
                                    # 文件信息已经在扫描时获取，直接使用（不再调用 get_file_info）
                                    try:
                                        if file_info:
                                            path_str = file_info.get('path', '')
                                            if len(path_str) > MAX_PATH_LENGTH:
                                                path_too_long_count += 1
                                                if path_too_long_count <= 20:
                                                    logger.warning(f"压缩扫描：跳过路径过长文件（{len(path_str)} 字符）: {format_path_for_log(path_str)}")
                                                continue
                                            if not self.should_exclude_file(path_str, exclude_patterns):
                                                current_batch.append(file_info)
                                                total_valid_files += 1  # 累计有效文件数
                                                total_scanned_size += file_info.get('size', 0) or 0
                                                if len(current_batch) >= batch_size:
                                                    yield current_batch
                                                    current_batch = []
                                            else:
                                                excluded_count += 1
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                        # 文件权限错误、不存在或IO错误：记录详细路径信息并跳过（不中止）
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(path_str) if path_str else "未知路径"
                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                            logger.warning(f"压缩扫描：文件错误（文件 #{permission_error_count}）: {path_display}，错误: {str(file_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(path_str if path_str else 'unknown')
                                        continue
                                    except Exception as file_error:
                                        # 其他错误：记录并跳过该文件（不中止）
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(path_str) if path_str else "未知路径"
                                        if permission_error_count <= 20:
                                            logger.warning(f"压缩扫描：跳过出错的文件（文件 #{permission_error_count}）: {path_display}，错误: {str(file_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(path_str if path_str else 'unknown')
                                        continue
                                elif file_path.is_dir():
                                    try:
                                        # 检查目录是否匹配排除规则（使用已字符串化的路径）
                                        if file_path_str and self.should_exclude_file(file_path_str, exclude_patterns):
                                            skipped_dirs += 1
                                            # 跳过该目录下的所有文件（rglob会继续，但我们在文件检查时会跳过）
                                            continue
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as dir_error:
                                        # 目录权限错误、不存在或IO错误：记录详细路径信息并跳过（不中止）
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        if permission_error_count <= 20:  # 只记录前20个权限错误
                                            logger.warning(f"压缩扫描：目录错误（目录 #{permission_error_count}）: {path_display}，错误: {str(dir_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                                    except Exception as dir_error:
                                        # 其他错误：记录并跳过该目录（不中止）
                                        permission_error_count += 1
                                        error_count += 1
                                        path_display = format_path_for_log(file_path_str) if file_path_str else "未知路径"
                                        if permission_error_count <= 20:
                                            logger.warning(f"压缩扫描：跳过出错的目录（目录 #{permission_error_count}）: {path_display}，错误: {str(dir_error)}")
                                        if len(error_paths) < 50:  # 只记录前50个错误路径
                                            error_paths.append(file_path_str if file_path_str else str(file_path))
                                        continue
                            except (PermissionError, OSError, FileNotFoundError, IOError) as path_error:
                                # 路径权限错误、不存在或IO错误：记录详细路径信息并跳过（不中止）
                                permission_error_count += 1
                                error_count += 1
                                try:
                                    path_str = str(file_path) if 'file_path' in locals() else "未知路径"
                                    path_display = format_path_for_log(path_str)
                                    if permission_error_count <= 20:  # 只记录前20个权限错误
                                        logger.warning(f"压缩扫描：路径错误（路径 #{permission_error_count}）: {path_display}，错误: {str(path_error)}")
                                except Exception:
                                    if permission_error_count <= 20:
                                        logger.warning(f"压缩扫描：路径错误（路径 #{permission_error_count}）: 无法获取路径，错误: {str(path_error)}")
                                if len(error_paths) < 50:  # 只记录前50个错误路径
                                    try:
                                        error_paths.append(str(file_path) if 'file_path' in locals() else "未知路径")
                                    except Exception:
                                        pass
                                continue
                            except Exception as path_error:
                                # 其他错误：记录并跳过该路径（不中止）
                                permission_error_count += 1
                                error_count += 1
                                try:
                                    path_str = str(file_path) if 'file_path' in locals() else "未知路径"
                                    path_display = format_path_for_log(path_str)
                                    if permission_error_count <= 20:
                                        logger.warning(f"压缩扫描：跳过出错的路径（路径 #{permission_error_count}）: {path_display}，错误: {str(path_error)}")
                                except Exception:
                                    if permission_error_count <= 20:
                                        logger.warning(f"压缩扫描：跳过出错的路径（路径 #{permission_error_count}）: 无法获取路径，错误: {str(path_error)}")
                                if len(error_paths) < 50:  # 只记录前50个错误路径
                                    try:
                                        error_paths.append(str(file_path) if 'file_path' in locals() else "未知路径")
                                    except Exception:
                                        pass
                                continue
                            
                            # 每处理100个文件后，yield控制权，避免长时间阻塞
                            if total_scanned % 100 == 0:
                                await asyncio.sleep(0)  # 让出控制权
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        # 确保错误路径被完整记录到日志中
                        error_path_display = format_path_for_log(source_path_str) if 'format_path_for_log' in globals() else source_path_str
                        logger.error(
                            f"⚠️ 扫描目录时发生访问错误，跳过目录: {error_path_display}，"
                            f"错误类型: {type(scan_error).__name__}，错误信息: {str(scan_error)}，继续扫描其他路径"
                        )
                        continue
                    except asyncio.TimeoutError as timeout_error:
                        # 超时错误：可能是队列操作超时或扫描器阻塞
                        # 注意：如果目录很大（50万+子目录），可能需要很长时间，这不是真正的错误
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        error_path_display = format_path_for_log(source_path_str) if 'format_path_for_log' in globals() else source_path_str
                        error_msg = str(timeout_error) if str(timeout_error) else "队列操作或扫描器超时"
                        logger.error(
                            f"⚠️ 扫描目录时发生超时错误，跳过目录: {error_path_display}，"
                            f"错误类型: TimeoutError，错误信息: {error_msg}，"
                            f"可能原因：1) 目录非常大（50万+子目录）需要更长时间 2) 队列操作超时 3) 扫描器阻塞，继续扫描其他路径"
                        )
                        # 记录完整的异常堆栈（DEBUG级别）
                        logger.debug(f"扫描目录超时错误详情: 目录={error_path_display}", exc_info=True)
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        # 确保错误路径被完整记录到日志中
                        error_path_display = format_path_for_log(source_path_str) if 'format_path_for_log' in globals() else source_path_str
                        error_msg = str(e) if str(e) else f"{type(e).__name__}异常（无详细信息）"
                        logger.error(
                            f"⚠️ 扫描目录时发生错误，跳过目录: {error_path_display}，"
                            f"错误类型: {type(e).__name__}，错误信息: {error_msg}，继续扫描其他路径"
                        )
                        # 记录完整的异常堆栈（DEBUG级别）
                        logger.debug(f"扫描目录错误详情: 目录={error_path_display}", exc_info=True)
                        continue
                    
                    # 根据使用的扫描方式添加前缀
                    scan_type_prefix = "[多线程扫描]" if use_concurrent_scanner else ("[顺序扫描]" if use_sequential_scanner else "")
                    logger.info(f"{scan_type_prefix} 目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 累计有效 {total_valid_files} 个（当前批次: {len(current_batch)} 个）, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个, 权限错误: {permission_error_count} 个, 路径过长: {path_too_long_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0 or permission_error_count > 0 or path_too_long_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限错误: {permission_error_count} 个, 路径过长: {path_too_long_count} 个），已跳过这些文件/目录")
                        if len(error_paths) > 0:
                            if len(error_paths) <= 10:
                                logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                            else:
                                logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue
        
        # 返回最后一批文件
        if current_batch:
            yield current_batch
        
        # 扫描完成，更新进度
        # 注意：不再在这里更新 total_bytes_actual 和 total_scanned_files
        # 这些统计由独立的后台扫描任务 _scan_for_progress_update 负责更新

        if backup_task and self.update_progress_callback:
            backup_task.progress_percent = 10.0
            await self.update_progress_callback(backup_task, total_scanned, total_scanned, "[准备压缩...]")
        
        # 根据使用的扫描方式添加前缀
        scan_type_prefix = scan_type_info if scan_type_info else ""
        logger.info(f"========== {scan_type_prefix} 扫描完成 ==========")
        logger.info(f"{scan_type_prefix} 共扫描 {total_scanned} 个文件，找到 {total_valid_files} 个有效文件，总大小 {total_scanned_size:,} 字节")
        if exclude_patterns:
            logger.info(f"{scan_type_prefix} 排除规则: {exclude_patterns}")
        logger.info(f"========== {scan_type_prefix} 扫描完成 ==========")
    
    async def scan_source_files(
        self, 
        source_paths: List[str], 
        exclude_patterns: List[str], 
        backup_task: Optional[object] = None
    ) -> List[Dict]:
        """扫描源文件（兼容旧接口，收集所有文件后返回）
        
        Args:
            source_paths: 源路径列表
            exclude_patterns: 排除模式列表
            backup_task: 备份任务对象（可选，用于进度更新）
            
        Returns:
            List[Dict]: 文件列表
        """
        file_list = []
        
        if not source_paths:
            logger.warning("源路径列表为空")
            return file_list

        # 不再估算总文件数，直接使用后台扫描任务提供的 total_files
        # 后台扫描任务会独立扫描并更新数据库中的 total_files 和 total_bytes
        # 这样可以避免重复扫描，特别是对于包含大量目录的场景（如40.5万个目录）
        
        total_scanned = 0
        
        for idx, source_path_str in enumerate(source_paths):
            logger.info(f"扫描源路径 {idx + 1}/{len(source_paths)}: {source_path_str}")
            source_path = Path(source_path_str)
            
            # 检查路径是否存在
            if not source_path.exists():
                logger.warning(f"源路径不存在，跳过: {source_path_str}")
                continue
            
            try:
                if source_path.is_file():
                    logger.debug(f"扫描文件: {source_path_str}")
                    try:
                        file_info = await self.get_file_info(source_path)
                        if file_info:
                            # 排除规则从计划任务获取（scheduled_task.action_config.exclude_patterns）
                            if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                file_list.append(file_info)
                                logger.debug(f"已添加文件: {file_info['path']}")
                        
                        total_scanned += 1
                        # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                        # 后台扫描任务会独立扫描并更新数据库中的 total_files，这里只需要更新已扫描的文件数
                        if backup_task and self.update_progress_callback:
                            # 从数据库读取最新的 total_files（由后台扫描任务更新）
                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                # 使用后台扫描任务提供的 total_files 计算进度
                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                backup_task.progress_percent = scan_progress
                            else:
                                # 如果后台扫描任务还没有更新 total_files，只更新状态，不显示具体进度
                                backup_task.progress_percent = 0.0
                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                    except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                        # 文件访问错误，跳过该文件，继续扫描
                        logger.warning(f"⚠️ 跳过无法访问的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                    except Exception as file_error:
                        # 其他错误，也跳过该文件
                        logger.warning(f"⚠️ 跳过出错的文件: {source_path_str} (错误: {str(file_error)})")
                        continue
                        
                elif source_path.is_dir():
                    logger.info(f"扫描目录: {source_path_str}")
                    
                    # 检查目录本身是否匹配排除规则
                    if self.should_exclude_file(str(source_path), exclude_patterns):
                        logger.info(f"目录匹配排除规则，跳过整个目录: {source_path_str}")
                        continue
                    
                    scanned_count = 0
                    excluded_count = 0
                    skipped_dirs = 0
                    error_count = 0
                    error_paths = []
                    
                    # 使用 os.scandir() 替代 rglob() 以提高性能（特别是对于大量目录）
                    # os.scandir() 在 Windows 上比 rglob() 更快，且内存占用更少
                    MAX_PATH_LENGTH = 260  # Windows路径最大长度（字符）
                    MAX_PATH_DISPLAY = 200  # 日志中显示的最大路径长度（字符）
                    
                    def truncate_path(path_str: str, max_len: int = MAX_PATH_DISPLAY) -> str:
                        """截断路径以便在日志中显示"""
                        if len(path_str) <= max_len:
                            return path_str
                        prefix_len = max_len - 50
                        return f"{path_str[:prefix_len]}...{path_str[-(max_len-prefix_len-3):]}"
                    
                    def format_path_for_log(path_str: str) -> str:
                        """格式化路径以便在日志中显示，考虑长度限制"""
                        try:
                            path_len = len(path_str)
                            if path_len > MAX_PATH_LENGTH:
                                return f"{truncate_path(path_str)} (路径长度: {path_len} 字符，超过Windows限制 {MAX_PATH_LENGTH} 字符)"
                            return truncate_path(path_str)
                        except Exception:
                            return str(path_str)[:MAX_PATH_DISPLAY]
                    
                    try:
                        # 使用迭代方式遍历目录（避免 rglob 的内存问题）
                        # 性能优化：使用deque替代list，popleft()是O(1)操作，比pop(0)的O(n)快得多
                        dirs_to_scan = deque([source_path])  # 待扫描的目录队列（使用deque提升性能）
                        scanned_dirs = set()  # 已扫描的目录集合（避免重复扫描）
                        # 优化：缓存路径字符串，避免重复解析
                        dir_path_cache = {}  # {Path对象: 字符串路径} 缓存
                        
                        while dirs_to_scan:
                            try:
                                # 性能优化：deque.popleft()是O(1)操作，比list.pop(0)的O(n)快得多
                                current_scan_dir = dirs_to_scan.popleft()
                                
                                # 性能优化：减少路径解析开销，使用缓存避免重复resolve
                                if current_scan_dir in dir_path_cache:
                                    current_scan_dir_str = dir_path_cache[current_scan_dir]
                                else:
                                    # 只在第一次访问时解析路径
                                    try:
                                        # 如果已经是字符串，直接使用；否则解析为绝对路径
                                        if isinstance(current_scan_dir, str):
                                            current_scan_dir_str = current_scan_dir
                                        else:
                                            current_scan_dir_str = str(current_scan_dir.resolve())
                                        # 缓存解析结果
                                        if not isinstance(current_scan_dir, str):
                                            dir_path_cache[current_scan_dir] = current_scan_dir_str
                                    except Exception:
                                        # 解析失败，使用字符串表示
                                        current_scan_dir_str = str(current_scan_dir)
                                        if not isinstance(current_scan_dir, str):
                                            dir_path_cache[current_scan_dir] = current_scan_dir_str
                                
                                if current_scan_dir_str in scanned_dirs:
                                    continue
                                
                                scanned_dirs.add(current_scan_dir_str)
                                
                                # 检查目录是否匹配排除规则
                                if self.should_exclude_file(current_scan_dir_str, exclude_patterns):
                                    skipped_dirs += 1
                                    continue
                                
                                try:
                                    # 使用 os.scandir() 扫描当前目录
                                    # 捕获所有错误（权限错误、IO错误等），记录日志后跳过
                                    try:
                                        entries = os.scandir(current_scan_dir_str)
                                    except (PermissionError, OSError, FileNotFoundError, IOError) as scandir_err:
                                        # 目录无法打开（权限不足、不存在等）：记录并跳过
                                        error_count += 1
                                        permission_error_count += 1
                                        try:
                                            path_display = format_path_for_log(current_scan_dir_str)
                                            if permission_error_count <= 20:
                                                logger.warning(f"扫描：无法打开目录（目录 #{permission_error_count}）: {path_display}，错误: {str(scandir_err)}")
                                        except Exception:
                                            if permission_error_count <= 20:
                                                logger.warning(f"扫描：无法打开目录（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scandir_err)}")
                                        continue
                                    except Exception as scandir_err:
                                        # 其他错误：记录并跳过
                                        error_count += 1
                                        permission_error_count += 1
                                        try:
                                            path_display = format_path_for_log(current_scan_dir_str)
                                            if permission_error_count <= 20:
                                                logger.warning(f"扫描：扫描目录时出错（目录 #{permission_error_count}）: {path_display}，错误: {str(scandir_err)}")
                                        except Exception:
                                            if permission_error_count <= 20:
                                                logger.warning(f"扫描：扫描目录时出错（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scandir_err)}")
                                        continue
                                    
                                    with entries:
                                        for entry in entries:
                                            try:
                                                # 优化：直接使用 entry.path 和 entry.stat()，不转换为 Path（除非需要）
                                                entry_path_str = entry.path
                                                
                                                # 检查路径长度
                                                try:
                                                    path_len = len(entry_path_str)
                                                    if path_len > MAX_PATH_LENGTH:
                                                        error_count += 1
                                                        if error_count <= 10:
                                                            logger.warning(f"扫描：路径过长（{path_len} 字符 > {MAX_PATH_LENGTH} 字符）: {format_path_for_log(entry_path_str)}")
                                                        continue
                                                except Exception as path_str_err:
                                                    logger.debug(f"扫描：路径字符串化失败: {str(path_str_err)}")
                                                    continue
                                                
                                                # 处理目录和文件
                                                try:
                                                    if entry.is_dir(follow_symlinks=False):
                                                        # 目录：添加到待扫描队列（需要 Path 对象用于队列）
                                                        dirs_to_scan.append(Path(entry_path_str))
                                                    elif entry.is_file(follow_symlinks=False):
                                                        # 文件：检查排除规则并处理
                                                        # 检查文件路径的父目录是否匹配排除规则
                                                        try:
                                                            parent_path = str(Path(entry_path_str).parent)
                                                            if self.should_exclude_file(parent_path, exclude_patterns):
                                                                skipped_dirs += 1
                                                                continue
                                                        except Exception:
                                                            pass
                                                        
                                                        scanned_count += 1
                                                        total_scanned += 1
                                                        
                                                        # 每扫描100个文件输出一次进度并更新数据库
                                                    if scanned_count % 10000 == 0:
                                                        logger.info(f"{context_prefix}压缩扫描：已扫描 {scanned_count} 个文件（当前源路径，仅为当前源路径的文件数），找到 {len(file_list)} 个有效文件...")
                                                        
                                                        # 每扫描50个文件更新一次进度（避免过于频繁）
                                                        if total_scanned % 50 == 0 and backup_task and self.update_progress_callback:
                                                            # 更新扫描进度（使用后台扫描任务提供的 total_files，如果没有则只更新状态）
                                                            if hasattr(backup_task, 'total_files') and backup_task.total_files and backup_task.total_files > 0:
                                                                scan_progress = min(10.0, (total_scanned / backup_task.total_files) * 10.0)
                                                                backup_task.progress_percent = scan_progress
                                                            else:
                                                                backup_task.progress_percent = 0.0
                                                            await self.update_progress_callback(backup_task, total_scanned, len(file_list))
                                                        
                                                        # 优化：直接使用 entry.stat() 一次性获取文件信息，避免额外调用
                                                        try:
                                                            file_info = self.get_file_info_from_entry(entry)
                                                            if file_info:
                                                                # 排除规则从计划任务获取
                                                                if not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                                    file_list.append(file_info)
                                                                else:
                                                                    excluded_count += 1
                                                        except (PermissionError, OSError, FileNotFoundError, IOError) as file_error:
                                                            error_count += 1
                                                            error_paths.append(entry_path_str)
                                                            if error_count <= 20:
                                                                logger.warning(f"⚠️ 跳过无法访问的文件: {format_path_for_log(entry_path_str)} (错误: {str(file_error)})")
                                                            continue
                                                        except Exception as file_error:
                                                            error_count += 1
                                                            error_paths.append(entry_path_str)
                                                            if error_count <= 20:
                                                                logger.warning(f"⚠️ 跳过出错的文件: {format_path_for_log(entry_path_str)} (错误: {str(file_error)})")
                                                            continue
                                                except (OSError, PermissionError) as entry_err:
                                                    # 无法判断类型，尝试作为文件处理
                                                    try:
                                                        if entry.is_file(follow_symlinks=False):
                                                            scanned_count += 1
                                                            total_scanned += 1
                                                            # 优化：直接使用 entry.stat() 一次性获取文件信息
                                                            file_info = self.get_file_info_from_entry(entry)
                                                            if file_info and not self.should_exclude_file(file_info['path'], exclude_patterns):
                                                                file_list.append(file_info)
                                                        elif entry.is_dir(follow_symlinks=False):
                                                            dirs_to_scan.append(Path(entry_path_str))
                                                    except Exception:
                                                        error_count += 1
                                                        if error_count <= 20:
                                                            logger.debug(f"扫描：无法访问路径: {format_path_for_log(entry_path_str)}，错误: {str(entry_err)}")
                                                        continue
                                                    
                                            except (PermissionError, OSError, FileNotFoundError, IOError) as entry_err:
                                                # 路径权限错误、不存在或IO错误：记录并跳过（不中止）
                                                error_count += 1
                                                try:
                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：路径错误（路径 #{error_count}）: {format_path_for_log(path_str)}，错误: {str(entry_err)}")
                                                except Exception:
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：路径错误（路径 #{error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                continue
                                            except Exception as entry_err:
                                                # 其他错误：记录并跳过（不中止）
                                                error_count += 1
                                                try:
                                                    path_str = str(entry.path) if hasattr(entry, 'path') else "未知路径"
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：跳过路径错误（路径 #{error_count}）: {format_path_for_log(path_str)}，错误: {str(entry_err)}")
                                                except Exception:
                                                    if error_count <= 20:
                                                        logger.warning(f"扫描：跳过路径错误（路径 #{error_count}）: 无法获取路径，错误: {str(entry_err)}")
                                                continue
                                except (PermissionError, OSError, FileNotFoundError, IOError) as scan_dir_err:
                                    # 目录权限错误、不存在或IO错误：记录并跳过该目录（不中止）
                                    error_count += 1
                                    permission_error_count += 1
                                    error_paths.append(current_scan_dir_str)
                                    try:
                                        path_display = format_path_for_log(current_scan_dir_str)
                                        if permission_error_count <= 20:
                                            logger.warning(f"扫描：目录错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                    except Exception:
                                        if permission_error_count <= 20:
                                            logger.warning(f"扫描：目录错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                    continue
                                except Exception as scan_dir_err:
                                    # 其他错误：记录并跳过（不中止）
                                    error_count += 1
                                    permission_error_count += 1
                                    error_paths.append(current_scan_dir_str)
                                    try:
                                        path_display = format_path_for_log(current_scan_dir_str)
                                        if permission_error_count <= 20:
                                            logger.warning(f"扫描：跳过目录错误（目录 #{permission_error_count}）: {path_display}，错误: {str(scan_dir_err)}")
                                    except Exception:
                                        if permission_error_count <= 20:
                                            logger.warning(f"扫描：跳过目录错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(scan_dir_err)}")
                                    continue
                                    
                            except Exception as dir_scan_err:
                                # 其他目录扫描错误：记录但继续（不中止）
                                error_count += 1
                                permission_error_count += 1
                                try:
                                    path_display = format_path_for_log(current_scan_dir_str) if current_scan_dir_str else "未知目录"
                                    if permission_error_count <= 20:
                                        logger.warning(f"扫描：目录扫描错误（目录 #{permission_error_count}）: {path_display}，错误: {str(dir_scan_err)}")
                                except Exception:
                                    if permission_error_count <= 20:
                                        logger.warning(f"扫描：目录扫描错误（目录 #{permission_error_count}）: 无法获取路径，错误: {str(dir_scan_err)}")
                                continue
                                
                    except (PermissionError, OSError, FileNotFoundError, IOError) as scan_error:
                        # 扫描目录时的访问错误，记录但继续扫描其他目录
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        # 确保错误路径被完整记录到日志中
                        error_path_display = format_path_for_log(source_path_str) if 'format_path_for_log' in globals() else source_path_str
                        logger.error(
                            f"⚠️ 扫描目录时发生访问错误，跳过目录: {error_path_display}，"
                            f"错误类型: {type(scan_error).__name__}，错误信息: {str(scan_error)}，继续扫描其他路径"
                        )
                        continue
                    except Exception as e:
                        # 其他扫描错误，记录但继续
                        error_count += 1
                        error_paths.append(str(source_path_str))
                        # 确保错误路径被完整记录到日志中
                        error_path_display = format_path_for_log(source_path_str) if 'format_path_for_log' in globals() else source_path_str
                        logger.error(
                            f"⚠️ 扫描目录时发生错误，跳过目录: {error_path_display}，"
                            f"错误类型: {type(e).__name__}，错误信息: {str(e)}，继续扫描其他路径"
                        )
                        # 记录完整的异常堆栈（DEBUG级别）
                        logger.debug(f"扫描目录错误详情: 目录={error_path_display}", exc_info=True)
                        continue
                    
                    logger.info(f"目录扫描完成: {source_path_str}, 扫描 {scanned_count} 个文件, 有效 {len(file_list)} 个, 排除 {excluded_count} 个文件, 跳过 {skipped_dirs} 个目录/文件, 错误 {error_count} 个")
                    if excluded_count > 0 or skipped_dirs > 0:
                        logger.warning(f"⚠️ 注意：已排除 {excluded_count} 个文件，跳过 {skipped_dirs} 个目录/文件（排除规则: {exclude_patterns if exclude_patterns else '无'}）")
                    if error_count > 0:
                        logger.warning(f"⚠️ 注意：遇到 {error_count} 个错误（权限、访问等），已跳过这些文件/目录")
                        if len(error_paths) <= 10:
                            logger.warning(f"⚠️ 错误路径示例: {', '.join(error_paths[:10])}")
                        else:
                            logger.warning(f"⚠️ 错误路径示例（前10个）: {', '.join(error_paths[:10])}... 共 {len(error_paths)} 个")
                else:
                    logger.warning(f"源路径既不是文件也不是目录: {source_path_str}")
            except Exception as e:
                logger.error(f"处理源路径失败 {source_path_str}: {str(e)}")
                continue

        return file_list

