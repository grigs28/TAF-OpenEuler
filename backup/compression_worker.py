#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
压缩工作线程
"""

import asyncio
import logging
import os
from typing import List, Dict, Optional, Any
import tempfile

from utils.scheduler.db_utils import is_opengauss
from config.settings import get_settings
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class CompressionWorker:
    """压缩工作线程"""

    def __init__(
        self,
        backup_db,
        compressor,
        backup_set,
        backup_task,
        settings,
        file_move_worker=None,
        backup_notifier=None,
        tape_file_mover=None,
        file_group_prefetcher=None
    ):
        self.backup_task = backup_task
        self.settings = settings
        self.backup_set = backup_set
        self.backup_db = backup_db
        self.compressor = compressor
        self.file_move_worker = file_move_worker
        self.backup_notifier = backup_notifier
        self.tape_file_mover = tape_file_mover
        self.file_group_prefetcher = file_group_prefetcher

        # 检查是否使用预取模式（openGauss模式且有预取器）
        self.use_prefetcher = is_opengauss() and file_group_prefetcher is not None
        logger.info(f"[压缩循环] 初始化: is_opengauss={is_opengauss()}, file_group_prefetcher={file_group_prefetcher is not None}, use_prefetcher={self.use_prefetcher}")

        self.compression_task: Optional[asyncio.Task] = None
        self._running = False

        # 压缩循环统计
        self.processed_files = 0
        self.total_size = 0  # 压缩后的总大小
        self.total_original_size = 0  # 原始文件的总大小（未压缩）
        self.group_idx = 0  # 文件组索引

        # 并行批次设置（基础值，会在异步方法中根据扫描状态动态调整）
        if self.use_prefetcher:
            settings = get_settings()
            self.base_parallel_batches = getattr(settings, 'COMPRESSION_PARALLEL_BATCHES', 3)
            self.batches_reduction = getattr(settings, 'COMPRESSION_BATCHES_REDUCTION', 1)  # 扫描时减少的批次数
            self.parallel_batches = self.base_parallel_batches  # 初始值，会在异步方法中调整
        else:
            self.base_parallel_batches = 1
            self.batches_reduction = 0  # 非预取模式不减少
            self.parallel_batches = 1  # 非openGauss模式，顺序执行
        
        # 存储每个压缩任务的进度（用于实时查询）
        # 格式: {group_idx: {'current': int, 'total': int, 'percent': float, 'group_size_bytes': int, 'compress_progress': Dict}}
        # compress_progress 是压缩函数中使用的进度字典，可以直接读取
        self.compression_progress_map: Dict[int, Dict[str, Any]] = {}
        
        # 并发控制：跟踪正在运行的压缩任务
        self.running_compression_futures: List[asyncio.Task] = []
        
        # 压缩进度更新任务（定期从数据库查询聚合进度）
        self.progress_update_task: Optional[asyncio.Task] = None
        
        # openGauss 数据库统一调度器（异步批量更新压缩信息和内存数据库同步）
        if is_opengauss():
            from backup.compression_db_updater import OpenGaussDBScheduler
            self.db_updater = OpenGaussDBScheduler(
                backup_set_db_id=backup_set.id,
                batch_size=3000  # 每3000个文件批量更新一次
            )
        else:
            self.db_updater = None

    def start(self):
        """启动压缩处理任务"""
        if self._running:
            return

        try:
            # 检查是否有运行中的事件循环
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.error("❌ 没有运行中的事件循环，无法创建任务")
                raise

            # 启动数据库更新器（如果使用）
            if self.db_updater:
                self.db_updater.start()
            
            self.compression_task = asyncio.create_task(self._compression_loop())
            self._running = True
            logger.info("压缩处理任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动压缩处理任务失败: {str(e)}", exc_info=True)
            raise

    async def stop(self):
        """停止压缩处理任务"""
        if not self._running:
            return

        logger.info("收到停止信号，正在停止压缩处理...")
        self._running = False

        # 取消所有正在运行的压缩任务
        if self.running_compression_futures:
            logger.info(f"取消 {len(self.running_compression_futures)} 个正在运行的压缩任务...")
            for task in self.running_compression_futures:
                if not task.done():
                    task.cancel()
            # 等待所有任务完成（包括被取消的）
            if self.running_compression_futures:
                done, _ = await asyncio.wait(self.running_compression_futures, timeout=10.0)
                for task in done:
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.debug("压缩任务已被取消")
                    except Exception as e:
                        logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
            self.running_compression_futures.clear()

        if self.compression_task:
            try:
                # 取消主压缩循环任务
                if not self.compression_task.done():
                    self.compression_task.cancel()
                await self.compression_task
                logger.info("压缩处理任务已自然结束")
            except asyncio.CancelledError:
                logger.info("压缩处理任务被取消")
            except KeyboardInterrupt:
                logger.warning("压缩处理收到KeyboardInterrupt")
                raise
            except Exception as e:
                logger.error(f"压缩处理任务异常: {str(e)}", exc_info=True)

        # 停止数据库更新器（处理剩余文件）
        if self.db_updater:
            logger.info("[压缩循环] 停止数据库更新器，处理剩余文件...")
            await self.db_updater.stop()

        logger.info("压缩处理任务已停止")

    async def _compression_loop(self):
        """简化的压缩处理"""
        logger.info("========== 压缩处理开始 ==========")

        if not self.backup_set.id:
            logger.error(f"⚠️⚠️ 错误：backup_set.id 为空！")
            return

        try:
            # 启动压缩进度更新任务（定期从数据库查询聚合进度）
            self.progress_update_task = asyncio.create_task(
                self._update_compression_progress_periodically()
            )
            
            if self.use_prefetcher:
                await self._process_prefetched_file_groups()
            else:
                await self._process_database_file_groups()
        except Exception as e:
            logger.error(f"❌ 压缩处理异常: {str(e)}", exc_info=True)
            raise
        finally:
            # 停止进度更新任务
            if self.progress_update_task:
                self.progress_update_task.cancel()
                try:
                    await self.progress_update_task
                except asyncio.CancelledError:
                    pass
                logger.info("压缩进度更新任务已停止")
    
    async def _adjust_parallel_batches(self):
        """根据扫描状态调整并行批次数量
        
        策略：扫描阶段减少同时运行的压缩任务数量，扫描结束后恢复正常
        """
        try:
            # 优先从内存对象获取，如果没有则从数据库查询
            scan_status = getattr(self.backup_task, "scan_status", None)
            if not scan_status and self.backup_task.id:
                try:
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                except Exception:
                    pass
            
            # 如果扫描未完成，减少并行批次数量（降低同时运行的压缩任务数）
            if scan_status not in (None, "completed"):
                adjusted_batches = max(0, self.base_parallel_batches - self.batches_reduction)
                if adjusted_batches != self.parallel_batches:
                    logger.info(
                        f"[压缩配置] 扫描阶段（scan_status={scan_status}），"
                        f"将并行批次数量从 {self.base_parallel_batches} 降为 {adjusted_batches} "
                        f"(减少 {self.batches_reduction} 批)"
                    )
                    self.parallel_batches = adjusted_batches
            else:
                if self.parallel_batches != self.base_parallel_batches:
                    logger.info(
                        f"[压缩配置] 扫描已完成（scan_status=completed），"
                        f"将并行批次数量从 {self.parallel_batches} 恢复为 {self.base_parallel_batches}"
                    )
                    self.parallel_batches = self.base_parallel_batches
                else:
                    logger.debug(
                        f"[压缩配置] 扫描已完成（scan_status=completed），"
                        f"使用正常并行批次数量: {self.parallel_batches}"
                    )
        except Exception as e:
            # 即使无法获取 scan_status，也不影响压缩主流程
            logger.debug(f"[压缩配置] 获取 scan_status 时出错，使用默认配置: {e}")

    async def _process_prefetched_file_groups(self):
        """处理预取的文件组（openGauss模式）- 并发控制"""
        # 根据扫描状态动态调整并行批次数量
        if self.use_prefetcher:
            await self._adjust_parallel_batches()
        logger.info(f"开始处理预取的文件组（并发控制模式，parallel_batches={self.parallel_batches}）")

        while self._running:
            try:
                # 定期检查扫描状态，动态调整并行批次数量（每10次循环检查一次，避免频繁查询）
                if self.use_prefetcher and self.group_idx % 10 == 0:
                    await self._adjust_parallel_batches()
                
                # 并发控制：如果达到并行限制，等待部分任务完成
                if len(self.running_compression_futures) >= self.parallel_batches:
                    logger.debug(f"达到并行限制 ({self.parallel_batches})，等待部分任务完成...")
                    # 等待至少一个任务完成
                    done, pending = await asyncio.wait(
                        self.running_compression_futures,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    # 移除已完成的任务并处理异常
                    for task in done:
                        if task in self.running_compression_futures:
                            self.running_compression_futures.remove(task)
                        try:
                            await task  # 获取任务结果（如果有异常会抛出）
                        except Exception as e:
                            logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                    logger.debug(f"当前运行中的压缩任务数: {len(self.running_compression_futures)}/{self.parallel_batches}")

                # 获取文件组（消费队列）
                result = await self.file_group_prefetcher.get_file_group(timeout=2.0)

                if result is None:
                    # 在等待文件组时，也检查扫描状态并调整并行批次数量
                    if self.use_prefetcher:
                        await self._adjust_parallel_batches()
                    
                    # 关键修复：先检查队列是否还有文件组，如果有则继续处理
                    queue_size = self.file_group_prefetcher.file_group_queue.qsize() if self.file_group_prefetcher else 0
                    if queue_size > 0:
                        logger.info(
                            f"队列中还有 {queue_size} 个文件组未处理，继续处理..."
                        )
                        # 继续循环，处理队列中的文件组
                        continue
                    
                    # 检查预取器是否还在运行（如果还在运行，即使 get_file_group 返回 None，也应该继续等待）
                    prefetch_running = getattr(self.file_group_prefetcher, '_running', False) if self.file_group_prefetcher else False
                    if prefetch_running:
                        logger.info(
                            f"预取器仍在运行，继续等待文件组...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    
                    # 检查停止条件：扫描完成 + 文件组预取无内容 + 队列为空 + 预取器已停止 + 所有压缩文件压缩完成 + 预取器执行次数 > 0
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    prefetch_loop_count = getattr(self.file_group_prefetcher, 'prefetch_loop_count', 0) if self.file_group_prefetcher else 0
                    is_rescanning = getattr(self.file_group_prefetcher, 'is_rescanning_missing_files', False) if self.file_group_prefetcher else False
                    
                    # 检查是否满足停止条件
                    scan_completed = scan_status == 'completed'
                    prefetch_no_content = result is None  # 预取器无内容
                    queue_empty = queue_size == 0  # 队列为空
                    all_compression_done = len(self.running_compression_futures) == 0  # 所有压缩任务完成
                    prefetch_executed = prefetch_loop_count > 0  # 预取器执行次数 > 0
                    
                    # 如果预取器正在重新检索遗漏文件，继续等待
                    if is_rescanning:
                        logger.info(
                            f"预取器正在重新检索遗漏文件，继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    
                    if not scan_completed:
                        logger.info(
                            f"无更多文件组，但扫描未完成（状态={scan_status}），"
                            f"继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}，"
                            f"预取器执行次数: {prefetch_loop_count}）"
                        )
                        # 扫描未完成时，不清空正在运行的任务，让它们继续执行
                        # 只等待一段时间，让预取器有时间预取更多文件组
                        await asyncio.sleep(2)
                        continue
                    elif not prefetch_executed:
                        logger.info(
                            f"扫描已完成，但预取器执行次数为 {prefetch_loop_count}（需要 > 0），"
                            f"继续等待...（当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}）"
                        )
                        await asyncio.sleep(2)
                        continue
                    elif not all_compression_done:
                        # 扫描已完成，预取器已执行，但还有正在运行的压缩任务，等待它们完成
                        logger.info(
                            f"扫描已完成，预取器执行次数: {prefetch_loop_count}，"
                            f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成..."
                        )
                        done, _ = await asyncio.wait(self.running_compression_futures)
                        # 处理所有已完成的任务（包括异常）
                        for task in done:
                            try:
                                await task
                            except Exception as e:
                                logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                        self.running_compression_futures.clear()
                        # 继续检查，确保所有条件都满足
                        continue
                    elif not queue_empty:
                        # 队列不为空，继续处理
                        logger.info(
                            f"队列中还有 {queue_size} 个文件组未处理，继续处理..."
                        )
                        continue
                    else:
                        # 所有条件都满足：扫描完成 + 预取器执行次数 > 0 + 队列为空 + 预取器已停止 + 所有压缩任务完成
                        logger.info(
                            f"✅ 满足停止条件：扫描完成={scan_completed}，"
                            f"预取器执行次数={prefetch_loop_count}，队列为空={queue_empty}，预取器已停止={not prefetch_running}，所有压缩任务完成={all_compression_done}"
                        )
                        # 在内存中设置压缩完成状态
                        if hasattr(self.backup_task, 'compression_completed'):
                            self.backup_task.compression_completed = True
                        logger.info("所有文件组处理完成，退出压缩循环")
                        break

                if isinstance(result, tuple) and len(result) == 2:
                    # 预取器返回格式：(file_groups, last_processed_id)
                    # file_groups 是一个列表，通常是 [[file1, file2, ..., fileN]]
                    file_groups, last_processed_id = result
                    # 检查是否是结束信号（空文件组且 last_processed_id == -1）
                    if not file_groups and last_processed_id == -1:
                        # 检查预取器执行次数
                        prefetch_loop_count = getattr(self.file_group_prefetcher, 'prefetch_loop_count', 0) if self.file_group_prefetcher else 0
                        if prefetch_loop_count <= 0:
                            logger.info(
                                f"收到结束信号，但预取器执行次数为 {prefetch_loop_count}（需要 > 0），"
                                f"继续等待..."
                            )
                            await asyncio.sleep(2)
                            continue
                        
                        # 等待所有正在运行的任务完成
                        if self.running_compression_futures:
                            logger.info(f"收到结束信号，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                            done, _ = await asyncio.wait(self.running_compression_futures)
                            # 处理所有已完成的任务（包括异常）
                            for task in done:
                                try:
                                    await task
                                except Exception as e:
                                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
                            self.running_compression_futures.clear()
                        
                        # 在内存中设置压缩完成状态
                        if hasattr(self.backup_task, 'compression_completed'):
                            self.backup_task.compression_completed = True
                        logger.info("收到预取器结束信号，退出压缩循环")
                        break
                    
                    # 取第一个文件组（通常只有一个文件组）
                    if file_groups and len(file_groups) > 0:
                        file_group = file_groups[0]
                        logger.info(f"[压缩循环] 从预取器获取文件组：{len(file_groups)} 个文件组，第一个文件组包含 {len(file_group)} 个文件")
                    else:
                        logger.warning("预取器返回的文件组列表为空，跳过")
                        continue
                    current_group_idx = self.group_idx
                else:
                    # 兼容旧格式（非元组）
                    logger.warning(f"收到非标准格式的文件组：{type(result)}，尝试直接使用")
                    file_group = result if isinstance(result, list) else []
                    current_group_idx = self.group_idx

                # 计算文件组总大小
                total_group_size = sum(f.get('size', 0) or f.get('file_size', 0) or 0 for f in file_group)
                
                # 创建压缩进度跟踪字典（与压缩函数中的compress_progress共享）
                # 这个字典会被压缩函数实时更新，我们可以从外部读取实际进度
                compress_progress = {
                    'bytes_written': 0,
                    'running': True,
                    'completed': False,
                    'current_file_index': 0,  # 初始化为0，压缩过程中会更新
                    'total_files_in_group': len(file_group),  # 文件组总文件数（固定值）
                    'processed_bytes': 0  # 已处理文件的实际大小总和（用于按文件大小计算百分比）
                }
                
                # 初始化进度映射，存储文件组的基本信息和共享的进度字典
                self.compression_progress_map[current_group_idx] = {
                    'compress_progress': compress_progress,  # 共享的进度字典，压缩函数会实时更新
                    'total_files': len(file_group),  # 文件组总文件数
                    'group_size_bytes': total_group_size  # 文件组总大小
                }
                
                # 检查是否应该停止（在启动新任务前）
                if not self._running:
                    logger.info("收到停止信号，不再启动新的压缩任务")
                    break
                
                # 启动压缩任务（并发执行，但每个任务内部顺序执行：压缩 → 标注 → 移动）
                compression_task = asyncio.create_task(
                    self._compress_file_group(file_group, current_group_idx, compress_progress)
                )
                self.running_compression_futures.append(compression_task)
                logger.info(
                    f"[压缩循环] ✅ 启动压缩任务 #{current_group_idx + 1}，"
                    f"当前运行中的任务数: {len(self.running_compression_futures)}/{self.parallel_batches}, "
                    f"队列大小: {self.file_group_prefetcher.file_group_queue.qsize()}/{self.file_group_prefetcher.queue_maxsize}"
                )

                self.group_idx += 1
                
                # 在启动任务后，也检查扫描状态并调整并行批次数量（确保及时恢复）
                if self.use_prefetcher:
                    await self._adjust_parallel_batches()
                
                # 顺序启动：启动一个任务后，等待3秒，再启动下一个（启动后的任务并行执行）
                # 这样可以避免队列竞争，确保任务顺序启动
                if len(self.running_compression_futures) < self.parallel_batches:
                    # 还没达到并行限制，等待3秒后继续启动下一个任务
                    # 这样可以顺序启动多个任务，避免队列竞争
                    await asyncio.sleep(3.0)
                else:
                    # 已达到并行限制，等待2秒后再检查
                    await asyncio.sleep(2.0)
                # 继续循环，获取下一个文件组（不等待当前任务完成）

            except asyncio.TimeoutError:
                # 超时时检查扫描状态，如果扫描未完成，继续等待
                # 同时检查并调整并行批次数量
                if self.use_prefetcher:
                    await self._adjust_parallel_batches()
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"获取文件组超时，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.info(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.info(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    logger.info("获取文件组超时，扫描已完成，退出压缩循环")
                    break
            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时也检查扫描状态
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    # 如果有正在运行的任务，等待它们完成
                    if self.running_compression_futures:
                        logger.info(f"等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    await asyncio.sleep(2)
                    continue
                else:
                    # 扫描已完成，等待所有正在运行的任务完成
                    if self.running_compression_futures:
                        logger.info(f"扫描已完成，等待 {len(self.running_compression_futures)} 个正在运行的压缩任务完成...")
                        await asyncio.wait(self.running_compression_futures)
                        self.running_compression_futures.clear()
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环")
                    break
        
        # 确保所有任务都已完成
        if self.running_compression_futures:
            logger.info(f"等待剩余的 {len(self.running_compression_futures)} 个压缩任务完成...")
            done, _ = await asyncio.wait(self.running_compression_futures)
            # 处理所有已完成的任务（包括异常）
            for task in done:
                try:
                    await task
                except Exception as e:
                    logger.error(f"压缩任务异常: {str(e)}", exc_info=True)
            self.running_compression_futures.clear()
        
        # 在内存中设置压缩完成状态
        if hasattr(self.backup_task, 'compression_completed'):
            self.backup_task.compression_completed = True
        
        # 压缩完成日志：换行输出，与其他日志有明显差异
        logger.info("=" * 80)
        logger.info("[压缩循环] ========== 压缩完成 ==========")
        logger.info(f"  处理文件组数: {self.group_idx}")
        logger.info(f"  处理文件数: {self.processed_files:,} 个文件")
        logger.info(f"  原始总大小: {format_bytes(self.total_original_size)}")
        logger.info(f"  压缩后总大小: {format_bytes(self.total_size)}")
        if self.total_original_size > 0:
            compression_ratio = (1 - self.total_size / self.total_original_size) * 100
            logger.info(f"  压缩率: {compression_ratio:.2f}%")
        logger.info("=" * 80)
    
    def get_aggregated_compression_progress(self) -> Optional[Dict[str, Any]]:
        """获取所有并行压缩任务的聚合进度和各个任务的进度列表
        
        从内存中的压缩程序获取实时进度，直接从每个运行中任务的compress_progress字典读取实际进度。
        
        Returns:
            Dict包含：
            - current: 所有并行任务已处理文件数的总和（实际值，用于兼容前端）
            - total: 所有并行任务文件组文件数的总和（实际值，用于兼容前端）
            - current_file_index: 所有并行任务已处理文件数的总和（实际值，不是估算）
            - total_files_in_group: 所有并行任务文件组文件数的总和（实际值，不是估算）
            - percent: 总进度百分比（按文件数）
            - group_size_bytes: 所有并行任务文件组大小的总和
            - running_count: 正在运行的压缩任务数
            - task_progress_list: 各个压缩任务的进度列表，每个任务包含：
                - percent: 该任务的进度百分比（按文件大小）
                - current: 已处理文件数
                - total: 文件组总文件数
                - group_size_bytes: 文件组大小（字节）
        """
        if not self.use_prefetcher or not self._running:
            # 非并行模式或未运行，返回None
            return None
        
        running_count = len(self.running_compression_futures)
        if running_count == 0:
            # 没有正在运行的压缩任务
            return None
        
        # 从compression_progress_map中读取每个运行中任务的实际进度
        total_current = 0
        total_total = 0
        total_group_size_bytes = 0
        active_count = 0
        task_progress_list = []  # 各个任务的进度列表
        
        # 遍历所有运行中的压缩任务，从它们的compress_progress字典读取实际进度
        logger.debug(
            f"[压缩进度聚合] 开始聚合进度: compression_progress_map 大小={len(self.compression_progress_map)}, "
            f"running_compression_futures 大小={running_count}"
        )
        
        # 同时检查 running_compression_futures 中的任务，确保所有正在运行的任务都被计入
        running_group_indices = set()
        for task in self.running_compression_futures:
            # 尝试从任务中获取 group_idx（如果任务有存储）
            # 注意：asyncio.Task 对象可能没有直接存储 group_idx，需要通过其他方式关联
            pass  # 暂时无法直接从 task 获取 group_idx
        
        for group_idx, progress_info in self.compression_progress_map.items():
            compress_progress = progress_info.get('compress_progress')
            if compress_progress:
                running = compress_progress.get('running', False)
                completed = compress_progress.get('completed', False)
                total_files_in_group = compress_progress.get('total_files_in_group', 0)
                logger.debug(
                    f"[压缩进度聚合] 任务#{group_idx}: running={running}, completed={completed}, "
                    f"total_files_in_group={total_files_in_group}, compress_progress存在={compress_progress is not None}"
                )
            
            if compress_progress and compress_progress.get('running', False) and not compress_progress.get('completed', False):
                # 从compress_progress字典读取实际进度
                current_file_index = compress_progress.get('current_file_index', 0)
                total_files_in_group = compress_progress.get('total_files_in_group', 0)
                group_size_bytes = progress_info.get('group_size_bytes', 0)
                
                logger.debug(
                    f"[压缩进度聚合] 任务#{group_idx}: current_file_index={current_file_index}, "
                    f"total_files_in_group={total_files_in_group}, group_size_bytes={group_size_bytes}"
                )
                
                # 如果 total_files_in_group 为 0，尝试从 progress_info 中获取（作为后备）
                if total_files_in_group == 0:
                    total_files_in_group = progress_info.get('total_files', 0)
                    if total_files_in_group > 0:
                        logger.warning(
                            f"[压缩进度聚合] 任务#{group_idx}: compress_progress.total_files_in_group=0，"
                            f"使用 progress_info.total_files={total_files_in_group} 作为后备"
                        )
                        # 更新 compress_progress 中的值，避免下次还是 0
                        compress_progress['total_files_in_group'] = total_files_in_group
                
                if total_files_in_group > 0:
                    # 累加每个任务的实际进度（这是正在处理任务的文件数和）
                    total_current += current_file_index  # 3102 = 所有正在处理任务的文件数总和
                    total_total += total_files_in_group  # 11051 = 所有正在处理任务的文件总数和
                    total_group_size_bytes += group_size_bytes
                    active_count += 1
                    
                    # 计算该任务的进度百分比（真正按文件大小计算）
                    # 获取已处理文件的实际大小（如果压缩函数已累计）
                    processed_bytes = compress_progress.get('processed_bytes', 0)
                    
                    if group_size_bytes > 0 and processed_bytes > 0:
                        # 真正按文件大小计算百分比：已处理文件大小 / 文件组总大小 * 100
                        size_percent = (processed_bytes / group_size_bytes * 100) if group_size_bytes > 0 else 0.0
                    elif group_size_bytes > 0 and total_files_in_group > 0:
                        # 如果没有累计已处理大小，回退到按文件数占比（假设文件大小分布均匀）
                        file_count_percent = (current_file_index / total_files_in_group * 100) if total_files_in_group > 0 else 0.0
                        size_percent = file_count_percent
                    elif total_files_in_group > 0:
                        # 如果没有文件组大小信息，仍然按文件数占比计算
                        size_percent = (current_file_index / total_files_in_group * 100) if total_files_in_group > 0 else 0.0
                    else:
                        size_percent = 0.0
                    
                    task_progress_list.append({
                        'percent': size_percent,
                        'current': current_file_index,
                        'total': total_files_in_group,
                        'group_size_bytes': group_size_bytes,
                        'processed_bytes': processed_bytes  # 已处理文件的实际大小
                    })
                    
                    logger.debug(
                        f"[压缩进度] 任务#{group_idx}: {current_file_index}/{total_files_in_group} 个文件 "
                        f"({size_percent:.1f}%), 大小={group_size_bytes / (1024**3):.2f}GB"
                    )
        
        # 即使 total_total == 0，如果有 running_count > 0，也应该返回数据结构（包含空的 task_progress_list）
        # 这样可以避免前端交替显示，让前端知道有任务在运行，只是暂时没有进度数据
        if total_total == 0:
            if running_count > 0:
                # 有任务在运行，但暂时没有进度数据（可能是刚启动或初始化阶段）
                logger.debug(
                    f"[压缩进度聚合] total_total=0 但 running_count={running_count} > 0，"
                    f"返回空进度数据结构。active_count={active_count}, "
                    f"compression_progress_map大小={len(self.compression_progress_map)}, "
                    f"task_progress_list大小={len(task_progress_list)}"
                )
                # 返回包含空 task_progress_list 的数据结构，而不是 None
                return {
                    'current': 0,
                    'total': 0,
                    'current_file_index': 0,
                    'total_files_in_group': 0,
                    'percent': 0.0,
                    'group_size_bytes': 0,
                    'running_count': running_count,
                    'task_progress_list': []  # 空的进度列表，但至少表明有任务在运行
                }
            else:
                # 没有任务在运行，返回 None
                logger.debug(
                    f"[压缩进度聚合] total_total=0 且 running_count=0，返回 None。"
                    f"active_count={active_count}, compression_progress_map大小={len(self.compression_progress_map)}"
                )
                return None
        
        percent = (total_current / total_total * 100) if total_total > 0 else 0.0
        
        logger.debug(
            f"[压缩进度聚合] 并行任务数={running_count}, 活跃任务数={active_count}, "
            f"聚合进度={total_current}/{total_total} ({percent:.1f}%), "
            f"任务进度列表: {[(t.get('percent', 0), t.get('current', 0), t.get('total', 0)) for t in task_progress_list]}"
        )
        
        return {
            # 兼容前端的字段名（用于"当前阶段"显示）
            'current': total_current,  # 所有正在处理任务的文件数总和（3102）
            'total': total_total,  # 所有正在处理任务的文件总数和（11051）
            # 原始字段名
            'current_file_index': total_current,
            'total_files_in_group': total_total,
            'percent': percent,
            'group_size_bytes': total_group_size_bytes,
            'running_count': running_count,
            'task_progress_list': task_progress_list  # 各个任务的进度列表，用于"各压缩任务进度"显示
        }

    async def _update_compression_progress_periodically(self):
        """定期更新压缩进度（从数据库查询聚合进度，支持多进程）
        
        每5秒查询一次数据库，获取所有已压缩文件数（is_copy_success = TRUE），
        更新 description 字段，显示聚合进度。
        
        进度显示格式：已处理的文件组数/压缩队列中的总文件数
        """
        try:
            # 等待压缩开始（避免在压缩开始前频繁查询）
            await asyncio.sleep(5.0)
            
            update_interval = 5.0  # 每5秒更新一次
            last_update_time = 0.0
            
            while self._running:
                try:
                    current_time = asyncio.get_event_loop().time()
                    
                    # 检查是否需要更新（每5秒更新一次）
                    if current_time - last_update_time >= update_interval:
                        # 从内存获取已压缩文件数（不再依赖数据库状态）
                        # 在 openGauss / 预取模式下，所有压缩进度都由当前进程维护在内存中
                        from utils.scheduler.db_utils import is_opengauss
                        if is_opengauss():
                            # 使用 CompressionWorker 自身统计的 processed_files 作为已压缩文件数
                            compressed_count = getattr(self, "processed_files", 0) or 0
                        else:
                            # 非 openGauss 模式保留原有行为（从数据库聚合查询进度）
                            compressed_count = await self.backup_db.get_compressed_files_count(
                                self.backup_set.id
                            )
                        
                        # 调试日志：记录查询结果
                        logger.debug(
                            f"[压缩进度更新] 查询已压缩文件数: compressed_count={compressed_count}, "
                            f"backup_set_id={self.backup_set.id}"
                        )
                        
                        # 获取需要处理的总文件数（优先使用backup_task.total_files，这是正在处理的文件总数）
                        total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                        
                        # 如果backup_task.total_files为0，尝试从预取器获取累计放入队列的文件数作为后备
                        if total_files_to_process == 0 and self.file_group_prefetcher:
                            total_files_to_process = self.file_group_prefetcher.get_queued_files_count()
                            logger.debug(
                                f"[压缩进度更新] backup_task.total_files为0，使用预取器累计文件数: {total_files_to_process}"
                            )
                        
                        # 调试日志：记录文件数来源
                        if self.file_group_prefetcher:
                            total_queued = getattr(self.file_group_prefetcher, 'total_queued_files_count', 0)
                            current_queued = getattr(self.file_group_prefetcher, 'queued_files_count', 0)
                            logger.debug(
                                f"[压缩进度更新] 需要处理的总文件数: {total_files_to_process} "
                                f"(backup_task.total_files={getattr(self.backup_task, 'total_files', 0)}, "
                                f"预取器累计={total_queued}, 预取器当前={current_queued})"
                            )
                        
                        # 计算进度百分比
                        if total_files_to_process > 0:
                            progress_percent = (compressed_count / total_files_to_process) * 100.0
                            progress_percent = min(100.0, max(0.0, progress_percent))
                            
                            # 更新 description 字段
                            # 格式：已处理的文件数/需要处理的总文件数
                            description = f"[压缩文件中...] {compressed_count}/{total_files_to_process} 个文件 ({progress_percent:.1f}%)"
                            await self.backup_db.update_task_stage_with_description(
                                self.backup_task,
                                "compress",
                                description
                            )
                            
                            # 只在进度变化超过1%时记录日志，减少日志输出
                            if not hasattr(self, '_last_logged_progress') or abs(progress_percent - self._last_logged_progress) >= 1.0:
                                logger.info(
                                    f"[压缩进度更新] 已处理: {compressed_count}/{total_files_to_process} 个文件 "
                                    f"({progress_percent:.1f}%) "
                                    f"[compressed_count={compressed_count}, total_files_to_process={total_files_to_process}]"
                                )
                                self._last_logged_progress = progress_percent
                        else:
                            logger.debug("[压缩进度更新] 需要处理的总文件数为0，跳过进度更新")
                        
                        # 更新 backup_task.current_compression_progress（包含 task_progress_list，供前端显示各任务进度）
                        # 使用 get_aggregated_compression_progress() 获取包含所有并行任务进度的聚合数据
                        aggregated_progress = self.get_aggregated_compression_progress()
                        if aggregated_progress:
                            # 将聚合进度（包含 task_progress_list）设置到 backup_task，供 UI 使用
                            if hasattr(self.backup_task, 'current_compression_progress'):
                                self.backup_task.current_compression_progress = aggregated_progress
                                logger.debug(
                                    f"[压缩进度更新] 已更新 backup_task.current_compression_progress，"
                                    f"包含 {len(aggregated_progress.get('task_progress_list', []))} 个任务进度"
                                )
                        # 注意：如果 get_aggregated_compression_progress() 返回 None（可能暂时没有活跃任务），
                        # 不更新 backup_task.current_compression_progress，保留上一次的值，避免前端交替显示
                        
                        last_update_time = current_time
                    
                    # 等待1秒后再次检查
                    await asyncio.sleep(1.0)
                    
                except asyncio.CancelledError:
                    logger.info("[压缩进度更新] 收到取消信号，停止进度更新")
                    break
                except Exception as e:
                    logger.warning(f"[压缩进度更新] 更新进度失败: {str(e)}", exc_info=True)
                    # 出错后等待更长时间再重试
                    await asyncio.sleep(5.0)
                    
        except asyncio.CancelledError:
            logger.info("[压缩进度更新] 进度更新任务被取消")
        except Exception as e:
            logger.error(f"[压缩进度更新] 进度更新任务异常: {str(e)}", exc_info=True)

    async def _process_database_file_groups(self):
        """处理数据库中的文件组（非openGauss模式）"""
        logger.info("开始处理数据库中的文件组")

        last_processed_id = 0

        while self._running:
            try:
                # 从数据库获取文件组
                file_groups = await self.backup_db.get_next_compression_group(
                    backup_set_id=self.backup_set.id,
                    batch_size=1,
                    last_processed_id=last_processed_id
                )

                if not file_groups:
                    scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                    if scan_status == 'completed':
                        logger.info("所有文件组处理完成")
                        break
                    logger.debug("无文件组，等待...")
                    await asyncio.sleep(2)
                    continue

                file_group = file_groups[0]

                # 获取最后一个文件的ID作为下次查询的起点
                if file_group:
                    last_processed_id = 0
                    for f in file_group:
                        if isinstance(f, dict):
                            last_processed_id = max(last_processed_id, f.get('id', 0))
                        elif isinstance(f, list) and len(f) > 0:
                            # 处理不同的列表格式
                            if isinstance(f[0], (int, float)):
                                # 标准格式：[id, file_path, relative_path, file_size, ...]
                                last_processed_id = max(last_processed_id, int(f[0]))
                            elif len(f) >= 8 and isinstance(f[7], dict) and 'id' in f[7]:
                                # 可能的嵌套格式：[..., ..., ..., ..., ..., ..., ..., {'id': id, ...}]
                                last_processed_id = max(last_processed_id, int(f[7]['id']))
                            else:
                                logger.warning(f"[数据库模式] 无法从列表格式获取文件ID: {f}")
                        else:
                            logger.warning(f"[数据库模式] 未知文件格式: {f}")

                # 顺序处理文件组
                await self._compress_file_group(file_group, self.group_idx)

                self.group_idx += 1

            except Exception as e:
                logger.error(f"处理文件组失败: {str(e)}", exc_info=True)
                # 发生异常时检查扫描状态，如果扫描未完成，继续等待
                scan_status = await self.backup_db.get_scan_status(self.backup_task.id)
                if scan_status != 'completed':
                    logger.info(f"处理文件组异常，但扫描未完成（状态={scan_status}），继续等待...")
                    await asyncio.sleep(2)
                    continue
                else:
                    logger.error("处理文件组异常，扫描已完成，退出压缩循环")
                    break

        logger.info("数据库文件组处理完成")

    async def _compress_file_group(self, file_group: List[Dict], group_idx: int, compress_progress: Optional[Dict] = None):
        """压缩单个文件组（内部顺序执行：压缩完成 → 标注完成 → 移动到final → 返回，队列已消费）"""
        # 检查是否应该停止
        if not self._running:
            logger.info(f"[#{group_idx + 1}] 收到停止信号，跳过压缩")
            return
        
        if not file_group:
            logger.warning(f"[#{group_idx + 1}] 文件组为空，跳过")
            return

        try:
            total_files = len(file_group)

            # 处理文件数据格式并转换为压缩器期望的格式
            total_size = 0
            processed_file_group = []

            for f in file_group:
                file_dict = None

                if isinstance(f, dict):
                    # 字典格式 - 直接使用
                    file_dict = f
                    total_size += file_dict.get('file_size', 0)
                elif isinstance(f, list):
                    # 检查是否是嵌套字典格式（最后一个元素是字典）
                    if f and isinstance(f[-1], dict):
                        # 嵌套字典格式：[..., {...dict...}]
                        file_dict = f[-1]
                        total_size += file_dict.get('size', 0)
                    elif len(f) >= 4 and isinstance(f[3], (int, float)):
                        # 标准格式：[id, file_path, relative_path, file_size, ...]
                        # 转换为字典格式
                        file_dict = {
                            'id': f[0] if len(f) > 0 else 0,
                            'file_path': f[1] if len(f) > 1 else '',
                            'relative_path': f[2] if len(f) > 2 else '',
                            'file_size': f[3] if len(f) > 3 else 0,
                            'modified_time': f[4] if len(f) > 4 else None,
                            'md5': f[5] if len(f) > 5 else '',
                            'is_copy_success': f[6] if len(f) > 6 else False,
                            'stage': f[7] if len(f) > 7 else 'pending'
                        }
                        total_size += int(f[3])
                    else:
                        logger.warning(f"[#{group_idx + 1}] 无法从列表格式获取文件大小: {f}")
                        continue
                else:
                    logger.warning(f"[#{group_idx + 1}] 未知文件格式: {f}")
                    continue

                processed_file_group.append(file_dict)

            logger.info(f"[#{group_idx + 1}] 开始压缩 {total_files} 个文件，总大小 {format_bytes(total_size)}")

            # 初始化共享的compress_progress字典（如果传入的字典还没有这些字段）
            if compress_progress is None:
                compress_progress = {
                    'bytes_written': 0,
                    'running': True,
                    'completed': False,
                    'current_file_index': 0,
                    'total_files_in_group': total_files
                }
            else:
                # 确保共享字典有必要的字段
                compress_progress['running'] = True
                compress_progress['completed'] = False
                compress_progress['current_file_index'] = compress_progress.get('current_file_index', 0)
                # 如果 total_files_in_group 不存在或为 0，设置为 total_files
                if compress_progress.get('total_files_in_group', 0) == 0:
                    compress_progress['total_files_in_group'] = total_files
                    logger.debug(f"[压缩任务#{group_idx + 1}] 初始化 total_files_in_group={total_files}")
                else:
                    # 确保值正确（如果传入的字典已经有值，但可能不正确，使用 total_files 覆盖）
                    compress_progress['total_files_in_group'] = total_files
                    logger.debug(f"[压缩任务#{group_idx + 1}] 更新 total_files_in_group={total_files}")
            
            # 更新进度映射中的信息
            if group_idx in self.compression_progress_map:
                self.compression_progress_map[group_idx]['compress_progress'] = compress_progress
                self.compression_progress_map[group_idx]['total_files'] = total_files
                self.compression_progress_map[group_idx]['group_size_bytes'] = total_size

            # 更新进度
            await self.backup_db.update_task_stage_with_description(
                self.backup_task,
                "compress",
                f"[压缩文件中...] 0/{total_files} 个文件 (0.0%)"
            )

            # 再次检查是否应该停止（在开始压缩前）
            if not self._running:
                logger.info(f"[#{group_idx + 1}] 收到停止信号，取消压缩")
                return
            
            # 准备压缩目录
            with tempfile.TemporaryDirectory() as temp_dir:
                # 传递共享的compress_progress字典给压缩函数
                # 压缩函数内部会实时更新这个字典，我们可以在外部读取实际进度
                compressed_info = await self.compressor.compress_file_group(
                    processed_file_group, 
                    self.backup_set, 
                    self.backup_task, 
                    self.processed_files, 
                    total_files,
                    shared_compress_progress=compress_progress  # 传递共享的进度字典
                )
                
                # 压缩完成后检查是否应该停止
                if not self._running:
                    logger.info(f"[#{group_idx + 1}] 压缩完成，但收到停止信号，跳过后续处理")
                    return
                
                # 压缩完成后，标记进度字典为完成状态
                if compress_progress:
                    compress_progress['completed'] = True
                    compress_progress['running'] = False
                    # 更新进度映射中的完成状态
                    if group_idx in self.compression_progress_map:
                        self.compression_progress_map[group_idx]['compress_progress']['completed'] = True
                        self.compression_progress_map[group_idx]['compress_progress']['running'] = False
                    
                    # 压缩完成后，更新统计和标记文件为已复制
                if compressed_info:
                    # 更新内存中的统计
                    self.processed_files += total_files
                    compressed_size = compressed_info.get('compressed_size', 0) or 0
                    original_size = compressed_info.get('original_size', 0) or 0
                    self.total_size += compressed_size
                    self.total_original_size += original_size
                    
                    # 只更新 chunk_number（不更新 is_copy_success，因为预取时已设置）
                    # 这样 get_compressed_files_count 才能查询到已压缩的文件（chunk_number IS NOT NULL）
                    try:
                        # 获取 chunk_number（使用 group_idx + 1 作为 chunk_number）
                        chunk_number = group_idx + 1
                        # 获取文件路径列表
                        file_paths = [f.get('file_path') or f.get('path') for f in processed_file_group if f.get('file_path') or f.get('path')]
                        
                        if file_paths:
                            # 使用数据库更新器异步批量更新（openGauss模式）
                            if self.db_updater:
                                # 提交给更新器，由更新器批量处理
                                await self.db_updater.submit_compressed_files(
                                    group_idx=group_idx,
                                    file_paths=file_paths,
                                    chunk_number=chunk_number,
                                    compressed_size=compressed_size,
                                    original_size=original_size
                                )
                                logger.info(
                                    f"[压缩工作器] ✅ 已提交压缩文件组 #{group_idx + 1} 给调度器: "
                                    f"{len(file_paths)} 个文件, chunk_number={chunk_number}, "
                                    f"压缩大小={format_bytes(compressed_size)}"
                                )
                            else:
                                # SQLite/Redis 模式：使用 mark_files_as_copied（它会处理这些模式）
                                # 但只更新 chunk_number 相关字段
                                compressed_file_info = {
                                    'compressed_size': compressed_size,
                                    'compression_enabled': compressed_info.get('compression_enabled', True),
                                    'checksum': compressed_info.get('checksum')
                                }
                                archive_path = compressed_info.get('path') or ''
                                await self.backup_db.mark_files_as_copied(
                                    backup_set=self.backup_set,
                                    file_group=processed_file_group,
                                    compressed_file=compressed_file_info,
                                    tape_file_path=archive_path,
                                    chunk_number=chunk_number
                                )
                                logger.info(f"[#{group_idx + 1}] ✅ 已更新 chunk_number={chunk_number}，文件数={len(processed_file_group)}")
                    except Exception as update_error:
                        logger.error(f"[#{group_idx + 1}] ⚠️ 提交压缩文件信息失败: {str(update_error)}", exc_info=True)
                    
                    # 从数据库读取当前值，然后累加（避免并发问题）
                    from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
                    if is_opengauss():
                        async with get_opengauss_connection() as conn:
                            row = await conn.fetchrow(
                                "SELECT processed_files, processed_bytes, compressed_bytes, total_files FROM backup_tasks WHERE id = $1",
                                self.backup_task.id
                            )
                            if row:
                                current_processed_files = row['processed_files'] or 0
                                current_processed_bytes = row['processed_bytes'] or 0
                                current_compressed_bytes = row['compressed_bytes'] or 0
                                total_files_to_process = row['total_files'] or 0
                            else:
                                current_processed_files = 0
                                current_processed_bytes = 0
                                current_compressed_bytes = 0
                                total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                    else:
                        # SQLite/Redis 模式：从backup_task对象读取
                        current_processed_files = getattr(self.backup_task, 'processed_files', 0) or 0
                        current_processed_bytes = getattr(self.backup_task, 'processed_bytes', 0) or 0
                        current_compressed_bytes = getattr(self.backup_task, 'compressed_bytes', 0) or 0
                        total_files_to_process = getattr(self.backup_task, 'total_files', 0) or 0
                    
                    # 累加新值
                    new_processed_files = current_processed_files + total_files
                    new_processed_bytes = current_processed_bytes + original_size
                    new_compressed_bytes = current_compressed_bytes + compressed_size
                    
                    # 计算进度百分比
                    if total_files_to_process > 0:
                        new_progress_percent = min(100.0, (new_processed_files / total_files_to_process) * 100.0)
                    else:
                        new_progress_percent = getattr(self.backup_task, 'progress_percent', 0.0) or 0.0
                    
                    # 更新backup_task对象
                    self.backup_task.processed_files = new_processed_files
                    self.backup_task.processed_bytes = new_processed_bytes
                    self.backup_task.compressed_bytes = new_compressed_bytes
                    self.backup_task.progress_percent = new_progress_percent
                    
                    # 更新数据库（使用update_scan_progress，它会更新这些字段）
                    await self.backup_db.update_scan_progress(
                        self.backup_task,
                        new_processed_files,  # scanned_count
                        new_processed_files,  # valid_count
                        None  # operation_status (不更新，保持description中的压缩进度)
                    )
                    
                    logger.info(
                        f"[#{group_idx + 1}] ✅ 压缩完成并已移动到final: {total_files} 个文件, "
                        f"原始大小: {format_bytes(original_size)}, 压缩后: {format_bytes(compressed_size)}, "
                        f"累计: {new_processed_files}/{total_files_to_process} 个文件 ({new_progress_percent:.1f}%)"
                    )
                
                archive_path = compressed_info.get('path') if compressed_info else None

                # 注意：文件移动到磁带由FinalDirMonitor独立线程监控final目录处理

        except Exception as e:
            logger.error(f"[#{group_idx + 1}] ❌ 压缩失败: {str(e)}", exc_info=True)
            raise