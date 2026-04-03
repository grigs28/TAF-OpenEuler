#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带文件移动队列管理器
Tape File Mover Queue Manager

功能：
1. 管理压缩文件到磁带机的移动队列
2. 顺序处理文件移动（磁带机只能顺序工作）
3. 异步处理，不影响压缩工作
"""

import asyncio
import logging
import threading
import shutil
from pathlib import Path
from typing import Dict, Optional, Callable
from queue import Queue, Empty
from dataclasses import dataclass
from datetime import datetime

from models.backup import BackupSet
from config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class MoveTask:
    """移动任务"""
    source_path: str  # 源文件路径（final目录中的文件）
    backup_set: BackupSet  # 备份集对象
    group_idx: int  # 组索引
    callback: Optional[Callable] = None  # 移动完成后的回调函数
    backup_task: Optional = None  # 备份任务对象（用于更新状态）


class TapeFileMover:
    """磁带文件移动队列管理器
    
    功能：
    1. 维护一个文件移动队列
    2. 使用单线程顺序处理文件移动到磁带机
    3. 确保磁带机操作是串行的
    """
    
    def __init__(self, tape_handler, settings=None, main_loop=None):
        """
        初始化文件移动队列管理器
        
        Args:
            tape_handler: TapeHandler实例，用于实际移动文件
            settings: 系统设置
            main_loop: 主事件循环（用于在线程中执行异步操作）
        """
        self.tape_handler = tape_handler
        self.settings = settings or get_settings()
        self._queue: Queue = Queue()
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        self._current_task: Optional[MoveTask] = None
        self._main_loop = main_loop  # 保存主事件循环引用
        
    def start(self):
        """启动移动队列工作线程"""
        with self._lock:
            if self._running:
                logger.warning("文件移动队列已经在运行")
                return
            
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="TapeFileMover",
                daemon=True
            )
            self._worker_thread.start()
            logger.info("文件移动队列管理器已启动")
    
    def stop(self):
        """停止移动队列工作线程"""
        with self._lock:
            if not self._running:
                return
            
            self._running = False
            # 等待队列中的任务完成
            self._queue.put(None)  # 发送停止信号
            
            if self._worker_thread and self._worker_thread.is_alive():
                self._worker_thread.join(timeout=30)
                if self._worker_thread.is_alive():
                    logger.warning("文件移动队列工作线程未能及时停止")
                else:
                    logger.info("文件移动队列管理器已停止")
    
    def add_file(self, source_path: str, backup_set: BackupSet, group_idx: int,
                 callback: Optional[Callable] = None, backup_task: Optional = None) -> bool:
        """
        添加文件到移动队列

        Args:
            source_path: 源文件路径（final目录中的文件）
            backup_set: 备份集对象
            group_idx: 组索引
            callback: 移动完成后的回调函数（可选）
            backup_task: 备份任务对象（用于更新状态）

        Returns:
            bool: 是否成功添加到队列
        """
        try:
            source_file = Path(source_path)
            if not source_file.exists():
                logger.error(f"要移动的文件不存在: {source_path}")
                return False
            
            task = MoveTask(
                source_path=source_path,
                backup_set=backup_set,
                group_idx=group_idx,
                callback=callback,
                backup_task=backup_task
            )
            
            self._queue.put(task)
            logger.info(f"文件已加入移动队列: {source_file.name} (队列大小: {self._queue.qsize()})")
            return True
            
        except Exception as e:
            logger.error(f"添加文件到移动队列失败: {str(e)}")
            return False
    
    def get_queue_size(self) -> int:
        """获取队列大小"""
        return self._queue.qsize()
    
    def is_processing(self) -> bool:
        """检查是否正在处理任务"""
        return self._current_task is not None
    
    def _worker_loop(self):
        """工作线程循环，顺序处理队列中的文件移动任务"""
        logger.info("文件移动队列工作线程已启动")
        
        while self._running:
            try:
                # 从队列获取任务（阻塞等待，最多等待1秒）
                try:
                    task = self._queue.get(timeout=1)
                except Empty:
                    continue
                
                # 检查停止信号
                if task is None:
                    logger.info("收到停止信号，退出工作线程")
                    break
                
                # 处理任务
                self._current_task = task
                try:
                    self._process_move_task(task)
                except Exception as e:
                    logger.error(f"处理移动任务失败: {str(e)}")
                finally:
                    self._current_task = None
                    self._queue.task_done()
                    
            except Exception as e:
                logger.error(f"文件移动队列工作线程错误: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("文件移动队列工作线程已退出")
    
    def _process_move_task(self, task: MoveTask):
        """
        处理单个移动任务
        
        Args:
            task: 移动任务
        """
        source_file = Path(task.source_path)
        logger.info(f"开始移动文件到磁带机: {source_file.name} (大小: {source_file.stat().st_size} 字节)")

        # 更新任务状态为"写入磁带"
        if task.backup_task:
            try:
                from backup.backup_db import BackupDB
                backup_db = BackupDB()
                # 使用主事件循环更新状态（如果可用）
                backup_db.update_task_stage(
                    task.backup_task,
                    "copy",
                    main_loop=self._main_loop,
                    description=f"[写入磁带中] 正在写入文件：{source_file.name}"
                )
                logger.info(f"任务 {task.backup_task.task_name} 状态更新为: 写入磁带")
            except Exception as stage_error:
                logger.warning(f"更新任务状态失败: {str(stage_error)}")

        start_time = datetime.now()
        
        try:
            # 使用tape_handler的write_to_tape_drive方法移动文件
            # 注意：这个方法会复制文件到磁带机，然后删除源文件
            # 由于是异步方法，我们需要在线程中运行事件循环
            # 在工作线程中运行异步操作
            # 由于我们在工作线程中，需要创建新的事件循环
            # 注意：不能使用主事件循环，因为它在主线程中运行
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                tape_file_path = loop.run_until_complete(
                    self.tape_handler.write_to_tape_drive(
                        task.source_path,
                        task.backup_set,
                        task.group_idx
                    )
                )
            finally:
                # 确保关闭事件循环，释放资源
                try:
                    # 取消所有未完成的任务
                    pending = asyncio.all_tasks(loop)
                    for task_obj in pending:
                        task_obj.cancel()
                    # 等待所有任务完成
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            if tape_file_path:
                logger.info(f"文件移动成功: {source_file.name} -> {tape_file_path} (耗时: {elapsed:.2f}秒)")

                # 更新任务状态：保持 copy 阶段，只更新 description 记录进度
                # 不再逐文件设为 finalize，任务完成由 _perform_backup 的四条件判定统一处理
                if task.backup_task:
                    try:
                        from backup.backup_db import BackupDB
                        backup_db = BackupDB()
                        backup_db.update_task_stage(
                            task.backup_task,
                            "copy",
                            main_loop=self._main_loop,
                            description=f"[写入磁带中] 已写入文件：{source_file.name}"
                        )
                        logger.info(f"任务 {task.backup_task.task_name} 文件写入磁带完成: {source_file.name}")
                    except Exception as stage_error:
                        logger.warning(f"更新任务状态失败: {str(stage_error)}")

                # 调用回调函数（如果提供）- 回调函数中会记录关键阶段
                if task.callback:
                    try:
                        task.callback(task.source_path, tape_file_path, True, None)
                    except Exception as callback_error:
                        logger.warning(f"回调函数执行失败: {str(callback_error)}")
            else:
                logger.error(f"文件移动失败: {source_file.name}")
                
                # 调用回调函数（如果提供）
                if task.callback:
                    try:
                        task.callback(task.source_path, None, False, "移动失败")
                    except Exception as callback_error:
                        logger.warning(f"回调函数执行失败: {str(callback_error)}")
                
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.error(f"移动文件到磁带机失败: {source_file.name}, 错误: {str(e)} (耗时: {elapsed:.2f}秒)")
            import traceback
            logger.error(traceback.format_exc())
            
            # 调用回调函数（如果提供）
            if task.callback:
                try:
                    task.callback(task.source_path, None, False, str(e))
                except Exception as callback_error:
                    logger.warning(f"回调函数执行失败: {str(callback_error)}")

