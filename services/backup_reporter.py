#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份进度汇报服务
Backup Progress Reporter Service

每30分钟通过微信汇报备份进度
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from utils.wechat_notifier import get_wechat_notifier
from config.settings import settings

logger = logging.getLogger(__name__)


class BackupReporter:
    """备份进度汇报器"""

    def __init__(self, report_interval_minutes: int = 30):
        """
        初始化汇报器

        Args:
            report_interval_minutes: 汇报间隔（分钟）
        """
        self.report_interval = report_interval_minutes * 60  # 转换为秒
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """启动定时汇报"""
        if self.running:
            logger.warning("备份汇报器已在运行")
            return

        self.running = True
        self._task = asyncio.create_task(self._report_loop())
        logger.info(f"备份汇报器已启动，间隔: {self.report_interval // 60} 分钟")

    async def stop(self):
        """停止定时汇报"""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("备份汇报器已停止")

    async def _report_loop(self):
        """汇报循环"""
        while self.running:
            try:
                await asyncio.sleep(self.report_interval)
                await self.send_report()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"汇报循环异常: {e}", exc_info=True)

    async def send_report(self):
        """发送备份进度汇报"""
        wechat = get_wechat_notifier()
        if not wechat:
            logger.debug("微信通知器未初始化，跳过汇报")
            return

        # 获取当前备份任务状态
        try:
            # 从数据库或任务管理器获取备份状态
            status = await self._get_backup_status()
            
            if not status:
                # 没有正在进行的备份任务
                logger.debug("无备份任务，跳过汇报")
                return

            await wechat.send_backup_report(
                task_name=status.get("task_name", "未知任务"),
                status=status.get("status", "unknown"),
                progress=status.get("progress", 0),
                files_processed=status.get("files_processed", 0),
                bytes_processed=status.get("bytes_processed", 0),
                speed=status.get("speed"),
                error=status.get("error")
            )
        except Exception as e:
            logger.error(f"发送备份汇报失败: {e}", exc_info=True)

    async def _get_backup_status(self) -> Optional[dict]:
        """
        获取当前备份任务状态

        Returns:
            dict: 备份状态信息，如果没有活动任务返回 None
        """
        # TODO: 从任务管理器或数据库获取实际状态
        # 这里需要根据项目的任务管理实现来获取
        try:
            # 示例：从全局任务管理器获取
            from services.backup.task_manager import get_task_manager
            
            task_manager = get_task_manager()
            active_tasks = task_manager.get_active_tasks()
            
            if not active_tasks:
                return None
            
            # 获取第一个活动任务的状态
            task = active_tasks[0]
            return {
                "task_name": task.name,
                "status": task.status,
                "progress": task.progress,
                "files_processed": task.files_processed,
                "bytes_processed": task.bytes_processed,
                "speed": task.speed,
                "error": task.error
            }
        except ImportError:
            # 如果没有任务管理器，返回 None
            return None
        except Exception as e:
            logger.error(f"获取备份状态失败: {e}")
            return None


# 全局实例
_reporter: Optional[BackupReporter] = None


def get_backup_reporter() -> Optional[BackupReporter]:
    """获取备份汇报器实例"""
    return _reporter


def init_backup_reporter() -> BackupReporter:
    """初始化备份汇报器"""
    global _reporter
    
    interval = getattr(settings, 'WECHAT_REPORT_INTERVAL', 30)
    _reporter = BackupReporter(report_interval_minutes=interval)
    return _reporter


async def start_backup_reporter():
    """启动备份汇报器"""
    reporter = get_backup_reporter()
    if reporter:
        await reporter.start()


async def stop_backup_reporter():
    """停止备份汇报器"""
    reporter = get_backup_reporter()
    if reporter:
        await reporter.stop()
