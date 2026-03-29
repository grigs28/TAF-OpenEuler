#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带挂载器模块
Tape Mounter Module

负责在任务开始时挂载磁带（如有需要则格式化）
从启动流程中分离出来，方便各种任务调用
"""

import asyncio
import logging
from typing import Tuple, Optional
from pathlib import Path

from config.settings import get_settings
from backup.tape_handler import TapeHandler
from tape.tape_manager import TapeManager
from utils.dingtalk_notifier import DingTalkNotifier
from models.backup import BackupTask


logger = logging.getLogger(__name__)


class TapeMounter:
    """
    磁带挂载器 - 负责在任务开始时挂载和格式化磁带

    用法示例：
        # 方式1: 直接实例化使用
        mounter = TapeMounter()
        await mounter.initialize(tape_manager, dingtalk_notifier)
        success, msg = await mounter.mount_and_format_tape(backup_task)

        # 方式2: 使用类方法快速挂载
        success, msg = await TapeMounter.quick_mount(
            tape_manager=tape_manager,
            dingtalk_notifier=dingtalk_notifier,
            backup_task=backup_task
        )
    """

    def __init__(self):
        self.settings = get_settings()
        self.tape_handler: Optional[TapeHandler] = None
        self._initialized = False

    async def initialize(
        self,
        tape_manager: TapeManager,
        dingtalk_notifier: Optional[DingTalkNotifier] = None
    ):
        """
        初始化磁带挂载器

        Args:
            tape_manager: 磁带管理器
            dingtalk_notifier: 钉钉通知器（可选）
        """
        self.tape_handler = TapeHandler(
            tape_manager=tape_manager,
            settings=self.settings
        )
        self.tape_handler.dingtalk_notifier = dingtalk_notifier
        self._initialized = True
        logger.info("[磁带挂载器] 初始化完成")

    async def mount_and_format_tape(
        self,
        backup_task: Optional[BackupTask] = None,
        force_format: bool = False
    ) -> Tuple[bool, str]:
        """
        挂载磁带（如有需要则格式化）

        Args:
            backup_task: 备份任务对象（用于通知和日志）
            force_format: 是否强制格式化（即使磁带已有LTFS格式）

        Returns:
            (是否成功, 消息)
        """
        if not self._initialized:
            return False, "磁带挂载器未初始化，请先调用 initialize()"

        logger.info("[磁带挂载器] 开始挂载磁带...")

        # 如果强制格式化，先卸载现有挂载
        if force_format:
            logger.info("[磁带挂载器] 强制格式化模式，先卸载现有挂载")
            await self.tape_handler.unmount_ltfs()
            await asyncio.sleep(2)
            success, msg = await self._format_and_mount(backup_task)
            return success, msg

        # 正常挂载流程（会自动处理非LTFS格式和有内容的磁带）
        success, msg = await self.tape_handler.mount_ltfs(backup_task)

        if success:
            logger.info(f"[磁带挂载器] ✅ 磁带挂载成功: {msg}")
        else:
            logger.error(f"[磁带挂载器] ❌ 磁带挂载失败: {msg}")

        return success, msg

    async def _format_and_mount(
        self,
        backup_task: Optional[BackupTask] = None
    ) -> Tuple[bool, str]:
        """
        格式化磁带并挂载

        Args:
            backup_task: 备份任务对象

        Returns:
            (是否成功, 消息)
        """
        # 重置设备
        await self.tape_handler.reset_tape_device()

        # 格式化
        volume_name = "BACKUP"
        serial = await self._get_tape_serial()
        success, format_msg = await self.tape_handler.format_as_ltfs(
            volume_name=volume_name,
            serial=serial
        )

        if not success:
            return False, f"格式化失败: {format_msg}"

        # 等待格式化完成
        await asyncio.sleep(2)

        # 挂载
        is_ltfs, msg = await self.tape_handler.check_ltfs_format()
        if is_ltfs:
            logger.info("[磁带挂载器] 格式化后挂载成功")
            return True, "格式化后挂载成功"

        return False, f"格式化后挂载失败: {msg}"

    async def _get_tape_serial(self) -> str:
        """获取磁带序列号"""
        try:
            if self.tape_handler.tape_manager:
                cartridges = await self.tape_handler.tape_manager.get_all_cartridges()
                if cartridges:
                    # 返回第一个磁带的序列号
                    return cartridges[0].tape_id
        except Exception as e:
            logger.warning(f"[磁带挂载器] 获取磁带序列号失败: {e}")

        # 使用默认序列号
        return None

    async def unmount_tape(self) -> bool:
        """
        卸载磁带

        Returns:
            是否成功
        """
        if not self._initialized:
            logger.warning("[磁带挂载器] 未初始化，无需卸载")
            return True

        logger.info("[磁带挂载器] 卸载磁带...")
        success = await self.tape_handler.unmount_ltfs()
        if success:
            logger.info("[磁带挂载器] 磁带已卸载")
        else:
            logger.warning("[磁带挂载器] 卸载失败")
        return success

    async def is_tape_mounted(self) -> bool:
        """
        检查磁带是否已挂载

        Returns:
            是否已挂载
        """
        if not self._initialized:
            return False
        return self.tape_handler._ltfs_mounted

    async def get_mount_point(self) -> Optional[Path]:
        """
        获取挂载点路径

        Returns:
            挂载点路径，如未挂载则返回 None
        """
        if not self._initialized or not self.tape_handler._ltfs_mounted:
            return None
        return self.tape_handler._ltfs_mount_point

    # ========== 类方法，用于快速调用 ==========

    @classmethod
    async def quick_mount(
        cls,
        tape_manager: TapeManager,
        dingtalk_notifier: Optional[DingTalkNotifier],
        backup_task: Optional[BackupTask] = None,
        force_format: bool = False
    ) -> Tuple[bool, str]:
        """
        快速挂载磁带（类方法，无需实例化）

        Args:
            tape_manager: 磁带管理器
            dingtalk_notifier: 钉钉通知器
            backup_task: 备份任务对象
            force_format: 是否强制格式化

        Returns:
            (是否成功, 消息)
        """
        mounter = cls()
        await mounter.initialize(tape_manager, dingtalk_notifier)
        return await mounter.mount_and_format_tape(backup_task, force_format)

    @classmethod
    async def quick_unmount(
        cls,
        tape_manager: TapeManager
    ) -> bool:
        """
        快速卸载磁带（类方法，无需实例化）

        Args:
            tape_manager: 磁带管理器

        Returns:
            是否成功
        """
        mounter = cls()
        await mounter.initialize(tape_manager, None)
        return await mounter.unmount_tape()


# ========== 便捷函数 ==========

async def mount_tape_for_backup(
    tape_manager: TapeManager,
    dingtalk_notifier: Optional[DingTalkNotifier],
    backup_task: Optional[BackupTask] = None
) -> Tuple[bool, str]:
    """
    为备份任务挂载磁带（便捷函数）

    Args:
        tape_manager: 磁带管理器
        dingtalk_notifier: 钉钉通知器
        backup_task: 备份任务对象

    Returns:
        (是否成功, 消息)
    """
    return await TapeMounter.quick_mount(tape_manager, dingtalk_notifier, backup_task)


async def unmount_tape_after_backup(
    tape_manager: TapeManager
) -> bool:
    """
    备份完成后卸载磁带（便捷函数）

    Args:
        tape_manager: 磁带管理器

    Returns:
        是否成功
    """
    return await TapeMounter.quick_unmount(tape_manager)
