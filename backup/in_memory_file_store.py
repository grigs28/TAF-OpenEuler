#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
纯内存文件存储 - 替代扫描阶段的数据库写入
In-Memory File Store - Replace database writes during scanning phase

当 SCAN_MEMORY_ONLY=true 时，扫描器将文件记录写入此内存存储，
预取器从此存储读取文件并成组，成组后直接从内存删除，下次始终从头部读取。
"""

import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class InMemoryFileStore:
    """线程安全的内存文件存储，替代扫描阶段的数据库写入

    数据流：
    1. SimpleScanner → add_files() 写入文件记录（追加到尾部）
    2. FileGroupPrefetcher → get_pending_files() 从头部读取未排队文件
    3. FileGroupPrefetcher → remove_files() 成组后删除已排队文件
    4. 下次读取始终从头部开始，无需游标定位
    """

    def __init__(self, backup_set_id: int):
        self.backup_set_id = backup_set_id

        # 文件记录列表（按写入顺序追加，成组后从头部删除）
        self._files: List[Dict] = []

        # 异步锁（保护 _files）
        self._lock = asyncio.Lock()

        # 扫描状态
        self._scan_completed: bool = False
        self._scan_completed_event = asyncio.Event()  # 通知预取器扫描完成
        self._new_files_event = asyncio.Event()  # 通知预取器有新文件

        # 统计信息
        self._total_files: int = 0
        self._total_bytes: int = 0

    async def add_files(self, file_info_batch: List[Dict]) -> int:
        """添加一批文件记录

        Args:
            file_info_batch: 文件信息列表，每个元素包含 path/name/size 等字段

        Returns:
            成功添加的文件数
        """
        if not file_info_batch:
            return 0

        async with self._lock:
            for fi in file_info_batch:
                self._files.append(fi)
                self._total_bytes += fi.get('size', 0) or 0
            self._total_files += len(file_info_batch)

        # 通知等待的预取器
        self._new_files_event.set()

        logger.debug(
            f"[内存存储] 添加 {len(file_info_batch)} 个文件，"
            f"总计: {self._total_files} 个文件，"
            f"总大小: {self._format_bytes(self._total_bytes)}"
        )
        return len(file_info_batch)

    async def get_pending_files(self, limit: int) -> List[Dict]:
        """从头部读取未排队文件（成组后由 remove_files 删除，所以始终从 index 0 读）

        Args:
            limit: 最大返回数量

        Returns:
            文件信息列表
        """
        async with self._lock:
            return list(self._files[:limit])

    async def remove_files(self, file_paths: List[str]):
        """成组后从内存中删除文件（释放内存，下次读取无需游标定位）

        Args:
            file_paths: 要删除的文件路径列表
        """
        if not file_paths:
            return

        path_set = set(file_paths)
        async with self._lock:
            before = len(self._files)
            self._files = [f for f in self._files if f.get('path', '') not in path_set]
            removed = before - len(self._files)

        logger.debug(f"[内存存储] 删除 {removed} 个已排队文件，剩余 {len(self._files)} 个")

    def get_scan_status(self) -> str:
        """获取扫描状态（替代数据库查询）

        Returns:
            'completed' 或 'running'
        """
        return 'completed' if self._scan_completed else 'running'

    async def set_scan_completed(self):
        """标记扫描完成"""
        async with self._lock:
            self._scan_completed = True
        self._scan_completed_event.set()
        logger.info(
            f"[内存存储] 扫描完成标记已设置，"
            f"总计: {self._total_files} 个文件，"
            f"总大小: {self._format_bytes(self._total_bytes)}"
        )

    async def get_pending_count(self) -> int:
        """获取未排队文件数量"""
        async with self._lock:
            return len(self._files)

    async def wait_for_new_files(self, timeout: float = 5.0) -> bool:
        """等待新文件添加

        Args:
            timeout: 超时时间（秒）

        Returns:
            True 表示有新文件或扫描完成，False 表示超时
        """
        # 清除之前的事件状态
        self._new_files_event.clear()

        # 等待新文件或扫描完成
        done, pending = await asyncio.wait(
            [
                asyncio.create_task(self._new_files_event.wait()),
                asyncio.create_task(self._scan_completed_event.wait()),
            ],
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # 取消未完成的任务
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        return len(done) > 0

    async def is_scan_done(self) -> bool:
        """扫描是否已完成"""
        return self._scan_completed

    async def get_all_pending_files(self) -> List[Dict]:
        """获取所有未排队文件（用于扫描完成后的兜底查询）"""
        async with self._lock:
            return list(self._files)

    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'total_files': self._total_files,
            'total_bytes': self._total_bytes,
            'pending_files': len(self._files),
            'scan_completed': self._scan_completed,
            'memory_estimate_mb': self._total_files * 0.5 / 1024,  # 每个文件约0.5KB
        }

    @staticmethod
    def _format_bytes(size: int) -> str:
        """格式化字节大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
