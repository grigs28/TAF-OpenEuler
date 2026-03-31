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
        # openGauss 模式：始终使用简洁扫描（SimpleScanner）
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

