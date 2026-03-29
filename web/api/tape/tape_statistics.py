#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_statistics
Tape Management API - tape_statistics
"""

import logging
import traceback
import json
import re
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from .tape_utils import normalize_tape_label, check_tape_exists_sqlite, count_serial_numbers_sqlite, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, is_redis, get_sqlite_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()


def parse_expiry_date_for_inventory(expiry_date):
    """解析过期日期（用于库存统计）"""
    from datetime import date, datetime
    if isinstance(expiry_date, date):
        return expiry_date
    if isinstance(expiry_date, datetime):
        return expiry_date.date()
    if isinstance(expiry_date, str):
        try:
            dt = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
            return dt.date()
        except:
            return date.today()
    return date.today()


async def check_tape_exists_sqlite(db_manager, tape_id: str, label: str) -> tuple[bool, bool]:
    """检查磁带是否存在（SQLite版本）"""

    async with get_sqlite_connection() as conn:
        # 检查 tape_id
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE tape_id = ?", (tape_id,))
        row = await cursor.fetchone()
        tape_exists = (row[0] > 0) if row else False

        # 检查 label
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE label = ?", (label,))
        row = await cursor.fetchone()
        label_exists = (row[0] > 0) if row else False

        return tape_exists, label_exists


async def count_serial_numbers_sqlite(db_manager, pattern: str) -> int:
    """统计序列号数量（SQLite版本）"""
    from utils.scheduler.db_utils import is_redis

    # 检查数据库类型
    if is_redis():
        raise ValueError("Redis模式下不支持磁带管理功能")
    if not is_sqlite():
        raise ValueError("当前数据库类型不支持磁带管理功能")

    async with get_sqlite_connection() as conn:
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM tape_cartridges
            WHERE serial_number IS NOT NULL AND serial_number LIKE ?
        """, (pattern,))
        row = await cursor.fetchone()
        return row[0] if row else 0


def normalize_tape_label(label: Optional[str], year: int, month: int) -> str:
    target_year = f"{year:04d}"
    target_month = f"{month:02d}"
    default_seq = "01"
    default_label = f"TP{target_year}{target_month}{default_seq}"

    if not label:
        return default_label

    clean_label = label.strip().upper()

    def build_label(seq: str, suffix: str = "") -> str:
        seq = (seq if seq and seq.isdigit() else default_seq).zfill(2)[:2]
        return f"TP{target_year}{target_month}{seq}{suffix}"

    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TP(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    return default_label


# Note: /inventory route is handled in tape_query.py to avoid conflicts
# The inventory statistics functionality has been moved to tape_query.py