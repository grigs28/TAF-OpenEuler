#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_delete
Tape Management API - tape_delete
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



@router.delete("/delete/{tape_id}")
async def delete_tape(tape_id: str, http_request: Request):
    """删除磁带记录"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "DELETE"
    request_url = str(http_request.url)
    
    try:
        from config.settings import get_settings
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            logger.warning(f"[Redis模式] 删除磁带暂未实现: {tape_id}")
            raise HTTPException(status_code=501, detail="Redis模式下暂不支持删除磁带功能")
        
        # 检查是否为 SQLite
        if is_sqlite():
            # SQLite 版本暂不支持删除磁带（需要实现）
            logger.warning(f"[SQLite模式] 删除磁带暂未实现: {tape_id}")
            raise HTTPException(status_code=501, detail="SQLite模式下暂不支持删除磁带功能")
        
        # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
        from utils.db_connection_helper import get_psycopg_connection_from_url
        
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        
        try:
            with conn.cursor() as cur:
                # 检查磁带是否存在
                cur.execute("""
                    SELECT tape_id, label FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    await log_operation(
                        operation_type=OperationType.DELETE,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="删除磁带",
                        operation_description=f"删除磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带 {tape_id} 不存在",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                tape_label = existing[1]
                
                # 删除磁带记录
                cur.execute("""
                    DELETE FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                
                conn.commit()
                logger.info(f"删除磁带记录: {tape_id}")
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=tape_label,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
            category="tape",
            success=True,
            result_message=f"磁带 {tape_id} 删除成功",
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 删除成功",
            "tape_id": tape_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"删除磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="delete_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))
