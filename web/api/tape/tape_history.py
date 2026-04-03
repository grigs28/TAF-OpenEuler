#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_history
Tape Management API - tape_history
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
from .tape_utils import normalize_tape_label, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import get_opengauss_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/history")
async def get_tape_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        # 使用openGauss连接查询系统日志（磁带相关）
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    id, log_time, log_level, category, message,
                    module, function, duration_ms, exception_type
                FROM system_logs
                WHERE category = $1
                ORDER BY log_time DESC
                LIMIT $2 OFFSET $3
            """, "tape", limit, offset)

            history = []
            for row in rows:
                ts = row['log_time']
                level = row['log_level']
                if hasattr(level, 'value'):
                    level = level.value
                else:
                    level = str(level) if level else "info"

                history.append({
                    "id": row['id'],
                    "operation_time": ts.isoformat() if ts else None,
                    "operation_type": row['function'] or row['module'] or "",
                    "operation_user": "system",
                    "resource_type": "tape",
                    "resource_id": row['module'] or "",
                    "details": row['message'] or "",
                    "ip_address": None,
                    "user_agent": None,
                    "result": "success" if level != "error" else "error",
                    "error_message": row['message'] if level == "error" else None,
                    "duration_ms": row['duration_ms']
                })

            # 获取总数
            count_row = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM system_logs
                WHERE category = $1
            """, "tape")
            total = count_row["total"] if count_row else 0

            return {"success": True, "data": history, "total": total}

    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取磁带操作历史失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_tape_history",
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))



