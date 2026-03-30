#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_history
Tape Management API - tape_history
"""

import logging
import traceback
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request

from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_system
from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/history")
async def get_tape_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        # 使用openGauss连接查询操作日志
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch("""
                SELECT * FROM operation_logs
                WHERE resource_type = $1
                ORDER BY operation_time DESC
                LIMIT $2 OFFSET $3
            """, "tape", limit, offset)

            history = []
            for row in rows:
                operation_time = row.get('operation_time')
                operation_time_str = operation_time.isoformat() if operation_time else None

                history.append({
                    "id": row.get("id"),
                    "operation_time": operation_time_str,
                    "operation_type": row.get("operation_type"),
                    "operation_user": row.get("operation_user"),
                    "resource_type": row.get("resource_type"),
                    "resource_id": row.get("resource_id"),
                    "details": row.get("details"),
                    "ip_address": row.get("ip_address"),
                    "user_agent": row.get("user_agent"),
                    "result": row.get("result"),
                    "error_message": row.get("error_message"),
                    "duration_ms": row.get("duration_ms")
                })

            # 获取总数
            count_row = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM operation_logs
                WHERE resource_type = $1
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
