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
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, get_sqlite_connection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/history")
async def get_tape_history(request: Request, limit: int = 50, offset: int = 0):
    """获取磁带操作历史（从新的日志系统获取，使用openGauss原生SQL）"""
    start_time = datetime.now()
    try:
        # 检查是否为openGauss
        if is_opengauss():
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

        # 非openGauss数据库
        else:
            # 检查是否为Redis数据库，Redis不支持操作日志表
            from utils.scheduler.db_utils import is_redis
            if is_redis():
                # Redis模式下不返回操作日志（Redis没有对应的表结构）
                return {"success": True, "data": [], "total": 0}

            # 非openGauss数据库，使用原生SQL（SQLite）

            # 再次检查是否为SQLite
            if not is_sqlite():
                return {"success": True, "data": [], "total": 0}

            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT * FROM operation_logs
                    WHERE resource_type = ?
                    ORDER BY operation_time DESC
                    LIMIT ? OFFSET ?
                """, ("tape", limit, offset))
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                operation_logs = [dict(zip(columns, row)) for row in rows]

                history = []
                for log in operation_logs:
                    operation_time = log.get('operation_time')
                    if operation_time and hasattr(operation_time, 'isoformat'):
                        operation_time_str = operation_time.isoformat()
                    elif operation_time:
                        operation_time_str = str(operation_time)
                    else:
                        operation_time_str = None

                    history.append({
                        "id": log.get("id"),
                        "operation_time": operation_time_str,
                        "operation_type": log.get("operation_type"),
                        "operation_user": log.get("operation_user"),
                        "resource_type": log.get("resource_type"),
                        "resource_id": log.get("resource_id"),
                        "details": log.get("details"),
                        "ip_address": log.get("ip_address"),
                        "user_agent": log.get("user_agent"),
                        "result": log.get("result"),
                        "error_message": log.get("error_message"),
                        "duration_ms": log.get("duration_ms")
                    })

                return {
                    "success": True,
                    "history": history,
                    "count": len(history)
                }

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