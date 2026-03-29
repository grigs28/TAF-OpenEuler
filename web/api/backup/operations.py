#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务操作
Backup Management API - Task Operations
"""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection, is_sqlite, get_sqlite_connection
from utils.log_utils import log_operation
from models.system_log import OperationType
from .utils import get_system_instance, _normalize_status_value

logger = logging.getLogger(__name__)
router = APIRouter()


@router.put("/tasks/{task_id}/cancel")
async def cancel_backup_task(task_id: int, http_request: Request):
    """取消备份任务（仅限执行记录）"""
    start_time = datetime.now()
    
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 先获取任务信息用于日志
        task_info = None
        if is_redis():
            # Redis 模式：使用 Redis 查询
            from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
            from config.redis_db import get_redis_client
            redis = await get_redis_client()
            task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
            task_data = await redis.hgetall(task_key)
            if task_data:
                task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                            v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                            for k, v in task_data.items()}
                task_info = {
                    "task_name": task_dict.get('task_name', ''),
                    "status": task_dict.get('status', 'pending')
                }
        elif is_opengauss():
            # 使用连接池
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT task_name, status FROM backup_tasks WHERE id = $1",
                    task_id
                )
                if row:
                    task_info = {
                        "task_name": row["task_name"],
                        "status": row["status"].value if hasattr(row["status"], "value") else str(row["status"])
                    }
        elif is_sqlite():
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute(
                    "SELECT task_name, status FROM backup_tasks WHERE id = ?",
                    (task_id,)
                )
                row = await cursor.fetchone()
                if row:
                    # 处理状态值
                    status_raw = row[1]
                    if isinstance(status_raw, str):
                        status_value = status_raw
                    else:
                        status_value = _normalize_status_value(status_raw)
                    task_info = {
                        "task_name": row[0],
                        "status": status_value
                    }

        success = await system.backup_engine.cancel_task(task_id)
        
        # 记录操作日志
        client_ip = http_request.client.host if http_request.client else None
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.EXECUTE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务: {task_info.get('task_name') if task_info else '未知'}" if task_info else f"备份任务: {task_id}",
                operation_name="取消备份任务",
                operation_description=f"取消备份任务: {task_info.get('task_name') if task_info else task_id}",
                category="backup",
                success=True,
                result_message="任务已取消",
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )
            
            return {"success": True, "message": "任务已取消"}
        else:
            await log_operation(
                operation_type=OperationType.EXECUTE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务: {task_info.get('task_name') if task_info else '未知'}" if task_info else f"备份任务: {task_id}",
                operation_name="取消备份任务",
                operation_description=f"取消备份任务失败: 任务不存在或无法取消",
                category="backup",
                success=False,
                error_message="任务不存在或无法取消",
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )
            
            raise HTTPException(status_code=404, detail="任务不存在或无法取消")

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.EXECUTE,
            resource_type="backup",
            resource_id=str(task_id),
            resource_name=f"备份任务: {task_id}",
            operation_name="取消备份任务",
            operation_description=f"取消备份任务失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"取消备份任务失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/tasks/{task_id}/status")
async def get_task_status(task_id: int, http_request: Request = None):
    """获取任务状态"""
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        status = await system.backup_engine.get_task_status(task_id)
        if status:
            return status
        else:
            raise HTTPException(status_code=404, detail="任务不存在")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

