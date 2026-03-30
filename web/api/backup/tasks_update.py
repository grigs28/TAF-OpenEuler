#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务更新
Backup Management API - Task Update
"""

import logging
import json
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.scheduler.db_utils import get_opengauss_connection
from models.system_log import OperationType
from utils.log_utils import log_operation
from .models import BackupTaskUpdate

logger = logging.getLogger(__name__)
router = APIRouter()


@router.put("/tasks/{task_id}")
async def update_backup_task(
    task_id: int,
    request: BackupTaskUpdate,
    http_request: Request
):
    """更新备份任务模板"""
    start_time = datetime.now()
    
    try:
        # 验证：只能更新模板
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                "SELECT is_template FROM backup_tasks WHERE id = $1",
                task_id
            )
            if not row:
                raise HTTPException(status_code=404, detail="备份任务不存在")
            if not row["is_template"]:
                raise HTTPException(status_code=400, detail="只能更新备份任务模板，不能更新执行记录")
        
        import json
        
        # 构建更新字段
        updates = {}
        if request.task_name is not None:
            updates["task_name"] = request.task_name
        if request.task_type is not None:
            updates["task_type"] = request.task_type.value
        if request.source_paths is not None:
            updates["source_paths"] = json.dumps(request.source_paths) if request.source_paths else None
        if request.exclude_patterns is not None:
            updates["exclude_patterns"] = json.dumps(request.exclude_patterns) if request.exclude_patterns else None
        if request.compression_enabled is not None:
            updates["compression_enabled"] = request.compression_enabled
        if request.encryption_enabled is not None:
            updates["encryption_enabled"] = request.encryption_enabled
        if request.retention_days is not None:
            updates["retention_days"] = request.retention_days
        if request.description is not None:
            updates["description"] = request.description
        if request.tape_device is not None:
            updates["tape_device"] = request.tape_device
        if request.enable_simple_scan is not None:
            updates["enable_simple_scan"] = request.enable_simple_scan
        
        if not updates:
            raise HTTPException(status_code=400, detail="没有提供要更新的字段")
        
        updates["updated_at"] = datetime.now()
        
        # 使用原生SQL更新（使用连接池）
        async with get_opengauss_connection() as conn:
            # 构建更新SQL
            set_clauses = []
            params = []
            param_index = 1

            for key, value in updates.items():
                if key == "source_paths" or key == "exclude_patterns":
                    set_clauses.append(f"{key} = ${param_index}::json")
                elif key == "task_type":
                    set_clauses.append(f"task_type = CAST(${param_index} AS backuptasktype)")
                elif key == "updated_at":
                    set_clauses.append(f"updated_at = ${param_index}")
                else:
                    set_clauses.append(f"{key} = ${param_index}")
                params.append(value)
                param_index += 1

            params.append(task_id)
            update_sql = f"""
                UPDATE backup_tasks
                SET {', '.join(set_clauses)}
                WHERE id = ${param_index}
            """
            await conn.execute(update_sql, *params)

            # 记录操作日志
            client_ip = http_request.client.host if http_request.client else None
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_operation(
                operation_type=OperationType.UPDATE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务模板: {updates.get('task_name', task_id)}",
                operation_name="更新备份任务模板",
                operation_description=f"更新备份任务模板: {updates.get('task_name', task_id)}",
                category="backup",
                success=True,
                result_message="备份任务模板已更新",
                old_values={},
                new_values=updates,
                changed_fields=list(updates.keys()),
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )

            return {"success": True, "message": "备份任务模板已更新"}
    
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="backup",
            resource_id=str(task_id),
            operation_name="更新备份任务模板",
            operation_description=f"更新备份任务模板失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"更新备份任务模板失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

