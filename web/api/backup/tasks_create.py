#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务创建
Backup Management API - Task Create
"""

import logging
import json
from typing import Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from models.backup import BackupTaskStatus
from utils.scheduler.db_utils import get_opengauss_connection
from models.system_log import OperationType
from utils.log_utils import log_operation
from .models import BackupTaskRequest
from .utils import get_system_instance

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/tasks", response_model=Dict[str, Any])
async def create_backup_task(
    request: BackupTaskRequest,
    http_request: Request
):
    """创建备份任务配置（模板）
    
    此接口创建备份任务配置模板，不立即执行。
    创建的模板可以在计划任务模块中选择并执行。
    """
    start_time = datetime.now()
    
    try:
        # 获取系统实例
        system = get_system_instance(http_request)
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 验证源路径
        for path in request.source_paths:
            if not path or not path.strip():
                raise HTTPException(status_code=400, detail="源路径不能为空")

        # 创建备份任务模板
        async with get_opengauss_connection() as conn:
            task_id = await conn.fetchval(
                """
                INSERT INTO backup_tasks (
                    task_name, task_type, status, is_template, source_paths, exclude_patterns,
                    compression_enabled, encryption_enabled, retention_days, description,
                    tape_device, enable_simple_scan, created_at, updated_at, created_by
                ) VALUES (
                    $1, CAST($2 AS backuptasktype), CAST($3 AS backuptaskstatus), $4, $5, $6,
                    $7, $8, $9, $10,
                    $11, $12, $13, $14, $15
                )
                RETURNING id
                """,
                request.task_name,
                request.task_type.value,
                BackupTaskStatus.PENDING.value,
                True,  # is_template
                json.dumps(request.source_paths) if request.source_paths else None,
                json.dumps(request.exclude_patterns) if request.exclude_patterns else None,
                request.compression_enabled,
                request.encryption_enabled,
                request.retention_days,
                request.description,
                request.tape_device,
                getattr(request, 'enable_simple_scan', True),  # enable_simple_scan，默认 True
                datetime.now(),
                datetime.now(),
                'backup_api'
            )

            # ========= 多表方案：为该模板任务预创建 backup_files 分组和物理表 =========
            # 物理表名采用固定前缀 + 任务ID，避免 SQL 注入
            table_name = f"backup_files_{task_id:06d}"

            # 1. 在 backup_files_groups 中创建元数据记录
            backup_files_group_id = await conn.fetchval(
                """
                INSERT INTO backup_files_groups (table_name, task_id)
                VALUES ($1, $2)
                RETURNING id
                """,
                table_name,
                task_id,
            )

            # 2. 更新 backup_tasks 表，写入 group_id 和 table_name
            await conn.execute(
                """
                UPDATE backup_tasks
                SET backup_files_group_id = $1,
                    backup_files_table = $2
                WHERE id = $3
                """,
                backup_files_group_id,
                table_name,
                task_id,
            )

            # 3. 为该任务创建物理表（基于 backup_files_template 结构）
            # 注意：表名不能用参数占位符，只能通过受控字符串拼接
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                LIKE backup_files_template INCLUDING ALL
            )
            """
            await conn.execute(create_sql)

            # openGauss / psycopg3 binary protocol 需要显式提交事务
            actual_conn = conn._conn if hasattr(conn, "_conn") else conn
            try:
                await actual_conn.commit()
            except Exception as commit_err:
                logger.warning(f"[备份任务模板] 提交创建任务及其 backup_files 分组/表事务失败（可能已自动提交）: {commit_err}")

            # 记录操作日志
            client_ip = http_request.client.host if http_request.client else None
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_operation(
                operation_type=OperationType.CREATE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"备份任务模板: {request.task_name}",
                operation_name="创建备份任务模板",
                operation_description=f"创建备份任务配置模板: {request.task_name}",
                category="backup",
                success=True,
                result_message="备份任务配置已创建",
                new_values={
                    "task_name": request.task_name,
                    "task_type": request.task_type.value,
                    "source_paths": request.source_paths,
                    "tape_device": request.tape_device
                },
                ip_address=client_ip,
                request_method="POST",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )

            return {
                "success": True,
                "task_id": task_id,
                "message": "备份任务配置已创建",
                "task_name": request.task_name,
                "is_template": True
            }

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="backup",
            resource_name=f"备份任务模板: {request.task_name}",
            operation_name="创建备份任务模板",
            operation_description=f"创建备份任务配置模板失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="POST",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"创建备份任务配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

