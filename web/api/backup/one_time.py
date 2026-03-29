#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一次性备份任务 API
One-time Backup Task API

用于创建并立即执行一次性备份任务，将源路径备份到磁带。
"""

import logging
import json
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, BackgroundTasks
from pydantic import BaseModel, Field

from models.backup import BackupTaskStatus, BackupTaskType
from models.data_classes import BackupTask
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from models.system_log import OperationType
from utils.log_utils import log_operation

logger = logging.getLogger(__name__)
router = APIRouter()


class OneTimeBackupRequest(BaseModel):
    """一次性备份任务请求模型"""
    source_paths: List[str] = Field(..., description="源路径列表")
    tape_id: Optional[str] = Field(None, description="目标磁带ID（可选，不指定则自动选择）")
    task_name: Optional[str] = Field(None, description="任务名称（可选）")
    description: Optional[str] = Field(None, description="任务描述（可选）")
    compression_enabled: bool = Field(True, description="是否启用压缩")
    exclude_patterns: Optional[List[str]] = Field(None, description="排除模式列表")


class OneTimeBackupResponse(BaseModel):
    """一次性备份任务响应模型"""
    success: bool
    message: str
    task_id: Optional[int] = None
    set_id: Optional[str] = None
    tape_id: Optional[str] = None


@router.post("/one-time", response_model=OneTimeBackupResponse)
async def create_one_time_backup(
    request: OneTimeBackupRequest,
    http_request: Request,
    background_tasks: BackgroundTasks
):
    """
    创建并立即执行一次性备份任务

    将指定源路径的文件备份到磁带。

    Args:
        request: 备份请求参数
        http_request: HTTP 请求对象

    Returns:
        OneTimeBackupResponse: 包含任务ID和状态
    """
    start_time = datetime.now()

    try:
        # 获取系统实例
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 验证源路径
        if not request.source_paths:
            raise HTTPException(status_code=400, detail="源路径不能为空")

        for path in request.source_paths:
            if not path or not path.strip():
                raise HTTPException(status_code=400, detail="源路径不能包含空值")

        # 生成任务名称
        task_name = request.task_name or f"一次性备份_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 确定目标磁带
        tape_id = request.tape_id
        if not tape_id:
            # 自动选择可用磁带
            tape_id = await _select_available_tape()
            if not tape_id:
                raise HTTPException(status_code=400, detail="没有可用的磁带，请先创建磁带")

        # 创建备份任务
        if is_opengauss():
            task_id, set_id = await _create_backup_task_opengauss(
                task_name=task_name,
                source_paths=request.source_paths,
                tape_id=tape_id,
                description=request.description,
                compression_enabled=request.compression_enabled,
                exclude_patterns=request.exclude_patterns
            )
        else:
            raise HTTPException(status_code=500, detail="仅支持 openGauss 数据库")

        # 在后台启动备份任务
        background_tasks.add_task(
            _execute_backup_task,
            system=system,
            task_id=task_id,
            set_id=set_id
        )

        # 记录操作日志
        client_ip = http_request.client.host if http_request.client else None
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="backup",
            resource_id=str(task_id),
            resource_name=task_name,
            operation_name="创建一次性备份任务",
            operation_description=f"创建并启动一次性备份任务: {task_name}",
            category="backup",
            success=True,
            result_message="备份任务已创建并启动",
            new_values={
                "task_name": task_name,
                "source_paths": request.source_paths,
                "tape_id": tape_id,
                "compression_enabled": request.compression_enabled
            },
            ip_address=client_ip,
            request_method="POST",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )

        return OneTimeBackupResponse(
            success=True,
            message=f"备份任务已创建并启动，任务ID: {task_id}",
            task_id=task_id,
            set_id=set_id,
            tape_id=tape_id
        )

    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.error(f"创建一次性备份任务失败: {error_msg}", exc_info=True)

        # 记录失败日志
        client_ip = http_request.client.host if http_request.client else None
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="backup",
            resource_name=request.task_name or "一次性备份",
            operation_name="创建一次性备份任务",
            operation_description=f"创建一次性备份任务失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="POST",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )

        raise HTTPException(status_code=500, detail=error_msg)


async def _select_available_tape() -> Optional[str]:
    """选择一个可用的磁带"""
    try:
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT tape_id FROM tape_cartridges
                WHERE status = 'available'
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            if row:
                return row['tape_id']
            return None
    except Exception as e:
        logger.error(f"查询可用磁带失败: {str(e)}")
        return None


async def _create_backup_task_opengauss(
    task_name: str,
    source_paths: List[str],
    tape_id: str,
    description: Optional[str],
    compression_enabled: bool,
    exclude_patterns: Optional[List[str]]
) -> tuple:
    """在 openGauss 中创建备份任务"""
    import uuid

    async with get_opengauss_connection() as conn:
        # 生成备份集ID
        set_id = f"SET{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # 插入备份任务
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
            task_name,
            BackupTaskType.FULL.value,
            BackupTaskStatus.PENDING.value,
            False,  # 不是模板，是实际任务
            json.dumps(source_paths),
            json.dumps(exclude_patterns) if exclude_patterns else None,
            compression_enabled,
            False,  # encryption_enabled
            180,  # retention_days (6个月)
            description or f"一次性备份到磁带 {tape_id}",
            tape_id,
            True,  # enable_simple_scan
            datetime.now(),
            datetime.now(),
            'one_time_api'
        )

        # 创建备份集记录
        now = datetime.now()
        await conn.execute(
            """
            INSERT INTO backup_sets (
                set_id, set_name, backup_group, backup_type, backup_time, backup_task_id, tape_id, status, total_files, total_bytes,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            set_id,
            task_name,  # set_name 使用任务名称
            'one_time',  # backup_group: 一次性备份
            'full',  # backup_type: 完整备份
            now,  # backup_time: 备份时间
            task_id,
            tape_id,
            'active',
            0,
            0,
            now,
            now
        )

        # 创建 backup_files 分组表
        table_name = f"backup_files_{task_id:06d}"

        backup_files_group_id = await conn.fetchval(
            """
            INSERT INTO backup_files_groups (table_name, task_id)
            VALUES ($1, $2)
            RETURNING id
            """,
            table_name,
            task_id,
        )

        # 更新 backup_tasks 表
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

        # 创建物理表
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            LIKE backup_files_template INCLUDING ALL
        )
        """
        await conn.execute(create_sql)

        # 提交事务
        actual_conn = conn._conn if hasattr(conn, "_conn") else conn
        try:
            await actual_conn.commit()
        except Exception as commit_err:
            logger.warning(f"提交事务失败（可能已自动提交）: {commit_err}")

        logger.info(f"创建一次性备份任务成功: task_id={task_id}, set_id={set_id}, tape_id={tape_id}")

        return task_id, set_id


async def _execute_backup_task(system, task_id: int, set_id: str):
    """执行备份任务（后台任务）"""
    try:
        logger.info(f"开始执行一次性备份任务: task_id={task_id}, set_id={set_id}")

        # 从数据库加载 BackupTask 对象
        backup_task = await _load_backup_task(task_id)
        if not backup_task:
            logger.error(f"无法加载备份任务: task_id={task_id}")
            return

        # 获取备份引擎
        if hasattr(system, 'backup_engine') and system.backup_engine:
            backup_engine = system.backup_engine

            # 执行备份
            success = await backup_engine.execute_backup_task(backup_task=backup_task, manual_run=True)
            if success:
                logger.info(f"一次性备份任务执行完成: task_id={task_id}")
            else:
                logger.error(f"一次性备份任务执行失败: task_id={task_id}")
        else:
            logger.error("备份引擎未初始化，无法执行备份任务")

    except Exception as e:
        logger.error(f"执行一次性备份任务失败: task_id={task_id}, error={str(e)}", exc_info=True)


async def _load_backup_task(task_id: int) -> Optional[BackupTask]:
    """从数据库加载备份任务"""
    try:
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT * FROM backup_tasks WHERE id = $1
                """,
                task_id
            )
            if not row:
                return None

            # 构建 BackupTask 对象
            task = BackupTask(
                id=row['id'],
                task_name=row['task_name'] or '',
                task_type=BackupTaskType(row['task_type']) if row['task_type'] else BackupTaskType.FULL,
                description=row['description'] or '',
                status=BackupTaskStatus(row['status']) if row['status'] else BackupTaskStatus.PENDING,
                is_template=row['is_template'] or False,
                template_id=row['template_id'],
                source_paths=row['source_paths'] if isinstance(row['source_paths'], list) else (json.loads(row['source_paths']) if row['source_paths'] else []),
                exclude_patterns=row['exclude_patterns'] if isinstance(row['exclude_patterns'], list) else (json.loads(row['exclude_patterns']) if row['exclude_patterns'] else []),
                compression_enabled=row['compression_enabled'] if row['compression_enabled'] is not None else True,
                encryption_enabled=row['encryption_enabled'] or False,
                retention_days=row['retention_days'] or 180,
                enable_simple_scan=row['enable_simple_scan'] if row['enable_simple_scan'] is not None else True,
                scheduled_time=row['scheduled_time'],
                started_at=row['started_at'],
                completed_at=row['completed_at'],
                executed_by=row['executed_by'] or '',
                worker_id=row['worker_id'] or '',
                total_files=row['total_files'] or 0,
                processed_files=row['processed_files'] or 0,
                total_bytes=row['total_bytes'] or 0,
                processed_bytes=row['processed_bytes'] or 0,
                compressed_bytes=row['compressed_bytes'] or 0,
                tape_device=row['tape_device'] or '',
                tape_id=row['tape_device'] or '',  # tape_device 存的是磁带ID
                backup_set_id=row['backup_set_id'] or '',
                progress_percent=float(row['progress_percent']) if row['progress_percent'] else 0.0,
                error_message=row['error_message'] or '',
                created_by=row['created_by'] or '',
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
            return task
    except Exception as e:
        logger.error(f"加载备份任务失败: task_id={task_id}, error={str(e)}", exc_info=True)
        return None


# 添加到 backup 路由器的注册方法
def register_one_time_router(backup_router):
    """注册一次性备份路由到备份路由器"""
    backup_router.include_router(router, tags=["一次性备份"])
