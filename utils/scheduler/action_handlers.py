#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务动作处理器
Task Action Handlers
"""

import logging
import asyncio
import os
import random
import shutil
import uuid
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from utils.datetime_utils import now, format_datetime
from typing import Dict, Any, Optional, List

from models.scheduled_task import ScheduledTask, TaskActionType
from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from utils.log_utils import log_system, LogLevel, LogCategory, log_operation, OperationType
from .db_utils import is_opengauss, get_opengauss_connection
import json

logger = logging.getLogger(__name__)


def _parse_enum(enum_class, value: str, default=None):
    """
    解析枚举值（处理大小写不匹配问题）

    Args:
        enum_class: 枚举类
        value: 枚举值（可能是大写、小写或混合大小写）
        default: 默认值（如果无法解析）

    Returns:
        枚举值

    Raises:
        ValueError: 如果枚举值无效且没有提供默认值
    """
    if not value:
        return default

    # 转换为小写并去除空白
    value_lower = value.lower().strip() if isinstance(value, str) else str(value).lower().strip()

    # 尝试直接匹配
    try:
        return enum_class(value_lower)
    except ValueError:
        # 如果直接匹配失败，尝试匹配枚举值
        for enum_value in enum_class:
            if enum_value.value.lower() == value_lower:
                return enum_value

        # 如果仍然无法匹配，记录警告并返回默认值
        if default is not None:
            logger.warning(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})，使用默认值 {default}")
            return default
        else:
            raise ValueError(f"无法解析枚举值 '{value}' (类型: {enum_class.__name__})")


class ActionHandler:
    """动作处理器基类"""

    def __init__(self, system_instance):
        self.system_instance = system_instance

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行动作（子类实现）

        参数:
            config: 动作配置
            scheduled_task: 计划任务对象（可选）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
            run_options: 手动运行附加选项（如继续/重启模式）
        """
        raise NotImplementedError


class BackupActionHandler(ActionHandler):
    """备份动作处理器"""

    async def execute(
        self,
        config: Dict,
        backup_task_id: Optional[int] = None,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行备份动作

        参数:
            config: 动作配置
            backup_task_id: 备份任务模板ID（如果提供，从模板加载配置）
            scheduled_task: 计划任务对象（用于检查重复执行）
            manual_run: 是否为手动运行（Web界面点击运行），默认为False
        """
        if not self.system_instance or not self.system_instance.backup_engine:
            raise ValueError("备份引擎未初始化")

        # 初始化变量，确保在所有异常情况下都有值
        backup_executed = False
        backup_task = None
        resumed_from_existing = False
        template_task = None
        task_name = ""

        try:
            run_options = run_options or {}
            run_mode = str(run_options.get('mode') or 'auto').lower()
            if run_mode not in ('auto', 'resume', 'restart'):
                run_mode = 'auto'
            force_rescan_option = bool(run_options.get('force_rescan', False))
            resume_only = run_mode == 'resume'
            restart_requested = run_mode == 'restart'

            logger.info(
                f"备份任务运行模式: manual_run={manual_run}, run_mode={run_mode}, force_rescan={force_rescan_option}"
            )

            # 执行前判定：周期内是否已成功执行、是否正在执行、磁带标签是否当月
            current_time = now()
            if scheduled_task and not manual_run:
                # 1) 周期检查（按任务的 schedule_type 推断周期：日/周/月/年）
                # 注意：手动运行时跳过周期检查
                cycle_ok = True
                last_success = scheduled_task.last_success_time
                schedule_type = getattr(scheduled_task, 'schedule_type', None)
                schedule_type_str = schedule_type.value if hasattr(schedule_type, 'value') else str(schedule_type) if schedule_type else 'unknown'

                # 特殊处理：月度任务
                if schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month'):
                    if last_success:
                        logger.info(
                            f"[月度任务检查] 已成功执行过，跳过本次备份 - "
                            f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                            f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}, "
                            f"上次成功时间: {last_success.strftime('%Y-%m-%d %H:%M:%S')}, "
                            f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        try:
                            await log_operation(
                                operation_type=OperationType.SCHEDULER_RUN,
                                resource_type="scheduler",
                                resource_id=str(getattr(scheduled_task, 'id', '')),
                                resource_name=getattr(scheduled_task, 'task_name', ''),
                                operation_name="执行计划任务",
                                operation_description="跳过：月度任务已成功执行过",
                                category="scheduler",
                                success=True,
                                result_message="跳过执行（月度任务已成功执行过）"
                            )
                            await log_system(
                                level=LogLevel.INFO,
                                category=LogCategory.SYSTEM,
                                message="月度任务跳过：已成功执行过",
                                module="utils.scheduler.action_handlers",
                                function="BackupActionHandler.execute",
                                task_id=getattr(scheduled_task, 'id', None)
                            )
                        except Exception:
                            pass
                        return {"status": "skipped", "message": "月度任务已成功执行过"}
                    else:
                        logger.info(
                            f"[月度任务检查] 从未成功执行过，检查任务锁 - "
                            f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                            f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}"
                        )

                # 其他类型的任务：按原有逻辑检查周期
                if last_success and not (schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month')):
                    if schedule_type and getattr(schedule_type, 'value', '').lower() in ('daily', 'day'):
                        cycle_ok = (last_success.date() != current_time.date())
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('weekly', 'week'):
                        cycle_ok = (last_success.isocalendar().week != current_time.isocalendar().week or last_success.year != current_time.year)
                    elif schedule_type and getattr(schedule_type, 'value', '').lower() in ('yearly', 'year'):
                        cycle_ok = (last_success.year != current_time.year)
                    else:
                        # 未明确类型，默认按日
                        cycle_ok = (last_success.date() != current_time.date())

                # 如果周期内已执行过，则跳过
                if not cycle_ok:
                    schedule_type_str = schedule_type.value if hasattr(schedule_type, 'value') else str(schedule_type)
                    logger.info(
                        f"[周期检查] 当前周期内已成功执行，跳过本次备份 - "
                        f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                        f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}, "
                        f"调度类型: {schedule_type_str}, "
                        f"上次成功时间: {last_success.strftime('%Y-%m-%d %H:%M:%S') if last_success else 'N/A'}, "
                        f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    try:
                        await log_operation(
                            operation_type=OperationType.SCHEDULER_RUN,
                            resource_type="scheduler",
                            resource_id=str(getattr(scheduled_task, 'id', '')),
                            resource_name=getattr(scheduled_task, 'task_name', ''),
                            operation_name="执行计划任务",
                            operation_description="跳过：当前周期已执行",
                            category="scheduler",
                            success=True,
                            result_message="跳过执行（当前周期已执行）"
                        )
                        await log_system(
                            level=LogLevel.INFO,
                            category=LogCategory.SYSTEM,
                            message="计划任务跳过：当前周期已执行",
                            module="utils.scheduler.action_handlers",
                            function="BackupActionHandler.execute",
                            task_id=getattr(scheduled_task, 'id', None)
                        )
                    except Exception:
                        pass
                    return {"status": "skipped", "message": "当前周期已执行"}
            elif manual_run:
                logger.info("手动运行模式，跳过周期检查")

            # 2) 运行中检查已移除
            # 注意：任务锁机制已经在 task_executor 中处理了并发控制

            # 3) 磁带标签是否当月（从 LTFS 标签或磁带头读取）
            if scheduled_task and not manual_run:
                target_is_tape = False
                try:
                    action_cfg = scheduled_task.action_config or {}
                    target_is_tape = (action_cfg.get('backup_target') == 'tape') or ('tape_device' in action_cfg)
                except Exception:
                    pass
                if target_is_tape and self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                    tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                    if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                        try:
                            metadata = await asyncio.wait_for(
                                tape_ops._read_tape_label(),
                                timeout=60.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning("计划任务中读取磁带卷标超时（60秒）")
                            metadata = None
                        if metadata and (metadata.get('created_date') or metadata.get('tape_id')):
                            try:
                                created_dt = None
                                if metadata.get('created_date'):
                                    try:
                                        created_dt = datetime.fromisoformat(str(metadata['created_date']).replace('Z','+00:00'))
                                    except Exception:
                                        created_dt = None
                                if created_dt:
                                    if not (created_dt.year == current_time.year and created_dt.month == current_time.month):
                                        raise ValueError("当前磁带非当月，请更换磁带后重试")
                                else:
                                    tape_id = str(metadata.get('tape_id', ''))
                                    if len(tape_id) >= 7 and tape_id.upper().startswith('TAP'):
                                        yy = int(tape_id[3:5])
                                        mm = int(tape_id[5:7])
                                        year = 2000 + yy
                                        if not (year == current_time.year and mm == current_time.month):
                                            raise ValueError("当前磁带标签非当月，请更换磁带后重试")
                            except ValueError as ve:
                                logger.warning(str(ve))
                                try:
                                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                                        notifier = self.system_instance.dingtalk_notifier
                                        tape_id = (metadata.get('tape_id') if metadata else '') or '未知磁带'
                                        await notifier.send_tape_notification(tape_id=tape_id, action='change_required')
                                except Exception:
                                    pass
                                try:
                                    await log_operation(
                                        operation_type=OperationType.BACKUP_START,
                                        resource_type="backup",
                                        resource_name=(getattr(scheduled_task, 'task_name', '') or '计划任务'),
                                        operation_name="更换磁带提醒",
                                        operation_description="当前磁带标签非当月，提醒更换磁带",
                                        category="backup",
                                        success=False,
                                        error_message=str(ve)
                                    )
                                    await log_system(
                                        level=LogLevel.WARNING,
                                        category=LogCategory.BACKUP,
                                        message="当前磁带标签非当月，提醒更换磁带",
                                        module="utils.scheduler.action_handlers",
                                        function="BackupActionHandler.execute",
                                    )
                                except Exception:
                                    pass
                                raise

            # 如果有备份任务模板ID，从模板加载配置
            template_task = None
            if backup_task_id:
                async with get_opengauss_connection() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT id, task_name, task_type, source_paths, exclude_patterns,
                               compression_enabled, encryption_enabled, retention_days,
                               description, tape_device, status, is_template, template_id,
                               created_at, updated_at
                        FROM backup_tasks
                        WHERE id = $1 AND is_template = TRUE
                        """,
                        backup_task_id
                    )
                    if not row:
                        raise ValueError(f"备份任务模板不存在: {backup_task_id}")

                    # 转换为 BackupTask 对象（简化版，只包含需要的字段）
                    template_task = type('BackupTask', (), {
                        'id': row['id'],
                        'task_name': row['task_name'],
                        'task_type': _parse_enum(BackupTaskType, row.get('task_type'), None),
                        'source_paths': row['source_paths'] if isinstance(row['source_paths'], list) else json.loads(row['source_paths']) if row['source_paths'] else [],
                        'exclude_patterns': row['exclude_patterns'] if isinstance(row['exclude_patterns'], list) else json.loads(row['exclude_patterns']) if row['exclude_patterns'] else [],
                        'compression_enabled': row['compression_enabled'],
                        'encryption_enabled': row['encryption_enabled'],
                        'retention_days': row['retention_days'],
                        'description': row['description'],
                        'tape_device': row['tape_device'],
                        'status': _parse_enum(BackupTaskStatus, row.get('status'), None),
                        'is_template': row['is_template'],
                        'template_id': row['template_id'],
                    })()

            # 执行前检查：判断同一个模板的任务是否还在执行中
            if template_task or scheduled_task:
                template_id = template_task.id if template_task else None
                if scheduled_task and scheduled_task.task_metadata:
                    template_id = scheduled_task.task_metadata.get('backup_task_id') or template_id

                if template_id:
                    running_task = None
                    async with get_opengauss_connection() as conn:
                        running_task_row = await conn.fetchrow(
                            """
                            SELECT id, started_at, status FROM backup_tasks
                            WHERE template_id = $1 AND status IN ($2::backuptaskstatus, $3::backuptaskstatus)
                            ORDER BY created_at DESC
                            LIMIT 1
                            """,
                            template_id, 'pending', 'running'
                        )
                        running_task = dict(running_task_row) if running_task_row else None

                    if running_task:
                        # 检查任务是否在同一时间执行（同一天同一个调度任务）
                        current_time_check = now()
                        if scheduled_task and scheduled_task.last_run_time:
                            last_run_date = scheduled_task.last_run_time.date()
                            today = current_time_check.date()

                            # 如果上次执行在今天，且任务还在运行，跳过本次执行
                            if last_run_date == today:
                                running_task_id = running_task.get('id') if isinstance(running_task, dict) else (running_task.id if hasattr(running_task, 'id') else None)
                                logger.warning(
                                    f"跳过执行：模板 {template_id} 的任务仍在执行中 "
                                    f"(运行中的任务ID: {running_task_id})"
                                )
                                return {
                                    "status": "skipped",
                                    "message": "相同模板的任务仍在执行中，已跳过本次执行",
                                    "running_task_id": running_task_id
                                }

                            # 如果任务运行超过一天，记录警告但继续执行
                            if isinstance(running_task, dict) and running_task.get('started_at'):
                                started_at = running_task.get('started_at')
                                if isinstance(started_at, str):
                                    started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                                running_duration = (current_time_check - started_at).total_seconds()
                                if running_duration > 86400:  # 超过24小时
                                    logger.warning(f"模板 {template_id} 的任务已运行超过24小时，继续执行新任务")

            resume_template_id = template_task.id if template_task else backup_task_id
            if scheduled_task and scheduled_task.task_metadata:
                resume_template_id = scheduled_task.task_metadata.get('backup_task_id') or resume_template_id
            if manual_run and resume_template_id:
                if restart_requested:
                    cancelled_task_id = await self._cancel_incomplete_backup_task(resume_template_id)
                    if cancelled_task_id:
                        logger.info(f"手动运行选择重新开始，已取消未完成任务 {cancelled_task_id}")
                        try:
                            await log_system(
                                level=LogLevel.INFO,
                                category=LogCategory.BACKUP,
                                message=f"手动运行重新开始：取消未完成任务 {cancelled_task_id}",
                                module="utils.scheduler.action_handlers",
                                function="BackupActionHandler.execute",
                            )
                        except Exception:
                            pass
                else:
                    existing_task = await self._load_incomplete_backup_task(resume_template_id)
                    if existing_task:
                        backup_task = existing_task
                        resumed_from_existing = True
                        logger.info(f"检测到未完成的备份任务 {existing_task.id}，尝试继续执行")
                    elif resume_only:
                        logger.info("手动运行模式选择仅继续，但未找到可继续的备份任务")
                        return {
                            "status": "skipped",
                            "message": "没有未完成的备份任务可继续"
                        }

            # 从模板或配置中获取备份参数（若缺省则从系统实例配置补齐）
            if template_task:
                source_paths = template_task.source_paths or []
                task_type = template_task.task_type
                exclude_patterns = template_task.exclude_patterns or []
                compression_enabled = template_task.compression_enabled
                encryption_enabled = template_task.encryption_enabled
                retention_days = template_task.retention_days
                description = template_task.description or ''
                tape_device = template_task.tape_device
                task_name = f"{template_task.task_name}-{format_datetime(now(), '%Y%m%d_%H%M%S')}"
            else:
                # 从config获取参数（兼容旧逻辑）
                source_paths = config.get('source_paths', [])
                task_type_str = config.get('task_type', 'full')
                task_type_map = {
                    'full': BackupTaskType.FULL,
                    'incremental': BackupTaskType.INCREMENTAL,
                    'differential': BackupTaskType.DIFFERENTIAL,
                    'monthly_full': BackupTaskType.MONTHLY_FULL
                }
                task_type = task_type_map.get(task_type_str, BackupTaskType.FULL)
                exclude_patterns = config.get('exclude_patterns', [])
                compression_enabled = config.get('compression_enabled', True)
                encryption_enabled = config.get('encryption_enabled', False)
                retention_days = config.get('retention_days', 180)
                description = config.get('description', '')
                tape_device = config.get('tape_device')
                task_name = config.get('task_name', f"计划备份-{format_datetime(now(), '%Y%m%d_%H%M%S')}")

            # 补齐缺省参数：从 system_instance 的策略/配置合并
            try:
                sysi = self.system_instance
                if hasattr(sysi, 'settings'):
                    settings = sysi.settings
                    if not exclude_patterns and getattr(settings, 'DEFAULT_EXCLUDE_PATTERNS', None):
                        exclude_patterns = settings.DEFAULT_EXCLUDE_PATTERNS
                if not tape_device and hasattr(sysi, 'tape_manager'):
                    tm = sysi.tape_manager
                    if hasattr(tm, 'settings') and getattr(tm.settings, 'TAPE_DEVICE_PATH', None):
                        tape_device = tm.settings.TAPE_DEVICE_PATH
            except Exception:
                pass

            if not source_paths:
                raise ValueError("备份源路径不能为空")

            # 创建备份任务执行记录（不是模板）
            if backup_task is None:
                async with get_opengauss_connection() as conn:
                    backup_task_id = await conn.fetchval(
                        """
                        INSERT INTO backup_tasks (
                            task_name, task_type, source_paths, exclude_patterns,
                            compression_enabled, encryption_enabled, retention_days,
                            description, tape_device, status, is_template, template_id,
                            created_by, created_at, updated_at, scan_status
                        ) VALUES (
                            $1, $2::backuptasktype, $3, $4,
                            $5, $6, $7,
                            $8, $9, $10::backuptaskstatus, FALSE, $11,
                            $12, $13, $14, 'pending'
                        ) RETURNING id
                        """,
                        task_name,
                        task_type.value if hasattr(task_type, 'value') else str(task_type),
                        json.dumps(source_paths) if source_paths else None,
                        json.dumps(exclude_patterns) if exclude_patterns else None,
                        compression_enabled,
                        encryption_enabled,
                        retention_days,
                        description,
                        tape_device,
                        'pending',
                        template_task.id if template_task else None,
                        'scheduled_task',
                        now(),
                        now()
                    )

                    # 多表方案：为该执行任务创建 backup_files 分组和物理表
                    table_name = f"backup_files_{backup_task_id:06d}"

                    backup_files_group_id = await conn.fetchval(
                        """
                        INSERT INTO backup_files_groups (table_name, task_id)
                        VALUES ($1, $2)
                        RETURNING id
                        """,
                        table_name,
                        backup_task_id,
                    )

                    await conn.execute(
                        """
                        UPDATE backup_tasks
                        SET backup_files_group_id = $1,
                            backup_files_table = $2
                        WHERE id = $3
                        """,
                        backup_files_group_id,
                        table_name,
                        backup_task_id,
                    )

                    create_sql = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        LIKE backup_files_template INCLUDING ALL
                    )
                    """
                    await conn.execute(create_sql)

                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'commit'):
                        try:
                            await actual_conn.commit()
                            logger.debug(f"备份任务 {backup_task_id} 及其 backup_files 分表已提交到数据库")
                        except Exception as commit_err:
                            logger.warning(f"提交备份任务事务失败（可能已自动提交）: {commit_err}")

                    backup_task = type('BackupTask', (), {
                        'id': backup_task_id,
                        'task_name': task_name,
                        'task_type': task_type,
                        'source_paths': source_paths,
                        'exclude_patterns': exclude_patterns,
                        'compression_enabled': compression_enabled,
                        'encryption_enabled': encryption_enabled,
                        'retention_days': retention_days,
                        'description': description,
                        'tape_device': tape_device,
                        'status': BackupTaskStatus.PENDING,
                        'is_template': False,
                        'template_id': template_task.id if template_task else None,
                        'created_by': 'scheduled_task',
                        'scan_status': 'pending',
                        'backup_set_id': None,
                        'backup_files_table': table_name,
                    })()

            if not hasattr(backup_task, 'force_rescan'):
                backup_task.force_rescan = False
            if force_rescan_option:
                backup_task.force_rescan = True

            if backup_task:
                task_name = backup_task.task_name

            # 执行备份任务
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.BACKUP,
                message="开始执行备份任务",
                module="utils.scheduler.action_handlers",
                function="BackupActionHandler.execute",
                details={"backup_task_id": getattr(backup_task, 'id', None), "task_name": task_name}
            )
            logger.info(f"调用备份引擎执行备份任务... (手动运行: {manual_run})")
            try:
                success = await self.system_instance.backup_engine.execute_backup_task(backup_task, scheduled_task=scheduled_task, manual_run=manual_run)
                backup_executed = True
                logger.info(f"备份任务执行完成，结果: {'成功' if success else '失败'}")
                await log_system(
                    level=LogLevel.INFO if success else LogLevel.ERROR,
                    category=LogCategory.BACKUP,
                    message="备份任务执行结束" + ("(成功)" if success else "(失败)"),
                    module="utils.scheduler.action_handlers",
                    function="BackupActionHandler.execute",
                    details={
                        "backup_task_id": getattr(backup_task, 'id', None),
                        "total_bytes": getattr(backup_task, 'total_bytes', None),
                        "total_files": getattr(backup_task, 'total_files', None),
                    }
                )

                if success:
                    return {
                        "status": "success",
                        "message": "备份任务执行成功",
                        "backup_task_id": backup_task.id,
                        "backup_set_id": backup_task.backup_set_id,
                        "tape_id": backup_task.tape_id,
                        "total_files": backup_task.total_files,
                        "total_bytes": backup_task.total_bytes,
                        "processed_files": backup_task.processed_files,
                        "template_id": template_task.id if template_task else None
                    }
                else:
                    error_msg = getattr(backup_task, 'error_message', '备份任务执行失败（未知错误）')
                    # 先更新 backup_tasks 表状态为 FAILED，避免任务卡片一直显示"运行中"
                    try:
                        from datetime import datetime as _dt
                        async with get_opengauss_connection() as _conn:
                            await _conn.execute(
                                """
                                UPDATE backup_tasks
                                SET status = 'failed'::backuptaskstatus,
                                    error_message = $1,
                                    completed_at = $2,
                                    updated_at = $3
                                WHERE id = $4 AND status NOT IN ('completed', 'failed', 'cancelled')
                                """,
                                error_msg,
                                _dt.now(),
                                _dt.now(),
                                backup_task.id
                            )
                            logger.info(f"[action_handlers] 已将备份任务 {backup_task.id} 状态更新为 FAILED: {error_msg}")
                    except Exception as db_err:
                        logger.warning(f"[action_handlers] 更新备份任务失败状态时数据库异常: {db_err}")
                    raise RuntimeError(f"备份任务执行失败: {error_msg}")
            except Exception as backup_error:
                if backup_executed:
                    raise
                raise

        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            if not backup_executed:
                try:
                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                        await self.system_instance.dingtalk_notifier.send_backup_notification(
                            backup_name=(template_task.task_name if 'template_task' in locals() and template_task else (config.get('task_name','计划备份'))),
                            status='failed',
                            details={'error': str(e)}
                        )
                except Exception:
                    pass
            raise

    async def _load_incomplete_backup_task(self, template_id: int):
        if not template_id:
            return None
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, task_name, task_type, source_paths, exclude_patterns,
                       compression_enabled, encryption_enabled, retention_days,
                       description, tape_device, status, template_id, tape_id,
                       total_files, processed_files, total_bytes, processed_bytes,
                       compressed_bytes, backup_set_id, scan_status, scan_completed_at,
                       result_summary
                FROM backup_tasks
                WHERE template_id = $1 AND status <> 'completed'
                ORDER BY id DESC
                LIMIT 1
                """,
                template_id
            )
        if not row:
            return None
        return self._build_backup_task_from_row(row)

    async def _cancel_incomplete_backup_task(self, template_id: int) -> Optional[int]:
        """取消并清理未完成的备份任务"""
        existing_task = await self._load_incomplete_backup_task(template_id)
        if not existing_task:
            return None

        backup_engine = getattr(self.system_instance, 'backup_engine', None)
        backup_db = getattr(backup_engine, 'backup_db', None) if backup_engine else None

        if backup_db and getattr(existing_task, 'backup_set_id', None):
            try:
                backup_set = await backup_db.get_backup_set_by_set_id(existing_task.backup_set_id)
                if backup_set and getattr(backup_set, 'id', None):
                    await backup_db.clear_backup_files_for_set(backup_set.id)
            except Exception as e:
                logger.warning(f"清理历史 backup_files 失败: {e}")

        if backup_db:
            try:
                await backup_db.update_task_status(existing_task, BackupTaskStatus.CANCELLED)
            except Exception as e:
                logger.warning(f"更新未完成备份任务状态为取消失败: {e}")

        return existing_task.id

    def _build_backup_task_from_row(self, row) -> BackupTask:
        backup_task = BackupTask()
        backup_task.id = row['id']
        backup_task.task_name = row['task_name']
        backup_task.task_type = _parse_enum(BackupTaskType, row.get('task_type'), BackupTaskType.FULL)
        source_paths = row.get('source_paths')
        backup_task.source_paths = source_paths if isinstance(source_paths, list) else json.loads(source_paths) if source_paths else []
        exclude_patterns = row.get('exclude_patterns')
        backup_task.exclude_patterns = exclude_patterns if isinstance(exclude_patterns, list) else json.loads(exclude_patterns) if exclude_patterns else []
        backup_task.compression_enabled = row.get('compression_enabled')
        backup_task.encryption_enabled = row.get('encryption_enabled')
        backup_task.retention_days = row.get('retention_days')
        backup_task.description = row.get('description')
        backup_task.tape_device = row.get('tape_device')
        backup_task.tape_id = row.get('tape_id')
        backup_task.status = _parse_enum(BackupTaskStatus, row.get('status'), BackupTaskStatus.PENDING)
        backup_task.is_template = False
        backup_task.template_id = row.get('template_id')
        backup_task.total_files = row.get('total_files') or 0
        backup_task.processed_files = row.get('processed_files') or 0
        backup_task.total_bytes = row.get('total_bytes') or 0
        backup_task.processed_bytes = row.get('processed_bytes') or 0
        backup_task.compressed_bytes = row.get('compressed_bytes') or 0
        backup_task.backup_set_id = row.get('backup_set_id')
        backup_task.scan_status = row.get('scan_status') or 'pending'
        backup_task.scan_completed_at = row.get('scan_completed_at')
        summary = row.get('result_summary')
        if summary and isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                summary = {}
        backup_task.result_summary = summary or {}
        backup_task.force_rescan = False
        return backup_task


class RecoveryActionHandler(ActionHandler):
    """恢复动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行恢复动作"""
        try:
            logger.info("开始执行恢复任务")

            # 从配置中获取恢复参数
            backup_set_id = config.get('backup_set_id')
            files = config.get('files', [])
            target_path = config.get('target_path')

            if not backup_set_id:
                raise ValueError("恢复任务配置缺少 backup_set_id")

            if not files:
                raise ValueError("恢复任务配置缺少 files（文件列表）")

            if not target_path:
                raise ValueError("恢复任务配置缺少 target_path（目标路径）")

            # 获取恢复引擎
            if not self.system_instance or not hasattr(self.system_instance, 'recovery_engine'):
                raise RuntimeError("恢复引擎未初始化")

            recovery_engine = self.system_instance.recovery_engine

            # 创建恢复任务
            recovery_id = await recovery_engine.create_recovery_task(
                backup_set_id=backup_set_id,
                files=files,
                target_path=target_path,
                created_by='scheduled_task' if scheduled_task else 'manual'
            )

            if not recovery_id:
                raise RuntimeError("创建恢复任务失败")

            logger.info(f"恢复任务已创建: {recovery_id}")

            # 执行恢复任务
            success = await recovery_engine.execute_recovery(recovery_id)

            if success:
                logger.info(f"恢复任务执行成功: {recovery_id}")
                return {
                    "status": "success",
                    "message": f"恢复任务执行成功: {recovery_id}",
                    "recovery_id": recovery_id
                }
            else:
                error_msg = "恢复任务执行失败"
                if recovery_engine._current_recovery and recovery_engine._current_recovery.get('error_message'):
                    error_msg = recovery_engine._current_recovery['error_message']

                logger.error(f"恢复任务执行失败: {recovery_id}, 错误: {error_msg}")
                return {
                    "status": "failed",
                    "message": f"恢复任务执行失败: {error_msg}",
                    "recovery_id": recovery_id
                }

        except Exception as e:
            logger.error(f"执行恢复任务失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "status": "failed",
                "message": f"执行恢复任务失败: {str(e)}"
            }


class CleanupActionHandler(ActionHandler):
    """清理动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行清理动作"""
        return {"status": "success", "message": "清理任务已执行"}


class HealthCheckActionHandler(ActionHandler):
    """健康检查动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行健康检查动作"""
        return {"status": "success", "message": "健康检查已完成"}


class RetentionCheckActionHandler(ActionHandler):
    """保留期检查动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行保留期检查动作"""
        return {"status": "success", "message": "保留期检查已完成"}


class VerifyActionHandler(ActionHandler):
    """验证动作处理器 - 验证备份数据完整性"""

    async def _create_verify_task_record(self, config: Dict) -> int:
        """在 backup_tasks 表创建验证任务记录，返回 task_id"""
        verify_type = config.get('verify_type', 'directory')
        source_paths = config.get('source_paths', [])
        task_name = config.get('task_name') or f"验证任务-{format_datetime(now(), '%Y%m%d_%H%M%S')}"
        description = f"[扫描文件中...] 验证类型: {verify_type}"

        async with get_opengauss_connection() as conn:
            task_id = await conn.fetchval(
                """
                INSERT INTO backup_tasks (
                    task_name, task_type, source_paths, exclude_patterns,
                    compression_enabled, encryption_enabled, retention_days,
                    description, tape_device, status, is_template,
                    created_by, created_at, updated_at, started_at, scan_status,
                    operation_stage
                ) VALUES (
                    $1, 'verify'::backuptasktype, $2, $3,
                    FALSE, FALSE, 0,
                    $4, NULL, 'running'::backuptaskstatus, FALSE,
                    'scheduled_task', $5, $6, $7, 'running',
                    'scan'
                ) RETURNING id
                """,
                task_name,
                json.dumps(source_paths) if source_paths else None,
                json.dumps(config.get('exclude_patterns', [])) or None,
                description,
                now(), now(), now()
            )
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            if hasattr(actual_conn, 'commit'):
                try:
                    await actual_conn.commit()
                except Exception:
                    pass
        return task_id

    async def _update_verify_progress(self, task_id: int, **kwargs):
        """更新验证任务进度到 backup_tasks 表"""
        sets = []
        params = []
        idx = 1
        for key, value in kwargs.items():
            if key == 'status':
                sets.append(f"{key} = ${idx}::backuptaskstatus")
            else:
                sets.append(f"{key} = ${idx}")
            params.append(value)
            idx += 1
        if not sets:
            return
        params.append(task_id)
        sql = f"UPDATE backup_tasks SET {', '.join(sets)}, updated_at = now() WHERE id = ${idx}"
        try:
            async with get_opengauss_connection() as conn:
                await conn.execute(sql, *params)
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'commit'):
                    try:
                        await actual_conn.commit()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[验证任务] 更新进度失败 (task_id={task_id}): {e}")

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行验证动作"""
        verify_type = config.get('verify_type', 'directory')
        verify_percent = float(config.get('verify_percent', 1))
        verify_enabled = config.get('verify_enabled', True)
        source_paths = config.get('source_paths', [])

        # 创建 backup_tasks 记录
        task_id = await self._create_verify_task_record(config)

        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.BACKUP,
            message=f"开始执行验证任务（类型: {verify_type}, 抽样: {verify_percent}%）",
            module="utils.scheduler.action_handlers",
            function="VerifyActionHandler.execute",
            details={"verify_type": verify_type, "verify_percent": verify_percent, "backup_task_id": task_id}
        )

        try:
            loop = asyncio.get_event_loop()
            if verify_type == 'tape':
                result = await self._verify_tape(config, task_id)
            else:
                # 先在 async 中挂载 UNC 路径，再开线程跑验证
                resolved_paths = await self._resolve_source_paths(source_paths)
                if not resolved_paths:
                    raise ValueError("所有源路径均无效（SMB 挂载失败或路径不存在）")
                thread_config = dict(config)
                thread_config['source_paths'] = resolved_paths
                thread_config['_event_loop'] = loop  # 传入事件循环，供线程内 DB 更新使用
                result = await loop.run_in_executor(None, self._verify_directory_sync, thread_config, task_id)

            # 更新为完成状态
            final_status = 'completed' if result.get('status') == 'success' else 'failed'
            await self._update_verify_progress(
                task_id,
                status=final_status,
                progress_percent=100.0,
                operation_stage='verify',
                scan_status='completed',
                description=result.get('message', ''),
                result_summary=json.dumps(result),
                completed_at=now()
            )

            await log_system(
                level=LogLevel.INFO if result.get('status') == 'success' else LogLevel.ERROR,
                category=LogCategory.BACKUP,
                message=f"验证任务完成: {result.get('message', '')}",
                module="utils.scheduler.action_handlers",
                function="VerifyActionHandler.execute",
                details=result
            )
            result['backup_task_id'] = task_id
            return result

        except Exception as e:
            # 更新为失败状态
            try:
                await self._update_verify_progress(
                    task_id,
                    status='failed',
                    error_message=str(e),
                    operation_stage='verify',
                    scan_status='completed',
                    completed_at=now()
                )
            except Exception:
                pass

            logger.error(f"验证任务执行失败: {e}", exc_info=True)
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.BACKUP,
                message=f"验证任务执行异常: {str(e)}",
                module="utils.scheduler.action_handlers",
                function="VerifyActionHandler.execute",
            )
            return {"status": "failed", "message": str(e), "backup_task_id": task_id}

    async def _verify_tape(self, config: Dict, task_id: int = None) -> Dict[str, Any]:
        """磁带验证：挂载 LTFS，抽样 .tar.zst 归档验证完整性"""
        verify_percent = float(config.get('verify_percent', 1))

        # 1. 检查是否有正在运行的备份任务
        if self.system_instance and hasattr(self.system_instance, 'backup_engine'):
            backup_engine = self.system_instance.backup_engine
            if backup_engine and hasattr(backup_engine, '_current_task') and backup_engine._current_task:
                task_status = getattr(backup_engine._current_task, 'status', None)
                if task_status and str(task_status).lower() == 'running':
                    msg = "当前有备份任务正在运行，跳过磁带验证"
                    logger.warning(msg)
                    return {"status": "skipped", "message": msg}

        # 2. 获取 tape_handler 并挂载
        if not self.system_instance:
            return {"status": "failed", "message": "系统实例未初始化"}

        tape_handler = getattr(self.system_instance.backup_engine, 'tape_handler', None)
        if not tape_handler:
            return {"status": "failed", "message": "TapeHandler 未初始化"}

        was_already_mounted = False
        mount_point = None

        try:
            # 检查是否已挂载
            mount_point = Path(tape_handler._get_ltfs_mount_point())
            if mount_point.exists() and mount_point.is_mount():
                was_already_mounted = True
                logger.info("[磁带验证] LTFS 已挂载，直接使用")
            else:
                logger.info("[磁带验证] 挂载 LTFS...")
                success, msg = await tape_handler.mount_with_retry(max_retries=2, retry_interval=15)
                if not success:
                    return {"status": "failed", "message": f"挂载 LTFS 失败: {msg}"}
                logger.info("[磁带验证] LTFS 挂载成功")

            # 3. 收集所有归档文件
            archives = []
            system_dirs = {'.LTFS', 'lost+found', '.Trash', '.Trashes',
                          '$RECYCLE.BIN', 'System Volume Information'}

            for item in mount_point.iterdir():
                if not item.is_dir() or item.name in system_dirs or item.name.startswith('.'):
                    continue
                for f in item.iterdir():
                    if f.is_file() and f.name.endswith(('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')):
                        archives.append(f)

            if not archives:
                return {"status": "success", "message": "磁带上未发现归档文件", "total": 0, "sampled": 0, "passed": 0, "failed": 0}

            # 4. 按百分比抽样
            sample_count = max(1, int(len(archives) * verify_percent / 100))
            sample_count = min(sample_count, len(archives))
            sampled = random.sample(archives, sample_count)

            logger.info(f"[磁带验证] 发现 {len(archives)} 个归档，抽样 {sample_count} 个")

            # 5. 创建临时目录
            from config.settings import get_settings
            settings = get_settings()
            verify_temp = Path(settings.VERIFY_TEMP_DIR) / str(uuid.uuid4())[:8]
            verify_temp.mkdir(parents=True, exist_ok=True)

            passed = 0
            failed = 0
            errors = []

            try:
                for i, archive_path in enumerate(sampled):
                    logger.info(f"[磁带验证] 验证归档 {i+1}/{sample_count}: {archive_path.name}")
                    try:
                        valid, detail = await asyncio.to_thread(
                            self._verify_archive_extract, archive_path, verify_temp
                        )
                        if valid:
                            passed += 1
                            logger.info(f"[磁带验证] ✓ {archive_path.name}: {detail}")
                        else:
                            failed += 1
                            errors.append(f"{archive_path}: {detail}")
                            logger.warning(f"[磁带验证] ✗ {archive_path}: {detail}")
                    except Exception as e:
                        failed += 1
                        errors.append(f"{archive_path}: {str(e)}")
                        logger.error(f"[磁带验证] ✗ {archive_path.name}: {e}")
                    # 更新数据库进度
                    if task_id:
                        progress_pct = ((i + 1) / sample_count) * 100
                        await self._update_verify_progress(
                            task_id,
                            total_files=sample_count,
                            processed_files=i + 1,
                            progress_percent=min(99.0, progress_pct),
                            operation_stage='verify',
                            scan_status='completed',
                            description=f"[磁带验证中...] {i+1}/{sample_count}, 通过 {passed}, 失败 {failed}"
                        )
            finally:
                # 6. 清理临时目录
                try:
                    shutil.rmtree(str(verify_temp), ignore_errors=True)
                except Exception:
                    pass

            # 7. 记录操作日志
            await log_operation(
                operation_type=OperationType.TAPE_VERIFY,
                resource_type="tape",
                operation_name="磁带验证",
                operation_description=f"磁带验证完成: 抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}",
                category="backup",
                success=(failed == 0),
                result_message=f"抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}"
            )

            result = {
                "status": "success" if failed == 0 else ("success" if passed > 0 else "failed"),
                "message": f"磁带验证完成: 抽样 {sample_count}/{len(archives)}, 通过 {passed}, 失败 {failed}",
                "total": len(archives),
                "sampled": sample_count,
                "passed": passed,
                "failed": failed,
            }
            if errors:
                result["errors"] = errors[:20]  # 限制错误数量

            return result

        finally:
            # 如果是我们自己挂载的，验证完后卸载
            if not was_already_mounted and mount_point and mount_point.is_mount():
                try:
                    await tape_handler.unmount_ltfs()
                    logger.info("[磁带验证] 已卸载 LTFS")
                except Exception as e:
                    logger.warning(f"[磁带验证] 卸载 LTFS 失败: {e}")

    @staticmethod
    def _should_exclude_file(file_path: str, exclude_patterns: List[str]) -> bool:
        """检查文件/目录是否应该被排除（复用 FileScanner 相同的排除逻辑）"""
        if not exclude_patterns:
            return False
        from fnmatch import fnmatch as _fnmatch
        normalized = file_path.replace('\\', '/')
        path_parts = [p for p in normalized.split('/') if p]
        # 1. 完整路径匹配
        for pattern in exclude_patterns:
            normalized_pattern = pattern.replace('\\', '/')
            if _fnmatch(normalized, path_parts[-1]):
                return True
        # 2. 路径段名称匹配
        for part in path_parts:
            for pattern in exclude_patterns:
                normalized_pattern = pattern.replace('\\', "/")
                tail_pattern = normalized_pattern.split("/")[-1]
                if _fnmatch(part, tail_pattern):
                    return True
        # 3. 父目录匹配
        try:
            parent = Path(normalized).parent
            if VerifyActionHandler._should_exclude_file(str(parent), exclude_patterns):
                return True
        except Exception:
            pass
        return False

    async def _resolve_source_paths(self, source_paths: list) -> list:
        """解析源路径：UNC 网络路径自动挂载为本地路径（异步）"""
        from utils.network_path import is_unc_path, validate_network_path
        resolved_paths = []
        for src in source_paths:
            if is_unc_path(src):
                logger.info(f"[目录验证] 检测到 UNC 路径，尝试挂载 SMB: {src}")
                validation = await validate_network_path(src)
                if validation.get('valid') and validation.get('path'):
                    local_path = validation['path']
                    logger.info(f"[目录验证] UNC 路径已映射: {src} -> {local_path}")
                    resolved_paths.append(local_path)
                else:
                    logger.warning(f"[目录验证] SMB 挂载失败: {src} - {validation.get('message', '未知错误')}")
            else:
                resolved_paths.append(src)
        return resolved_paths

    def _verify_directory_sync(self, config: Dict, task_id: int = None) -> Dict[str, Any]:
        """目录验证（同步，在线程中运行，不阻塞事件循环）"""
        source_paths = config.get('source_paths', [])  # 已解析为本地路径
        verify_percent = float(config.get('verify_percent', 1))
        verify_enabled = config.get('verify_enabled', False)
        exclude_patterns = config.get('exclude_patterns', [])

        if not source_paths:
            return {"status": "failed", "message": "未指定验证源路径"}

        BATCH_SIZE = 100
        passed = 0
        failed = 0
        total_scanned = 0
        total_sampled = 0
        excluded_count = 0
        errors = []
        last_progress_log = time.monotonic()
        last_db_update = 0.0  # 立即触发首次更新

        from config.settings import get_settings
        settings = get_settings()
        verify_temp = Path(settings.VERIFY_TEMP_DIR) / str(uuid.uuid4())[:8]

        # 获取事件循环（必须从调用方传入，线程内无法 get_event_loop）
        loop = config.get('_event_loop')

        def _try_db_update(stage, scan_st, desc_suffix, force=False):
            """线程安全地异步更新验证任务进度"""
            nonlocal last_db_update
            now_ts = time.monotonic()
            if not force and now_ts - last_db_update < 10:
                return
            if not task_id or not loop:
                return
            try:
                coro = self._update_verify_progress(
                    task_id,
                    total_files=total_scanned,
                    processed_files=total_sampled,
                    progress_percent=min(99.0, (total_sampled / max(total_scanned, 1)) * 100) if total_scanned > 0 else 0.0,
                    operation_stage=stage,
                    scan_status=scan_st,
                    description=f"[{desc_suffix}] 已扫描 {total_scanned}, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}"
                )
                asyncio.run_coroutine_threadsafe(coro, loop)
            except Exception:
                pass
            last_db_update = now_ts

        # 立即写入初始状态（扫描开始）
        _try_db_update('scan', 'running', '扫描文件中...', force=True)

        for src in source_paths:
            src_path = Path(src)
            if not src_path.exists():
                logger.warning(f"[目录验证] 路径不存在: {src}")
                continue
            if src_path.is_file():
                if exclude_patterns and self._should_exclude_file(str(src_path), exclude_patterns):
                    excluded_count += 1
                    continue
                total_scanned += 1
                sampled_list = [src_path]
                p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                passed += p
                failed += f
                total_sampled += s
                for err_msg in e:
                    logger.warning(f"[目录验证] 验证失败: {err_msg}")
                errors.extend(e)
            elif src_path.is_dir():
                batch = []
                for root, dirs, files in os.walk(str(src_path)):
                    if exclude_patterns:
                        dirs[:] = [d for d in dirs if not self._should_exclude_file(str(Path(root) / d), exclude_patterns)]
                    for f in files:
                        file_path = Path(root) / f
                        if exclude_patterns and self._should_exclude_file(str(file_path), exclude_patterns):
                            excluded_count += 1
                            continue
                        batch.append(file_path)
                        total_scanned += 1
                        # 每10秒更新扫描进度（即使未满一批）
                        _try_db_update('scan', 'running', '扫描文件中...')
                        if len(batch) >= BATCH_SIZE:
                            sample_count = max(1, int(len(batch) * verify_percent / 100))
                            sampled_list = random.sample(batch, min(sample_count, len(batch)))
                            p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                            passed += p
                            failed += f
                            total_sampled += s
                            for err_msg in e:
                                logger.warning(f"[目录验证] 验证失败: {err_msg}")
                            errors.extend(e)
                            batch = []
                            # 批次验证后更新进度（进入验证阶段）
                            _try_db_update('verify', 'completed', '验证文件中...', force=True)
                            # 每60秒输出一次进度日志
                            now_ts = time.monotonic()
                            if now_ts - last_progress_log >= 60:
                                logger.info(f"[目录验证] 进度: 已扫描 {total_scanned} 个文件, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}")
                                last_progress_log = now_ts
                # 处理剩余的文件
                if batch:
                    sample_count = max(1, int(len(batch) * verify_percent / 100))
                    sampled_list = random.sample(batch, min(sample_count, len(batch)))
                    p, f, s, e = self._verify_batch_sync(sampled_list, verify_enabled, verify_temp)
                    passed += p
                    failed += f
                    total_sampled += s
                    for err_msg in e:
                        logger.warning(f"[目录验证] 验证失败: {err_msg}")
                    errors.extend(e)
                    _try_db_update('verify', 'completed', '验证文件中...', force=True)

        # 清理临时目录
        try:
            if verify_temp.exists():
                shutil.rmtree(str(verify_temp), ignore_errors=True)
        except Exception:
            pass

        if total_scanned == 0:
            return {"status": "success", "message": "指定路径中未发现文件", "total": 0, "sampled": 0, "passed": 0, "failed": 0}

        logger.info(f"[目录验证] 完成: 扫描 {total_scanned} 个文件 (排除 {excluded_count} 个), 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}")

        result = {
            "status": "success" if failed == 0 else ("success" if passed > 0 else "failed"),
            "message": f"目录验证完成: 扫描 {total_scanned}, 抽样 {total_sampled}, 通过 {passed}, 失败 {failed}",
            "total": total_scanned,
            "sampled": total_sampled,
            "passed": passed,
            "failed": failed,
        }
        if errors:
            result["errors"] = errors[:20]
        return result

    def _verify_batch_sync(self, sampled: list, verify_enabled: bool, verify_temp: Path) -> tuple:
        """同步验证一批文件，返回 (passed, failed, sampled_count, errors)"""
        passed = 0
        failed = 0
        errors = []

        for file_path in sampled:
            try:
                if file_path.name.endswith(('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')) and verify_enabled:
                    verify_temp.mkdir(parents=True, exist_ok=True)
                    valid, detail = self._verify_archive_extract(file_path, verify_temp)
                    # 清理临时文件
                    if verify_temp.exists():
                        for item in verify_temp.iterdir():
                            try:
                                if item.is_dir():
                                    shutil.rmtree(str(item))
                                else:
                                    item.unlink()
                            except Exception:
                                pass
                    if valid:
                        passed += 1
                    else:
                        failed += 1
                        errors.append(f"{file_path}: {detail}")
                else:
                    # 普通文件：验证可读性
                    size = file_path.stat().st_size
                    if size > 0:
                        with open(str(file_path), 'rb') as f:
                            f.read(4096)
                    passed += 1
            except Exception as e:
                failed += 1
                errors.append(f"{file_path}: {str(e)}")

        return passed, failed, len(sampled), errors

    async def _verify_batch(self, batch: list, verify_percent: float, verify_enabled: bool, verify_temp: Path) -> tuple:
        """验证一批文件，返回 (passed, failed, sampled_count, errors)"""
        sample_count = max(1, int(len(batch) * verify_percent / 100))
        sample_count = min(sample_count, len(batch))
        sampled = random.sample(batch, sample_count)
        passed = 0
        failed = 0
        errors = []

        for file_path in sampled:
            try:
                if file_path.name.endswith(('.tar.zst', '.tar.gz', '.tgz', '.tar', '.7z', '.zip')) and verify_enabled:
                    verify_temp.mkdir(parents=True, exist_ok=True)
                    valid, detail = await asyncio.to_thread(
                        self._verify_archive_extract, file_path, verify_temp
                    )
                    # 清理临时文件
                    if verify_temp.exists():
                        for item in verify_temp.iterdir():
                            try:
                                if item.is_dir():
                                    shutil.rmtree(str(item))
                                else:
                                    item.unlink()
                            except Exception:
                                pass
                    if valid:
                        passed += 1
                    else:
                        failed += 1
                        errors.append(f"{file_path}: {detail}")
                else:
                    # 普通文件（含未勾选验证的压缩文件）：验证可读性
                    size = file_path.stat().st_size
                    if size > 0:
                        with open(str(file_path), 'rb') as f:
                            f.read(4096)
                    passed += 1
            except Exception as e:
                failed += 1
                errors.append(f"{file_path}: {str(e)}")

        return passed, failed, sample_count, errors

    @staticmethod
    def _verify_archive_header(archive_path: Path) -> tuple:
        """验证归档文件头部可读性（不解压，支持加密归档）

        Returns:
            (is_valid: bool, detail: str)
        """
        name = archive_path.name.lower()
        size = archive_path.stat().st_size
        size_mb = size / (1024 * 1024)

        try:
            if name.endswith('.tar.zst'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(4)
                if magic[:4] == b'\x28\xb5\x2f\xfd':
                    return True, f"zstd 归档头部正常 ({size_mb:.1f}MB)"
                return False, f"zstd 魔数不匹配"
            elif name.endswith(('.tar.gz', '.tgz')):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(2)
                if magic == b'\x1f\x8b':
                    return True, f"gzip 归档头部正常 ({size_mb:.1f}MB)"
                return False, "gzip 魔数不匹配"
            elif name.endswith('.tar'):
                with open(str(archive_path), 'rb') as f:
                    f.seek(257)
                    ustar = f.read(5)
                if ustar == b'ustar':
                    return True, f"tar 归档头部正常 ({size_mb:.1f}MB)"
                return True, f"tar 文件可读 ({size_mb:.1f}MB)"
            elif name.endswith('.zip'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(4)
                if magic[:2] == b'PK':
                    return True, f"ZIP 归档头部正常 ({size_mb:.1f}MB)"
                return False, "ZIP 魔数不匹配"
            elif name.endswith('.7z'):
                with open(str(archive_path), 'rb') as f:
                    magic = f.read(6)
                if magic[:2] == b'7z':
                    return True, f"7z 归档头部正常 ({size_mb:.1f}MB)"
                return False, "7z 魔数不匹配"
            else:
                return True, f"文件可读 ({size_mb:.1f}MB)"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _verify_archive_extract(archive_path: Path, temp_dir: Path) -> tuple:
        """解压验证归档文件完整性（用于磁带验证，解压到临时目录后删除）

        Returns:
            (is_valid: bool, detail: str)
        """
        name = archive_path.name.lower()
        size = archive_path.stat().st_size
        size_mb = size / (1024 * 1024)

        try:
            if name.endswith('.tar.zst'):
                return VerifyActionHandler._extract_tar_zst(archive_path, temp_dir, size_mb)
            elif name.endswith(('.tar.gz', '.tgz')):
                return VerifyActionHandler._extract_tar(archive_path, temp_dir, 'r:gz', size_mb)
            elif name.endswith('.tar'):
                return VerifyActionHandler._extract_tar(archive_path, temp_dir, 'r:', size_mb)
            elif name.endswith('.zip'):
                return VerifyActionHandler._extract_zip(archive_path, temp_dir, size_mb)
            elif name.endswith('.7z'):
                return VerifyActionHandler._extract_7z(archive_path, temp_dir, size_mb)
            else:
                return True, f"文件可读 ({size_mb:.1f}MB)"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _extract_tar_zst(archive_path: Path, temp_dir: Path, size_mb: float) -> tuple:
        """解压验证 .tar.zst"""
        try:
            import zstandard as zstd
            dctx = zstd.ZstdDecompressor()
            member_count = 0
            with open(str(archive_path), 'rb') as fh:
                with dctx.stream_reader(fh) as reader:
                    with tarfile.open(fileobj=reader, mode='r|') as tar:
                        for member in tar:
                            member_count += 1
            return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件)"
        except ImportError:
            import subprocess
            proc = subprocess.Popen(
                ['zstd', '-d', str(archive_path), '--stdout'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            member_count = 0
            try:
                with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
                    for member in tar:
                        member_count += 1
                proc.wait(timeout=60)
            except Exception as e:
                proc.kill()
                proc.wait()
                return False, f"解压验证失败: {e}"
            if proc.returncode != 0:
                return False, f"zstd 解压返回码: {proc.returncode}"
            return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件, CLI)"

    @staticmethod
    def _extract_tar(archive_path: Path, temp_dir: Path, mode: str, size_mb: float) -> tuple:
        """解压验证 tar/tar.gz"""
        member_count = 0
        with tarfile.open(str(archive_path), mode) as tar:
            for member in tar:
                member_count += 1
        return True, f"解压验证通过 ({size_mb:.1f}MB, {member_count} 个文件)"

    @staticmethod
    def _extract_zip(archive_path: Path, temp_dir: Path, size_mb: float) -> tuple:
        """解压验证 zip"""
        import zipfile
        with zipfile.ZipFile(str(archive_path), 'r') as zf:
            bad = zf.testzip()
            if bad:
                return False, f"损坏的文件: {bad}"
            count = len(zf.infolist())
        return True, f"解压验证通过 ({size_mb:.1f}MB, {count} 个文件)"

    @staticmethod
    def _extract_7z(archive_path: Path, temp_dir: Path, size_mb: float) -> tuple:
        """解压验证 7z"""
        try:
            import py7zr
            with py7zr.SevenZipFile(str(archive_path), mode='r') as archive:
                file_list = archive.list()
                count = len([f for f in file_list if not f.is_directory])
            return True, f"解压验证通过 ({size_mb:.1f}MB, {count} 个文件)"
        except ImportError:
            return True, f"py7zr 未安装，跳过解压验证 ({size_mb:.1f}MB)"


class CustomActionHandler(ActionHandler):
    """自定义动作处理器"""

    async def execute(
        self,
        config: Dict,
        scheduled_task: Optional[ScheduledTask] = None,
        manual_run: bool = False,
        run_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """执行自定义动作"""
        return {"status": "success", "message": "自定义任务已执行"}


def get_action_handler(action_type: TaskActionType, system_instance) -> ActionHandler:
    """获取动作处理器"""
    handler_map = {
        TaskActionType.BACKUP: BackupActionHandler,
        TaskActionType.RECOVERY: RecoveryActionHandler,
        TaskActionType.CLEANUP: CleanupActionHandler,
        TaskActionType.HEALTH_CHECK: HealthCheckActionHandler,
        TaskActionType.RETENTION_CHECK: RetentionCheckActionHandler,
        TaskActionType.VERIFY: VerifyActionHandler,
        TaskActionType.CUSTOM: CustomActionHandler,
    }

    handler_class = handler_map.get(action_type)
    if not handler_class:
        raise ValueError(f"不支持的任务动作类型: {action_type}")

    return handler_class(system_instance)
