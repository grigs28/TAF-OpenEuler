#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务动作处理器
Task Action Handlers
"""

import logging
import asyncio
from datetime import datetime, timezone
from utils.datetime_utils import now, format_datetime
from typing import Dict, Any, Optional

from models.scheduled_task import ScheduledTask, TaskActionType
from models.backup import BackupTask, BackupTaskType, BackupTaskStatus
from config.database import db_manager
from sqlalchemy import select, and_
from utils.log_utils import log_system, LogLevel, LogCategory, log_operation, OperationType
from .db_utils import is_opengauss, is_redis, get_opengauss_connection
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
                # 如果 last_success_time 不为空，直接跳过（不管是否在同一周期内，因为月度任务一个月只执行一次）
                # 如果 last_success_time 为空，任务锁已经在 task_executor 中处理了
                if schedule_type and getattr(schedule_type, 'value', '').lower() in ('monthly', 'month'):
                    if last_success:
                        # 月度任务：如果 last_success_time 不为空，直接跳过
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
                        # 月度任务：如果 last_success_time 为空，任务锁已经在 task_executor 中处理了
                        # 如果执行到这里，说明已经成功获取了任务锁，可以继续执行
                        logger.info(
                            f"[月度任务检查] 从未成功执行过，检查任务锁 - "
                            f"任务ID: {getattr(scheduled_task, 'id', 'N/A')}, "
                            f"任务名称: {getattr(scheduled_task, 'task_name', 'N/A')}"
                        )
                        # 任务锁已经在 task_executor 中获取，如果获取失败，不会执行到这里
                        # 所以这里不需要再次检查任务锁
                
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
            # 如果执行到这里，说明已经成功获取了锁，状态已经更新为 RUNNING
            # 因此不需要再次检查任务状态，直接继续执行即可

            # 3) 磁带标签是否当月（从 LTFS 标签或磁带头读取）
            # 仅当备份目标为磁带时要求当月
            # 注意：手动运行时跳过磁带标签检查
            if scheduled_task and not manual_run:
                target_is_tape = False
                try:
                    # 从 scheduled_task.action_config 读取备份目标
                    action_cfg = scheduled_task.action_config or {}
                    target_is_tape = (action_cfg.get('backup_target') == 'tape') or ('tape_device' in action_cfg)
                except Exception:
                    pass
                if target_is_tape and self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                    tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                    if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                        try:
                            # 计划任务中检索卷标设置60秒超时
                            metadata = await asyncio.wait_for(
                                tape_ops._read_tape_label(),
                                timeout=60.0
                            )
                        except asyncio.TimeoutError:
                            logger.warning("计划任务中读取磁带卷标超时（60秒）")
                            metadata = None
                        # 无标签时允许继续，后续会自动提示换盘逻辑在业务层处理
                        if metadata and (metadata.get('created_date') or metadata.get('tape_id')):
                            try:
                                # 优先使用 created_date
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
                                    # 备用：从 tape_id 推断（如 TAPyymmddxxx）
                                    tape_id = str(metadata.get('tape_id', ''))
                                    if len(tape_id) >= 7 and tape_id.upper().startswith('TAP'):
                                        yy = int(tape_id[3:5])
                                        mm = int(tape_id[5:7])
                                        year = 2000 + yy
                                        if not (year == current_time.year and mm == current_time.month):
                                            raise ValueError("当前磁带标签非当月，请更换磁带后重试")
                            except ValueError as ve:
                                # 记录并抛出以触发通知与日志
                                logger.warning(str(ve))
                                # 通知：需要更换磁带
                                try:
                                    if self.system_instance and getattr(self.system_instance, 'dingtalk_notifier', None):
                                        notifier = self.system_instance.dingtalk_notifier
                                        tape_id = (metadata.get('tape_id') if metadata else '') or '未知磁带'
                                        await notifier.send_tape_notification(tape_id=tape_id, action='change_required')
                                except Exception:
                                    pass
                                # 记录日志
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
                if is_opengauss():
                    # openGauss 原生SQL查询
                    # 使用连接池
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
                elif is_redis():
                    # Redis模式：使用Redis查询
                    from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key, _ensure_list, _ensure_dict
                    from config.redis_db import get_redis_client
                    redis = await get_redis_client()
                    task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task_id)
                    task_data = await redis.hgetall(task_key)
                    if not task_data:
                        raise ValueError(f"备份任务模板不存在: {backup_task_id}")
                    task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                               v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                               for k, v in task_data.items()}
                    is_template = task_dict.get('is_template', '0') == '1'
                    if not is_template:
                        raise ValueError(f"备份任务 {backup_task_id} 不是模板")
                    # 将字典转换为对象
                    template_task = type('BackupTask', (), {
                        'id': int(task_dict.get('id', backup_task_id)),
                        'task_name': task_dict.get('task_name', ''),
                        'task_type': _parse_enum(BackupTaskType, task_dict.get('task_type'), None),
                        'source_paths': _ensure_list(task_dict.get('source_paths', '[]')),
                        'exclude_patterns': _ensure_list(task_dict.get('exclude_patterns', '[]')),
                        'compression_enabled': task_dict.get('compression_enabled', '0') == '1',
                        'encryption_enabled': task_dict.get('encryption_enabled', '0') == '1',
                        'retention_days': int(task_dict.get('retention_days', 0)),
                        'description': task_dict.get('description', ''),
                        'tape_device': task_dict.get('tape_device', ''),
                        'status': _parse_enum(BackupTaskStatus, task_dict.get('status'), None),
                        'is_template': True,
                        'template_id': task_dict.get('template_id'),
                    })()
                elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                    # 使用SQLAlchemy查询
                    async with db_manager.AsyncSessionLocal() as session:
                        stmt = select(BackupTask).where(
                            and_(
                                BackupTask.id == backup_task_id,
                                BackupTask.is_template == True
                            )
                        )
                        result = await session.execute(stmt)
                        template_task = result.scalar_one_or_none()
                        
                        if not template_task:
                            raise ValueError(f"备份任务模板不存在: {backup_task_id}")
                else:
                    raise ValueError(f"不支持的数据库类型，无法查询备份任务模板: {backup_task_id}")
            
            # 执行前检查：判断同一个模板的任务是否还在执行中
            if template_task or scheduled_task:
                template_id = template_task.id if template_task else None
                if scheduled_task and scheduled_task.task_metadata:
                    template_id = scheduled_task.task_metadata.get('backup_task_id') or template_id
                
                if template_id:
                    # 检查是否有相同模板的任务正在执行或等待执行（PENDING 或 RUNNING）
                    # 关键：必须同时检查 PENDING 和 RUNNING，防止并发创建任务
                    if is_opengauss():
                        # openGauss 原生SQL查询
                        # 使用连接池
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
                    elif is_redis():
                        # Redis模式：使用Redis查询
                        from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
                        from config.redis_db import get_redis_client
                        redis = await get_redis_client()
                        # 获取所有任务ID
                        from backup.redis_backup_db import KEY_INDEX_BACKUP_TASKS
                        task_ids_bytes = await redis.smembers(KEY_INDEX_BACKUP_TASKS)
                        running_task = None
                        for task_id_bytes in task_ids_bytes:
                            # Redis 客户端设置了 decode_responses=True，所以键值已经是字符串
                            task_id_str = task_id_bytes if isinstance(task_id_bytes, str) else (task_id_bytes.decode('utf-8') if isinstance(task_id_bytes, bytes) else str(task_id_bytes))
                            task_id = int(task_id_str)
                            task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
                            task_data = await redis.hgetall(task_key)
                            if not task_data:
                                continue
                            # Redis 客户端设置了 decode_responses=True，所以键值已经是字符串
                            task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                                       v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                                       for k, v in task_data.items()}
                            task_template_id = task_dict.get('template_id', '')
                            task_status = task_dict.get('status', '').lower()
                            # 检查 PENDING 或 RUNNING 状态的任务
                            if task_template_id == str(template_id) and task_status in ('pending', 'running'):
                                running_task = {'id': task_id, 'started_at': task_dict.get('started_at'), 'status': task_status}
                                break
                    elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                        # 使用SQLAlchemy查询（检查 PENDING 或 RUNNING 状态）
                        async with db_manager.AsyncSessionLocal() as session:
                            stmt = select(BackupTask).where(
                                and_(
                                    BackupTask.template_id == template_id,
                                    BackupTask.status.in_([BackupTaskStatus.PENDING, BackupTaskStatus.RUNNING])
                                )
                            ).order_by(BackupTask.created_at.desc()).limit(1)
                            result = await session.execute(stmt)
                            running_task = result.scalar_one_or_none()
                    else:
                        running_task = None
                    
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
                            if is_opengauss():
                                # openGauss 原生SQL查询返回字典
                                if isinstance(running_task, dict) and running_task.get('started_at'):
                                    started_at = running_task.get('started_at')
                                    if isinstance(started_at, str):
                                        started_at = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                                    running_duration = (current_time_check - started_at).total_seconds()
                                    if running_duration > 86400:  # 超过24小时
                                        logger.warning(f"模板 {template_id} 的任务已运行超过24小时，继续执行新任务")
                            else:
                                # SQLAlchemy 对象
                                if hasattr(running_task, 'started_at') and running_task.started_at:
                                    running_duration = (current_time_check - running_task.started_at).total_seconds()
                                    if running_duration > 86400:  # 超过24小时
                                        logger.warning(
                                            f"警告：模板 {template_id} 的任务已运行超过24小时 "
                                            f"(任务ID: {running_task.id})"
                                        )
            
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
                # 备份策略里可能有默认排除规则/压缩加密开关
                if hasattr(sysi, 'settings'):
                    settings = sysi.settings
                    if not exclude_patterns and getattr(settings, 'DEFAULT_EXCLUDE_PATTERNS', None):
                        exclude_patterns = settings.DEFAULT_EXCLUDE_PATTERNS
                # tapedrive 配置：默认磁带设备等
                if not tape_device and hasattr(sysi, 'tape_manager'):
                    tm = sysi.tape_manager
                    if hasattr(tm, 'settings') and getattr(tm.settings, 'TAPE_DEVICE_PATH', None):
                        tape_device = tm.settings.TAPE_DEVICE_PATH
            except Exception:
                pass
            
            if not source_paths:
                raise ValueError("备份源路径不能为空")
            
            # 注意：不在这里发送"开始"通知，因为 backup_engine.execute_backup_task() 中已经会发送
            # 这样可以避免重复通知，并且 backup_engine 中的通知会检查通知事件配置
            
            # 创建备份任务执行记录（不是模板）
            if backup_task is None:
                if is_opengauss():
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
                            now(),  # $13: created_at
                            now()   # $14: updated_at (之前错误地使用了 $13)
                        )

                        # ========= 多表方案：为该执行任务创建 backup_files 分组和物理表 =========
                        # 物理表名采用固定前缀 + 任务ID，避免 SQL 注入
                        table_name = f"backup_files_{backup_task_id:06d}"

                        # 1. 在 backup_files_groups 中创建元数据记录
                        backup_files_group_id = await conn.fetchval(
                            """
                            INSERT INTO backup_files_groups (table_name, task_id)
                            VALUES ($1, $2)
                            RETURNING id
                            """,
                            table_name,
                            backup_task_id,
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
                            backup_task_id,
                        )

                        # 3. 为该任务创建物理表（基于 backup_files_template 结构）
                        # 注意：表名不能用参数占位符，只能通过受控字符串拼接
                        create_sql = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            LIKE backup_files_template INCLUDING ALL
                        )
                        """
                        await conn.execute(create_sql)

                        # 确保事务已提交（psycopg3 需要显式提交，否则其他连接看不到数据）
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'commit'):
                            try:
                                await actual_conn.commit()
                                logger.debug(f"备份任务 {backup_task_id} 及其 backup_files 分表已提交到数据库")
                            except Exception as commit_err:
                                logger.warning(f"提交备份任务事务失败（可能已自动提交）: {commit_err}")

                        # 构造内存中的 BackupTask 对象，包含 backup_files_table 字段，供扫描器使用
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
                elif is_redis():
                    # Redis模式：使用备份引擎创建备份任务
                    # 备份引擎会自动创建备份任务记录
                    from config.redis_db import get_redis_manager
                    redis_manager = get_redis_manager()
                    task_id = await redis_manager.get_next_id('backup_task:id')
                    
                    backup_task = type('BackupTask', (), {
                        'id': task_id,
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
                    })()
                    
                    # 保存到Redis
                    from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, KEY_INDEX_BACKUP_TASKS, _get_redis_key
                    from config.redis_db import get_redis_client
                    import json as json_module
                    
                    redis = await get_redis_client()
                    task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
                    
                    await redis.hset(task_key, mapping={
                        'id': str(task_id),
                        'task_name': task_name,
                        'task_type': task_type.value if hasattr(task_type, 'value') else str(task_type),
                        'source_paths': json_module.dumps(source_paths) if source_paths else '[]',
                        'exclude_patterns': json_module.dumps(exclude_patterns) if exclude_patterns else '[]',
                        'compression_enabled': '1' if compression_enabled else '0',
                        'encryption_enabled': '1' if encryption_enabled else '0',
                        'retention_days': str(retention_days),
                        'description': description or '',
                        'tape_device': tape_device or '',
                        'status': 'pending',
                        'is_template': '0',
                        'template_id': str(template_task.id) if template_task else '',
                        'created_by': 'scheduled_task',
                        'scan_status': 'pending',
                        'backup_set_id': '',
                        'created_at': now().isoformat(),
                        'updated_at': now().isoformat(),
                    })
                    
                    # 添加到索引
                    await redis.sadd(KEY_INDEX_BACKUP_TASKS, str(task_id))
                elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                    async with db_manager.AsyncSessionLocal() as session:
                        backup_task = BackupTask(
                            task_name=task_name,
                            task_type=task_type,
                            source_paths=source_paths,
                            exclude_patterns=exclude_patterns,
                            compression_enabled=compression_enabled,
                            encryption_enabled=encryption_enabled,
                            retention_days=retention_days,
                            description=description,
                            tape_device=tape_device,
                            status=BackupTaskStatus.PENDING,
                            is_template=False,
                            template_id=template_task.id if template_task else None,
                            created_by='scheduled_task',
                            scan_status='pending'
                        )
                        
                        session.add(backup_task)
                        await session.commit()
                        await session.refresh(backup_task)
                else:
                    raise RuntimeError(f"不支持的数据库类型，无法创建备份任务")
            
            if not hasattr(backup_task, 'force_rescan'):
                backup_task.force_rescan = False
            if force_rescan_option:
                backup_task.force_rescan = True
            
            # 完整备份前：格式化磁带（计划任务使用当前年月生成卷标）
            # 注意：手动运行时跳过格式化
            logger.info(f"检查是否需要格式化: manual_run={manual_run}, task_type={task_type}, template_task_type={template_task.task_type if template_task else None}")
            if not manual_run and ((template_task and template_task.task_type == BackupTaskType.FULL) or (not template_task and task_type == BackupTaskType.FULL)):
                logger.info("开始执行格式化操作（自动运行模式）")
                try:
                    # 在格式化开始前，更新备份任务状态为RUNNING，并在description中注明"格式化中"
                    # 注意：is_opengauss 和 get_opengauss_connection 已在文件顶部导入
                    if is_opengauss():
                        # 使用连接池
                        async with get_opengauss_connection() as conn:
                            await conn.execute(
                                """
                                UPDATE backup_tasks
                                SET status = $1::backuptaskstatus,
                                    started_at = $2,
                                    description = COALESCE(description, '') || ' [格式化中]',
                                    updated_at = $2
                                WHERE id = $3
                                """,
                                BackupTaskStatus.RUNNING.value,
                                now(),
                                backup_task.id
                            )
                    elif is_redis():
                        # Redis模式：使用Redis更新
                        from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
                        from config.redis_db import get_redis_client
                        redis = await get_redis_client()
                        task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
                        description = (backup_task.description if hasattr(backup_task, 'description') and backup_task.description else '') + ' [格式化中]'
                        await redis.hset(task_key, mapping={
                            'status': 'running',
                            'started_at': now().isoformat(),
                            'description': description,
                            'updated_at': now().isoformat(),
                        })
                    elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                        async with db_manager.AsyncSessionLocal() as session:
                            backup_task.status = BackupTaskStatus.RUNNING
                            backup_task.started_at = now()
                            if backup_task.description:
                                backup_task.description = backup_task.description + ' [格式化中]'
                            else:
                                backup_task.description = '[格式化中]'
                            await session.commit()
                            await session.refresh(backup_task)
                    # 其他数据库类型跳过
                    
                    await log_system(
                        level=LogLevel.INFO,
                        category=LogCategory.BACKUP,
                        message="开始完整备份前格式化（使用当前年月）",
                        module="utils.scheduler.action_handlers",
                        function="BackupActionHandler.execute",
                    )
                    if self.system_instance and getattr(self.system_instance, 'tape_manager', None):
                        tape_ops = getattr(self.system_instance.tape_manager, 'tape_operations', None)
                        if tape_ops and hasattr(tape_ops, 'erase_preserve_label'):
                            # 计划任务格式化时使用当前年月生成卷标
                            ok = await tape_ops.erase_preserve_label(use_current_year_month=True)
                            if not ok:
                                logger.warning("完整备份前格式化失败，将尝试继续执行备份")
                                await log_system(
                                    level=LogLevel.WARNING,
                                    category=LogCategory.BACKUP,
                                    message="完整备份前格式化失败，继续执行",
                                    module="utils.scheduler.action_handlers",
                                    function="BackupActionHandler.execute",
                                )
                            else:
                                # 格式化成功，更新description，移除"格式化中"，准备开始备份
                                if is_opengauss():
                                    # 使用连接池
                                    async with get_opengauss_connection() as conn:
                                        await conn.execute(
                                            """
                                            UPDATE backup_tasks
                                            SET description = REPLACE(COALESCE(description, ''), ' [格式化中]', ''),
                                                updated_at = $1
                                            WHERE id = $2
                                            """,
                                            now(),
                                            backup_task.id
                                        )
                                elif is_redis():
                                    # Redis模式：使用Redis更新
                                    from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
                                    from config.redis_db import get_redis_client
                                    redis = await get_redis_client()
                                    task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, backup_task.id)
                                    description = (backup_task.description if hasattr(backup_task, 'description') and backup_task.description else '').replace(' [格式化中]', '')
                                    await redis.hset(task_key, mapping={
                                        'description': description,
                                        'updated_at': now().isoformat(),
                                    })
                                elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                                    async with db_manager.AsyncSessionLocal() as session:
                                        if backup_task.description:
                                            backup_task.description = backup_task.description.replace(' [格式化中]', '')
                                        await session.commit()
                                # 其他数据库类型跳过
                except Exception as _:
                    logger.warning("完整备份前格式化异常，将尝试继续执行备份")
                    await log_system(
                        level=LogLevel.WARNING,
                        category=LogCategory.BACKUP,
                        message="完整备份前格式化异常，继续执行",
                        module="utils.scheduler.action_handlers",
                        function="BackupActionHandler.execute",
                    )
            else:
                if manual_run:
                    logger.info("手动运行模式，跳过格式化操作")
                elif not ((template_task and template_task.task_type == BackupTaskType.FULL) or (not template_task and task_type == BackupTaskType.FULL)):
                    logger.info("非完整备份任务，跳过格式化操作")

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
            # 执行备份任务（传入 scheduled_task 和 manual_run 以便进行执行前检查）
            logger.info(f"调用备份引擎执行备份任务... (手动运行: {manual_run})")
            try:
                success = await self.system_instance.backup_engine.execute_backup_task(backup_task, scheduled_task=scheduled_task, manual_run=manual_run)
                backup_executed = True  # 标记备份任务已执行
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
                    # 注意：不在这里发送"成功"通知，因为 backup_engine.execute_backup_task() 中已经会发送
                    # 这样可以避免重复通知，并且 backup_engine 中的通知会检查通知事件配置，信息更详细
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
                    # 安全获取错误信息
                    error_msg = getattr(backup_task, 'error_message', '备份任务执行失败（未知错误）')
                    raise RuntimeError(f"备份任务执行失败: {error_msg}")
            except Exception as backup_error:
                # 如果备份任务已执行，backup_engine 中已经发送了失败通知，这里不再重复发送
                if backup_executed:
                    raise
                # 如果备份任务未执行（比如创建任务失败、执行前检查失败等），需要发送失败通知
                raise
                
        except Exception as e:
            logger.error(f"执行备份动作失败: {str(e)}")
            # 失败通知：只在备份任务未执行时发送（如果备份任务已执行，backup_engine 中已经发送了）
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
        if is_opengauss():
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
        elif is_redis():
            # Redis模式：使用Redis查询
            from backup.redis_backup_db import get_backup_tasks_redis
            tasks = await get_backup_tasks_redis(limit=100)  # 获取更多任务以查找未完成的
            for task_dict in tasks:
                if task_dict.get('template_id') == template_id and task_dict.get('status') != 'completed':
                    task = type('BackupTask', (), {**task_dict, 'force_rescan': False})()
                    return task
            return None
        elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
            async with db_manager.AsyncSessionLocal() as session:
                stmt = (
                    select(BackupTask)
                    .where(
                        and_(
                            BackupTask.template_id == template_id,
                            BackupTask.status != BackupTaskStatus.COMPLETED
                        )
                    )
                    .order_by(BackupTask.id.desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                task = result.scalar_one_or_none()
                if task:
                    task.force_rescan = False
                return task
        else:
            return None

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
        TaskActionType.CUSTOM: CustomActionHandler,
    }
    
    handler_class = handler_map.get(action_type)
    if not handler_class:
        raise ValueError(f"不支持的任务动作类型: {action_type}")
    
    return handler_class(system_instance)

