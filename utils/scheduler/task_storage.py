#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务存储模块
Task Storage Module - 处理所有数据库CRUD操作
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional, List

from models.scheduled_task import ScheduledTask, ScheduleType, ScheduledTaskStatus, TaskActionType
from models.system_log import OperationType
from .db_utils import get_opengauss_connection
from utils.log_utils import log_operation

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


def row_to_task(row) -> ScheduledTask:
    """将数据库行转换为ScheduledTask对象"""
    task = ScheduledTask()
    task.id = row['id']
    task.task_name = row['task_name']
    task.description = row['description']
    task.schedule_type = _parse_enum(ScheduleType, row.get('schedule_type'), None)
    task.action_type = _parse_enum(TaskActionType, row.get('action_type'), None)
    task.status = _parse_enum(ScheduledTaskStatus, row.get('status'), ScheduledTaskStatus.INACTIVE)
    task.schedule_config = json.loads(row['schedule_config']) if row['schedule_config'] and isinstance(row['schedule_config'], str) else (row['schedule_config'] if row['schedule_config'] else {})
    task.action_config = json.loads(row['action_config']) if row['action_config'] and isinstance(row['action_config'], str) else (row['action_config'] if row['action_config'] else {})
    task.enabled = row['enabled']
    task.next_run_time = row['next_run_time']
    task.last_run_time = row['last_run_time']
    task.last_success_time = row.get('last_success_time')
    task.last_failure_time = row.get('last_failure_time')
    task.total_runs = row.get('total_runs') or 0
    task.success_runs = row.get('success_runs') or 0
    task.failure_runs = row.get('failure_runs') or 0
    task.average_duration = row.get('average_duration')
    task.last_error = row.get('last_error')
    task.created_at = row.get('created_at')
    task.updated_at = row.get('updated_at')
    task.task_metadata = json.loads(row['task_metadata']) if row.get('task_metadata') and isinstance(row.get('task_metadata'), str) else (row.get('task_metadata') if row.get('task_metadata') else {})
    task.tags = json.loads(row['tags']) if row.get('tags') and isinstance(row.get('tags'), str) else (row.get('tags') if row.get('tags') else [])
    task.backup_task_id = row.get('backup_task_id')
    return task


async def load_tasks_from_db(enabled_only: bool = True) -> List[ScheduledTask]:
    """从数据库加载计划任务"""
    try:
        async with get_opengauss_connection() as conn:
            if enabled_only:
                rows = await conn.fetch("SELECT * FROM scheduled_tasks WHERE enabled = true ORDER BY id")
            else:
                rows = await conn.fetch("SELECT * FROM scheduled_tasks ORDER BY id")

            tasks = []
            for row in rows:
                tasks.append(row_to_task(row))

            return tasks
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"从数据库加载任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        return []


# ===== 运行记录 =====
async def record_run_start(task_id: int, execution_id: str, started_at: datetime) -> None:
    """记录任务开始运行"""
    try:
        async with get_opengauss_connection() as conn:
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn

            # 确保表存在
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_runs (
                    id SERIAL PRIMARY KEY,
                    task_id INTEGER NOT NULL,
                    execution_id VARCHAR(64) UNIQUE NOT NULL,
                    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    completed_at TIMESTAMP WITH TIME ZONE,
                    status VARCHAR(16) DEFAULT 'running',
                    result JSONB,
                    error_message TEXT
                )
                """
            )
            if hasattr(actual_conn, 'commit'):
                await actual_conn.commit()

            # 插入记录
            await conn.execute(
                """
                INSERT INTO task_runs (task_id, execution_id, started_at, status)
                VALUES ($1, $2, $3, 'running')
                """,
                task_id, execution_id, started_at
            )
            if hasattr(actual_conn, 'commit'):
                await actual_conn.commit()
    except Exception as e:
        logger.warning(f"记录任务开始失败（忽略继续）: {str(e)}")


async def record_run_end(execution_id: str, completed_at: datetime, status: str,
                         result: Optional[Dict[str, Any]] = None,
                         error_message: Optional[str] = None) -> None:
    """记录任务结束"""
    try:
        async with get_opengauss_connection() as conn:
            try:
                # 确保表存在（如果 record_run_start 没有被调用，表可能不存在）
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS task_runs (
                        id SERIAL PRIMARY KEY,
                        task_id INTEGER NOT NULL,
                        execution_id VARCHAR(64) UNIQUE NOT NULL,
                        started_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        completed_at TIMESTAMP WITH TIME ZONE,
                        status VARCHAR(16) DEFAULT 'running',
                        result JSONB,
                        error_message TEXT
                    )
                    """
                )

                await conn.execute(
                    """
                    UPDATE task_runs
                    SET completed_at = $1,
                        status = $2,
                        result = $3,
                        error_message = $4
                    WHERE execution_id = $5
                    """,
                    completed_at, status, json.dumps(result) if result is not None else None,
                    error_message, execution_id
                )

                # 显式提交事务
                await conn.commit()

            except Exception as db_error:
                # 异常时显式回滚，避免长事务锁表
                logger.error(f"record_run_end: 数据库操作失败: {str(db_error)}", exc_info=True)
                try:
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug(f"record_run_end: 异常时事务已回滚（execution_id={execution_id}）")
                except Exception as rollback_err:
                    logger.warning(f"record_run_end: 回滚事务失败: {str(rollback_err)}")
                raise  # 重新抛出异常
    except Exception as e:
        logger.warning(f"记录任务结束失败（忽略继续）: {str(e)}")


# ===== 并发锁 =====
async def acquire_task_lock(task_id: int, execution_id: str) -> bool:
    """尝试获取任务锁（同一任务仅允许一个运行实例）"""
    try:
        async with get_opengauss_connection() as conn:
            # 确保锁表存在（task_id 唯一）
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS task_locks (
                    task_id INTEGER PRIMARY KEY,
                    execution_id VARCHAR(64) NOT NULL,
                    locked_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE
                )
                """
            )
            # 如果表已存在但缺少is_active字段，添加它
            try:
                column_exists = await conn.fetchval(
                    """
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'task_locks' AND column_name = 'is_active'
                    """
                )
                if not column_exists:
                    await conn.execute(
                        """
                        ALTER TABLE task_locks
                        ADD COLUMN is_active BOOLEAN DEFAULT TRUE
                        """
                    )
                    logger.info("已为 task_locks 表添加 is_active 字段")
            except Exception as e:
                logger.warning(f"检查/添加 is_active 字段失败（忽略继续）: {str(e)}")

            existing = await conn.fetchrow(
                """
                SELECT task_id, is_active FROM task_locks
                WHERE task_id = $1
                """,
                task_id
            )

            if existing:
                if existing['is_active']:
                    logger.debug(f"任务 {task_id} 已有活跃锁，获取失败")
                    return False
                else:
                    await conn.execute(
                        """
                        UPDATE task_locks
                        SET execution_id = $1, locked_at = $2, is_active = TRUE
                        WHERE task_id = $3
                        """,
                        execution_id, datetime.now(), task_id
                    )
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    try:
                        await actual_conn.commit()
                    except Exception as commit_err:
                        logger.warning(f"提交任务锁更新事务失败（可能已自动提交）: {commit_err}")
                    logger.info(f"任务 {task_id} 的锁已重新激活（更新现有记录）")
                    return True
            else:
                try:
                    await conn.execute(
                        """
                        INSERT INTO task_locks (task_id, execution_id, locked_at, is_active)
                        VALUES ($1, $2, $3, TRUE)
                        """,
                        task_id, execution_id, datetime.now()
                    )
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    try:
                        await actual_conn.commit()
                    except Exception as commit_err:
                        logger.warning(f"提交任务锁插入事务失败（可能已自动提交）: {commit_err}")
                    logger.info(f"任务 {task_id} 的新锁已创建")
                    return True
                except Exception as insert_error:
                    existing_after = await conn.fetchrow(
                        """
                        SELECT task_id, is_active FROM task_locks
                        WHERE task_id = $1
                        """,
                        task_id
                    )
                    if existing_after:
                        if existing_after['is_active']:
                            logger.warning(f"任务 {task_id} 在并发插入时被其他进程占用")
                            return False
                        else:
                            await conn.execute(
                                """
                                UPDATE task_locks
                                SET execution_id = $1, locked_at = $2, is_active = TRUE
                                WHERE task_id = $3
                                """,
                                execution_id, datetime.now(), task_id
                            )
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            try:
                                await actual_conn.commit()
                            except Exception as commit_err:
                                logger.warning(f"提交任务锁更新事务失败（可能已自动提交）: {commit_err}")
                            logger.info(f"任务 {task_id} 的锁已重新激活（并发插入后更新）")
                            return True
                    else:
                        logger.warning(f"插入任务锁失败（可能被其他进程占用）: {str(insert_error)}")
                        return False
    except Exception as e:
        logger.error(f"获取任务锁失败: {str(e)}", exc_info=True)
        return False


async def release_task_lock(task_id: int, execution_id: str) -> None:
    """释放任务锁"""
    try:
        async with get_opengauss_connection() as conn:
            await conn.execute(
                """
                UPDATE task_locks
                SET is_active = FALSE
                WHERE task_id = $1 AND execution_id = $2
                """,
                task_id, execution_id
            )
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            try:
                await actual_conn.commit()
            except Exception as commit_err:
                logger.warning(f"提交任务锁释放事务失败（可能已自动提交）: {commit_err}")
    except Exception as e:
        logger.warning(f"释放任务锁失败（忽略继续）: {str(e)}")


async def release_task_locks_by_task(task_id: int) -> None:
    """释放指定任务的所有活跃锁"""
    try:
        async with get_opengauss_connection() as conn:
            count_row = await conn.fetchrow(
                """
                SELECT COUNT(*) as count
                FROM task_locks
                WHERE task_id = $1 AND is_active = TRUE
                """,
                task_id
            )
            lock_count = count_row['count'] if count_row else 0

            await conn.execute(
                """
                UPDATE task_locks
                SET is_active = FALSE
                WHERE task_id = $1 AND is_active = TRUE
                """,
                task_id
            )

            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            try:
                await actual_conn.commit()
                if lock_count > 0:
                    logger.info(f"已释放任务 {task_id} 的 {lock_count} 个活跃锁（事务已提交）")
                else:
                    logger.info(f"任务 {task_id} 没有活跃锁需要释放")
            except Exception as commit_err:
                logger.warning(f"提交解锁事务失败（可能已自动提交）: {commit_err}")
                if lock_count > 0:
                    logger.info(f"已释放任务 {task_id} 的 {lock_count} 个活跃锁")
                else:
                    logger.info(f"任务 {task_id} 没有活跃锁需要释放")
    except Exception as e:
        logger.warning(f"释放指定任务锁失败（忽略继续）: {str(e)}")


async def release_all_active_locks() -> None:
    """释放所有活跃的任务锁（用于程序退出时清理）"""
    try:
        async with get_opengauss_connection() as conn:
            count_row = await conn.fetchrow(
                """
                SELECT COUNT(*) as count
                FROM task_locks
                WHERE is_active = TRUE
                """
            )
            lock_count = count_row['count'] if count_row else 0

            await conn.execute(
                """
                UPDATE task_locks
                SET is_active = FALSE
                WHERE is_active = TRUE
                """
            )

            if lock_count > 0:
                logger.info(f"已释放所有活跃的任务锁，共 {lock_count} 个")
            else:
                logger.info("没有活跃的任务锁需要释放")
    except Exception as e:
        logger.warning(f"释放所有任务锁失败（忽略继续）: {str(e)}")


async def get_task_by_id(task_id: int) -> Optional[ScheduledTask]:
    """根据ID获取计划任务"""
    try:
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow("SELECT * FROM scheduled_tasks WHERE id = $1", task_id)
            if not row:
                return None
            return row_to_task(row)
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"获取计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        return None


async def get_all_tasks(enabled_only: bool = False) -> List[ScheduledTask]:
    """获取所有计划任务"""
    try:
        async with get_opengauss_connection() as conn:
            if enabled_only:
                query = "SELECT * FROM scheduled_tasks WHERE enabled = true ORDER BY id"
            else:
                query = "SELECT * FROM scheduled_tasks ORDER BY id"

            rows = await conn.fetch(query)

            tasks = []
            for row in rows:
                tasks.append(row_to_task(row))

            return tasks
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"获取计划任务列表失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        return []


async def add_task(scheduled_task: ScheduledTask) -> bool:
    """添加计划任务"""
    try:
        async with get_opengauss_connection() as conn:
            # 准备插入SQL
            insert_sql = """
                INSERT INTO scheduled_tasks (
                    task_name, description, schedule_type, schedule_config,
                    action_type, action_config, enabled, status,
                    next_run_time, last_run_time, last_success_time, last_failure_time,
                    total_runs, success_runs, failure_runs, average_duration,
                    last_error, task_metadata, tags, backup_task_id,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, CAST($3 AS scheduletype), $4, CAST($5 AS taskactiontype), $6, $7, CAST($8 AS scheduledtaskstatus),
                    $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
                ) RETURNING id
            """

            # 准备数据
            schedule_config_json = json.dumps(scheduled_task.schedule_config) if scheduled_task.schedule_config else None
            action_config_json = json.dumps(scheduled_task.action_config) if scheduled_task.action_config else None
            task_metadata_json = json.dumps(scheduled_task.task_metadata) if hasattr(scheduled_task, 'task_metadata') and scheduled_task.task_metadata else None
            tags_json = json.dumps(scheduled_task.tags) if hasattr(scheduled_task, 'tags') and scheduled_task.tags else None

            # 使用CAST确保枚举值正确转换
            schedule_type_val = scheduled_task.schedule_type.value if scheduled_task.schedule_type else None
            action_type_val = scheduled_task.action_type.value if scheduled_task.action_type else None
            status_val = scheduled_task.status.value if scheduled_task.status else ScheduledTaskStatus.INACTIVE.value

            task_id = await conn.fetchval(
                insert_sql,
                scheduled_task.task_name,
                scheduled_task.description,
                schedule_type_val,
                schedule_config_json,
                action_type_val,
                action_config_json,
                scheduled_task.enabled,
                status_val,
                scheduled_task.next_run_time,
                scheduled_task.last_run_time,
                scheduled_task.last_success_time,
                scheduled_task.last_failure_time,
                scheduled_task.total_runs if hasattr(scheduled_task, 'total_runs') else 0,
                scheduled_task.success_runs if hasattr(scheduled_task, 'success_runs') else 0,
                scheduled_task.failure_runs if hasattr(scheduled_task, 'failure_runs') else 0,
                scheduled_task.average_duration if hasattr(scheduled_task, 'average_duration') else None,
                scheduled_task.last_error,
                task_metadata_json,
                tags_json,
                scheduled_task.backup_task_id if hasattr(scheduled_task, 'backup_task_id') else None,
                datetime.now(),
                datetime.now()
            )

            # psycopg3 binary protocol 需要显式提交事务
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            try:
                await actual_conn.commit()
                logger.debug(f"计划任务插入事务已提交: task_id={task_id}")
            except Exception as commit_err:
                logger.warning(f"提交计划任务插入事务失败（可能已自动提交）: {commit_err}")
                try:
                    await actual_conn.rollback()
                except:
                    pass

            scheduled_task.id = task_id
            logger.info(f"使用原生SQL插入计划任务成功: {scheduled_task.task_name} (ID: {task_id})")

            # 记录操作日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_CREATE,
                resource_type="scheduler",
                resource_id=str(task_id),
                resource_name=scheduled_task.task_name,
                operation_name="创建计划任务",
                operation_description=f"创建计划任务: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务创建成功 (ID: {task_id})",
                new_values={
                    "task_name": scheduled_task.task_name,
                    "description": scheduled_task.description,
                    "schedule_type": scheduled_task.schedule_type.value if scheduled_task.schedule_type else None,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "enabled": scheduled_task.enabled
                }
            )

            return True

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"添加计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")

        # 记录操作日志（失败）
        task_name = getattr(scheduled_task, 'task_name', '未知任务')
        await log_operation(
            operation_type=OperationType.SCHEDULER_CREATE,
            resource_type="scheduler",
            resource_name=task_name,
            operation_name="创建计划任务",
            operation_description=f"创建计划任务失败: {task_name}",
            category="scheduler",
            success=False,
            error_message=str(e)
        )

        return False


async def delete_task(task_id: int) -> bool:
    """删除计划任务"""
    try:
        async with get_opengauss_connection() as conn:
            # 先检查任务是否存在
            row = await conn.fetchrow("SELECT task_name FROM scheduled_tasks WHERE id = $1", task_id)
            if not row:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False

            task_name = row['task_name']

            # 记录操作日志（删除前）
            await log_operation(
                operation_type=OperationType.SCHEDULER_DELETE,
                resource_type="scheduler",
                resource_id=str(task_id),
                resource_name=task_name,
                operation_name="删除计划任务",
                operation_description=f"删除计划任务: {task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务删除成功 (ID: {task_id})",
                old_values={
                    "task_name": task_name,
                    "task_id": task_id
                }
            )

            # 删除任务
            await conn.execute("DELETE FROM scheduled_tasks WHERE id = $1", task_id)

            # 提交事务
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            if hasattr(actual_conn, 'commit'):
                try:
                    await actual_conn.commit()
                    logger.debug(f"计划任务 {task_id} 删除事务已提交")
                except Exception as commit_err:
                    logger.warning(f"提交删除事务失败（可能已自动提交）: {commit_err}")

            logger.info(f"使用原生SQL删除计划任务成功: {task_name} (ID: {task_id})")
            return True

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"删除计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")

        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_DELETE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="删除计划任务",
            operation_description=f"删除计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )

        return False


async def update_task(task_id: int, updates: Dict[str, Any], next_run_time: Optional[datetime] = None) -> Optional[ScheduledTask]:
    """更新计划任务

    Returns:
        更新后的ScheduledTask对象，如果失败则返回None
    """
    try:
        async with get_opengauss_connection() as conn:
            # 先获取任务
            row = await conn.fetchrow("SELECT * FROM scheduled_tasks WHERE id = $1", task_id)
            if not row:
                logger.warning(f"未找到任务 ID: {task_id}")
                return None

            # 转换为ScheduledTask对象
            task = row_to_task(row)

            # 记录旧值（用于日志）
            old_values = {
                "task_name": task.task_name,
                "description": task.description,
                "schedule_type": task.schedule_type.value if task.schedule_type else None,
                "action_type": task.action_type.value if task.action_type else None,
                "enabled": task.enabled,
                "status": task.status.value if task.status else None
            }

            # 更新字段
            for key, value in updates.items():
                if hasattr(task, key):
                    setattr(task, key, value)

            # 如果提供了next_run_time，使用它，否则使用updates中的值
            if next_run_time is not None:
                task.next_run_time = next_run_time
            elif 'next_run_time' in updates:
                task.next_run_time = updates['next_run_time']

            # 构建更新SQL - 更新所有传入的字段
            update_fields = []
            update_values = []
            param_index = 1

            # 处理所有更新的字段
            for key, value in updates.items():
                if key == 'schedule_type':
                    update_fields.append(f"schedule_type = CAST(${param_index} AS scheduletype)")
                    update_values.append(value.value if value else None)
                elif key == 'action_type':
                    update_fields.append(f"action_type = CAST(${param_index} AS taskactiontype)")
                    update_values.append(value.value if value else None)
                elif key == 'status':
                    update_fields.append(f"status = CAST(${param_index} AS scheduledtaskstatus)")
                    update_values.append(value.value if value else None)
                elif key in ['schedule_config', 'action_config', 'task_metadata']:
                    update_fields.append(f"{key} = ${param_index}")
                    update_values.append(json.dumps(value) if value else None)
                elif key == 'tags':
                    update_fields.append(f"tags = ${param_index}")
                    update_values.append(json.dumps(value) if value else None)
                else:
                    update_fields.append(f"{key} = ${param_index}")
                    update_values.append(value)
                param_index += 1

            # 始终更新 next_run_time（如果重新计算过）和 updated_at
            if next_run_time is not None or 'next_run_time' in updates:
                update_fields.append(f"next_run_time = ${param_index}")
                update_values.append(task.next_run_time)
                param_index += 1

            update_fields.append(f"updated_at = ${param_index}")
            update_values.append(datetime.now())
            param_index += 1

            # 添加task_id作为WHERE条件
            update_values.append(task_id)

            # 执行更新
            if update_fields:
                update_sql = f"""
                    UPDATE scheduled_tasks
                    SET {', '.join(update_fields)}
                    WHERE id = ${param_index}
                """

                await conn.execute(update_sql, *update_values)

            # 显式提交事务
            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
            try:
                await actual_conn.commit()
                logger.debug(f"计划任务更新事务已提交: task_id={task_id}")
            except Exception as commit_err:
                logger.warning(f"提交计划任务更新事务失败（可能已自动提交）: {commit_err}")

            logger.info(f"使用原生SQL更新计划任务成功: {task.task_name} (ID: {task_id})")

            # 记录操作日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_UPDATE,
                resource_type="scheduler",
                resource_id=str(task_id),
                resource_name=task.task_name,
                operation_name="更新计划任务",
                operation_description=f"更新计划任务: {task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"计划任务更新成功 (ID: {task_id})",
                old_values=old_values,
                new_values={
                    "task_name": task.task_name,
                    "description": task.description,
                    "schedule_type": task.schedule_type.value if task.schedule_type else None,
                    "action_type": task.action_type.value if task.action_type else None,
                    "enabled": task.enabled,
                    "status": task.status.value if task.status else None
                },
                changed_fields=list(updates.keys())
            )

            # 重新获取更新后的任务（在事务提交后）
            return await get_task_by_id(task_id)

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"更新计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")

        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_UPDATE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="更新计划任务",
            operation_description=f"更新计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e)
        )

        return None
