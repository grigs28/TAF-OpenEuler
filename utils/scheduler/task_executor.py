#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务执行器
Task Executor
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Callable, Optional

from models.scheduled_task import ScheduledTask, ScheduledTaskLog, ScheduledTaskStatus, TaskActionType
from models.system_log import OperationType, LogLevel, LogCategory
from config.database import db_manager
from sqlalchemy import select
from .action_handlers import get_action_handler
from .schedule_calculator import calculate_next_run_time
from utils.log_utils import log_operation, log_system
from .task_storage import record_run_start, record_run_end, acquire_task_lock, release_task_lock
from .db_utils import is_opengauss, is_redis, get_opengauss_connection

logger = logging.getLogger(__name__)


def create_task_executor(
    scheduled_task: ScheduledTask,
    system_instance,
    manual_run: bool = False,
    run_options: Optional[Dict[str, Any]] = None
) -> Callable:
    """创建任务执行函数
    
    Args:
        scheduled_task: 计划任务对象
        system_instance: 系统实例
        manual_run: 是否为手动运行（Web界面点击运行），默认为False
    """
    async def executor():
        execution_id = str(uuid.uuid4())
        start_time = datetime.now()
        lock_acquired = False
        
        logger.info(f"[任务执行器] 开始执行任务 - ID: {scheduled_task.id}, 名称: {scheduled_task.task_name}, 执行ID: {execution_id}, 手动运行: {manual_run}")
        
        try:
            # 获取任务并发锁（openGauss原生），获取失败则跳过
            logger.debug(f"[任务执行器] 尝试获取任务锁 - 任务ID: {scheduled_task.id}, 执行ID: {execution_id}")
            got_lock = await acquire_task_lock(scheduled_task.id, execution_id)
            if not got_lock:
                error_msg = f"获取任务锁失败，任务已在执行中 - 任务ID: {scheduled_task.id}, 执行ID: {execution_id}"
                logger.info(f"[任务执行器] {error_msg}")
                await log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.SYSTEM,
                    message="任务已在执行中，跳过",
                    module="scheduler",
                    function="task_executor",
                    task_id=scheduled_task.id,
                    details={"execution_id": execution_id}
                )
                # 如果是手动运行，抛出异常以便调用者能够捕获并返回错误信息
                if manual_run:
                    raise RuntimeError("任务已在执行中，无法重复运行")
                return
            
            logger.info(f"[任务执行器] 成功获取任务锁 - 任务ID: {scheduled_task.id}, 执行ID: {execution_id}")
            
            lock_acquired = True

            # 记录任务执行开始日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"开始执行计划任务: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"任务执行开始 (执行ID: {execution_id})"
            )
            
            # 更新任务状态为运行中（openGauss使用原生SQL，Redis使用Redis函数，其他使用SQLAlchemy）
            if is_redis():
                # Redis模式：使用Redis更新函数
                from .redis_task_storage import update_task_redis
                await update_task_redis(
                    scheduled_task.id,
                    {'status': ScheduledTaskStatus.RUNNING, 'last_run_time': start_time}
                )
            elif is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    try:
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus, last_run_time = $2
                            WHERE id = $3
                            """,
                            'running', start_time, scheduled_task.id
                        )
                        
                        # psycopg3 binary protocol 需要显式提交事务
                        await conn.commit()
                        
                        # 验证事务提交状态
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status == 0:  # IDLE: 事务成功提交
                                logger.debug(f"任务 {scheduled_task.id} 状态更新已提交到数据库")
                            elif transaction_status == 1:  # INTRANS: 事务未提交
                                logger.warning(f"任务 {scheduled_task.id} 状态更新事务未提交，状态={transaction_status}，尝试回滚")
                                await actual_conn.rollback()
                                raise Exception("事务提交失败")
                            elif transaction_status == 3:  # INERROR: 错误状态
                                logger.error(f"任务 {scheduled_task.id} 状态更新连接处于错误状态，回滚事务")
                                await actual_conn.rollback()
                                raise Exception("连接处于错误状态")
                    except Exception as db_error:
                        # 异常时显式回滚，避免长事务锁表
                        logger.error(f"任务 {scheduled_task.id} 状态更新失败: {str(db_error)}", exc_info=True)
                        try:
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            if hasattr(actual_conn, 'info'):
                                transaction_status = actual_conn.info.transaction_status
                                if transaction_status in (1, 3):  # INTRANS or INERROR
                                    await actual_conn.rollback()
                                    logger.debug(f"任务 {scheduled_task.id} 状态更新异常时事务已回滚")
                        except Exception as rollback_err:
                            logger.warning(f"任务 {scheduled_task.id} 状态更新回滚事务失败: {str(rollback_err)}")
                        raise  # 重新抛出异常
            elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                async with db_manager.AsyncSessionLocal() as session:
                    scheduled_task.status = ScheduledTaskStatus.RUNNING
                    scheduled_task.last_run_time = start_time
                    session.add(scheduled_task)
                    await session.commit()
            
            # 创建执行日志（openGauss使用原生SQL，Redis使用Redis函数，其他使用SQLAlchemy）
            if is_redis():
                # Redis模式：使用Redis记录函数
                from .redis_task_storage import record_run_start_redis
                try:
                    await record_run_start_redis(scheduled_task.id, execution_id, start_time)
                except Exception:
                    pass
            elif is_opengauss():
                # openGauss 原生记录运行开始
                try:
                    await record_run_start(scheduled_task.id, execution_id, start_time)
                except Exception:
                    pass
            elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                task_log = ScheduledTaskLog(
                    scheduled_task_id=scheduled_task.id,
                    execution_id=execution_id,
                    started_at=start_time,
                    status='running'
                )
                async with db_manager.AsyncSessionLocal() as session:
                    session.add(task_log)
                    await session.commit()
            
            # 执行任务动作
            action_type = scheduled_task.action_type
            action_config = scheduled_task.action_config or {}
            
            # 检查action_type是否有效
            if not action_type:
                raise ValueError(f"任务动作类型为空: {scheduled_task.task_name}")
            
            # 如果action_type是字符串，尝试转换为枚举
            if isinstance(action_type, str):
                try:
                    action_type = TaskActionType(action_type)
                except ValueError:
                    raise ValueError(f"无效的任务动作类型: {action_type} (任务: {scheduled_task.task_name})")
            
            logger.info(f"执行任务动作: {action_type.value if hasattr(action_type, 'value') else action_type} (任务: {scheduled_task.task_name})")
            
            # 从task_metadata中获取backup_task_id
            backup_task_id = None
            if scheduled_task.task_metadata and isinstance(scheduled_task.task_metadata, dict):
                backup_task_id = scheduled_task.task_metadata.get('backup_task_id')
            
            # 获取动作处理器
            try:
                handler = get_action_handler(action_type, system_instance)
            except Exception as e:
                logger.error(f"获取动作处理器失败: {str(e)}, action_type: {action_type}, type: {type(action_type)}")
                raise
            
            # 根据动作类型执行
            logger.info(f"[任务执行器] 准备调用动作处理器 - 动作类型: {action_type.value if hasattr(action_type, 'value') else action_type}, 任务: {scheduled_task.task_name}")
            if action_type == TaskActionType.BACKUP:
                # 备份动作需要传递backup_task_id参数和manual_run参数
                logger.info(f"[任务执行器] 调用备份动作处理器 - backup_task_id: {backup_task_id}, manual_run: {manual_run}")
                result = await handler.execute(
                    action_config,
                    backup_task_id=backup_task_id,
                    scheduled_task=scheduled_task,
                    manual_run=manual_run,
                    run_options=run_options
                )
                logger.info(f"[任务执行器] 备份动作处理器执行完成 - 结果: {result}")
            else:
                logger.info(f"[任务执行器] 调用动作处理器 - 动作类型: {action_type.value if hasattr(action_type, 'value') else action_type}")
                result = await handler.execute(
                    action_config,
                    scheduled_task=scheduled_task,
                    manual_run=manual_run,
                    run_options=run_options
                )
                logger.info(f"[任务执行器] 动作处理器执行完成 - 结果: {result}")
            
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            
            # 更新执行日志和任务统计（openGauss使用原生SQL，其他使用SQLAlchemy）
            if is_opengauss():
                # openGauss 原生记录结束（成功）
                try:
                    await record_run_end(execution_id, end_time, 'success', result=result)
                except Exception:
                    pass
                
                # 更新任务统计（使用原生SQL）
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 获取当前统计值
                    current_task = await conn.fetchrow(
                        """
                        SELECT total_runs, success_runs, average_duration
                        FROM scheduled_tasks
                        WHERE id = $1
                        """,
                        scheduled_task.id
                    )
                    
                    total_runs = (current_task['total_runs'] or 0) + 1
                    success_runs = (current_task['success_runs'] or 0) + 1
                    
                    # 计算平均执行时长
                    avg_duration = duration // 1000  # 秒
                    if current_task['average_duration']:
                        avg_duration = int((current_task['average_duration'] + avg_duration) / 2)
                    
                    # 先更新内存中的 scheduled_task 对象，确保 calculate_next_run_time 能读取到最新数据
                    scheduled_task.last_success_time = end_time
                    scheduled_task.total_runs = total_runs
                    scheduled_task.success_runs = success_runs
                    scheduled_task.average_duration = avg_duration
                    scheduled_task.status = ScheduledTaskStatus.ACTIVE
                    
                    # 计算下次执行时间（此时 scheduled_task 已更新，能正确读取 last_success_time）
                    next_run = calculate_next_run_time(scheduled_task)
                    
                    # 更新任务到数据库
                    await conn.execute(
                        """
                        UPDATE scheduled_tasks
                        SET status = $1::scheduledtaskstatus,
                            last_success_time = $2,
                            total_runs = $3,
                            success_runs = $4,
                            average_duration = $5,
                            next_run_time = $6
                        WHERE id = $7
                        """,
                        'active', end_time, total_runs, success_runs, avg_duration, next_run, scheduled_task.id
                    )
                    
                    # 更新内存中的 next_run_time
                    scheduled_task.next_run_time = next_run
            elif is_redis():
                # Redis模式下，任务日志和统计更新由 record_run_end 和 storage_update_task 处理
                # 这里只需要记录成功日志，不需要额外更新
                logger.debug("[Redis模式] 任务执行成功，任务日志和统计已由 record_run_end 和 storage_update_task 更新")
            else:
                # 使用SQLAlchemy更新（SQLite模式）
                if db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                    async with db_manager.AsyncSessionLocal() as session:
                        stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                        log_result = await session.execute(stmt)
                        task_log = log_result.scalar_one()
                        
                        task_log.completed_at = end_time
                        task_log.duration = duration // 1000  # 秒
                        task_log.status = 'success'
                        task_log.result = result
                        
                        # 更新任务统计
                        scheduled_task.total_runs = (scheduled_task.total_runs or 0) + 1
                        scheduled_task.success_runs = (scheduled_task.success_runs or 0) + 1
                        scheduled_task.last_success_time = end_time
                        scheduled_task.status = ScheduledTaskStatus.ACTIVE
                        
                        # 计算平均执行时长
                        if scheduled_task.average_duration:
                            scheduled_task.average_duration = int(
                                (scheduled_task.average_duration + duration // 1000) / 2
                            )
                        else:
                            scheduled_task.average_duration = duration // 1000
                        
                        # 计算下次执行时间
                        next_run = calculate_next_run_time(scheduled_task)
                        scheduled_task.next_run_time = next_run
                        
                        session.add(scheduled_task)
                        await session.commit()
                else:
                    logger.warning("[任务执行器] SQLAlchemy不可用，跳过任务日志和统计更新")

            # 详细的任务执行成功日志输出
            duration_seconds = duration / 1000.0
            logger.info("=" * 80)
            logger.info(f"✅ 计划任务执行成功")
            logger.info(f"   任务名称: {scheduled_task.task_name}")
            logger.info(f"   任务ID: {scheduled_task.id}")
            logger.info(f"   执行ID: {execution_id}")
            logger.info(f"   动作类型: {scheduled_task.action_type.value if scheduled_task.action_type else 'N/A'}")
            logger.info(f"   开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"   结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"   执行耗时: {duration_seconds:.2f}秒 ({duration}毫秒)")
            
            # 输出执行结果详情
            if result:
                logger.info(f"   执行结果:")
                if isinstance(result, dict):
                    for key, value in result.items():
                        if key == 'status':
                            logger.info(f"     状态: {value}")
                        elif key == 'message':
                            logger.info(f"     消息: {value}")
                        elif key == 'backup_task_id':
                            logger.info(f"     备份任务ID: {value}")
                        elif key == 'backup_set_id':
                            logger.info(f"     备份集ID: {value}")
                        elif key == 'tape_id':
                            logger.info(f"     磁带ID: {value}")
                        elif key == 'total_files':
                            logger.info(f"     总文件数: {value}")
                        elif key == 'total_bytes':
                            # total_bytes 在备份引擎中表示所有扫描到的文件总数（批次相加的文件数），不是字节数
                            logger.info(f"     扫描文件总数: {value:,}" if value else "     扫描文件总数: N/A")
                        elif key == 'processed_files':
                            logger.info(f"     已处理文件数: {value}")
                        else:
                            logger.info(f"     {key}: {value}")
                else:
                    logger.info(f"     {result}")
            
            # 输出任务统计信息
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    current_task = await conn.fetchrow(
                        """
                        SELECT total_runs, success_runs, failure_runs, average_duration
                        FROM scheduled_tasks
                        WHERE id = $1
                        """,
                        scheduled_task.id
                    )
                    if current_task:
                        logger.info(f"   任务统计:")
                        logger.info(f"     总执行次数: {current_task['total_runs'] or 0}")
                        logger.info(f"     成功次数: {current_task['success_runs'] or 0}")
                        logger.info(f"     失败次数: {current_task['failure_runs'] or 0}")
                        if current_task['average_duration']:
                            logger.info(f"     平均执行时长: {current_task['average_duration']}秒")
            else:
                logger.info(f"   任务统计:")
                logger.info(f"     总执行次数: {scheduled_task.total_runs or 0}")
                logger.info(f"     成功次数: {scheduled_task.success_runs or 0}")
                logger.info(f"     失败次数: {scheduled_task.failure_runs or 0}")
                if scheduled_task.average_duration:
                    logger.info(f"     平均执行时长: {scheduled_task.average_duration}秒")
            
            # 输出下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            logger.info(f"   下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else 'N/A'}")
            logger.info("=" * 80)
            
            # 记录任务执行成功日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"计划任务执行成功: {scheduled_task.task_name}",
                category="scheduler",
                success=True,
                result_message=f"任务执行成功 (执行ID: {execution_id}, 耗时: {duration}ms)",
                duration_ms=duration
            )
            
            # 记录系统日志
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.SYSTEM,
                message=f"计划任务执行成功: {scheduled_task.task_name}",
                module="scheduler",
                function="task_executor",
                task_id=scheduled_task.id,
                details={
                    "execution_id": execution_id,
                    "task_name": scheduled_task.task_name,
                    "task_id": scheduled_task.id,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "duration_ms": duration,
                    "result": result
                },
                duration_ms=duration
            )
            
        except KeyboardInterrupt:
            # 处理 Ctrl+C 中断
            # 注意：finally 块会在 raise 之前执行，确保锁被释放
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)
            error_msg = "任务被用户中断（Ctrl+C）"
            logger.warning(f"任务执行被中断: {scheduled_task.task_name}")
            
            # 更新任务状态为错误（使用 try-except 确保即使失败也不影响 finally 执行）
            try:
                if is_opengauss():
                    # 使用连接池
                    async with get_opengauss_connection() as conn:
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus,
                                last_error = $2
                            WHERE id = $3
                            """,
                            'error', error_msg, scheduled_task.id
                        )
            except Exception as update_error:
                logger.error(f"更新任务状态失败（忽略继续）: {str(update_error)}")
            
            # 重新抛出异常，让上层处理
            # finally 块会在 raise 之前执行
            raise
            
        except Exception as e:
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds() * 1000)  # 转换为毫秒
            error_msg = str(e)

            # 如果是"任务已在执行中"的错误，直接重新抛出，不记录为 ERROR
            if "任务已在执行中" in error_msg:
                logger.info(f"[任务执行器] 任务已在执行中，跳过 - 任务ID: {scheduled_task.id}")
                raise

            import traceback
            stack_trace = traceback.format_exc()

            # 详细的任务执行失败日志输出
            duration_seconds = duration / 1000.0
            logger.error("=" * 80)
            logger.error(f"❌ 计划任务执行失败")
            logger.error(f"   任务名称: {scheduled_task.task_name}")
            logger.error(f"   任务ID: {scheduled_task.id}")
            logger.error(f"   执行ID: {execution_id}")
            logger.error(f"   动作类型: {scheduled_task.action_type.value if scheduled_task.action_type else 'N/A'}")
            logger.error(f"   开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.error(f"   结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.error(f"   执行耗时: {duration_seconds:.2f}秒 ({duration}毫秒)")
            logger.error(f"   错误类型: {type(e).__name__}")
            logger.error(f"   错误消息: {error_msg}")
            logger.error(f"   异常堆栈:")
            for line in stack_trace.split('\n'):
                if line.strip():
                    logger.error(f"     {line}")
            
            # 更新执行日志和任务统计（openGauss使用原生SQL，Redis使用Redis函数，其他使用SQLAlchemy）
            if is_redis():
                # Redis模式：使用Redis更新函数
                from .redis_task_storage import record_run_end_redis, update_task_redis
                try:
                    await record_run_end_redis(execution_id, end_time, 'failed', result=None, error_message=error_msg)
                except Exception:
                    pass
                
                # 更新任务统计
                try:
                    # 获取当前任务数据
                    from .redis_task_storage import get_task_by_id_redis
                    current_task = await get_task_by_id_redis(scheduled_task.id)
                    if current_task:
                        total_runs = (current_task.total_runs or 0) + 1
                        failure_runs = (current_task.failure_runs or 0) + 1
                        
                        # 更新任务
                        await update_task_redis(
                            scheduled_task.id,
                            {
                                'status': ScheduledTaskStatus.ERROR,
                                'total_runs': total_runs,
                                'failure_runs': failure_runs,
                                'last_failure_time': end_time,
                                'last_error': error_msg
                            }
                        )
                except Exception as update_error:
                    logger.error(f"更新任务统计失败: {str(update_error)}")
            elif is_opengauss():
                # openGauss 原生记录结束（失败）
                try:
                    await record_run_end(execution_id, end_time, 'failed', result=None, error_message=error_msg)
                except Exception:
                    pass
                
                # 更新任务统计（使用原生SQL）
                try:
                    # 使用连接池
                    async with get_opengauss_connection() as conn:
                        # 获取当前统计值
                        current_task = await conn.fetchrow(
                            """
                            SELECT total_runs, failure_runs
                            FROM scheduled_tasks
                            WHERE id = $1
                            """,
                            scheduled_task.id
                        )
                        
                        total_runs = (current_task['total_runs'] or 0) + 1
                        failure_runs = (current_task['failure_runs'] or 0) + 1
                        
                        # 更新任务
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus,
                                total_runs = $2,
                                failure_runs = $3,
                                last_failure_time = $4,
                                last_error = $5
                            WHERE id = $6
                            """,
                            'error', total_runs, failure_runs, end_time, error_msg, scheduled_task.id
                        )
                except Exception as db_error:
                    logger.error(f"更新任务日志失败: {str(db_error)}")
            elif is_sqlite() and db_manager.AsyncSessionLocal and callable(db_manager.AsyncSessionLocal):
                # 使用SQLAlchemy更新
                try:
                    async with db_manager.AsyncSessionLocal() as session:
                        stmt = select(ScheduledTaskLog).where(ScheduledTaskLog.execution_id == execution_id)
                        log_result = await session.execute(stmt)
                        task_log = log_result.scalar_one()
                        
                        task_log.completed_at = end_time
                        task_log.duration = duration // 1000  # 秒
                        task_log.status = 'failed'
                        task_log.error_message = error_msg
                        
                        # 更新任务统计
                        scheduled_task.total_runs = (scheduled_task.total_runs or 0) + 1
                        scheduled_task.failure_runs = (scheduled_task.failure_runs or 0) + 1
                        scheduled_task.last_failure_time = end_time
                        scheduled_task.last_error = error_msg
                        scheduled_task.status = ScheduledTaskStatus.ERROR
                        
                        session.add(scheduled_task)
                        await session.commit()
                except Exception as db_error:
                    logger.error(f"更新任务日志失败: {str(db_error)}")
            
            # 输出任务统计信息（失败后）
            if is_opengauss():
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    current_task = await conn.fetchrow(
                        """
                        SELECT total_runs, success_runs, failure_runs, average_duration
                        FROM scheduled_tasks
                        WHERE id = $1
                        """,
                        scheduled_task.id
                    )
                    if current_task:
                        logger.error(f"   任务统计:")
                        logger.error(f"     总执行次数: {current_task['total_runs'] or 0}")
                        logger.error(f"     成功次数: {current_task['success_runs'] or 0}")
                        logger.error(f"     失败次数: {current_task['failure_runs'] or 0}")
                        if current_task['average_duration']:
                            logger.error(f"     平均执行时长: {current_task['average_duration']}秒")
            else:
                logger.error(f"   任务统计:")
                logger.error(f"     总执行次数: {scheduled_task.total_runs or 0}")
                logger.error(f"     成功次数: {scheduled_task.success_runs or 0}")
                logger.error(f"     失败次数: {scheduled_task.failure_runs or 0}")
                if scheduled_task.average_duration:
                    logger.error(f"     平均执行时长: {scheduled_task.average_duration}秒")
            
            # 输出下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            logger.error(f"   下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else 'N/A'}")
            logger.error("=" * 80)

            # 记录任务执行失败日志
            await log_operation(
                operation_type=OperationType.SCHEDULER_RUN,
                resource_type="scheduler",
                resource_id=str(scheduled_task.id),
                resource_name=scheduled_task.task_name,
                operation_name="执行计划任务",
                operation_description=f"计划任务执行失败: {scheduled_task.task_name}",
                category="scheduler",
                success=False,
                error_message=error_msg,
                duration_ms=duration
            )
            
            # 记录系统日志（错误）
            await log_system(
                level=LogLevel.ERROR,
                category=LogCategory.SYSTEM,
                message=f"计划任务执行失败: {scheduled_task.task_name}",
                module="scheduler",
                function="task_executor",
                task_id=scheduled_task.id,
                details={
                    "execution_id": execution_id,
                    "task_name": scheduled_task.task_name,
                    "task_id": scheduled_task.id,
                    "action_type": scheduled_task.action_type.value if scheduled_task.action_type else None,
                    "duration_ms": duration,
                    "error": error_msg
                },
                exception_type=type(e).__name__,
                stack_trace=stack_trace,
                duration_ms=duration
            )
            
            # 发送钉钉通知（计划任务失败时）
            try:
                if system_instance and hasattr(system_instance, 'dingtalk_notifier') and system_instance.dingtalk_notifier:
                    # 根据动作类型发送不同的通知
                    if scheduled_task.action_type == TaskActionType.BACKUP:
                        await system_instance.dingtalk_notifier.send_backup_notification(
                            backup_name=scheduled_task.task_name,
                            status='failed',
                            details={
                                'error': error_msg,
                                'task_id': scheduled_task.id,
                                'execution_id': execution_id,
                                'duration_ms': duration
                            }
                        )
                    else:
                        # 其他动作类型发送系统通知
                        await system_instance.dingtalk_notifier.send_system_notification(
                            title="❌ 计划任务执行失败",
                            content=f"""## 计划任务执行失败通知

**任务名称**: {scheduled_task.task_name}
**任务ID**: {scheduled_task.id}
**执行ID**: {execution_id}
**动作类型**: {scheduled_task.action_type.value if scheduled_task.action_type else 'N/A'}
**执行耗时**: {duration_seconds:.2f}秒
**错误类型**: {type(e).__name__}
**错误信息**: {error_msg}

请检查任务配置和执行日志。
"""
                        )
            except Exception as notify_error:
                logger.error(f"发送计划任务失败钉钉通知异常: {str(notify_error)}", exc_info=True)
        
        finally:
            # 确保无论成功还是失败，都释放任务锁
            # 这个 finally 块会在所有退出路径（return、raise、正常结束）之前执行
            if lock_acquired:
                try:
                    await release_task_lock(scheduled_task.id, execution_id)
                    logger.debug(f"任务锁已释放: {scheduled_task.task_name} (执行ID: {execution_id})")
                except Exception as lock_error:
                    # 即使释放锁失败，也要记录错误，但不影响其他流程
                    logger.error(f"释放任务锁失败: {scheduled_task.task_name} (执行ID: {execution_id}), 错误: {str(lock_error)}")
                    # 注意：即使释放锁失败，我们也不应该重新抛出异常，因为这会掩盖原始错误
    
    return executor

