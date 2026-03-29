#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务解锁器 - SQLite 支持
Scheduled Task Unlocker - SQLite Support
"""

import logging
from config.database import db_manager
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.scheduler.task_storage import release_task_locks_by_task

logger = logging.getLogger(__name__)


async def unlock_task_and_reset_status(task_id: int) -> bool:
    """解锁任务并重置状态为 ACTIVE（支持 SQLite）"""
    try:
        # 1. 先释放任务锁
        await release_task_locks_by_task(task_id)
        
        # 2. 更新任务状态为 ACTIVE（如果当前是 RUNNING）
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                # 检查当前状态
                row = await conn.fetchrow(
                    "SELECT status FROM scheduled_tasks WHERE id = $1",
                    task_id
                )
                if row:
                    current_status = row['status']
                    if isinstance(current_status, str):
                        current_status = current_status.lower()
                    
                    # 如果状态是 RUNNING，更新为 ACTIVE
                    if current_status == 'running':
                        await conn.execute(
                            """
                            UPDATE scheduled_tasks
                            SET status = $1::scheduledtaskstatus, updated_at = NOW()
                            WHERE id = $2
                            """,
                            'active', task_id
                        )
                        # 显式提交事务（openGauss 模式需要显式提交）
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        try:
                            await actual_conn.commit()
                            logger.info(f"任务 {task_id} 状态已从 RUNNING 重置为 ACTIVE（事务已提交）")
                        except Exception as commit_err:
                            logger.warning(f"提交解锁事务失败（可能已自动提交）: {commit_err}")
                    else:
                        logger.info(f"任务 {task_id} 当前状态为 {current_status}，无需重置")
        else:
            # SQLite 版本：使用 SQLite 实现文件
            
            task = await get_task_by_id_sqlite(task_id)
            if task:
                if task.status == ScheduledTaskStatus.RUNNING:
                    updated_task = await update_task_sqlite(task_id, {'status': ScheduledTaskStatus.ACTIVE})
                    if updated_task:
                        logger.info(f"任务 {task_id} 状态已从 RUNNING 重置为 ACTIVE")
                    else:
                        logger.warning(f"任务 {task_id} 状态更新失败")
                        return False
                else:
                    logger.info(f"任务 {task_id} 当前状态为 {task.status.value}，无需重置")
            else:
                logger.warning(f"任务 {task_id} 不存在")
                return False
        
        return True
    except Exception as e:
        logger.error(f"解锁任务并重置状态失败: {str(e)}", exc_info=True)
        return False


async def unlock_all_tasks_and_reset_status() -> int:
    """解锁所有任务并重置 RUNNING 状态为 ACTIVE（支持 SQLite）"""
    try:
        count = 0
        
        if is_opengauss():
            async with get_opengauss_connection() as conn:
                # 释放所有锁
                await conn.execute(
                    "UPDATE task_locks SET is_active = FALSE WHERE is_active = TRUE"
                )
                
                # 重置所有 RUNNING 状态的任务
                result = await conn.execute(
                    """
                    UPDATE scheduled_tasks
                    SET status = $1::scheduledtaskstatus, updated_at = NOW()
                    WHERE status = $2::scheduledtaskstatus
                    """,
                    'active', 'running'
                )
                count = result.split()[-1] if isinstance(result, str) else 0
                logger.info(f"已解锁所有任务并重置 {count} 个 RUNNING 状态的任务")
        else:
            # SQLite 版本：使用 SQLite 实现文件
            
            all_tasks = await get_all_tasks_sqlite(enabled_only=False)
            for task in all_tasks:
                if task.status == ScheduledTaskStatus.RUNNING:
                    updated_task = await update_task_sqlite(task.id, {'status': ScheduledTaskStatus.ACTIVE})
                    if updated_task:
                        count += 1
            
            logger.info(f"已重置 {count} 个 RUNNING 状态的任务为 ACTIVE")
        
        return count
    except Exception as e:
        logger.error(f"解锁所有任务并重置状态失败: {str(e)}", exc_info=True)
        return 0

