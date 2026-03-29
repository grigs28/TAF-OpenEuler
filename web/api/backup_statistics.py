#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份统计 API - SQLite 支持
Backup Statistics API - SQLite Support
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from models.backup import BackupTask, BackupTaskStatus
from models.scheduled_task import ScheduledTask, TaskActionType
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, is_redis, get_sqlite_connection

logger = logging.getLogger(__name__)


async def get_backup_statistics() -> Dict[str, Any]:
    """获取备份统计信息（支持 SQLite、openGauss、Redis）"""
    try:
        from utils.scheduler.db_utils import is_redis
        
        if is_redis():
            return await _get_backup_statistics_redis()
        elif is_opengauss():
            return await _get_backup_statistics_opengauss()
        elif is_sqlite():
            return await _get_backup_statistics_sqlite()
        else:
            # 未知数据库类型，返回默认值
            logger.warning("未知的数据库类型，返回默认统计信息")
            return {
                "total_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "running_tasks": 0,
                "pending_tasks": 0,
                "success_rate": 0.0,
                "total_data_backed_up": 0,
                "compression_ratio": 0.0,
                "average_task_duration": 0,
                "recent_24h": {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "data_backed_up": 0
                }
            }
    except Exception as e:
        logger.error(f"获取备份统计信息失败: {str(e)}", exc_info=True)
        raise


async def _get_backup_statistics_opengauss() -> Dict[str, Any]:
    """获取备份统计信息（openGauss 版本）"""
    async with get_opengauss_connection() as conn:
        # 总任务数（包含模板与执行记录）
        total_row = await conn.fetchrow("SELECT COUNT(*) as total FROM backup_tasks")
        total_tasks = total_row["total"] if total_row else 0
        
        # 计划任务中的备份任务数量（计入总任务与pending）
        sched_total_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM scheduled_tasks WHERE LOWER(action_type::text)=LOWER('BACKUP')"
        )
        sched_total = sched_total_row["total"] if sched_total_row else 0
        total_tasks += sched_total
        
        # 按状态统计（执行记录与模板均统计各自status）
        completed_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.COMPLETED.value
        )
        completed_tasks = completed_row["total"] if completed_row else 0
        
        failed_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.FAILED.value
        )
        failed_tasks = failed_row["total"] if failed_row else 0
        
        running_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE is_template = false AND LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.RUNNING.value
        )
        running_tasks = running_row["total"] if running_row else 0
        
        pending_row = await conn.fetchrow(
            "SELECT COUNT(*) as total FROM backup_tasks WHERE LOWER(status::text)=LOWER($1)",
            BackupTaskStatus.PENDING.value
        )
        pending_tasks = (pending_row["total"] if pending_row else 0) + sched_total
        
        # 成功率
        success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
        
        # 总备份数据量
        bytes_row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(processed_bytes), 0) as total 
            FROM backup_tasks 
            WHERE is_template = false AND LOWER(status::text)=LOWER($1)
            """,
            BackupTaskStatus.COMPLETED.value
        )
        total_data_backed_up = bytes_row["total"] if bytes_row else 0
        
        # 最近24小时统计
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        recent_row = await conn.fetchrow(
            """
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($1)) as completed,
                COUNT(*) FILTER (WHERE LOWER(status::text)=LOWER($2)) as failed,
                COALESCE(SUM(processed_bytes), 0) as data
            FROM backup_tasks
            WHERE is_template = false AND created_at >= $3
            """,
            BackupTaskStatus.COMPLETED.value,
            BackupTaskStatus.FAILED.value,
            twenty_four_hours_ago
        )
        
        recent_total = recent_row["total"] if recent_row else 0
        recent_completed = recent_row["completed"] if recent_row else 0
        recent_failed = recent_row["failed"] if recent_row else 0
        recent_data = recent_row["data"] if recent_row else 0
        
        # 平均任务时长
        avg_duration_row = await conn.fetchrow(
            """
            SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_duration
            FROM backup_tasks
            WHERE is_template = false 
              AND LOWER(status::text)=LOWER($1)
              AND completed_at IS NOT NULL 
              AND started_at IS NOT NULL
            """,
            BackupTaskStatus.COMPLETED.value
        )
        avg_duration = int(avg_duration_row["avg_duration"]) if avg_duration_row and avg_duration_row["avg_duration"] else 3600
        
        # 压缩比
        compression_row = await conn.fetchrow(
            """
            SELECT 
                COALESCE(SUM(processed_bytes), 0) as processed,
                COALESCE(SUM(compressed_bytes), 0) as compressed
            FROM backup_tasks
            WHERE is_template = false 
              AND LOWER(status::text)=LOWER($1)
              AND compressed_bytes > 0
            """,
            BackupTaskStatus.COMPLETED.value
        )
        if compression_row and compression_row["processed"] > 0:
            compression_ratio = float(compression_row["compressed"]) / float(compression_row["processed"])
        else:
            compression_ratio = 0.65  # 默认值
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "success_rate": round(success_rate, 2),
            "total_data_backed_up": total_data_backed_up,
            "compression_ratio": round(compression_ratio, 2),
            "average_task_duration": avg_duration,
            "recent_24h": {
                "total_tasks": recent_total,
                "completed_tasks": recent_completed,
                "failed_tasks": recent_failed,
                "data_backed_up": recent_data
            }
        }


async def _get_backup_statistics_sqlite() -> Dict[str, Any]:
    """获取备份统计信息（SQLite 版本）"""
    async with get_sqlite_connection() as conn:
        # 总任务数（只查询非模板任务）
        cursor = await conn.execute("SELECT COUNT(*) FROM backup_tasks WHERE is_template = 0")
        row = await cursor.fetchone()
        total_tasks = row[0] if row else 0
        
        # 计划任务中的备份任务数量
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM scheduled_tasks
            WHERE LOWER(action_type) = LOWER(?)
        """, (TaskActionType.BACKUP.value,))
        row = await cursor.fetchone()
        sched_total = row[0] if row else 0
        total_tasks += sched_total
        
        # 按状态统计
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        completed_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.FAILED.value,))
        row = await cursor.fetchone()
        failed_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.RUNNING.value,))
        row = await cursor.fetchone()
        running_tasks = row[0] if row else 0
        
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.PENDING.value,))
        row = await cursor.fetchone()
        pending_tasks = row[0] if row else 0
        pending_tasks += sched_total
        
        # 成功率
        success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
        
        # 总备份数据量
        cursor = await conn.execute("""
            SELECT COALESCE(SUM(processed_bytes), 0) FROM backup_tasks
            WHERE is_template = 0 AND LOWER(status) = LOWER(?)
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        total_data_backed_up = row[0] if row else 0
        
        # 最近24小时统计
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        cursor = await conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN LOWER(status) = LOWER(?) THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN LOWER(status) = LOWER(?) THEN 1 ELSE 0 END) as failed,
                COALESCE(SUM(processed_bytes), 0) as data
            FROM backup_tasks
            WHERE is_template = 0 AND created_at >= ?
        """, (BackupTaskStatus.COMPLETED.value, BackupTaskStatus.FAILED.value, twenty_four_hours_ago))
        row = await cursor.fetchone()
        
        recent_total = row[0] if row else 0
        recent_completed = row[1] if row else 0
        recent_failed = row[2] if row else 0
        recent_data = row[3] if row else 0
        
        # 平均任务时长（使用秒数计算）
        cursor = await conn.execute("""
            SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400) as avg_duration
            FROM backup_tasks
            WHERE is_template = 0 
              AND LOWER(status) = LOWER(?)
              AND completed_at IS NOT NULL
              AND started_at IS NOT NULL
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        avg_duration = int(row[0]) if row and row[0] else 3600
        
        # 压缩比
        cursor = await conn.execute("""
            SELECT 
                COALESCE(SUM(processed_bytes), 0) as total_bytes,
                COALESCE(SUM(compressed_bytes), 0) as compressed_bytes
            FROM backup_tasks
            WHERE is_template = 0 
              AND LOWER(status) = LOWER(?)
              AND compressed_bytes > 0
        """, (BackupTaskStatus.COMPLETED.value,))
        row = await cursor.fetchone()
        
        if row and row[0] and row[0] > 0:
            compression_ratio = float(row[1] or 0) / float(row[0])
        else:
            compression_ratio = 0.65  # 默认值
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "success_rate": round(success_rate, 2),
            "total_data_backed_up": total_data_backed_up or 0,
            "compression_ratio": round(compression_ratio, 2),
            "average_task_duration": avg_duration,
            "recent_24h": {
                "total_tasks": recent_total,
                "completed_tasks": recent_completed,
                "failed_tasks": recent_failed,
                "data_backed_up": recent_data or 0
            }
        }


async def _get_backup_statistics_redis() -> Dict[str, Any]:
    """获取备份统计信息（Redis 版本）"""
    try:
        from backup.redis_backup_db import (
            KEY_PREFIX_BACKUP_TASK, KEY_INDEX_BACKUP_TASKS,
            _get_redis_key, _parse_datetime_value, _ensure_list
        )
        from config.redis_db import get_redis_client
        from utils.scheduler.redis_task_storage import KEY_INDEX_SCHEDULED_TASKS, KEY_PREFIX_SCHEDULED_TASK
        
        redis = await get_redis_client()
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        
        # 获取所有备份任务ID
        task_ids_bytes = await redis.smembers(KEY_INDEX_BACKUP_TASKS)
        task_ids = [int(tid if isinstance(tid, str) else (tid.decode('utf-8') if isinstance(tid, bytes) else str(tid))) 
                   for tid in task_ids_bytes]
        
        # 统计变量
        total_tasks = 0
        completed_tasks = 0
        failed_tasks = 0
        running_tasks = 0
        pending_tasks = 0
        total_data_backed_up = 0
        total_processed_bytes = 0
        total_compressed_bytes = 0
        completed_durations = []  # 用于计算平均任务时长
        
        # 最近24小时统计
        recent_total = 0
        recent_completed = 0
        recent_failed = 0
        recent_data = 0
        
        # 遍历所有备份任务（非模板）
        for task_id in task_ids:
            task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
            task_data = await redis.hgetall(task_key)
            
            if not task_data:
                continue
            
            # 转换为字典
            task_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                        v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                        for k, v in task_data.items()}
            
            # 只统计非模板任务
            is_template = task_dict.get('is_template', '0') == '1'
            if is_template:
                continue
            
            total_tasks += 1
            task_status = task_dict.get('status', 'pending').lower()
            
            # 按状态统计
            if task_status == 'completed':
                completed_tasks += 1
                processed_bytes = int(task_dict.get('processed_bytes', 0) or 0)
                total_data_backed_up += processed_bytes
                
                # 计算任务时长
                started_at_str = task_dict.get('started_at')
                completed_at_str = task_dict.get('completed_at')
                if started_at_str and completed_at_str:
                    started_at = _parse_datetime_value(started_at_str)
                    completed_at = _parse_datetime_value(completed_at_str)
                    if started_at and completed_at:
                        duration = (completed_at - started_at).total_seconds()
                        if duration > 0:
                            completed_durations.append(duration)
                
                # 统计压缩数据
                compressed_bytes = int(task_dict.get('compressed_bytes', 0) or 0)
                if compressed_bytes > 0:
                    total_processed_bytes += processed_bytes
                    total_compressed_bytes += compressed_bytes
                    
            elif task_status == 'failed':
                failed_tasks += 1
            elif task_status == 'running':
                running_tasks += 1
            elif task_status == 'pending':
                pending_tasks += 1
            
            # 最近24小时统计
            created_at_str = task_dict.get('created_at')
            if created_at_str:
                created_at = _parse_datetime_value(created_at_str)
                if created_at and created_at >= twenty_four_hours_ago:
                    recent_total += 1
                    processed_bytes = int(task_dict.get('processed_bytes', 0) or 0)
                    
                    if task_status == 'completed':
                        recent_completed += 1
                        recent_data += processed_bytes
                    elif task_status == 'failed':
                        recent_failed += 1
        
        # 获取计划任务中的备份任务数量
        sched_ids_bytes = await redis.smembers(KEY_INDEX_SCHEDULED_TASKS)
        sched_count = 0
        for sched_id_bytes in sched_ids_bytes:
            sched_id_str = sched_id_bytes if isinstance(sched_id_bytes, str) else (sched_id_bytes.decode('utf-8') if isinstance(sched_id_bytes, bytes) else str(sched_id_bytes))
            sched_id = int(sched_id_str)
            sched_key = f"{KEY_PREFIX_SCHEDULED_TASK}:{sched_id}"
            sched_data = await redis.hgetall(sched_key)
            
            if not sched_data:
                continue
            
            # 转换为字典
            sched_dict = {k if isinstance(k, str) else k.decode('utf-8'): 
                         v if isinstance(v, str) else (v.decode('utf-8') if isinstance(v, bytes) else str(v))
                         for k, v in sched_data.items()}
            
            # 检查action_type是否为BACKUP
            action_type = sched_dict.get('action_type', '').lower()
            if action_type == 'backup':
                enabled = sched_dict.get('enabled', '1') == '1'
                if enabled:
                    sched_count += 1
        
        # 将计划任务计入总任务数和pending任务数
        total_tasks += sched_count
        pending_tasks += sched_count
        
        # 计算成功率
        success_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0.0
        
        # 计算平均任务时长（秒）
        if completed_durations:
            avg_duration = int(sum(completed_durations) / len(completed_durations))
        else:
            avg_duration = 3600  # 默认值
        
        # 计算压缩比
        if total_processed_bytes > 0:
            compression_ratio = float(total_compressed_bytes) / float(total_processed_bytes)
        else:
            compression_ratio = 0.65  # 默认值
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "failed_tasks": failed_tasks,
            "running_tasks": running_tasks,
            "pending_tasks": pending_tasks,
            "success_rate": round(success_rate, 2),
            "total_data_backed_up": total_data_backed_up,
            "compression_ratio": round(compression_ratio, 2),
            "average_task_duration": avg_duration,
            "recent_24h": {
                "total_tasks": recent_total,
                "completed_tasks": recent_completed,
                "failed_tasks": recent_failed,
                "data_backed_up": recent_data
            }
        }
        
    except Exception as e:
        logger.error(f"[Redis模式] 获取备份统计信息失败: {str(e)}", exc_info=True)
        # 返回默认值，避免影响前端展示
        return {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "running_tasks": 0,
            "pending_tasks": 0,
            "success_rate": 0.0,
            "total_data_backed_up": 0,
            "compression_ratio": 0.65,
            "average_task_duration": 3600,
            "recent_24h": {
                "total_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0,
                "data_backed_up": 0
            }
        }

