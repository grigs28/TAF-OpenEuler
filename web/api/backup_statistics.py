#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份统计 API
Backup Statistics API
"""

import logging
from typing import Dict, Any
from datetime import datetime, timedelta
from models.backup import BackupTaskStatus
from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)


async def get_backup_statistics() -> Dict[str, Any]:
    """获取备份统计信息"""
    try:
        return await _get_backup_statistics_opengauss()
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

