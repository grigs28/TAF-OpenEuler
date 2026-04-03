#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - statistics
System Management API - statistics
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/statistics")
async def get_system_statistics(request: Request):
    """获取系统统计信息（从数据库查询真实数据）"""
    try:
        system = request.app.state.system
        
        # 1. 备份任务统计
        backup_tasks_stats = await _get_backup_tasks_statistics()
        
        # 2. 磁带库存统计
        tape_inventory_stats = await _get_tape_inventory_statistics()
        
        # 3. 存储统计
        storage_stats = await _get_storage_statistics()
        
        # 4. 最近备份活动
        recent_backups = await _get_recent_backups(limit=5)
        
        # 5. 存储使用趋势（最近12个月按月统计）
        storage_trend = await _get_storage_trend(days=365)
        
        # 6. 成功率统计
        success_rate_stats = await _get_success_rate_statistics()
        
        # 7. 系统运行时间
        uptime = await _get_system_uptime()
        
        return {
            "uptime": uptime,
            "backup_tasks": backup_tasks_stats,
            "tape_inventory": tape_inventory_stats,
            "storage": storage_stats,
            "recent_backups": recent_backups,
            "storage_trend": storage_trend,
            "success_rate": success_rate_stats
        }

    except Exception as e:
        logger.error(f"获取系统统计信息失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


async def _get_backup_tasks_statistics() -> Dict[str, Any]:
    """获取备份任务统计"""
    try:
        async with get_opengauss_connection() as conn:
            # 统计运行中的任务
            running_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('RUNNING') AND is_template = FALSE"
            ) or 0

            # 统计已完成的任务
            completed_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('COMPLETED') AND is_template = FALSE"
            ) or 0

            # 统计失败的任务
            failed_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE LOWER(status::text) = LOWER('FAILED') AND is_template = FALSE"
            ) or 0

            # 总任务数（执行记录，不包括模板）
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE"
            ) or 0

            return {
                "total": total_count,
                "running": running_count,
                "completed": completed_count,
                "failed": failed_count
            }
    except Exception as e:
        logger.error(f"获取备份任务统计失败: {str(e)}")
        return {"total": 0, "running": 0, "completed": 0, "failed": 0}


async def _get_tape_inventory_statistics() -> Dict[str, Any]:
    """获取磁带库存统计"""
    try:
        async with get_opengauss_connection() as conn:
            total_count = await conn.fetchval("SELECT COUNT(*) FROM tape_cartridges") or 0
            available_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('AVAILABLE')"
            ) or 0
            in_use_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('IN_USE')"
            ) or 0
            expired_count = await conn.fetchval(
                "SELECT COUNT(*) FROM tape_cartridges WHERE LOWER(status::text) = LOWER('EXPIRED')"
            ) or 0

            # 计算在线和离线数量（可用+使用中=在线，其他=离线）
            online_count = available_count + in_use_count
            offline_count = total_count - online_count

            return {
                "total": total_count,
                "available": available_count,
                "in_use": in_use_count,
                "expired": expired_count,
                "online": online_count,
                "offline": offline_count
            }
    except Exception as e:
        logger.error(f"获取磁带库存统计失败: {str(e)}")
        return {"total": 0, "available": 0, "in_use": 0, "expired": 0, "online": 0, "offline": 0}


async def _get_storage_statistics() -> Dict[str, Any]:
    """获取存储统计"""
    try:
        async with get_opengauss_connection() as conn:
            # 从磁带统计总容量
            tape_result = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(capacity_bytes), 0) as total_capacity,
                    COALESCE(SUM(used_bytes), 0) as used_capacity
                FROM tape_cartridges
                """
            )

            total_capacity = tape_result['total_capacity'] or 0
            used_capacity = tape_result['used_capacity'] or 0

            # 如果磁带used_bytes为0，从备份任务和备份集综合计算已用存储
            if used_capacity == 0:
                # 优先从备份集统计（不限status）
                set_result = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(compressed_bytes), 0) as total_compressed
                    FROM backup_sets
                    """
                )
                compressed_from_sets = set_result['total_compressed'] or 0

                if compressed_from_sets > 0:
                    used_capacity = compressed_from_sets
                else:
                    # 从已执行过的备份任务统计（排除模板和未开始的任务）
                    task_result = await conn.fetchrow(
                        """
                        SELECT COALESCE(SUM(total_bytes), 0) as total_bytes
                        FROM backup_tasks
                        WHERE is_template = FALSE
                          AND started_at IS NOT NULL
                          AND total_bytes > 0
                        """
                    )
                    used_capacity = task_result['total_bytes'] or 0

            # 如果没有磁带容量数据，使用已用存储估算
            if total_capacity == 0:
                total_capacity = used_capacity * 2 if used_capacity > 0 else 0

            usage_percent = (used_capacity / total_capacity * 100) if total_capacity > 0 else 0

            return {
                "total_capacity": total_capacity,
                "used_capacity": used_capacity,
                "usage_percent": round(usage_percent, 1)
            }
    except Exception as e:
        logger.error(f"获取存储统计失败: {str(e)}")
        return {"total_capacity": 0, "used_capacity": 0, "usage_percent": 0.0}


async def _get_recent_backups(limit: int = 5) -> List[Dict[str, Any]]:
    """获取最近备份活动"""
    try:
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, task_name, task_type, status, total_bytes, started_at, completed_at, error_message
                FROM backup_tasks
                WHERE is_template = FALSE
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT $1
                """,
                limit
            )

            backups = []
            for row in rows:
                status = row['status'].value if hasattr(row['status'], 'value') else str(row['status'])
                task_type = row['task_type'].value if hasattr(row['task_type'], 'value') else str(row['task_type'])

                backups.append({
                    "id": row['id'],
                    "task_name": row['task_name'],
                    "task_type": task_type,
                    "status": status,
                    "size_bytes": row['total_bytes'] or 0,
                    "started_at": row['started_at'].isoformat() if row['started_at'] else None,
                    "completed_at": row['completed_at'].isoformat() if row['completed_at'] else None,
                    "error_message": row['error_message']
                })

            return backups
    except Exception as e:
        logger.error(f"获取最近备份活动失败: {str(e)}")
        return []


async def _get_storage_trend(days: int = 365) -> List[Dict[str, Any]]:
    """获取存储使用趋势（按月统计备份量）"""
    try:
        async with get_opengauss_connection() as conn:
            start_date = datetime.now() - timedelta(days=days)

            # 优先从 backup_sets 统计（不限status，按月汇总压缩后大小）
            rows = await conn.fetch(
                """
                SELECT
                    DATE_TRUNC('month', backup_time)::date as backup_month,
                    SUM(compressed_bytes) as monthly_bytes
                FROM backup_sets
                WHERE backup_time >= $1
                GROUP BY DATE_TRUNC('month', backup_time)
                ORDER BY backup_month ASC
                """,
                start_date
            )

            # 如果 backup_sets 没有数据，从 backup_tasks 统计
            if not rows or all((r['monthly_bytes'] or 0) == 0 for r in rows):
                rows = await conn.fetch(
                    """
                    SELECT
                        DATE_TRUNC('month', started_at)::date as backup_month,
                        SUM(total_bytes) as monthly_bytes
                    FROM backup_tasks
                    WHERE is_template = FALSE
                      AND started_at >= $1
                      AND total_bytes > 0
                    GROUP BY DATE_TRUNC('month', started_at)
                    ORDER BY backup_month ASC
                    """,
                    start_date
                )

            trend = []
            for row in rows:
                trend.append({
                    "date": row['backup_month'].isoformat() if isinstance(row['backup_month'], datetime) else str(row['backup_month']),
                    "bytes": row['monthly_bytes'] or 0
                })

            return trend
    except Exception as e:
        logger.error(f"获取存储使用趋势失败: {str(e)}")
        return []


async def _get_success_rate_statistics() -> Dict[str, Any]:
    """获取成功率统计（已完成的任务：completed=成功, failed/cancelled=未成功）"""
    try:
        async with get_opengauss_connection() as conn:
            # 已结束的任务状态（排除 running/pending 等进行中状态）
            finished_statuses = "('completed', 'failed', 'cancelled')"

            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND LOWER(status::text) IN {finished_statuses}"
            ) or 0

            success = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND LOWER(status::text) = 'completed'"
            ) or 0

            # 本月统计
            this_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            this_month_total = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) IN {finished_statuses}
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            this_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) = 'completed'
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            # 上月统计
            last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
            last_month_total = await conn.fetchval(
                f"""
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) IN {finished_statuses}
                  AND started_at >= $1 AND started_at < $2
                """,
                last_month_start, this_month_start
            ) or 0

            last_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND LOWER(status::text) = 'completed'
                  AND started_at >= $1 AND started_at < $2
                """,
                last_month_start, this_month_start
            ) or 0

            success_rate = (success / total * 100) if total > 0 else 0
            this_month_rate = (this_month_success / this_month_total * 100) if this_month_total > 0 else 0
            last_month_rate = (last_month_success / last_month_total * 100) if last_month_total > 0 else 0

            rate_change = this_month_rate - last_month_rate

            return {
                "overall": round(success_rate, 1),
                "this_month": round(this_month_rate, 1),
                "last_month": round(last_month_rate, 1),
                "change": round(rate_change, 1),
                "this_month_count": this_month_success,
                "last_month_count": last_month_success
            }
    except Exception as e:
        logger.error(f"获取成功率统计失败: {str(e)}")
        return {"overall": 0.0, "this_month": 0.0, "last_month": 0.0, "change": 0.0, "this_month_count": 0, "last_month_count": 0}


async def _get_system_uptime() -> int:
    """获取系统运行时间（秒）"""
    try:
        from web.api.system.info import _SYSTEM_START_TIME
        delta = datetime.now() - _SYSTEM_START_TIME
        return int(delta.total_seconds())
    except Exception as e:
        logger.error(f"获取系统运行时间失败: {str(e)}")
        return 0

