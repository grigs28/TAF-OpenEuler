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
        
        # 5. 存储使用趋势（最近30天）
        storage_trend = await _get_storage_trend(days=30)
        
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
            # 从备份集统计存储使用情况
            result = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(total_bytes), 0) as total_bytes,
                    COALESCE(SUM(compressed_bytes), 0) as compressed_bytes
                FROM backup_sets
                WHERE LOWER(status::text) = LOWER('ACTIVE')
                """
            )

            total_bytes = result['total_bytes'] or 0
            compressed_bytes = result['compressed_bytes'] or 0

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

            # 如果没有磁带数据，使用备份集数据
            if total_capacity == 0:
                total_capacity = total_bytes * 2  # 估算总容量
                used_capacity = compressed_bytes if compressed_bytes > 0 else total_bytes

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


async def _get_storage_trend(days: int = 30) -> List[Dict[str, Any]]:
    """获取存储使用趋势"""
    try:
        async with get_opengauss_connection() as conn:
            # 按日期分组统计每天的存储使用量
            start_date = datetime.now() - timedelta(days=days)
            rows = await conn.fetch(
                """
                SELECT
                    backup_time::date as backup_date,
                    SUM(compressed_bytes) as daily_bytes
                FROM backup_sets
                WHERE backup_time >= $1
                  AND LOWER(status::text) = LOWER('ACTIVE')
                GROUP BY backup_time::date
                ORDER BY backup_date ASC
                """,
                start_date
            )

            trend = []
            for row in rows:
                trend.append({
                    "date": row['backup_date'].isoformat() if isinstance(row['backup_date'], datetime) else str(row['backup_date']),
                    "bytes": row['daily_bytes'] or 0
                })

            return trend
    except Exception as e:
        logger.error(f"获取存储使用趋势失败: {str(e)}")
        return []


async def _get_success_rate_statistics() -> Dict[str, Any]:
    """获取成功率统计"""
    try:
        async with get_opengauss_connection() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND status::text IN ('completed', 'failed')"
            ) or 0

            success = await conn.fetchval(
                "SELECT COUNT(*) FROM backup_tasks WHERE is_template = FALSE AND status::text = 'completed'"
            ) or 0

            # 本月统计
            this_month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            this_month_total = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND status::text IN ('completed', 'failed')
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            this_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND status::text = 'completed'
                  AND started_at >= $1
                """,
                this_month_start
            ) or 0

            # 上月统计
            last_month_start = (this_month_start - timedelta(days=1)).replace(day=1)
            last_month_total = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND status::text IN ('completed', 'failed')
                  AND started_at >= $1 AND started_at < $2
                """,
                last_month_start, this_month_start
            ) or 0

            last_month_success = await conn.fetchval(
                """
                SELECT COUNT(*) FROM backup_tasks
                WHERE is_template = FALSE
                  AND status::text = 'completed'
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
        # 这里可以从系统启动时间计算，暂时返回固定值
        # 实际应该从系统启动时间或数据库记录中获取
        return 86400  # 默认1天
    except Exception as e:
        logger.error(f"获取系统运行时间失败: {str(e)}")
        return 0

