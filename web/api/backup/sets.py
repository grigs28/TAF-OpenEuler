#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 备份集查询
Backup Management API - Backup Sets Query
"""

import logging
from typing import Optional
from fastapi import APIRouter, HTTPException
from utils.scheduler.db_utils import is_redis, is_opengauss, is_sqlite, get_sqlite_connection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/backup-sets")
async def get_backup_sets(
    backup_group: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """获取备份集列表"""
    try:
        if is_redis():
            # Redis 模式：使用 Redis 查询
            from backup.redis_backup_db import list_backup_sets_redis
            return await list_backup_sets_redis(
                backup_group=backup_group,
                limit=limit,
                offset=offset
            )
        elif is_opengauss():
            # openGauss 模式：使用原生 SQL 查询
            from utils.scheduler.db_utils import get_opengauss_connection
            async with get_opengauss_connection() as conn:
                # 构建 WHERE 子句
                where_clauses = []
                params = []
                param_idx = 1
                
                if backup_group:
                    where_clauses.append(f"backup_group = ${param_idx}")
                    params.append(backup_group)
                    param_idx += 1
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 获取总数
                count_sql = f"SELECT COUNT(*) as total FROM backup_sets WHERE {where_sql}"
                count_row = await conn.fetchrow(count_sql, *params)
                total = count_row['total'] if count_row else 0
                
                # 查询备份集
                sql = f"""
                    SELECT set_id, set_name, backup_group, backup_type, backup_time,
                           total_files, total_bytes, tape_id, status
                    FROM backup_sets
                    WHERE {where_sql}
                    ORDER BY backup_time DESC
                    LIMIT ${param_idx} OFFSET ${param_idx + 1}
                """
                params.extend([limit, offset])
                rows = await conn.fetch(sql, *params)
                
                # 转换为响应格式
                sets_list = []
                for row in rows:
                    sets_list.append({
                        "set_id": row['set_id'],
                        "set_name": row['set_name'],
                        "backup_group": row['backup_group'],
                        "backup_type": row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                        "backup_time": row['backup_time'].isoformat() if row['backup_time'] else None,
                        "total_files": row['total_files'] or 0,
                        "total_bytes": row['total_bytes'] or 0,
                        "tape_id": row['tape_id'],
                        "status": row['status'].value if hasattr(row['status'], 'value') else str(row['status']).lower()
                    })
                
                return {
                    "backup_sets": sets_list,
                    "total": total,
                    "limit": limit,
                    "offset": offset
                }
        elif is_sqlite():
            # SQLite 模式：使用原生 SQL 查询
            async with get_sqlite_connection() as conn:
                # 构建 WHERE 子句
                where_clauses = []
                params = []
                
                if backup_group:
                    where_clauses.append("backup_group = ?")
                    params.append(backup_group)
                
                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
                
                # 获取总数
                count_sql = f"SELECT COUNT(*) as total FROM backup_sets WHERE {where_sql}"
                cursor = await conn.execute(count_sql, tuple(params))
                count_row = await cursor.fetchone()
                total = count_row[0] if count_row else 0
                
                # 查询备份集
                sql = f"""
                    SELECT set_id, set_name, backup_group, backup_type, backup_time,
                           total_files, total_bytes, tape_id, status
                    FROM backup_sets
                    WHERE {where_sql}
                    ORDER BY backup_time DESC
                    LIMIT ? OFFSET ?
                """
                params.extend([limit, offset])
                cursor = await conn.execute(sql, tuple(params))
                rows = await cursor.fetchall()
                
                # 获取列名
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # 转换为响应格式
                sets_list = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    sets_list.append({
                        "set_id": row_dict.get('set_id'),
                        "set_name": row_dict.get('set_name'),
                        "backup_group": row_dict.get('backup_group'),
                        "backup_type": str(row_dict.get('backup_type', 'full')).lower(),
                        "backup_time": row_dict.get('backup_time').isoformat() if row_dict.get('backup_time') and hasattr(row_dict.get('backup_time'), 'isoformat') else (str(row_dict.get('backup_time')) if row_dict.get('backup_time') else None),
                        "total_files": row_dict.get('total_files') or 0,
                        "total_bytes": row_dict.get('total_bytes') or 0,
                        "tape_id": row_dict.get('tape_id'),
                        "status": str(row_dict.get('status', 'active')).lower()
                    })
                
                return {
                    "backup_sets": sets_list,
                    "total": total,
                    "limit": limit,
                    "offset": offset
                }
        else:
            # 未知数据库类型，返回空列表
            logger.warning("未知的数据库类型，返回空备份集列表")
            return {
                "backup_sets": [],
                "total": 0,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"获取备份集列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/backup-sets/{set_id}")
async def delete_backup_set(set_id: str):
    """删除备份集（包括关联的备份文件）"""
    try:
        logger.info(f"开始删除备份集: {set_id}")
        
        if is_redis():
            # Redis 模式：使用 Redis 删除
            from backup.redis_backup_db import delete_backup_set_redis
            success = await delete_backup_set_redis(set_id)
            if success:
                logger.info(f"已删除备份集: {set_id}")
                return {"success": True, "message": f"备份集 {set_id} 已删除"}
            else:
                raise HTTPException(status_code=404, detail=f"备份集 {set_id} 不存在")
        elif is_opengauss():
            # openGauss 模式：使用原生 SQL 删除
            from utils.scheduler.db_utils import get_opengauss_connection
            
            async with get_opengauss_connection() as conn:
                # 先查询备份集是否存在
                set_row = await conn.fetchrow(
                    "SELECT id FROM backup_sets WHERE set_id = $1",
                    set_id
                )
                
                if not set_row:
                    raise HTTPException(status_code=404, detail=f"备份集 {set_id} 不存在")
                
                backup_set_id = set_row['id']

                # 多表方案：根据 backup_set_id 决定物理表名
                from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                table_name = await get_backup_files_table_by_set_id(conn, backup_set_id)
                
                # 删除关联的备份文件
                files_result = await conn.execute(
                    f"DELETE FROM {table_name} WHERE backup_set_id = $1",
                    backup_set_id
                )
                files_deleted = files_result if hasattr(files_result, '__int__') else 0
                logger.info(f"已删除备份集 {set_id} 的 {files_deleted} 个文件记录")
                
                # 删除备份集
                set_result = await conn.execute(
                    "DELETE FROM backup_sets WHERE id = $1",
                    backup_set_id
                )
                sets_deleted = set_result if hasattr(set_result, '__int__') else 0
                
                if sets_deleted > 0:
                    logger.info(f"已删除备份集: {set_id}（文件数: {files_deleted}）")
                    return {
                        "success": True,
                        "message": f"备份集 {set_id} 已删除",
                        "files_deleted": files_deleted
                    }
                else:
                    raise HTTPException(status_code=500, detail="删除备份集失败")
        elif is_sqlite():
            # SQLite 模式：使用原生 SQL 删除
            
            async with get_sqlite_connection() as conn:
                # 先查询备份集是否存在
                cursor = await conn.execute(
                    "SELECT id FROM backup_sets WHERE set_id = ?",
                    (set_id,)
                )
                set_row = await cursor.fetchone()
                
                if not set_row:
                    raise HTTPException(status_code=404, detail=f"备份集 {set_id} 不存在")
                
                backup_set_id = set_row[0]
                
                # 删除关联的备份文件
                files_cursor = await conn.execute(
                    "DELETE FROM backup_files WHERE backup_set_id = ?",
                    (backup_set_id,)
                )
                files_deleted = files_cursor.rowcount if hasattr(files_cursor, 'rowcount') else 0
                logger.info(f"已删除备份集 {set_id} 的 {files_deleted} 个文件记录")
                
                # 删除备份集
                set_cursor = await conn.execute(
                    "DELETE FROM backup_sets WHERE id = ?",
                    (backup_set_id,)
                )
                sets_deleted = set_cursor.rowcount if hasattr(set_cursor, 'rowcount') else 0
                
                if sets_deleted > 0:
                    logger.info(f"已删除备份集: {set_id}（文件数: {files_deleted}）")
                    return {
                        "success": True,
                        "message": f"备份集 {set_id} 已删除",
                        "files_deleted": files_deleted
                    }
                else:
                    raise HTTPException(status_code=500, detail="删除备份集失败")
        else:
            raise HTTPException(status_code=500, detail="未知的数据库类型")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除备份集失败: {set_id}, 错误: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"删除备份集失败: {str(e)}")
