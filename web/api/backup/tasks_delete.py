#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务删除
Backup Management API - Task Delete
"""

import logging
import asyncio
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection, is_sqlite, get_sqlite_connection
from models.system_log import OperationType
from utils.log_utils import log_operation
from .utils import _normalize_status_value

logger = logging.getLogger(__name__)
router = APIRouter()


async def _opengauss_query_with_retry(conn, query_func, *args, max_retries=3, operation_name="查询"):
    """
    openGauss 查询重试包装器（使用原生 openGauss SQL）
    
    Args:
        conn: openGauss 连接对象
        query_func: 查询函数（如 conn.fetchval, conn.fetchrow, conn.fetch）
        *args: 查询函数的参数
        max_retries: 最大重试次数
        operation_name: 操作名称（用于日志）
    
    Returns:
        查询结果
    """
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            result = await query_func(*args)
            return result
        except (AssertionError, ConnectionError, OSError) as buffer_error:
            # openGauss 缓冲区错误或连接错误，进行重试
            retry_count += 1
            error_type = type(buffer_error).__name__
            error_msg = str(buffer_error) if buffer_error else "未知错误"
            
            if retry_count >= max_retries:
                logger.error(
                    f"[openGauss查询重试] {operation_name}失败（{error_type}）: {error_msg}，"
                    f"已重试 {max_retries} 次，放弃重试",
                    exc_info=True
                )
                raise
            else:
                logger.warning(
                    f"[openGauss查询重试] {operation_name}失败（{error_type}），"
                    f"将在 {1.0 * retry_count} 秒后重试（第 {retry_count + 1}/{max_retries} 次）..."
                )
                await asyncio.sleep(1.0 * retry_count)  # 递增延迟重试
        except Exception as e:
            # 其他错误，不重试（可能是数据问题或语法错误）
            logger.error(f"[openGauss查询] {operation_name}失败: {str(e)}", exc_info=True)
            raise
    
    # 理论上不会到达这里
    raise Exception(f"{operation_name}失败：已达到最大重试次数")


@router.delete("/tasks/{task_id}")
async def delete_backup_task(task_id: int, http_request: Request):
    """删除备份任务（模板或执行记录）"""
    start_time = datetime.now()
    
    try:
        # 获取任务信息
        task_name = None
        is_template = None
        task_status = None
        
        if is_redis():
            # Redis 模式：使用 Redis 查询
            from backup.redis_backup_db import KEY_PREFIX_BACKUP_TASK, _get_redis_key
            from config.redis_db import get_redis_client
            redis = await get_redis_client()
            task_key = _get_redis_key(KEY_PREFIX_BACKUP_TASK, task_id)
            task_data = await redis.hgetall(task_key)
            if not task_data:
                raise HTTPException(status_code=404, detail="备份任务不存在")
            # Redis 客户端设置了 decode_responses=True，所以返回的是字符串字典
            task_name = task_data.get('task_name', '')
            is_template = task_data.get('is_template', '0') == '1'
            task_status = task_data.get('status', 'pending')
        elif is_opengauss():
            # 使用原生 openGauss SQL（通过连接池）
            async with get_opengauss_connection() as conn:
                row = await _opengauss_query_with_retry(
                    conn,
                    conn.fetchrow,
                    "SELECT task_name, is_template, status FROM backup_tasks WHERE id = $1",
                    task_id,
                    operation_name="查询备份任务信息"
                )
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                task_name = row["task_name"]
                is_template = row["is_template"]
                task_status = row["status"].value if hasattr(row["status"], "value") else str(row["status"])
        elif is_sqlite():
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute(
                    "SELECT task_name, is_template, status FROM backup_tasks WHERE id = ?",
                    (task_id,)
                )
                row = await cursor.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                task_name = row[0]
                is_template = bool(row[1]) if row[1] is not None else False
                # 处理状态值
                status_raw = row[2]
                if isinstance(status_raw, str):
                    task_status = status_raw
                else:
                    task_status = _normalize_status_value(status_raw)
        else:
            raise HTTPException(status_code=500, detail="不支持的数据库类型")
        
        if is_redis():
            # Redis 模式：使用 Redis 删除
            from backup.redis_backup_db import delete_backup_task_redis
            deleted = await delete_backup_task_redis(task_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="备份任务不存在或已被删除")
            
            # 记录操作日志
            client_ip = http_request.client.host if http_request.client else None
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            resource_type_name = "备份任务模板" if is_template else "备份任务执行记录"
            await log_operation(
                operation_type=OperationType.DELETE,
                resource_type="backup",
                resource_id=str(task_id),
                resource_name=f"{resource_type_name}: {task_name}",
                operation_name=f"删除{resource_type_name}",
                operation_description=f"删除{resource_type_name}: {task_name} (状态: {task_status})",
                category="backup",
                success=True,
                result_message=f"{resource_type_name}已删除",
                ip_address=client_ip,
                request_method="DELETE",
                request_url=str(http_request.url),
                duration_ms=duration_ms
            )
            
            return {"success": True, "message": f"{resource_type_name}已删除"}
        elif is_opengauss():
            # 使用原生 openGauss SQL 删除（使用连接池）
            async with get_opengauss_connection() as conn:
                # 先检查是否有外键约束（backup_sets表可能引用此任务）
                # 删除顺序：backup_files -> backup_sets -> backup_tasks
                # 使用 fetchrow 代替 fetchval，避免 openGauss 缓冲区错误
                # 使用原生 openGauss SQL
                backup_sets_row = await _opengauss_query_with_retry(
                    conn,
                    conn.fetchrow,
                    "SELECT COUNT(*) as count FROM backup_sets WHERE backup_task_id = $1",
                    task_id,
                    operation_name="查询备份集数量"
                )
                backup_sets_count = backup_sets_row['count'] if backup_sets_row else 0
                
                if backup_sets_count and backup_sets_count > 0:
                    # 获取所有关联的备份集ID
                    backup_sets_rows = await _opengauss_query_with_retry(
                        conn,
                        conn.fetch,
                        "SELECT id FROM backup_sets WHERE backup_task_id = $1",
                        task_id,
                        operation_name="查询备份集ID列表"
                    )
                    
                    backup_set_ids = [row['id'] for row in backup_sets_rows] if backup_sets_rows else []
                    
                    # 先删除关联的备份文件（backup_files表引用backup_sets）
                    if backup_set_ids:
                        total_files_deleted = 0
                        for backup_set_id in backup_set_ids:
                            # 多表方案：根据 backup_set_id 决定物理表名
                            from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                            table_name = await get_backup_files_table_by_set_id(conn, backup_set_id)

                            # 尝试查询文件数量（用于日志），如果失败则直接删除
                            files_count = 0
                            query_success = False
                            try:
                                # 使用原生 openGauss SQL 查询文件数量
                                count_row = await asyncio.wait_for(
                                    conn.fetchrow(
                                        f"SELECT COUNT(*) as count FROM {table_name} WHERE backup_set_id = $1",
                                        backup_set_id
                                    ),
                                    timeout=30.0
                                )
                                files_count = count_row['count'] if count_row else 0
                                query_success = True
                            except (AssertionError, asyncio.TimeoutError, Exception) as count_error:
                                # 查询失败，记录警告但继续执行删除
                                logger.warning(
                                    f"查询备份集 {backup_set_id} 的文件数量失败: {str(count_error)}，"
                                    f"将直接执行删除操作（不统计文件数，表={table_name}）"
                                )
                            
                            # 执行删除操作（无论查询是否成功都执行）
                            # execute 方法会自动创建新事务，即使前面的查询失败也不会影响
                            try:
                                await conn.execute(
                                    f"DELETE FROM {table_name} WHERE backup_set_id = $1",
                                    backup_set_id
                                )
                                
                                # 只有查询成功时才统计文件数
                                if query_success and files_count > 0:
                                    total_files_deleted += files_count
                                    logger.debug(f"已删除备份集 {backup_set_id} 的 {files_count} 个文件记录（表={table_name}）")
                                else:
                                    logger.debug(f"已删除备份集 {backup_set_id} 的文件记录（数量未知，表={table_name}）")
                            except Exception as delete_error:
                                error_msg = str(delete_error)
                                # 如果表不存在，记录警告但不报错（可能是数据库未初始化）
                                if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                                    logger.warning(
                                        f"备份集 {backup_set_id} 的文件表不存在，跳过删除（可能是数据库未初始化）: {error_msg}"
                                    )
                                else:
                                    logger.error(
                                        f"删除备份集 {backup_set_id} 的文件记录失败: {error_msg}",
                                        exc_info=True
                                    )
                                # 删除失败不影响流程，继续处理其他备份集
                                continue
                        
                        if total_files_deleted > 0:
                            logger.debug(f"已删除 {total_files_deleted} 个备份文件记录")
                    
                    # 再删除备份集
                    try:
                        await conn.execute(
                            "DELETE FROM backup_sets WHERE backup_task_id = $1",
                            task_id
                        )
                        logger.debug(f"已删除 {backup_sets_count} 个关联的备份集")
                    except Exception as delete_sets_error:
                        error_msg = str(delete_sets_error)
                        # 如果表不存在，记录警告但不报错
                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                            logger.warning(
                                f"备份集表不存在，跳过删除（可能是数据库未初始化）: {error_msg}"
                            )
                        else:
                            logger.error(
                                f"删除备份集失败: {error_msg}",
                                exc_info=True
                            )
                            raise
                
                # 检查是否有执行记录引用此模板（template_id外键）
                if is_template:
                    # 使用 fetchrow 代替 fetchval，避免 openGauss 缓冲区错误
                    # 使用原生 openGauss SQL
                    child_tasks_row = await _opengauss_query_with_retry(
                        conn,
                        conn.fetchrow,
                        "SELECT COUNT(*) as count FROM backup_tasks WHERE template_id = $1",
                        task_id,
                        operation_name="查询子任务数量"
                    )
                    child_tasks_count = child_tasks_row['count'] if child_tasks_row else 0
                    if child_tasks_count and child_tasks_count > 0:
                        # 如果有执行记录引用此模板，先删除执行记录（递归处理）
                        logger.info(f"发现 {child_tasks_count} 个执行记录引用此模板，将一并删除")
                        child_tasks_rows = await _opengauss_query_with_retry(
                            conn,
                            conn.fetch,
                            "SELECT id FROM backup_tasks WHERE template_id = $1",
                            task_id,
                            operation_name="查询子任务ID列表"
                        )
                        child_task_ids = [row['id'] for row in child_tasks_rows] if child_tasks_rows else []
                        for child_task_id in child_task_ids:
                            # 获取子任务的备份集
                            backup_sets_for_child = await _opengauss_query_with_retry(
                                conn,
                                conn.fetch,
                                "SELECT id FROM backup_sets WHERE backup_task_id = $1",
                                child_task_id,
                                operation_name=f"查询子任务 {child_task_id} 的备份集"
                            )
                            # 先删除备份文件（通过备份集）
                            if backup_sets_for_child:
                                for bs_row in backup_sets_for_child:
                                    try:
                                        await conn.execute(
                                            "DELETE FROM backup_files WHERE backup_set_id = $1",
                                            bs_row['id']
                                        )
                                    except Exception as delete_files_error:
                                        error_msg = str(delete_files_error)
                                        # 如果表不存在，记录警告但继续
                                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                                            logger.warning(
                                                f"备份文件表不存在，跳过删除（可能是数据库未初始化）: {error_msg}"
                                            )
                                        else:
                                            logger.error(f"删除备份文件失败: {error_msg}")
                                            raise
                                # 再删除备份集
                                try:
                                    await conn.execute(
                                        "DELETE FROM backup_sets WHERE backup_task_id = $1",
                                        child_task_id
                                    )
                                except Exception as delete_sets_error:
                                    error_msg = str(delete_sets_error)
                                    if "does not exist" in error_msg.lower() or "relation" in error_msg.lower():
                                        logger.warning(
                                            f"备份集表不存在，跳过删除（可能是数据库未初始化）: {error_msg}"
                                        )
                                    else:
                                        logger.error(f"删除备份集失败: {error_msg}")
                                        raise
                            # 最后删除执行记录
                            await conn.execute(
                                "DELETE FROM backup_tasks WHERE id = $1",
                                child_task_id
                            )
                        logger.debug(f"已删除 {child_tasks_count} 个关联的执行记录")
                
                # 执行删除操作
                # asyncpg的execute返回字符串格式，如 "DELETE 1" 或 "DELETE 0"
                try:
                    result = await conn.execute(
                        "DELETE FROM backup_tasks WHERE id = $1",
                        task_id
                    )
                    
                    # 解析删除结果
                    # psycopg3 返回整数（受影响的行数），asyncpg 返回字符串（如 "DELETE 1"）
                    deleted_count = 0
                    if isinstance(result, str):
                        if result.startswith("DELETE"):
                            try:
                                deleted_count = int(result.split()[-1]) if len(result.split()) > 1 else 0
                            except:
                                deleted_count = 0
                        else:
                            # 可能返回其他格式，尝试解析
                            logger.warning(f"删除操作返回未知格式: {result}")
                    elif isinstance(result, int):
                        # psycopg3 返回整数（受影响的行数）
                        deleted_count = result
                    else:
                        # 如果不是字符串或整数，尝试其他方式
                        logger.warning(f"删除操作返回非预期类型: {type(result)}, 值: {result}")
                    
                    # 检查是否真的删除了记录
                    if deleted_count == 0:
                        # 再次查询确认任务是否存在
                        check_row = await _opengauss_query_with_retry(
                            conn,
                            conn.fetchrow,
                            "SELECT id FROM backup_tasks WHERE id = $1",
                            task_id,
                            operation_name="验证任务是否已删除"
                        )
                        if check_row:
                            raise HTTPException(status_code=400, detail="删除失败：可能存在外键约束或其他限制")
                        else:
                            raise HTTPException(status_code=404, detail="备份任务不存在或已被删除")
                    
                    # 提交事务（psycopg3 需要显式提交，否则连接释放时会回滚，参考 SQLite 模式的 commit）
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'commit'):
                        try:
                            await actual_conn.commit()
                            logger.debug(f"备份任务 {task_id} 删除事务已提交")
                        except Exception as commit_err:
                            logger.warning(f"提交删除事务失败（可能已自动提交）: {commit_err}")
                    
                except HTTPException:
                    raise
                except Exception as db_error:
                    error_msg = str(db_error)
                    logger.error(f"删除备份任务时数据库错误: {error_msg}")
                    # 检查是否是外键约束错误
                    if "foreign key" in error_msg.lower() or "constraint" in error_msg.lower():
                        raise HTTPException(status_code=400, detail=f"删除失败：任务存在关联数据，请先删除关联的备份集")
                    else:
                        raise HTTPException(status_code=400, detail=f"删除失败：{error_msg}")
                
                # 记录操作日志
                client_ip = http_request.client.host if http_request.client else None
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                resource_type_name = "备份任务模板" if is_template else "备份任务执行记录"
                await log_operation(
                    operation_type=OperationType.DELETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=f"{resource_type_name}: {task_name}",
                    operation_name=f"删除{resource_type_name}",
                    operation_description=f"删除{resource_type_name}: {task_name} (状态: {task_status})",
                    category="backup",
                    success=True,
                    result_message=f"{resource_type_name}已删除",
                    ip_address=client_ip,
                    request_method="DELETE",
                    request_url=str(http_request.url),
                    duration_ms=duration_ms
                )
                
                return {"success": True, "message": f"{resource_type_name}已删除"}
        elif is_sqlite():
            # 使用原生SQL删除（SQLite）
            async with get_sqlite_connection() as conn:
                # 查找关联的备份集ID
                cursor = await conn.execute(
                    "SELECT id FROM backup_sets WHERE backup_task_id = ?",
                    (task_id,)
                )
                backup_set_ids = [row[0] for row in await cursor.fetchall()]
                
                # 删除备份集中的所有文件
                if backup_set_ids:
                    placeholders = ','.join('?' * len(backup_set_ids))
                    await conn.execute(f"DELETE FROM backup_files WHERE backup_set_id IN ({placeholders})", backup_set_ids)
                    await conn.execute(f"DELETE FROM backup_sets WHERE id IN ({placeholders})", backup_set_ids)
                
                # 删除备份任务
                await conn.execute("DELETE FROM backup_tasks WHERE id = ?", (task_id,))
                await conn.commit()
                
                # 记录操作日志
                client_ip = http_request.client.host if http_request.client else None
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                resource_type_name = "备份任务模板" if is_template else "备份任务执行记录"
                await log_operation(
                    operation_type=OperationType.DELETE,
                    resource_type="backup",
                    resource_id=str(task_id),
                    resource_name=f"{resource_type_name}: {task_name}",
                    operation_name=f"删除{resource_type_name}",
                    operation_description=f"删除{resource_type_name}: {task_name} (状态: {task_status})",
                    category="backup",
                    success=True,
                    result_message=f"{resource_type_name}已删除",
                    ip_address=client_ip,
                    request_method="DELETE",
                    request_url=str(http_request.url),
                    duration_ms=duration_ms
                )
                
                return {"success": True, "message": f"{resource_type_name}已删除"}
    
    except HTTPException:
        raise
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = http_request.client.host if http_request.client else None
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="backup",
            resource_id=str(task_id),
            operation_name="删除备份任务",
            operation_description=f"删除备份任务失败: {error_msg}",
            category="backup",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="DELETE",
            request_url=str(http_request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"删除备份任务失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

