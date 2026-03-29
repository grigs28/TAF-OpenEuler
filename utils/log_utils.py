#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志记录工具函数
Logging utility functions for OperationLog and SystemLog
"""

import logging
import json
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum

from models.system_log import OperationLog, SystemLog, LogLevel, LogCategory, OperationType
from config.database import db_manager
from .scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, is_redis, get_sqlite_connection

logger = logging.getLogger(__name__)

# 全局标志，防止应用关闭时记录系统日志
_shutting_down = False

def set_shutting_down():
    """设置应用正在关闭标志"""
    global _shutting_down
    _shutting_down = True


async def log_operation(
    operation_type: OperationType,
    resource_type: str,
    resource_id: Optional[str] = None,
    resource_name: Optional[str] = None,
    operation_name: Optional[str] = None,
    operation_description: Optional[str] = None,
    category: Optional[str] = None,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
    success: bool = True,
    result_message: Optional[str] = None,
    error_message: Optional[str] = None,
    duration_ms: Optional[int] = None,
    old_values: Optional[Dict[str, Any]] = None,
    new_values: Optional[Dict[str, Any]] = None,
    changed_fields: Optional[List[str]] = None,
    ip_address: Optional[str] = None,
    request_method: Optional[str] = None,
    request_url: Optional[str] = None,
    **kwargs
) -> bool:
    """记录操作日志
    
    Args:
        operation_type: 操作类型（OperationType枚举）
        resource_type: 资源类型（如'scheduler', 'tape', 'backup'等）
        resource_id: 资源ID（字符串）
        resource_name: 资源名称
        operation_name: 操作名称
        operation_description: 操作描述
        category: 操作分类（如'scheduler', 'backup'等）
        user_id: 用户ID
        username: 用户名
        success: 是否成功
        result_message: 结果消息
        error_message: 错误消息
        duration_ms: 持续时间（毫秒）
        old_values: 修改前的值
        new_values: 修改后的值
        changed_fields: 变更的字段列表
        ip_address: IP地址
        request_method: 请求方法（GET/POST等）
        request_url: 请求URL
        **kwargs: 其他字段
        
    Returns:
        是否成功记录日志
    """
    try:
        # 检查是否为Redis数据库
        from utils.scheduler.db_utils import is_redis
        
        if is_redis():
            # Redis模式：使用Redis存储操作日志
            from utils.redis_operation_log import create_operation_log_redis
            try:
                await create_operation_log_redis(
                    operation_type=operation_type,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    resource_name=resource_name,
                    operation_name=operation_name,
                    operation_description=operation_description,
                    category=category,
                    user_id=user_id,
                    username=username,
                    success=success,
                    result_message=result_message,
                    error_message=error_message,
                    duration_ms=duration_ms,
                    old_values=old_values,
                    new_values=new_values,
                    changed_fields=changed_fields,
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    **kwargs
                )
                return True
            except Exception as e:
                logger.error(f"[Redis模式] 记录操作日志失败: {str(e)}", exc_info=True)
                return False
        
        operation_time = datetime.now()
        
        if is_opengauss():
            # 使用原生SQL插入操作日志
            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 构建SQL语句
                sql = """
                    INSERT INTO operation_logs (
                        user_id, username, operation_type, resource_type, resource_id, resource_name,
                        operation_name, operation_description, category, operation_time, duration_ms,
                        request_method, request_url, success, result_message, error_message,
                        old_values, new_values, changed_fields, ip_address
                    ) VALUES (
                        $1, $2, $3::operationtype, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12, $13, $14, $15, $16, $17, $18, $19, $20
                    )
                """
                
                # 准备参数
                params = [
                    user_id,
                    username,
                    operation_type.value if isinstance(operation_type, Enum) else str(operation_type),
                    resource_type,
                    resource_id,
                    resource_name,
                    operation_name,
                    operation_description,
                    category,
                    operation_time,
                    duration_ms,
                    request_method,
                    request_url,
                    success,
                    result_message,
                    error_message,
                    json.dumps(old_values) if old_values else None,
                    json.dumps(new_values) if new_values else None,
                    json.dumps(changed_fields) if changed_fields else None,
                    ip_address
                ]
                
                await conn.execute(sql, *params)
                
                # psycopg3 binary protocol 需要显式提交事务
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    await actual_conn.commit()
                    logger.debug("操作日志插入事务已提交")
                except Exception as commit_err:
                    logger.warning(f"提交操作日志插入事务失败（可能已自动提交）: {commit_err}")
                    # 如果不在事务中，commit() 可能会失败，尝试回滚
                    try:
                        await actual_conn.rollback()
                    except:
                        pass
                
                return True
        else:
            # 使用SQLAlchemy插入操作日志（SQLite）
            # 检查是否为SQLite数据库
            if not is_sqlite() or db_manager.AsyncSessionLocal is None:
                logger.debug("[数据库类型错误] 当前数据库类型不支持使用SQLAlchemy会话记录操作日志，跳过记录")
                return True
            
            async with db_manager.AsyncSessionLocal() as session:
                operation_log = OperationLog(
                    user_id=user_id,
                    username=username,
                    operation_type=operation_type,
                    resource_type=resource_type,
                    resource_id=str(resource_id) if resource_id else None,
                    resource_name=resource_name,
                    operation_name=operation_name,
                    operation_description=operation_description,
                    category=category or resource_type,
                    operation_time=operation_time,
                    duration_ms=duration_ms,
                    request_method=request_method,
                    request_url=request_url,
                    success=success,
                    result_message=result_message,
                    error_message=error_message,
                    old_values=old_values,
                    new_values=new_values,
                    changed_fields=changed_fields,
                    ip_address=ip_address
                )
                session.add(operation_log)
                await session.commit()
                return True
                
    except Exception as e:
        logger.error(f"记录操作日志失败: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
        return False


async def log_system(
    level: LogLevel,
    category: LogCategory,
    message: str,
    module: Optional[str] = None,
    function: Optional[str] = None,
    file_path: Optional[str] = None,
    line_number: Optional[int] = None,
    user_id: Optional[int] = None,
    task_id: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    exception_type: Optional[str] = None,
    stack_trace: Optional[str] = None,
    duration_ms: Optional[int] = None,
    memory_usage_mb: Optional[float] = None,
    cpu_usage_percent: Optional[float] = None,
    **kwargs
) -> bool:
    """记录系统日志
    
    Args:
        level: 日志级别（LogLevel枚举）
        category: 日志分类（LogCategory枚举）
        message: 日志消息
        module: 模块名
        function: 函数名
        file_path: 文件路径
        line_number: 行号
        user_id: 用户ID
        task_id: 任务ID
        details: 详细信息（字典）
        exception_type: 异常类型
        stack_trace: 堆栈跟踪
        duration_ms: 持续时间（毫秒）
        memory_usage_mb: 内存使用（MB）
        cpu_usage_percent: CPU使用率（百分比）
        **kwargs: 其他字段
        
    Returns:
        是否成功记录日志
    """
    # 检查应用是否正在关闭
    global _shutting_down
    if _shutting_down:
        return False

    try:
        log_time = datetime.now()

        if is_opengauss():
            # 使用原生SQL插入系统日志，严禁SQLAlchemy解析openGauss
            # 使用连接池
            import json  # 确保在 openGauss 分支中可以使用 json 模块
            async with get_opengauss_connection() as conn:
                # 构建SQL语句
                sql = """
                    INSERT INTO system_logs (
                        log_level, category, message, module, function, file_path, line_number,
                        user_id, task_id, log_time, details, exception_type, stack_trace,
                        duration_ms, memory_usage_mb, cpu_usage_percent
                    ) VALUES (
                        $1::loglevel, $2::logcategory, $3, $4, $5, $6, $7,
                        $8, $9, $10, $11, $12, $13, $14, $15, $16
                    )
                """
                
                # 准备参数（确保task_id和user_id是字符串或None）
                params = [
                    level.value if isinstance(level, Enum) else str(level),
                    category.value if isinstance(category, Enum) else str(category),
                    message,
                    module,
                    function,
                    file_path,
                    line_number,
                    str(user_id) if user_id is not None else None,
                    str(task_id) if task_id is not None else None,  # task_id必须是字符串
                    log_time,
                    json.dumps(details) if details else None,
                    exception_type,
                    stack_trace,
                    duration_ms,
                    memory_usage_mb,
                    cpu_usage_percent
                ]
                
                await conn.execute(sql, *params)
                
                # psycopg3 binary protocol 需要显式提交事务
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    await actual_conn.commit()
                    logger.debug("系统日志插入事务已提交")
                except Exception as commit_err:
                    logger.warning(f"提交系统日志插入事务失败（可能已自动提交）: {commit_err}")
                    # 如果不在事务中，commit() 可能会失败，尝试回滚
                    try:
                        await actual_conn.rollback()
                    except:
                        pass
                
                return True
        else:
            # 检查是否为Redis数据库，Redis不支持系统日志表，跳过记录
            from utils.scheduler.db_utils import is_redis
            if is_redis():
                # Redis模式下不记录系统日志到数据库（Redis没有对应的表结构）
                # 日志仍然会通过日志处理器记录到文件
                logger.debug(f"Redis模式下跳过数据库系统日志记录: {message[:100] if message else ''}")
                return True
            
            # 使用原生 SQL 插入系统日志（SQLite 版本，避免 RETURNING 子句问题）
            import json
            
            # 再次检查是否为SQLite
            if not is_sqlite():
                logger.warning(f"当前数据库类型不支持系统日志记录，跳过: {message[:100] if message else ''}")
                return True
            
            async with get_sqlite_connection() as conn:
                # 使用原生 SQL 插入，避免 SQLAlchemy 的 RETURNING 子句导致的游标问题
                sql = """
                    INSERT INTO system_logs (
                        log_level, category, message, details, log_time, timestamp,
                        module, function, line_number, file_path, thread_id, process_id,
                        request_id, session_id, hostname, environment, version,
                        user_id, task_id, correlation_id, duration_ms, memory_usage_mb,
                        cpu_usage_percent, exception_type, stack_trace, created_by, updated_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = [
                    level.value if isinstance(level, Enum) else str(level),
                    category.value if isinstance(category, Enum) else str(category),
                    message,
                    json.dumps(details) if details else None,
                    log_time,
                    int(log_time.timestamp() * 1000) if log_time else None,
                    module,
                    function,
                    line_number,
                    file_path,
                    None,  # thread_id
                    None,  # process_id
                    None,  # request_id
                    None,  # session_id
                    None,  # hostname
                    None,  # environment
                    None,  # version
                    user_id,
                    str(task_id) if task_id is not None else None,
                    None,  # correlation_id
                    duration_ms,
                    memory_usage_mb,
                    cpu_usage_percent,
                    exception_type,
                    stack_trace,
                    None,  # created_by
                    None   # updated_by
                ]
                
                await conn.execute(sql, params)
                await conn.commit()
                return True
                
    except Exception as e:
        # 忽略关闭期间的连接错误和异步生成器错误
        error_msg = str(e).lower()
        if any(keyword in error_msg for keyword in [
            "shutting down", "connection_lost", "asynchronous generator",
            "cancellederror", "connection closed", "cursor needed to be reset",
            "interfaceerror", "no such column"
        ]):
            return False

        # 对于 SQLite 的 InterfaceError，静默忽略（日志记录失败不应影响主程序）
        import sqlite3
        if isinstance(e, (sqlite3.InterfaceError, sqlite3.OperationalError)):
            logger.debug(f"记录系统日志失败（SQLite并发问题，已忽略）: {str(e)}")
            return False

        logger.error(f"记录系统日志失败: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
        return False

