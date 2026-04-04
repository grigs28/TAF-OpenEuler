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
from .scheduler.db_utils import is_opengauss, get_opengauss_connection

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
    """记录操作日志"""
    if _shutting_down:
        return False
    try:
        operation_time = datetime.now()

        # 使用原生SQL插入操作日志
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
                try:
                    await actual_conn.rollback()
                except:
                    pass
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
    """记录系统日志"""
    # 检查应用是否正在关闭
    global _shutting_down
    if _shutting_down:
        return False
    try:
        log_time = datetime.now()
        async with get_opengauss_connection() as conn:
            # 构建SQL语句
            sql = """
                INSERT INTO system_logs (
                    log_level, category, message, module, function, file_path, line_number,
                    user_id, task_id, log_time, details, exception_type, stack_trace,
                    duration_ms, memory_usage_mb, cpu_usage_percent
                ) VALUES (
                    $1::loglevel, $2::logcategory, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13,
                    $14, $15, $16
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
                str(task_id) if task_id is not None else None,
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
                try:
                    await actual_conn.rollback()
                except:
                    pass
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
        logger.error(f"记录系统日志失败: {str(e)}")
        logger.error(f"错误详情:\n{traceback.format_exc()}")
        return False
