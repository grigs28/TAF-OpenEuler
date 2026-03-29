#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - logs
System Management API - logs
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 系统日志相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/logs")
async def get_system_logs(
    category: Optional[str] = None,
    level: Optional[str] = None,
    operation_type: Optional[str] = None,
    resource_type: Optional[str] = None,
    user_id: Optional[int] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
    request: Request = None
):
    """获取系统日志
    
    Args:
        category: 日志分类（system/backup/recovery/tape/user/security等）
        level: 日志级别（debug/info/warning/error/critical）
        operation_type: 操作类型（login/backup/recovery/tape_load等）
        resource_type: 资源类型（tape/backup/recovery/scheduler/user/system）
        user_id: 用户ID
        start_time: 开始时间
        end_time: 结束时间
        limit: 返回数量限制
        offset: 偏移量
    """
    try:
        from config.database import db_manager
        from datetime import datetime, timedelta
        import json
        
        # 如果没有指定时间范围，默认查询最近24小时的日志
        if not start_time:
            start_time = datetime.now() - timedelta(days=1)
        if not end_time:
            end_time = datetime.now()
        
        logs = []
        
        # 检查是否为 openGauss 数据库
        from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
        
        if is_opengauss():
            # 使用原生SQL查询（openGauss）
            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 查询操作日志
                operation_where = []
                params = []
                param_idx = 1
                
                operation_where.append(f"operation_time >= ${param_idx}")
                params.append(start_time)
                param_idx += 1
                
                operation_where.append(f"operation_time <= ${param_idx}")
                params.append(end_time)
                param_idx += 1
                
                if category:
                    operation_where.append(f"category = ${param_idx}")
                    params.append(category)
                    param_idx += 1
                if operation_type:
                    operation_where.append(f"operation_type = ${param_idx}::operationtype")
                    params.append(operation_type)
                    param_idx += 1
                if resource_type:
                    operation_where.append(f"resource_type = ${param_idx}")
                    params.append(resource_type)
                    param_idx += 1
                if user_id:
                    operation_where.append(f"user_id = ${param_idx}")
                    params.append(user_id)
                    param_idx += 1
                
                # 添加LIMIT和OFFSET参数
                limit_param_idx = param_idx
                offset_param_idx = param_idx + 1
                params.extend([limit, offset])
                
                operation_sql = f"""
                    SELECT * FROM operation_logs
                    WHERE {' AND '.join(operation_where)}
                    ORDER BY operation_time DESC
                    LIMIT ${limit_param_idx} OFFSET ${offset_param_idx}
                """
                
                operation_rows = await conn.fetch(operation_sql, *params)
                
                # 查询系统日志
                system_where = []
                system_params = []
                system_param_idx = 1
                
                system_where.append(f"log_time >= ${system_param_idx}")
                system_params.append(start_time)
                system_param_idx += 1
                
                system_where.append(f"log_time <= ${system_param_idx}")
                system_params.append(end_time)
                system_param_idx += 1
                
                if category:
                    system_where.append(f"category = ${system_param_idx}::logcategory")
                    system_params.append(category)
                    system_param_idx += 1
                if level:
                    system_where.append(f"log_level = ${system_param_idx}::loglevel")
                    system_params.append(level.lower())
                    system_param_idx += 1
                
                # 添加LIMIT和OFFSET参数
                system_limit_param_idx = system_param_idx
                system_offset_param_idx = system_param_idx + 1
                system_params.extend([limit, offset])
                
                system_sql = f"""
                    SELECT * FROM system_logs
                    WHERE {' AND '.join(system_where)}
                    ORDER BY log_time DESC
                    LIMIT ${system_limit_param_idx} OFFSET ${system_offset_param_idx}
                """
                
                system_rows = await conn.fetch(system_sql, *system_params)
                
                # 格式化操作日志
                for row in operation_rows:
                    logs.append({
                        "id": row['id'],
                        "type": "operation",
                        "timestamp": row['operation_time'].isoformat() if row['operation_time'] else None,
                        "level": "info" if row.get('success', True) else "error",
                        "category": row.get('category') or "operation",
                        "operation_type": row.get('operation_type') if isinstance(row.get('operation_type'), str) else (row.get('operation_type').value if hasattr(row.get('operation_type'), 'value') else None),
                        "resource_type": row.get('resource_type'),
                        "resource_id": row.get('resource_id'),
                        "resource_name": row.get('resource_name'),
                        "user_id": row.get('user_id'),
                        "username": row.get('username'),
                        "operation_name": row.get('operation_name'),
                        "operation_description": row.get('operation_description'),
                        "success": row.get('success', True),
                        "result_message": row.get('result_message'),
                        "error_message": row.get('error_message'),
                        "ip_address": row.get('ip_address'),
                        "duration_ms": row.get('duration_ms'),
                        "details": {
                            "request_method": row.get('request_method'),
                            "request_url": row.get('request_url'),
                            "response_status": row.get('response_status'),
                            "old_values": json.loads(row['old_values']) if isinstance(row.get('old_values'), str) else row.get('old_values'),
                            "new_values": json.loads(row['new_values']) if isinstance(row.get('new_values'), str) else row.get('new_values')
                        }
                    })
                
                # 格式化系统日志
                for row in system_rows:
                    logs.append({
                        "id": row['id'],
                        "type": "system",
                        "timestamp": row['log_time'].isoformat() if row['log_time'] else None,
                        "level": row.get('log_level') if isinstance(row.get('log_level'), str) else (row.get('log_level').value if hasattr(row.get('log_level'), 'value') else "info"),
                        "category": row.get('category') if isinstance(row.get('category'), str) else (row.get('category').value if hasattr(row.get('category'), 'value') else "system"),
                        "message": row.get('message'),
                        "module": row.get('module'),
                        "function": row.get('function'),
                        "file_path": row.get('file_path'),
                        "line_number": row.get('line_number'),
                        "user_id": row.get('user_id'),
                        "task_id": row.get('task_id'),
                        "details": json.loads(row['details']) if isinstance(row.get('details'), str) else row.get('details'),
                        "exception_type": row.get('exception_type'),
                        "stack_trace": row.get('stack_trace'),
                        "duration_ms": row.get('duration_ms'),
                        "memory_usage_mb": row.get('memory_usage_mb'),
                        "cpu_usage_percent": row.get('cpu_usage_percent')
                    })
                
        else:
            # 检查是否为Redis数据库
            from utils.scheduler.db_utils import is_redis, is_sqlite, get_sqlite_connection
            
            if is_redis():
                # Redis模式下不返回日志（Redis没有日志表）
                logger.debug("[Redis模式] 系统日志查询暂未实现（Redis没有日志表），返回空列表")
                return {
                    "success": True,
                    "total": 0,
                    "logs": [],
                    "pagination": {
                        "limit": limit,
                        "offset": offset,
                        "has_more": False
                    }
                }
            
            if not is_sqlite():
                from utils.scheduler.db_utils import is_opengauss
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.debug(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询系统日志，返回空列表")
                return {
                    "success": True,
                    "total": 0,
                    "logs": [],
                    "pagination": {
                        "limit": limit,
                        "offset": offset,
                        "has_more": False
                    }
                }
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                # 构建操作日志查询条件
                operation_where = ["operation_time >= ?", "operation_time <= ?"]
                operation_params = [start_time, end_time]
                
                if category:
                    operation_where.append("category = ?")
                    operation_params.append(category)
                if operation_type:
                    operation_where.append("operation_type = ?")
                    operation_params.append(operation_type)
                if resource_type:
                    operation_where.append("resource_type = ?")
                    operation_params.append(resource_type)
                if user_id:
                    operation_where.append("user_id = ?")
                    operation_params.append(user_id)
                
                operation_sql = f"""
                    SELECT * FROM operation_logs
                    WHERE {' AND '.join(operation_where)}
                    ORDER BY operation_time DESC
                    LIMIT ? OFFSET ?
                """
                operation_params.extend([limit, offset])
                
                operation_cursor = await conn.execute(operation_sql, operation_params)
                operation_rows = await operation_cursor.fetchall()
                
                # 构建系统日志查询条件
                system_where = ["log_time >= ?", "log_time <= ?"]
                system_params = [start_time, end_time]
                
                if category:
                    system_where.append("category = ?")
                    system_params.append(category)
                if level:
                    system_where.append("LOWER(log_level) = LOWER(?)")
                    system_params.append(level)
                
                system_sql = f"""
                    SELECT * FROM system_logs
                    WHERE {' AND '.join(system_where)}
                    ORDER BY log_time DESC
                    LIMIT ? OFFSET ?
                """
                system_params.extend([limit, offset])
                
                system_cursor = await conn.execute(system_sql, system_params)
                system_rows = await system_cursor.fetchall()
                
                # 获取列名
                operation_columns = [desc[0] for desc in operation_cursor.description] if operation_cursor.description else []
                system_columns = [desc[0] for desc in system_cursor.description] if system_cursor.description else []
                
                # 格式化操作日志
                for row in operation_rows:
                    row_dict = dict(zip(operation_columns, row))
                    # 处理 JSON 字段
                    old_values = row_dict.get('old_values')
                    if isinstance(old_values, str):
                        try:
                            old_values = json.loads(old_values)
                        except:
                            pass
                    new_values = row_dict.get('new_values')
                    if isinstance(new_values, str):
                        try:
                            new_values = json.loads(new_values)
                        except:
                            pass
                    
                    logs.append({
                        "id": row_dict.get('id'),
                        "type": "operation",
                        "timestamp": row_dict.get('operation_time').isoformat() if row_dict.get('operation_time') and hasattr(row_dict.get('operation_time'), 'isoformat') else (str(row_dict.get('operation_time')) if row_dict.get('operation_time') else None),
                        "level": "info" if row_dict.get('success', True) else "error",
                        "category": row_dict.get('category') or "operation",
                        "operation_type": row_dict.get('operation_type'),
                        "resource_type": row_dict.get('resource_type'),
                        "resource_id": row_dict.get('resource_id'),
                        "resource_name": row_dict.get('resource_name'),
                        "user_id": row_dict.get('user_id'),
                        "username": row_dict.get('username'),
                        "operation_name": row_dict.get('operation_name'),
                        "operation_description": row_dict.get('operation_description'),
                        "success": row_dict.get('success', True),
                        "result_message": row_dict.get('result_message'),
                        "error_message": row_dict.get('error_message'),
                        "ip_address": row_dict.get('ip_address'),
                        "duration_ms": row_dict.get('duration_ms'),
                        "details": {
                            "request_method": row_dict.get('request_method'),
                            "request_url": row_dict.get('request_url'),
                            "response_status": row_dict.get('response_status'),
                            "old_values": old_values,
                            "new_values": new_values
                        }
                    })
                
                # 格式化系统日志
                for row in system_rows:
                    row_dict = dict(zip(system_columns, row))
                    # 处理 JSON 字段
                    details = row_dict.get('details')
                    if isinstance(details, str):
                        try:
                            details = json.loads(details)
                        except:
                            pass
                    
                    logs.append({
                        "id": row_dict.get('id'),
                        "type": "system",
                        "timestamp": row_dict.get('log_time').isoformat() if row_dict.get('log_time') and hasattr(row_dict.get('log_time'), 'isoformat') else (str(row_dict.get('log_time')) if row_dict.get('log_time') else None),
                        "level": row_dict.get('log_level') or "info",
                        "category": row_dict.get('category') or "system",
                        "message": row_dict.get('message'),
                        "module": row_dict.get('module'),
                        "function": row_dict.get('function'),
                        "file_path": row_dict.get('file_path'),
                        "line_number": row_dict.get('line_number'),
                        "user_id": row_dict.get('user_id'),
                        "task_id": row_dict.get('task_id'),
                        "details": details,
                        "exception_type": row_dict.get('exception_type'),
                        "stack_trace": row_dict.get('stack_trace'),
                        "duration_ms": row_dict.get('duration_ms'),
                        "memory_usage_mb": row_dict.get('memory_usage_mb'),
                        "cpu_usage_percent": row_dict.get('cpu_usage_percent')
                    })
        
        # 按时间排序（最新的在前）
        logs.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        
        # 限制返回数量
        logs = logs[:limit]

        return {
            "success": True,
            "total": len(logs),
            "logs": logs,
            "pagination": {
            "limit": limit,
                "offset": offset,
                "has_more": len(logs) == limit
            }
        }

    except Exception as e:
        logger.error(f"获取系统日志失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

