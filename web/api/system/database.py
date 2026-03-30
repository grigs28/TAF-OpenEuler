#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - database
System Management API - database
"""

import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .models import DatabaseConfig
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/database/config")
async def get_database_config():
    """获取数据库配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()

        db_url = settings.DATABASE_URL or ""
        db_info = {
            "db_type": "opengauss",
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "query_dop": getattr(settings, 'DB_QUERY_DOP', 16),
            # openGauss 连接参数
            "db_host": settings.DB_HOST,
            "db_port": settings.DB_PORT,
            "db_user": settings.DB_USER,
            "db_database": settings.DB_DATABASE,
            "db_password": settings.DB_PASSWORD or ""
        }

        return db_info

    except Exception as e:
        logger.error(f"获取数据库配置失败: {str(e)}")
        return {
            "db_type": "opengauss",
            "pool_size": 10,
            "max_overflow": 20
        }


@router.post("/database/test")
async def test_database_connection(config: DatabaseConfig):
    """测试数据库连接"""
    try:
        from config.database import DatabaseManager

        # 构建 openGauss 数据库URL
        if config.db_type == "opengauss":
            if not all([config.db_host, config.db_port, config.db_user, config.db_database]):
                raise ValueError("openGauss 数据库需要完整的连接参数")
            # 将 opengauss URL 显式写为 opengauss 协议，后续在 DatabaseManager 内部转换为 postgresql 以兼容驱动
            db_url = f"opengauss://{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
        else:
            raise ValueError(f"不支持的数据库类型: {config.db_type}")

        # 通过 DatabaseManager 测试
        temp_db = DatabaseManager()
        # 手动设置URL进行测试
        temp_db.settings.DATABASE_URL = db_url
        temp_db.settings.DB_POOL_SIZE = config.pool_size
        temp_db.settings.DB_MAX_OVERFLOW = config.max_overflow

        # 尝试初始化连接
        await temp_db.initialize()

        # 测试查询
        success = await temp_db.health_check()

        # 关闭连接
        await temp_db.close()

        if success:
            return {
                "success": True,
                "message": "数据库连接测试成功",
                "db_type": config.db_type
            }
        else:
            return {
                "success": False,
                "message": "数据库连接测试失败"
            }

    except Exception as e:
        logger.error(f"测试数据库连接失败: {str(e)}")
        return {
            "success": False,
            "message": f"连接失败: {str(e)}"
        }


@router.put("/database/config")
async def update_database_config(config: DatabaseConfig, request: Request):
    """更新数据库配置"""
    start_time = datetime.now()
    old_values = {}
    new_values = {}
    
    try:
        import os
        from pathlib import Path
        from config.settings import get_settings
        
        # 获取当前配置，用于填充缺失的密码和记录旧值
        current_settings = get_settings()
        old_values = {
            "db_type": "opengauss",
            "db_host": current_settings.DB_HOST or "",
            "db_port": current_settings.DB_PORT or 0,
            "db_user": current_settings.DB_USER or "",
            "db_database": current_settings.DB_DATABASE or "",
            "pool_size": current_settings.DB_POOL_SIZE,
            "max_overflow": current_settings.DB_MAX_OVERFLOW
        }

        logger.info(f"更新数据库配置: type={config.db_type}, host={config.db_host}, user={config.db_user}")

        # 验证配置
        if config.db_type == "opengauss":
            # 如果密码为空，使用当前配置的密码
            if not config.db_password:
                # 从当前URL提取密码
                current_url = current_settings.DATABASE_URL
                if "@" in current_url and (current_url.startswith("postgresql://") or current_url.startswith("opengauss://")):
                    try:
                        # 解析当前URL获取密码
                        parts = current_url.split("@")
                        auth_part = parts[0].split("://")[1]
                        if ":" in auth_part:
                            _, existing_password = auth_part.split(":", 1)
                            config.db_password = existing_password
                    except:
                        pass
                
                # 如果仍然没有密码，使用当前设置的DB_PASSWORD
                if not config.db_password:
                    config.db_password = current_settings.DB_PASSWORD
            
            # 验证必需参数（允许空字符串，将使用当前配置）
            if not all([config.db_host, config.db_port, config.db_user, config.db_database]):
                raise ValueError("openGauss 数据库需要主机、端口、用户名和数据库名")
            
            # 确保有密码
            if not config.db_password:
                raise ValueError("openGauss 数据库密码不能为空")
            
            # 统一写入 opengauss 协议，DatabaseManager 内部再转换为 postgresql 用于驱动
            db_url = f"opengauss://{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_database}"
        else:
            raise ValueError(f"不支持的数据库类型: {config.db_type}")
        
        # 使用EnvFileManager保存配置到.env文件
        from config.env_file_manager import get_env_manager
        
        env_manager = get_env_manager()
        
        # 构建更新字典
        updates = {
            "DATABASE_URL": db_url,
            "DB_FLAVOR": "opengauss",
            "DB_HOST": config.db_host or "",
            "DB_PORT": str(config.db_port or ""),
            "DB_USER": config.db_user or "",
            "DB_PASSWORD": config.db_password or "",
            "DB_DATABASE": config.db_database or "",
            "DB_POOL_SIZE": str(config.pool_size),
            "DB_MAX_OVERFLOW": str(config.max_overflow)
        }

        # openGauss/PostgreSQL特有配置：查询并行度（配置项保存在 .env 中，供连接池使用）
        # 从当前配置获取 query_dop，如果没有则使用默认值 16
        query_dop = getattr(current_settings, 'DB_QUERY_DOP', 16)
        updates["DB_QUERY_DOP"] = str(query_dop)
        
        # 使用EnvFileManager写入文件（自动处理备份和并发问题）
        success = env_manager.write_env_file(updates, backup=True)
        
        if not success:
            raise ValueError("写入.env文件失败")
        
        # 记录新值（隐藏密码）
        new_values = {
            "db_type": config.db_type,
            "db_host": config.db_host or "",
            "db_port": config.db_port or 0,
            "db_user": config.db_user or "",
            "db_database": config.db_database or "",
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow
        }
        
        # 计算持续时间
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录操作日志
        changed_fields = []
        for key in new_values:
            if old_values.get(key) != new_values.get(key):
                changed_fields.append(key)
        
        # 获取客户端IP
        client_ip = request.client.host if request.client else None
        
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="database",
            resource_name="数据库配置",
            operation_name="更新数据库配置",
            operation_description=f"更新数据库配置: {config.db_type}",
            category="system",
            success=True,
            result_message="数据库配置更新成功",
            old_values=old_values,
            new_values=new_values,
            changed_fields=changed_fields,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        # 记录系统日志
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.SYSTEM,
            message=f"数据库配置已更新: {config.db_type}",
            details={
                "db_type": config.db_type,
                "db_host": config.db_host,
                "db_port": config.db_port,
                "db_user": config.db_user,
                "db_database": config.db_database,
                "pool_size": config.pool_size,
                "max_overflow": config.max_overflow
            },
            module="web.api.system.database",
            function="update_database_config"
        )
        
        logger.info(f"数据库配置已更新: {config.db_type}")
        
        return {
            "success": True,
            "message": "数据库配置更新成功",
            "db_url": db_url.split("@")[0] + "@***" if "@" in db_url else db_url  # 隐藏密码
        }
        
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="database",
            resource_name="数据库配置",
            operation_name="更新数据库配置",
            operation_description=f"更新数据库配置失败: {error_msg}",
            category="system",
            success=False,
            error_message=error_msg,
            old_values=old_values,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        # 记录系统错误日志
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.SYSTEM,
            message=f"更新数据库配置失败: {error_msg}",
            details={
                "error": error_msg,
                "config": {
                    "db_type": config.db_type,
                    "db_host": config.db_host,
                    "db_port": config.db_port,
                    "db_user": config.db_user,
                    "db_database": config.db_database
                }
            },
            module="web.api.system.database",
            function="update_database_config",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        logger.error(f"更新数据库配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/database/status")
async def get_database_status(request: Request):
    """获取数据库状态"""
    try:
        system = request.app.state.system
        if not system:
            return {
                "status": "unknown",
                "message": "系统未初始化"
            }
        
        from config.settings import get_settings
        settings = get_settings()
        
        # 检查数据库连接状态
        db_healthy = await system.db_manager.health_check()
        
        db_info = {
            "status": "online" if db_healthy else "offline",
            "db_type": "openGauss",
            "pool_size": settings.DB_POOL_SIZE,
            "max_overflow": settings.DB_MAX_OVERFLOW,
            "db_host": settings.DB_HOST,
            "db_port": settings.DB_PORT,
            "db_user": settings.DB_USER,
            "db_database": settings.DB_DATABASE
        }

        return db_info
        
    except Exception as e:
        logger.error(f"获取数据库状态失败: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

