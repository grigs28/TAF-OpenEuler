#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API模块
System Management API Module
"""

from fastapi import APIRouter

# 导入所有子模块
from . import info, config, logs, statistics, database, tape_config, notification, file_system, env_config, compression_config, service

# 创建主路由器
router = APIRouter()

# 注册所有子模块的路由
router.include_router(info.router, tags=["系统信息"])
router.include_router(config.router, tags=["系统配置"])
router.include_router(logs.router, tags=["系统日志"])
router.include_router(statistics.router, tags=["系统统计"])
router.include_router(database.router, tags=["数据库配置"])
router.include_router(tape_config.router, tags=["磁带配置"])
router.include_router(notification.router, tags=["通知配置"])
router.include_router(file_system.router, tags=["文件系统"])
router.include_router(env_config.router, tags=["环境配置"])
router.include_router(compression_config.router, tags=["压缩配置"])
router.include_router(service.router, tags=["服务管理"])

__all__ = ['router']

