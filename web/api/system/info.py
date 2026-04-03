#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - info
System Management API - info
"""

import logging
import sys
import importlib
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

# 系统信息相关路由，无需导入模型

logger = logging.getLogger(__name__)
router = APIRouter()

# 记录系统启动时间
_SYSTEM_START_TIME = datetime.now()


def _format_uptime(td) -> str:
    """将 timedelta 格式化为人类可读的运行时间"""
    total_seconds = int(td.total_seconds())
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []
    if days > 0:
        parts.append(f"{days}天")
    if hours > 0:
        parts.append(f"{hours}小时")
    if minutes > 0:
        parts.append(f"{minutes}分钟")
    parts.append(f"{seconds}秒")
    return "".join(parts)


def _get_package_versions() -> list:
    """动态获取已安装的关键包版本"""
    packages = [
        ("FastAPI", "fastapi"),
        ("Pydantic", "pydantic"),
        ("Hypercorn", "hypercorn"),
        ("asyncpg", "asyncpg"),
        ("psycopg", "psycopg"),
        ("zstandard", "zstandard"),
        ("py7zr", "py7zr"),
        ("pgzip", "pgzip"),
    ]
    result = []
    for display_name, import_name in packages:
        try:
            mod = importlib.import_module(import_name)
            version = getattr(mod, "__version__", None)
            if version:
                result.append({"name": display_name, "version": version})
        except ImportError:
            pass
    return result


@router.get("/info")
async def get_system_info():
    """获取系统信息"""
    try:
        from config.settings import get_settings

        settings = get_settings()

        return {
            "app_name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "platform": "Linux openEuler",
            "database": "openGauss",
            "compression": "zstd/pgzip/7z",
        }

    except Exception as e:
        logger.error(f"获取系统信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/about")
async def get_about_info():
    """获取关于系统页面的动态信息（运行时间、组件版本）"""
    try:
        from config.settings import get_settings

        settings = get_settings()
        uptime = datetime.now() - _SYSTEM_START_TIME

        return {
            "success": True,
            "version": settings.APP_VERSION,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "start_time": _SYSTEM_START_TIME.strftime("%Y-%m-%d %H:%M:%S"),
            "uptime": _format_uptime(uptime),
            "components": _get_package_versions(),
        }
    except Exception as e:
        logger.error(f"获取系统信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/version")
async def get_version():
    """获取版本信息和CHANGELOG"""
    try:
        from config.settings import get_settings
        from pathlib import Path
        import re
        
        settings = get_settings()
        
        # 读取CHANGELOG.md
        changelog_path = Path("CHANGELOG.md")
        changelog_content = ""
        
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                changelog_content = f.read()
        
        return {
            "version": settings.APP_VERSION,
            "app_name": settings.APP_NAME,
            "changelog": changelog_content
        }

    except Exception as e:
        logger.error(f"获取版本信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check(request: Request):
    """系统健康检查"""
    try:
        system = request.app.state.system
        if not system:
            return {"status": "unhealthy", "message": "系统未初始化"}

        checks = {
            "database": await system.db_manager.health_check(),
            "tape_drive": await system.tape_manager.health_check(),
            "scheduler": system.scheduler.running if system.scheduler else False
        }

        overall_healthy = all(checks.values())

        return {
            "status": "healthy" if overall_healthy else "unhealthy",
            "checks": checks
        }

    except Exception as e:
        logger.error(f"系统健康检查失败: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }

