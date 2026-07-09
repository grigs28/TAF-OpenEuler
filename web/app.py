#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web应用主模块
Web Application Main Module
"""

import logging
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from contextlib import asynccontextmanager

from config.settings import get_settings
from utils.logger import get_logger
from web.api import backup, recovery, tape, system, user, scheduler, tools, wechat
# system、tape 和 backup 现在已经是模块包，直接导入 router
from web.middleware.logging_middleware import LoggingMiddleware

logger = get_logger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    logger.info("Web应用启动中...")
    # 初始化 Syslog JSON 转发 handler
    from web.api.system.notification import _update_syslog_handler
    _update_syslog_handler()
    try:
        yield
    finally:
        # 关闭时执行
        logger.info("Web应用关闭中...")


def create_app(system_instance=None) -> FastAPI:
    """创建FastAPI应用"""
    # 从CHANGELOG.md解析版本号
    app_version = "0.0.1"  # 默认版本
    try:
        from pathlib import Path
        import re
        changelog_path = Path("CHANGELOG.md")
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                content = f.read()
            version_match = re.search(r'## \[(\d+\.\d+\.\d+)\]', content)
            if version_match:
                app_version = version_match.group(1)
    except Exception:
        pass
    
    app = FastAPI(
        title=settings.APP_NAME,
        version=app_version,
        description="企业级磁带备份系统管理界面",
        lifespan=lifespan
    )

    # 配置静态文件
    app.mount("/static", StaticFiles(directory="web/static"), name="static")

    # Favicon路由 - 处理浏览器默认请求的/favicon.ico
    @app.get("/favicon.ico")
    async def favicon():
        from fastapi.responses import FileResponse
        import os
        favicon_path = os.path.join("web", "static", "favicon.ico")
        if os.path.exists(favicon_path):
            return FileResponse(favicon_path)
        else:
            # 如果static目录下没有，尝试templates目录
            favicon_path = os.path.join("web", "templates", "favicon.ico")
            if os.path.exists(favicon_path):
                return FileResponse(favicon_path)
            raise HTTPException(status_code=404, detail="Favicon not found")

    # 配置模板
    templates = Jinja2Templates(directory="web/templates")

    # 添加CORS中间件
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # 添加自定义中间件
    app.add_middleware(LoggingMiddleware)

    # SSO 认证中间件（替代原 AuthMiddleware）
    try:
        from web.yz_auth import router as yz_router, sso_middleware
        from starlette.middleware.base import BaseHTTPMiddleware
        app.include_router(yz_router)
        app.add_middleware(BaseHTTPMiddleware, dispatch=sso_middleware)
        logger.info("YZ SSO 登录已启用")
    except ImportError:
        logger.warning("yz_auth 模块未找到，跳过 SSO 认证")

    # 存储系统实例引用
    app.state.system = system_instance

    # 注册页面路由（必须在API路由之前注册，确保页面路由优先匹配）
    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        """首页"""
        return templates.TemplateResponse("index.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/backup", response_class=HTMLResponse)
    async def backup_page(request: Request):
        """备份管理页面"""
        return templates.TemplateResponse("backup.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/recovery", response_class=HTMLResponse)
    async def recovery_page(request: Request):
        """恢复管理页面"""
        return templates.TemplateResponse("recovery.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/tape", response_class=HTMLResponse)
    async def tape_page(request: Request):
        """磁带管理页面"""
        return templates.TemplateResponse("tape.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/system", response_class=HTMLResponse)
    async def system_page(request: Request):
        """系统设置页面"""
        return templates.TemplateResponse("system.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/tapedrive", response_class=HTMLResponse)
    async def tapedrive_page(request: Request):
        """磁带机配置页面"""
        return templates.TemplateResponse("tapedrive.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    @app.get("/scheduler", response_class=HTMLResponse)
    async def scheduler_page(request: Request):
        """计划任务管理页面"""
        logger.debug(f"访问计划任务页面: {request.url.path}")
        try:
            return templates.TemplateResponse("scheduler.html", {
                "request": request,
                "app_name": settings.APP_NAME,
                "version": app_version
            })
        except Exception as e:
            logger.error(f"渲染计划任务页面失败: {str(e)}", exc_info=True)
            raise

    @app.get("/tools", response_class=HTMLResponse)
    async def tools_page(request: Request):
        """工具管理页面"""
        return templates.TemplateResponse("tools.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "version": app_version
        })

    # 注册API路由（在页面路由之后注册）
    app.include_router(backup.router, prefix="/api/backup", tags=["备份管理"])
    app.include_router(recovery.router, prefix="/api/recovery", tags=["恢复管理"])
    app.include_router(tape.router, prefix="/api/tape", tags=["磁带管理"])
    app.include_router(system.router, prefix="/api/system", tags=["系统管理"])
    app.include_router(user.router, prefix="/api/user", tags=["用户管理"])
    app.include_router(scheduler.router, prefix="/api/scheduler", tags=["计划任务管理"])
    app.include_router(tools.router, tags=["工具管理"])
    app.include_router(wechat.router, tags=["微信通知"])

    @app.get("/health")
    async def health_check():
        """健康检查"""
        return {
            "status": "healthy",
            "timestamp": "2024-10-30T04:20:00Z",
            "version": app_version
        }

    return app