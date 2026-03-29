#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信通知 API 路由
WeChat Notification API Routes
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from utils.wechat_notifier import get_wechat_notifier, init_wechat_notifier
from services.backup_reporter import get_backup_reporter, init_backup_reporter, start_backup_reporter, stop_backup_reporter
from config.settings import settings

router = APIRouter(prefix="/wechat", tags=["微信通知"])


class WeChatConfigRequest(BaseModel):
    """微信配置请求"""
    webhook_url: str
    enabled: bool = True
    report_interval: int = 30  # 汇报间隔（分钟）


class TestMessageRequest(BaseModel):
    """测试消息请求"""
    message: str


class ManualReportRequest(BaseModel):
    """手动汇报请求"""
    task_name: str
    status: str
    progress: float
    files_processed: int
    bytes_processed: int
    speed: Optional[float] = None
    error: Optional[str] = None


@router.post("/config")
async def configure_wechat(config: WeChatConfigRequest):
    """
    配置微信通知

    设置企业微信机器人 Webhook URL 和汇报间隔
    """
    try:
        # 初始化微信通知器
        notifier = init_wechat_notifier(
            webhook_url=config.webhook_url,
            enabled=config.enabled
        )

        # 初始化并启动备份汇报器
        reporter = init_backup_reporter()
        await stop_backup_reporter()  # 先停止旧的
        await start_backup_reporter()  # 启动新的

        return {
            "success": True,
            "message": "微信通知配置成功",
            "enabled": config.enabled,
            "report_interval": config.report_interval
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"配置失败: {str(e)}")


@router.post("/test")
async def test_wechat(request: TestMessageRequest):
    """
    测试微信通知

    发送一条测试消息到企业微信
    """
    notifier = get_wechat_notifier()
    if not notifier:
        raise HTTPException(status_code=400, detail="微信通知未配置")

    success = await notifier.send_message(request.message)
    if success:
        return {"success": True, "message": "测试消息发送成功"}
    else:
        raise HTTPException(status_code=500, detail="消息发送失败")


@router.post("/report")
async def manual_report(request: ManualReportRequest):
    """
    手动发送备份汇报

    立即发送一条备份进度汇报
    """
    notifier = get_wechat_notifier()
    if not notifier:
        raise HTTPException(status_code=400, detail="微信通知未配置")

    success = await notifier.send_backup_report(
        task_name=request.task_name,
        status=request.status,
        progress=request.progress,
        files_processed=request.files_processed,
        bytes_processed=request.bytes_processed,
        speed=request.speed,
        error=request.error
    )

    if success:
        return {"success": True, "message": "汇报发送成功"}
    else:
        raise HTTPException(status_code=500, detail="汇报发送失败")


@router.get("/status")
async def get_wechat_status():
    """
    获取微信通知状态

    返回当前配置和运行状态
    """
    notifier = get_wechat_notifier()
    reporter = get_backup_reporter()

    return {
        "configured": notifier is not None,
        "enabled": notifier.enabled if notifier else False,
        "reporter_running": reporter.running if reporter else False,
        "report_interval": settings.WECHAT_REPORT_INTERVAL if hasattr(settings, 'WECHAT_REPORT_INTERVAL') else 30
    }


@router.post("/reporter/start")
async def start_reporter():
    """启动定时汇报器"""
    await start_backup_reporter()
    return {"success": True, "message": "定时汇报器已启动"}


@router.post("/reporter/stop")
async def stop_reporter():
    """停止定时汇报器"""
    await stop_backup_reporter()
    return {"success": True, "message": "定时汇报器已停止"}
