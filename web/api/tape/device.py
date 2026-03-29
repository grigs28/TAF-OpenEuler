#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - device
Tape Management API - device (Linux LTFS Mode)
"""

import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/health")
async def check_tape_health(request: Request):
    """检查磁带健康状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用 LinuxTapeOperator 获取状态
        if system.tape_manager.linux_tape_operator:
            status = await system.tape_manager.linux_tape_operator.status()
            is_healthy = status.get('online', False) and not status.get('write_protected', False)

            return {
                "healthy": is_healthy,
                "health_score": 100 if is_healthy else 0,
                "usage_stats": {
                    "online": status.get('online', False),
                    "write_protected": status.get('write_protected', False),
                    "file_number": status.get('file_number', 0),
                    "block_number": status.get('block_number', 0)
                }
            }
        else:
            return {
                "healthy": False,
                "health_score": 0,
                "usage_stats": {},
                "message": "LinuxTapeOperator 不可用"
            }

    except Exception as e:
        logger.error(f"检查磁带健康状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage")
async def get_tape_usage_stats(request: Request):
    """获取磁带使用统计信息"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用 LinuxTapeOperator 获取状态
        if system.tape_manager.linux_tape_operator:
            status = await system.tape_manager.linux_tape_operator.status()

            return {
                "success": True,
                "usage_stats": {
                    "online": status.get('online', False),
                    "write_protected": status.get('write_protected', False),
                    "file_number": status.get('file_number', 0),
                    "block_number": status.get('block_number', 0)
                }
            }
        else:
            return {
                "success": False,
                "usage_stats": {},
                "message": "LinuxTapeOperator 不可用"
            }

    except Exception as e:
        logger.error(f"获取磁带使用统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/devices")
async def get_tape_devices(request: Request, force_rescan: bool = False):
    """获取磁带设备列表（默认使用缓存，force_rescan=true时强制重新扫描）"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 优先使用缓存
        if not force_rescan:
            devices = await system.tape_manager.get_cached_devices()
        else:
            # 强制重新扫描
            devices = await system.tape_manager._detect_tape_devices()
            if devices:
                await system.tape_manager._save_cached_devices(devices)
                system.tape_manager.cached_devices = devices

        return {"devices": devices, "cached": not force_rescan and len(devices) > 0}

    except Exception as e:
        logger.error(f"获取磁带设备列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
