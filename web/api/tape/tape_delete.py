#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_delete
Tape Management API - tape_delete
"""

import logging
import traceback
import json
import re
import os
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from .tape_utils import normalize_tape_label, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()





@router.delete("/delete/{tape_id}")
async def delete_tape(tape_id: str, http_request: Request):
    """删除磁带记录"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "DELETE"
    request_url = str(http_request.url)
    
    try:
        from config.settings import get_settings

        settings = get_settings()
        database_url = settings.DATABASE_URL

        # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
        from utils.db_connection_helper import get_psycopg_connection_from_url
        
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        
        try:
            with conn.cursor() as cur:
                # 检查磁带是否存在
                cur.execute("""
                    SELECT tape_id, label FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    await log_operation(
                        operation_type=OperationType.DELETE,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="删除磁带",
                        operation_description=f"删除磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带 {tape_id} 不存在",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                tape_label = existing[1]
                
                # 删除磁带记录
                cur.execute("""
                    DELETE FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                
                conn.commit()
                logger.info(f"删除磁带记录: {tape_id}")
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=tape_label,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
            category="tape",
            success=True,
            result_message=f"磁带 {tape_id} 删除成功",
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 删除成功",
            "tape_id": tape_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"删除磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="删除磁带",
            operation_description=f"删除磁带 {tape_id}",
            category="tape",
            success=False,
            error_message=str(e),
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="delete_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))
