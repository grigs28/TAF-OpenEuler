#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - label
Tape Management API - label
"""

import logging
import traceback
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from .models import WriteTapeLabelRequest, UpdateTapeRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, is_redis, get_sqlite_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/read-label")
async def read_tape_label(request: Request):
    """读取磁带标签"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "GET"
    request_url = str(request.url)
    
    logger.info("========== 读取磁带标签API被调用 ==========")
    try:
        logger.info("检查系统实例...")
        system = request.app.state.system
        if not system:
            logger.error("系统未初始化")
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        logger.info("系统实例检查通过，准备调用tape_operations._read_tape_label")
        
        # 通过磁带操作读取标签（60秒超时）
        try:
            import asyncio
            metadata = await asyncio.wait_for(
                system.tape_manager.tape_operations._read_tape_label(),
                timeout=60.0
            )
        except asyncio.TimeoutError:
            logger.warning("读取磁带卷标超时（60秒）")
            metadata = None
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        logger.info(f"读取标签完成，结果: {metadata is not None}")
        if metadata:
            tape_id = metadata.get('tape_id', 'N/A')
            logger.info(f"成功读取标签: {tape_id}")
            await log_operation(
                operation_type=OperationType.TAPE_READ_LABEL,
                resource_type="tape",
                resource_id=tape_id,
                resource_name=metadata.get('label'),
                operation_name="读取磁带标签",
                operation_description=f"读取磁带标签: {tape_id}",
                category="tape",
                success=True,
                result_message=f"成功读取磁带标签: {tape_id}",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"成功读取磁带标签: {tape_id}",
                module="web.api.tape.label",
                function="read_tape_label",
                duration_ms=duration_ms
            )
            return {
                "success": True,
                "metadata": metadata
            }
        else:
            logger.warning("无法读取磁带标签或磁带为空")
            await log_operation(
                operation_type=OperationType.TAPE_READ_LABEL,
                resource_type="tape",
                operation_name="读取磁带标签",
                operation_description="读取磁带标签",
                category="tape",
                success=False,
                error_message="无法读取磁带标签或磁带为空",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {
                "success": False,
                "message": "无法读取磁带标签或磁带为空"
            }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"读取磁带标签异常: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_READ_LABEL,
            resource_type="tape",
            operation_name="读取磁带标签",
            operation_description="读取磁带标签",
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
            module="web.api.tape.label",
            function="read_tape_label",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/write-label")
async def write_tape_label(request: WriteTapeLabelRequest, http_request: Request):
    """写入磁带标签"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "POST"
    request_url = str(http_request.url)
    
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        # 检查是否为Redis数据库
        from utils.scheduler.db_utils import is_redis
        
        if is_redis():
            logger.warning(f"[Redis模式] 写入磁带标签暂未实现: {request.tape_id}")
            raise HTTPException(status_code=501, detail="Redis模式下暂不支持写入磁带标签功能")
        
        # 从数据库中获取磁带的过期时间等信息
        from config.settings import get_settings
        from utils.db_connection_helper import get_psycopg_connection_from_url
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 SQLite
        if is_sqlite():
            # SQLite 版本暂不支持写入标签（需要实现）
            logger.warning(f"[SQLite模式] 写入磁带标签暂未实现: {request.tape_id}")
            raise HTTPException(status_code=501, detail="SQLite模式下暂不支持写入磁带标签功能")
        
        # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        try:
            cur = conn.cursor()
            
            # 查询磁带信息
            cur.execute(
                "SELECT expiry_date, created_date FROM tape_cartridges WHERE tape_id = %s",
                (request.tape_id,)
            )
            result = cur.fetchone()
            
            if not result:
                conn.close()
                await log_operation(
                    operation_type=OperationType.TAPE_WRITE_LABEL,
                    resource_type="tape",
                    resource_id=request.tape_id,
                    resource_name=request.label,
                    operation_name="写入磁带标签",
                    operation_description=f"写入磁带标签: {request.tape_id}",
                    category="tape",
                    success=False,
                    error_message=f"未找到磁带: {request.tape_id}",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                )
                raise HTTPException(status_code=404, detail=f"未找到磁带: {request.tape_id}")
            
            expiry_date, created_date = result
            
            # 准备磁带信息
            tape_info = {
                "tape_id": request.tape_id,
                "label": request.label,
                "serial_number": request.serial_number,
                "created_date": created_date or datetime.now(),
                "expiry_date": expiry_date or datetime.now(),
            }
            
            # 写入物理磁带标签
            write_result = await system.tape_manager.tape_operations._write_tape_label(tape_info)
            
            conn.close()
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            if write_result:
                await log_operation(
                    operation_type=OperationType.TAPE_WRITE_LABEL,
                    resource_type="tape",
                    resource_id=request.tape_id,
                    resource_name=request.label,
                    operation_name="写入磁带标签",
                    operation_description=f"写入磁带标签: {request.tape_id}",
                    category="tape",
                    success=True,
                    result_message=f"磁带标签写入成功: {request.label}",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                await log_system(
                    level=LogLevel.INFO,
                    category=LogCategory.TAPE,
                    message=f"成功写入磁带标签: {request.tape_id} ({request.label})",
                    module="web.api.tape.label",
                    function="write_tape_label",
                    duration_ms=duration_ms
                )
                return {
                    "success": True,
                    "message": f"磁带标签写入成功: {request.label}"
                }
            else:
                await log_operation(
                    operation_type=OperationType.TAPE_WRITE_LABEL,
                    resource_type="tape",
                    resource_id=request.tape_id,
                    resource_name=request.label,
                    operation_name="写入磁带标签",
                    operation_description=f"写入磁带标签: {request.tape_id}",
                    category="tape",
                    success=False,
                    error_message="磁带标签写入失败",
                    ip_address=ip_address,
                    request_method=request_method,
                    request_url=request_url,
                    duration_ms=duration_ms
                )
                return {
                    "success": False,
                    "message": "磁带标签写入失败"
                }
        
        except Exception as e:
            conn.close()
            raise
    
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"写入磁带标签失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_WRITE_LABEL,
            resource_type="tape",
            resource_id=getattr(request, 'tape_id', None),
            resource_name=getattr(request, 'label', None),
            operation_name="写入磁带标签",
            operation_description=f"写入磁带标签失败",
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
            module="web.api.tape.label",
            function="write_tape_label",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


