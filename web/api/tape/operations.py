#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - operations
Tape Management API - operations
"""

import logging
import traceback
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends
from pydantic import BaseModel

from .models import FormatRequest
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/load")
async def load_tape(request: Request, tape_id: str):
    """加载磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.load_tape(tape_id)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_LOAD,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="加载磁带",
                operation_description=f"加载磁带 {tape_id}",
                category="tape",
                success=True,
                result_message=f"磁带 {tape_id} 加载成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": f"磁带 {tape_id} 加载成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_LOAD,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="加载磁带",
                operation_description=f"加载磁带 {tape_id}",
                category="tape",
                success=False,
                error_message="磁带加载失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带加载失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"加载磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 记录详细的错误信息
        error_detail = f"{type(e).__name__}: {str(e)}"
        logger.error(f"错误详情: {error_detail}")
        logger.error(f"异常堆栈:\n{traceback.format_exc()}")
        
        await log_operation(
            operation_type=OperationType.TAPE_LOAD,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="加载磁带",
            operation_description=f"加载磁带 {tape_id}",
            category="tape",
            success=False,
            error_message=error_detail,
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.operations",
            function="load_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        # 返回更详细的错误信息
        raise HTTPException(status_code=500, detail=f"加载磁带失败: {error_detail}")


@router.post("/unload")
async def unload_tape(request: Request):
    """卸载磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.unload_tape()
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_UNLOAD,
                resource_type="tape",
                operation_name="卸载磁带",
                operation_description="卸载磁带",
                category="tape",
                success=True,
                result_message="磁带卸载成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": "磁带卸载成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_UNLOAD,
                resource_type="tape",
                operation_name="卸载磁带",
                operation_description="卸载磁带",
                category="tape",
                success=False,
                error_message="磁带卸载失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带卸载失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"卸载磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_UNLOAD,
            resource_type="tape",
            operation_name="卸载磁带",
            operation_description="卸载磁带",
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
            module="web.api.tape.operations",
            function="unload_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/erase")
async def erase_tape(request: Request, tape_id: str):
    """擦除磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        success = await system.tape_manager.erase_tape(tape_id)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            await log_operation(
                operation_type=OperationType.TAPE_ERASE,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="擦除磁带",
                operation_description=f"擦除磁带 {tape_id}",
                category="tape",
                success=True,
                result_message=f"磁带 {tape_id} 擦除成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": f"磁带 {tape_id} 擦除成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_ERASE,
                resource_type="tape",
                resource_id=tape_id,
                operation_name="擦除磁带",
                operation_description=f"擦除磁带 {tape_id}",
                category="tape",
                success=False,
                error_message="磁带擦除失败",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            raise HTTPException(status_code=500, detail="磁带擦除失败")

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"擦除磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_ERASE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="擦除磁带",
            operation_description=f"擦除磁带 {tape_id}",
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
            module="web.api.tape.operations",
            function="erase_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check-format")
async def check_tape_format(request: Request):
    """检查磁带是否已格式化
    
    判断逻辑：
    - 能成功读取到标签 -> 已格式化
    - 无法读取标签（标签文件不存在或读取失败）-> 未格式化
    - 系统错误或设备错误 -> 返回错误，不返回格式化状态
    """
    try:
        system = request.app.state.system
        if not system:
            return {
                "success": False,
                "formatted": None,  # 无法确定
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备
        try:
            devices = await system.tape_manager.get_cached_devices()
        except Exception:
            devices = []
        if not devices:
            return {
                "success": False,
                "formatted": None,  # 无法确定
                "message": "未检测到磁带设备"
            }

        # 使用 LTFS 检查格式化状态
        try:
            # 使用 LTFS 检查格式化状态
            is_formatted = await system.tape_manager.tape_operations._is_tape_formatted()

            # 尝试读取磁带标签（用于获取标签信息，60秒超时）
            try:
                metadata = await asyncio.wait_for(
                    system.tape_manager.tape_operations._read_tape_label(),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                logger.warning("读取磁带卷标超时（60秒）")
                metadata = None

            if is_formatted:
                # LTFS 确认已格式化
                if metadata and metadata.get('tape_id'):
                    return {
                        "success": True,
                        "formatted": True,
                        "metadata": metadata,
                        "message": f"磁带已格式化，标签: {metadata.get('tape_id')}"
                    }
                else:
                    # 已格式化但没有标签文件
                    return {
                        "success": True,
                        "formatted": True,
                        "metadata": None,
                        "message": "磁带已格式化（但无标签文件）"
                    }
            else:
                # LTFS 确认未格式化
                return {
                    "success": True,
                    "formatted": False,
                    "metadata": None,
                    "message": "磁带未格式化（非 LTFS 格式）"
                }
        except Exception as e:
            # 检测失败，认为未格式化
            logger.warning(f"检测磁带格式化状态失败: {str(e)}")
            return {
                "success": False,
                "formatted": False,  # 检测失败也认为未格式化
                "message": f"检测失败: {str(e)}"
            }
    
    except Exception as e:
        # 其他错误
        logger.error(f"检查磁带格式异常: {str(e)}")
        return {
            "success": False,
            "formatted": None,  # 无法确定
            "message": str(e)
        }


class FormatRequest(BaseModel):
    """格式化请求模型"""
    force: bool = False


@router.post("/format")
async def format_tape(request: Request, format_request: FormatRequest = FormatRequest()):
    """格式化磁带"""
    start_time = datetime.now()
    ip_address = request.client.host if request.client else None
    request_method = "POST"
    request_url = str(request.url)
    
    try:
        system = request.app.state.system
        if not system:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="系统未初始化",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )
            return {
                "success": False,
                "message": "系统未初始化"
            }
        
        # 检查是否有磁带设备（优先使用缓存）
        devices = await system.tape_manager.get_cached_devices()
        if not devices:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="未检测到磁带设备",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )
            return {
                "success": False,
                "message": "未检测到磁带设备"
            }
        
        # 先读取现有标签（如果有），格式化后重新写入以保持标签不变
        existing_label = None
        
        # 检查是否已格式化（能读到标签的不要格式化，60秒超时）
        try:
            try:
                existing_label = await asyncio.wait_for(
                    system.tape_manager.tape_operations._read_tape_label(),
                    timeout=60.0
                )
            except asyncio.TimeoutError:
                logger.warning("读取磁带卷标超时（60秒）")
                existing_label = None
            if existing_label and existing_label.get('tape_id'):
                # 能成功读取到标签，说明已格式化，拒绝格式化（除非强制）
                if not format_request.force:
                    tape_id = existing_label.get('tape_id')
                    await log_operation(
                        operation_type=OperationType.TAPE_FORMAT,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="格式化磁带",
                        operation_description=f"格式化磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message=f"磁带已格式化（标签: {tape_id}），如需强制格式化请使用force=true参数",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": f"磁带已格式化（标签: {tape_id}），如需强制格式化请使用force=true参数"
                    }
                else:
                    logger.info(f"强制格式化已格式化的磁带（标签: {existing_label.get('tape_id')}）")
        except FileNotFoundError:
            # 标签文件不存在，说明未格式化，可以格式化
            logger.debug("磁带标签文件不存在，可以格式化")
            existing_label = None
        except Exception as e:
            # 读取失败，无法确定是否格式化，为了安全拒绝格式化
            logger.warning(f"读取磁带标签失败，无法确定格式化状态，拒绝格式化: {str(e)}")
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message=f"无法确定磁带格式化状态，拒绝格式化: {str(e)}",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
            )
            return {
                "success": False,
                "message": f"无法确定磁带格式化状态，拒绝格式化。如需强制格式化请使用force=true参数"
            }
        
        # 使用 LTFS 格式化代替 ITDT 擦除
        try:
            from backup.tape_handler import TapeHandler
            tape_handler = TapeHandler(
                tape_manager=system.tape_manager,
                settings=system.tape_manager.settings,
                dingtalk_notifier=system.dingtalk_notifier
            )
            success, msg = await tape_handler.format_as_ltfs()
            if not success:
                logger.error(f"LTFS 格式化失败: {msg}")
        except Exception as _e:
            logger.error(f"LTFS 格式化异常: {str(_e)}")
            success = False
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if success:
            # 格式化成功，发送钉钉通知
            try:
                await system.dingtalk_notifier.send_tape_format_notification(
                    tape_id=existing_label.get('tape_id') if existing_label else '未知磁带',
                    status="success"
                )
            except Exception as notify_error:
                logger.warning(f"发送格式化钉钉通知失败: {str(notify_error)}")
            # 如果格式化前有标签，重新写入以保持标签不变
            if existing_label:
                try:
                    write_success = await system.tape_manager.tape_operations._write_tape_label(existing_label)
                    if write_success:
                        logger.info(f"格式化后重新写入磁带标签: {existing_label.get('tape_id')}")
                        await log_operation(
                            operation_type=OperationType.TAPE_FORMAT,
                            resource_type="tape",
                            resource_id=existing_label.get('tape_id'),
                            operation_name="格式化磁带",
                            operation_description=f"格式化磁带 {existing_label.get('tape_id')}，标签已保留",
                            category="tape",
                            success=True,
                            result_message="磁带格式化成功，标签已保留",
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url,
                            duration_ms=duration_ms
                        )
                        return {"success": True, "message": "磁带格式化成功，标签已保留"}
                    else:
                        logger.warning("格式化成功，但重新写入标签失败")
                        await log_operation(
                            operation_type=OperationType.TAPE_FORMAT,
                            resource_type="tape",
                            resource_id=existing_label.get('tape_id'),
                            operation_name="格式化磁带",
                            operation_description=f"格式化磁带 {existing_label.get('tape_id')}（但标签未重写）",
                            category="tape",
                            success=True,
                            result_message="磁带格式化成功（但标签未重写）",
                            ip_address=ip_address,
                            request_method=request_method,
                            request_url=request_url,
                            duration_ms=duration_ms
                        )
                        return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
                except Exception as e:
                    logger.warning(f"重新写入标签时出错: {str(e)}")
                    await log_operation(
                        operation_type=OperationType.TAPE_FORMAT,
                        resource_type="tape",
                        resource_id=existing_label.get('tape_id'),
                        operation_name="格式化磁带",
                        operation_description=f"格式化磁带 {existing_label.get('tape_id')}（但标签未重写）",
                        category="tape",
                        success=True,
                        result_message="磁带格式化成功（但标签未重写）",
                        error_message=f"重新写入标签失败: {str(e)}",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=duration_ms
                    )
                    return {"success": True, "message": "磁带格式化成功（但标签未重写）"}
            
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=True,
                result_message="磁带格式化成功",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {"success": True, "message": "磁带格式化成功"}
        else:
            await log_operation(
                operation_type=OperationType.TAPE_FORMAT,
                resource_type="tape",
                operation_name="格式化磁带",
                operation_description="格式化磁带",
                category="tape",
                success=False,
                error_message="磁带格式化失败，请检查设备状态和磁带是否正确加载",
                ip_address=ip_address,
                request_method=request_method,
                request_url=request_url,
                duration_ms=duration_ms
            )
            return {
                "success": False,
                "message": "磁带格式化失败，请检查设备状态和磁带是否正确加载"
            }

    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"格式化磁带异常: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.TAPE_FORMAT,
            resource_type="tape",
            operation_name="格式化磁带",
            operation_description="格式化磁带",
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
            module="web.api.tape.operations",
            function="format_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        return {
            "success": False,
            "message": f"格式化失败: {str(e)}"
        }


@router.post("/rewind")
async def rewind_tape(request: Request, tape_id: str = None):
    """倒带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 使用 LinuxTapeOperator 倒带
        success = await system.tape_manager.tape_operations._rewind()
        if success:
            return {"success": True, "message": "磁带倒带成功"}
        else:
            raise HTTPException(status_code=500, detail="磁带倒带失败")

    except Exception as e:
        logger.error(f"倒带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/space")
async def space_tape_blocks(request: Request, blocks: int = 1, direction: str = "forward"):
    """按块定位磁带"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # LTFS 模式下按块定位暂不支持
        raise HTTPException(status_code=501, detail="磁带按块定位暂不支持（LTFS 模式）")

    except Exception as e:
        logger.error(f"磁带定位失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


