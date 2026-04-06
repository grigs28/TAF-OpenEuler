#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - notification
System Management API - notification
"""

import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .models import DingTalkConfig, NotificationEvents, NotificationUser, SyslogConfig
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/notification/config")
async def get_notification_config():
    """获取钉钉通知配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()
        
        return {
            "success": True,
            "config": {
                "dingtalk_api_url": settings.DINGTALK_API_URL,
                "dingtalk_api_key": settings.DINGTALK_API_KEY,
                "dingtalk_default_phone": settings.DINGTALK_DEFAULT_PHONE
            }
        }
        
    except Exception as e:
        logger.error(f"获取通知配置失败: {str(e)}")
        return {"success": False, "message": str(e)}


@router.put("/notification/config")
async def update_notification_config(config: DingTalkConfig, request: Request):
    """更新钉钉通知配置"""
    start_time = datetime.now()
    old_values = {}
    new_values = {}
    
    try:
        from pathlib import Path
        from config.settings import get_settings
        
        # 获取当前配置，用于记录旧值
        current_settings = get_settings()
        old_values = {
            "dingtalk_api_url": current_settings.DINGTALK_API_URL or "",
            "dingtalk_api_key": current_settings.DINGTALK_API_KEY or "",
            "dingtalk_default_phone": current_settings.DINGTALK_DEFAULT_PHONE or ""
        }
        
        # 使用EnvFileManager保存配置到.env文件
        from config.env_file_manager import get_env_manager
        
        env_manager = get_env_manager()
        
        # 构建更新字典
        updates = {
            "DINGTALK_API_URL": config.dingtalk_api_url or "",
            "DINGTALK_API_KEY": config.dingtalk_api_key or "",
            "DINGTALK_DEFAULT_PHONE": config.dingtalk_default_phone or ""
        }
        
        # 使用EnvFileManager写入文件（自动处理备份和并发问题）
        success = env_manager.write_env_file(updates, backup=True)
        
        if not success:
            raise ValueError("写入.env文件失败")
        
        # 记录新值（隐藏敏感信息）
        new_values = {
            "dingtalk_api_url": config.dingtalk_api_url,
            "dingtalk_api_key": "***" if config.dingtalk_api_key else "",  # 隐藏API密钥
            "dingtalk_default_phone": config.dingtalk_default_phone
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
            resource_type="notification",
            resource_name="通知配置",
            operation_name="更新通知配置",
            operation_description="更新钉钉通知配置",
            category="system",
            success=True,
            result_message="通知配置更新成功",
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
            message="钉钉通知配置已更新",
            details={
                "dingtalk_api_url": config.dingtalk_api_url,
                "dingtalk_default_phone": config.dingtalk_default_phone
            },
            module="web.api.system.notification",
            function="update_notification_config"
        )
        
        logger.info("钉钉通知配置已更新")
        
        return {
            "success": True,
            "message": "通知配置更新成功"
        }
        
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="notification",
            resource_name="通知配置",
            operation_name="更新通知配置",
            operation_description=f"更新通知配置失败: {error_msg}",
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
            message=f"更新通知配置失败: {error_msg}",
            details={
                "error": error_msg,
                "config": {
                    "dingtalk_api_url": config.dingtalk_api_url,
                    "dingtalk_default_phone": config.dingtalk_default_phone
                }
            },
            module="web.api.system.notification",
            function="update_notification_config",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        logger.error(f"更新通知配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.post("/notification/test")
async def test_notification(request: Request, phone: str = ""):
    """测试钉钉通知（发送给数据库中所有启用的通知人员）"""
    try:
        system = request.app.state.system
        if not system or not hasattr(system, 'dingtalk_notifier') or not system.dingtalk_notifier:
            return {"success": False, "message": "钉钉通知器未初始化"}

        from utils.notify import notify, get_all_phones

        # 获取所有通知人员（数据库 + 默认手机号 + 额外手机号）
        extra = [phone] if phone else None
        phones = await get_all_phones(extra)

        if not phones:
            return {"success": False, "message": "没有可用的通知人员"}

        # 使用程序中的统一通知函数发送
        await notify(
            notifier=system.dingtalk_notifier,
            title="测试通知",
            content="这是一条测试通知，用于验证钉钉通知配置是否正常工作。",
            message_type="markdown",
            extra_phones=extra
        )

        logger.info(f"测试通知已发送给: {', '.join(phones)}")
        return {
            "success": True,
            "message": f"测试通知已发送给: {', '.join(phones)}"
        }

    except Exception as e:
        logger.error(f"测试通知失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notification/events")
async def get_notification_events():
    """获取通知事件配置"""
    try:
        from pathlib import Path
        import json
        from config.settings import get_settings
        
        # 从.env文件读取
        env_file = Path(".env")
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    # 跳过注释和空行
                    if not line or line.startswith('#'):
                        continue
                    if line.startswith("NOTIFICATION_EVENTS="):
                        events_json = line.split("=", 1)[1].strip()
                        # 处理可能的引号（单引号或双引号）
                        if events_json.startswith('"') and events_json.endswith('"'):
                            events_json = events_json[1:-1].replace('\\"', '"')
                        elif events_json.startswith("'") and events_json.endswith("'"):
                            events_json = events_json[1:-1].replace("\\'", "'")
                        # 尝试解析 JSON
                        try:
                            events_dict = json.loads(events_json)
                            # 验证是否为字典类型
                            if isinstance(events_dict, dict):
                                return {
                                    "success": True,
                                    "events": events_dict
                                }
                            else:
                                logger.warning(f"NOTIFICATION_EVENTS 配置不是字典类型，使用默认配置")
                        except json.JSONDecodeError as json_err:
                            logger.warning(
                                f"解析 NOTIFICATION_EVENTS JSON 失败: {str(json_err)}，"
                                f"原始值: {events_json[:100]}，使用默认配置"
                            )
        
        # 如果.env中没有，返回默认配置
        default_events = {
            "notify_backup_success": True,
            "notify_backup_started": True,
            "notify_backup_failed": True,
            "notify_recovery_success": True,
            "notify_recovery_failed": True,
            "notify_tape_change": True,
            "notify_tape_expired": True,
            "notify_tape_error": True,
            "notify_capacity_warning": True,
            "notify_system_error": True,
            "notify_system_started": True
        }
        
        return {
            "success": True,
            "events": default_events
        }
        
    except Exception as e:
        logger.error(f"获取通知事件配置失败: {str(e)}")
        return {
            "success": False,
            "message": str(e)
        }


@router.put("/notification/events")
async def update_notification_events(events: NotificationEvents, request: Request):
    """更新通知事件配置"""
    start_time = datetime.now()
    old_values = {}
    new_values = {}
    
    try:
        from pathlib import Path
        import json
        from config.settings import get_settings
        
        # 获取当前配置，用于记录旧值
        current_settings = get_settings()
        # 尝试从.env文件读取旧的事件配置
        env_file = Path(".env")
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line_stripped = line.strip()
                    # 跳过注释和空行
                    if not line_stripped or line_stripped.startswith('#'):
                        continue
                    if line_stripped.startswith("NOTIFICATION_EVENTS="):
                        try:
                            events_json = line_stripped.split("=", 1)[1].strip()
                            # 处理可能的引号（单引号或双引号）
                            if events_json.startswith('"') and events_json.endswith('"'):
                                events_json = events_json[1:-1].replace('\\"', '"')
                            elif events_json.startswith("'") and events_json.endswith("'"):
                                events_json = events_json[1:-1].replace("\\'", "'")
                            old_values = json.loads(events_json)
                        except Exception as parse_err:
                            logger.warning(f"解析旧的通知事件配置失败: {str(parse_err)}")
                        break
        
        # 如果未找到旧值，使用默认值
        if not old_values:
            old_values = {
                "notify_backup_success": True,
                "notify_backup_started": True,
                "notify_backup_failed": True,
                "notify_recovery_success": True,
                "notify_recovery_failed": True,
                "notify_tape_change": True,
                "notify_tape_expired": True,
                "notify_tape_error": True,
                "notify_capacity_warning": True,
                "notify_system_error": True,
                "notify_system_started": True
            }
        
        # 将事件配置转换为JSON字符串
        events_dict = events.dict()
        events_json = json.dumps(events_dict, ensure_ascii=False)
        new_values = events_dict
        
        # 使用EnvFileManager保存配置到.env文件
        from config.env_file_manager import get_env_manager
        
        env_manager = get_env_manager()
        
        # 构建更新字典
        updates = {
            "NOTIFICATION_EVENTS": events_json
        }
        
        # 使用EnvFileManager写入文件（自动处理备份和并发问题）
        success = env_manager.write_env_file(updates, backup=True)
        
        if not success:
            raise ValueError("写入.env文件失败")
        
        # 重新加载配置，使后续调用get_settings()能获取最新配置
        from config.settings import reload_settings
        reload_settings()
        logger.info("通知事件配置已重新加载，新配置将立即生效")
        
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
            resource_type="notification",
            resource_name="通知事件配置",
            operation_name="更新通知事件配置",
            operation_description="更新通知事件配置",
            category="system",
            success=True,
            result_message="通知事件配置更新成功",
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
            message="通知事件配置已更新",
            details=new_values,
            module="web.api.system.notification",
            function="update_notification_events"
        )
        
        logger.info("通知事件配置已更新")
        
        return {
            "success": True,
            "message": "通知事件配置更新成功"
        }
        
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="notification",
            resource_name="通知事件配置",
            operation_name="更新通知事件配置",
            operation_description=f"更新通知事件配置失败: {error_msg}",
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
            message=f"更新通知事件配置失败: {error_msg}",
            details={
                "error": error_msg,
                "events": events.dict() if events else {}
            },
            module="web.api.system.notification",
            function="update_notification_events",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        logger.error(f"更新通知事件配置失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


# ===== 通知人员管理API =====

@router.get("/notification/users")
async def get_notification_users(request: Request):
    """获取通知人员列表"""
    try:
        # 使用原生SQL查询
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch("""
                SELECT id, phone, name, remark, enabled, created_at, updated_at, created_by, updated_by
                FROM notification_users
                ORDER BY created_at DESC
            """)
            users = []
            for row in rows:
                users.append({
                    "id": row["id"],
                    "phone": row["phone"],
                    "name": row["name"],
                    "remark": row["remark"],
                    "enabled": row["enabled"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "created_by": row["created_by"],
                    "updated_by": row["updated_by"]
                })
            return {"success": True, "users": users}

    except Exception as e:
        logger.error(f"获取通知人员列表失败: {str(e)}", exc_info=True)
        return {"success": False, "message": str(e)}


@router.post("/notification/users")
async def create_notification_user(user: NotificationUser, request: Request):
    """创建通知人员"""
    start_time = datetime.now()
    
    try:
        # 使用 psycopg 同步连接插入（与添加磁带相同的方式）
        from utils.db_connection_helper import get_psycopg_connection_from_url, set_autocommit
        from config.settings import get_settings as _get_settings

        _settings = _get_settings()
        _conn, _is_psycopg3 = get_psycopg_connection_from_url(_settings.DATABASE_URL, prefer_psycopg3=True)
        try:
            set_autocommit(_conn, _is_psycopg3, autocommit=True)
            with _conn.cursor() as cur:
                # 检查手机号是否已存在
                cur.execute("SELECT id FROM notification_users WHERE phone = %s", (user.phone,))
                if cur.fetchone():
                    raise ValueError(f"手机号 {user.phone} 已存在")

                # 插入新记录
                cur.execute(
                    """
                    INSERT INTO notification_users (phone, name, remark, enabled, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        user.phone,
                        user.name,
                        user.remark,
                        user.enabled,
                        datetime.now(),
                        datetime.now()
                    )
                )
                user_id = cur.fetchone()[0]
        finally:
            _conn.close()

        # 记录操作日志
        client_ip = request.client.host if request.client else None
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="notification",
            resource_id=str(user_id),
            resource_name=f"通知人员: {user.name} ({user.phone})",
            operation_name="创建通知人员",
            operation_description=f"创建通知人员: {user.name} ({user.phone})",
            category="system",
            success=True,
            result_message="通知人员创建成功",
            new_values={
                "phone": user.phone,
                "name": user.name,
                "remark": user.remark,
                "enabled": user.enabled
            },
            ip_address=client_ip,
            request_method="POST",
            request_url=str(request.url),
            duration_ms=duration_ms
        )

        return {"success": True, "message": "通知人员创建成功", "user_id": user_id}
                
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.CREATE,
            resource_type="notification",
            resource_name=f"通知人员: {user.name} ({user.phone})",
            operation_name="创建通知人员",
            operation_description=f"创建通知人员失败: {error_msg}",
            category="system",
            success=False,
            error_message=error_msg,
            ip_address=client_ip,
            request_method="POST",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"创建通知人员失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.put("/notification/users/{user_id}")
async def update_notification_user(user_id: int, user: NotificationUser, request: Request):
    """更新通知人员"""
    start_time = datetime.now()
    old_values = {}
    
    try:
        # 使用原生SQL更新
        async with get_opengauss_connection() as conn:
            # 获取旧值
            old_row = await conn.fetchrow(
                "SELECT phone, name, remark, enabled FROM notification_users WHERE id = $1",
                user_id
            )
            if not old_row:
                raise ValueError(f"通知人员 ID {user_id} 不存在")

            old_values = {
                "phone": old_row["phone"],
                "name": old_row["name"],
                "remark": old_row["remark"],
                "enabled": old_row["enabled"]
            }

            # 检查手机号是否已被其他用户使用
            existing = await conn.fetchrow(
                "SELECT id FROM notification_users WHERE phone = $1 AND id != $2",
                user.phone,
                user_id
            )
            if existing:
                raise ValueError(f"手机号 {user.phone} 已被其他用户使用")

            # 更新记录
            await conn.execute(
                """
                UPDATE notification_users
                SET phone = $1, name = $2, remark = $3, enabled = $4, updated_at = $5
                WHERE id = $6
                """,
                user.phone,
                user.name,
                user.remark,
                user.enabled,
                datetime.now(),
                user_id
            )

            # 记录操作日志
            new_values = {
                "phone": user.phone,
                "name": user.name,
                "remark": user.remark,
                "enabled": user.enabled
            }
            changed_fields = [key for key in new_values if old_values.get(key) != new_values.get(key)]

            client_ip = request.client.host if request.client else None
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_operation(
                operation_type=OperationType.UPDATE,
                resource_type="notification",
                resource_id=str(user_id),
                resource_name=f"通知人员: {user.name} ({user.phone})",
                operation_name="更新通知人员",
                operation_description=f"更新通知人员: {user.name} ({user.phone})",
                category="system",
                success=True,
                result_message="通知人员更新成功",
                old_values=old_values,
                new_values=new_values,
                changed_fields=changed_fields,
                ip_address=client_ip,
                request_method="PUT",
                request_url=str(request.url),
                duration_ms=duration_ms
            )

            return {"success": True, "message": "通知人员更新成功"}
                
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="notification",
            resource_id=str(user_id),
            resource_name=f"通知人员: {user.name} ({user.phone})",
            operation_name="更新通知人员",
            operation_description=f"更新通知人员失败: {error_msg}",
            category="system",
            success=False,
            error_message=error_msg,
            old_values=old_values,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"更新通知人员失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


@router.delete("/notification/users/{user_id}")
async def delete_notification_user(user_id: int, request: Request):
    """删除通知人员"""
    start_time = datetime.now()
    old_values = {}
    
    try:
        # 使用原生SQL删除
        async with get_opengauss_connection() as conn:
            # 获取旧值
            old_row = await conn.fetchrow(
                "SELECT phone, name, remark, enabled FROM notification_users WHERE id = $1",
                user_id
            )
            if not old_row:
                raise ValueError(f"通知人员 ID {user_id} 不存在")

            old_values = {
                "phone": old_row["phone"],
                "name": old_row["name"],
                "remark": old_row["remark"],
                "enabled": old_row["enabled"]
            }

            # 删除记录
            await conn.execute(
                "DELETE FROM notification_users WHERE id = $1",
                user_id
            )

            # 记录操作日志
            client_ip = request.client.host if request.client else None
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_operation(
                operation_type=OperationType.DELETE,
                resource_type="notification",
                resource_id=str(user_id),
                resource_name=f"通知人员: {old_values.get('name')} ({old_values.get('phone')})",
                operation_name="删除通知人员",
                operation_description=f"删除通知人员: {old_values.get('name')} ({old_values.get('phone')})",
                category="system",
                success=True,
                result_message="通知人员删除成功",
                old_values=old_values,
                ip_address=client_ip,
                request_method="DELETE",
                request_url=str(request.url),
                duration_ms=duration_ms
            )

            return {"success": True, "message": "通知人员删除成功"}
                
    except Exception as e:
        error_msg = str(e)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        # 记录失败的操作日志
        client_ip = request.client.host if request.client else None
        await log_operation(
            operation_type=OperationType.DELETE,
            resource_type="notification",
            resource_id=str(user_id),
            resource_name="通知人员",
            operation_name="删除通知人员",
            operation_description=f"删除通知人员失败: {error_msg}",
            category="system",
            success=False,
            error_message=error_msg,
            old_values=old_values,
            ip_address=client_ip,
            request_method="DELETE",
            request_url=str(request.url),
            duration_ms=duration_ms
        )
        
        logger.error(f"删除通知人员失败: {error_msg}", exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)


# ========== Syslog 转发配置 ==========

@router.get("/notification/syslog")
async def get_syslog_config():
    """获取 Syslog 转发配置"""
    try:
        from config.settings import get_settings
        settings = get_settings()

        return {
            "success": True,
            "config": {
                "syslog_enabled": settings.SYSLOG_ENABLED,
                "syslog_host": settings.SYSLOG_HOST,
                "syslog_port": settings.SYSLOG_PORT,
                "syslog_level": settings.SYSLOG_LEVEL
            }
        }
    except Exception as e:
        logger.error(f"获取 Syslog 配置失败: {str(e)}")
        return {"success": False, "message": str(e)}


@router.put("/notification/syslog")
async def update_syslog_config(config: SyslogConfig, request: Request):
    """更新 Syslog 转发配置"""
    start_time = datetime.now()

    try:
        from config.settings import get_settings, reload_settings
        from config.env_file_manager import EnvFileManager

        current_settings = get_settings()
        old_values = {
            "syslog_enabled": current_settings.SYSLOG_ENABLED,
            "syslog_host": current_settings.SYSLOG_HOST,
            "syslog_port": current_settings.SYSLOG_PORT,
            "syslog_level": current_settings.SYSLOG_LEVEL
        }

        # 验证级别
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        level = config.syslog_level.upper()
        if level not in valid_levels:
            return {"success": False, "message": f"无效的日志级别: {config.syslog_level}"}

        # 验证端口
        if not (1 <= config.syslog_port <= 65535):
            return {"success": False, "message": "端口范围应为 1-65535"}

        # 写入 .env
        env_manager = EnvFileManager()
        updates = {
            "SYSLOG_ENABLED": str(config.syslog_enabled).lower(),
            "SYSLOG_HOST": config.syslog_host,
            "SYSLOG_PORT": str(config.syslog_port),
            "SYSLOG_LEVEL": level
        }
        env_manager.write_env_file(updates, backup=True)

        # 重新加载配置
        reload_settings()

        # 动态更新 syslog handler（不重启服务）
        _update_syslog_handler()

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        # 记录操作日志
        client_ip = request.client.host if request.client else "unknown"
        await log_operation(
            operation_type=OperationType.CONFIG,
            resource_type="system",
            resource_id="syslog",
            resource_name="Syslog转发配置",
            operation_name="更新Syslog配置",
            operation_description=f"更新Syslog配置: {config.syslog_host}:{config.syslog_port}",
            category="system",
            success=True,
            old_values=old_values,
            new_values=updates,
            ip_address=client_ip,
            request_method="PUT",
            request_url=str(request.url),
            duration_ms=duration_ms
        )

        return {
            "success": True,
            "message": "Syslog 配置已更新",
            "config": updates
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"更新 Syslog 配置失败: {error_msg}", exc_info=True)
        return {"success": False, "message": error_msg}


@router.post("/notification/syslog/test")
async def test_syslog_connection(request: Request):
    """测试 Syslog 连接（JSON 格式，VictoriaLogs 兼容）"""
    try:
        from config.settings import get_settings
        settings = get_settings()
        host = settings.SYSLOG_HOST
        port = settings.SYSLOG_PORT

        _send_syslog_json("Syslog 连接测试 - " + datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') + " UTC")

        return {
            "success": True,
            "message": f"测试消息已发送到 {host}:{port}"
        }

    except socket.timeout:
        return {"success": False, "message": f"连接超时: {host}:{port}"}
    except Exception as e:
        return {"success": False, "message": f"连接失败: {str(e)}"}


def _send_syslog_json(message: str, level_name: str = "INFO"):
    """通过原生 socket 发送 JSON 格式日志到 Vector/VictoriaLogs

    字段说明：
    - _msg: VictoriaLogs 要求的日志内容字段
    - timestamp: UTC 时间戳
    - level: 日志级别
    - log_source: 日志来源主机名
    """
    import socket as _socket
    from datetime import datetime as _dt
    from config.settings import get_settings

    settings = get_settings()
    if not settings.SYSLOG_ENABLED:
        return

    import platform
    hostname = platform.node().split('.')[0] or "taf"

    data = json.dumps({
        "timestamp": _dt.utcnow().isoformat() + "Z",
        "hostname": hostname,
        "app_name": "taf",
        "level": level_name.lower(),
        "_msg": message,
        "log_source": hostname,
    })

    try:
        sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        sock.sendto(data.encode('utf-8'), (settings.SYSLOG_HOST, settings.SYSLOG_PORT))
        sock.close()
    except Exception:
        pass


def _update_syslog_handler():
    """动态更新 syslog handler（配置变更后调用）

    使用自定义 Handler 通过原生 socket 发送 JSON 格式到 Vector/VictoriaLogs。
    """
    from config.settings import get_settings

    settings = get_settings()
    root_logger = logging.getLogger()

    # 移除现有的自定义 syslog handler
    for handler in root_logger.handlers[:]:
        if isinstance(handler, _JsonSyslogHandler):
            root_logger.removeHandler(handler)
            handler.close()

    # 如果启用，重新添加
    if settings.SYSLOG_ENABLED:
        try:
            handler = _JsonSyslogHandler(settings)
            handler.setLevel(getattr(logging, settings.SYSLOG_LEVEL.upper(), logging.WARNING))
            root_logger.addHandler(handler)
            logger.info(f"Syslog handler 已更新: {settings.SYSLOG_HOST}:{settings.SYSLOG_PORT} (级别: {settings.SYSLOG_LEVEL})")
        except Exception as e:
            logger.warning(f"更新 Syslog handler 失败: {str(e)}")
    else:
        logger.info("Syslog 转发已关闭")


class _JsonSyslogHandler(logging.Handler):
    """自定义 Handler：将日志以 JSON 格式通过原生 socket 发送到 Vector"""

    def __init__(self, settings):
        super().__init__()
        self.host = settings.SYSLOG_HOST
        self.port = settings.SYSLOG_PORT

    def emit(self, record):
        try:
            _send_syslog_json(
                message=record.getMessage(),
                level_name=record.levelname,
            )
        except Exception:
            pass
