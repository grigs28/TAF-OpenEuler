#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
公共通知发送模块
Public Notification Module

统一从数据库读取启用的通知人员，支持批量发送钉钉通知。
所有需要发送通知的地方都应通过此模块发送，确保通知人员列表一致。

用法：
    # 异步（在 async 函数中）
    from utils.notify import notify
    await notify(notifier, "标题", "内容")

    # 同步（在线程中）
    from utils.notify import notify_sync
    notify_sync(notifier, "标题", "内容")
"""

import asyncio
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

# 缓存通知人员列表（避免每次发送都查数据库）
_phones_cache: List[str] = []
_cache_valid = False


def invalidate_phones_cache():
    """清除手机号缓存，在添加/修改/删除通知人员时调用"""
    global _phones_cache, _cache_valid
    _phones_cache = []
    _cache_valid = False


async def get_enabled_phones() -> List[str]:
    """从数据库查询所有启用的通知人员手机号

    Returns:
        List[str]: 去重后的手机号列表
    """
    phones = []

    try:
        from utils.scheduler.db_utils import is_opengauss, is_sqlite

        if is_opengauss():
            from utils.scheduler.db_utils import get_opengauss_connection

            async with get_opengauss_connection() as conn:
                rows = await conn.fetch(
                    "SELECT phone FROM notification_users WHERE enabled = TRUE"
                )
                phones = [row["phone"] for row in rows if row["phone"]]

        elif is_sqlite():
            import aiosqlite
            from config.settings import get_settings

            settings = get_settings()
            db_path = settings.SQLITE_DB_FILE or "data/taf_backup.db"

            async with aiosqlite.connect(db_path) as conn:
                conn.row_factory = aiosqlite.Row
                cursor = await conn.execute(
                    "SELECT phone FROM notification_users WHERE enabled = 1"
                )
                rows = await cursor.fetchall()
                phones = [row["phone"] for row in rows if row["phone"]]

    except Exception as e:
        logger.debug(f"查询通知人员失败: {e}")

    return phones


async def get_all_phones(extra_phones: Optional[List[str]] = None) -> List[str]:
    """获取所有通知目标手机号（数据库人员 + 默认手机号 + 额外手机号）

    Args:
        extra_phones: 调用方额外传入的手机号列表

    Returns:
        List[str]: 去重后的手机号列表
    """
    phones = []

    # 1. 从数据库获取启用的通知人员
    db_phones = await get_enabled_phones()
    phones.extend(db_phones)

    # 2. 加入 .env 中配置的默认手机号
    try:
        from config.settings import get_settings
        settings = get_settings()
        default_phone = settings.DINGTALK_DEFAULT_PHONE
        if default_phone and default_phone not in phones:
            phones.append(default_phone)
    except Exception:
        pass

    # 3. 加入调用方传入的额外手机号
    if extra_phones:
        for p in extra_phones:
            if p and p not in phones:
                phones.append(p)

    return phones


async def notify(
    notifier,
    title: str,
    content: str,
    message_type: str = "markdown",
    extra_phones: Optional[List[str]] = None,
):
    """发送通知给所有启用的通知人员（异步版本）

    Args:
        notifier: DingTalkNotifier 实例
        title: 消息标题
        content: 消息内容（Markdown 格式）
        message_type: 消息类型，默认 "markdown"
        extra_phones: 额外的手机号列表
    """
    if not notifier:
        logger.warning("通知器实例为空，跳过发送")
        return

    # 获取所有目标手机号
    phones = await get_all_phones(extra_phones)

    if not phones:
        logger.warning("没有可用的通知人员，跳过发送")
        return

    # 发送通知
    if len(phones) == 1:
        await notifier.send_message(phones[0], title, content, message_type)
    else:
        await notifier.send_batch_message(phones, title, content, message_type)


def notify_sync(
    notifier,
    title: str,
    content: str,
    message_type: str = "markdown",
    extra_phones: Optional[List[str]] = None,
):
    """发送通知给所有启用的通知人员（同步版本，供线程中使用）

    Args:
        notifier: DingTalkNotifier 实例
        title: 消息标题
        content: 消息内容
        message_type: 消息类型
        extra_phones: 额外的手机号列表
    """
    if not notifier:
        logger.warning("通知器实例为空，跳过发送")
        return

    # 在同步上下文中获取手机号列表
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, get_all_phones(extra_phones))
                phones = future.result(timeout=10)
        else:
            phones = loop.run_until_complete(get_all_phones(extra_phones))
    except RuntimeError:
        phones = asyncio.run(get_all_phones(extra_phones))
    except Exception as e:
        logger.warning(f"获取通知人员列表失败: {e}")
        try:
            from config.settings import get_settings
            default_phone = get_settings().DINGTALK_DEFAULT_PHONE
            phones = [default_phone] if default_phone else []
        except Exception:
            phones = []

    if not phones:
        logger.warning("没有可用的通知人员，跳过发送")
        return

    if len(phones) == 1:
        notifier.send_message_sync(phones[0], title, content, message_type)
    else:
        for phone in phones:
            notifier.send_message_sync(phone, title, content, message_type)
