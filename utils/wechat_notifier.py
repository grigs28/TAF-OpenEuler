#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微信通知模块
WeChat Notification Module

支持企业微信机器人 Webhook 推送
"""

import httpx
import logging
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class WeChatNotifier:
    """微信通知类（企业微信机器人）"""

    def __init__(
        self,
        webhook_url: str,
        enabled: bool = True,
        timeout: int = 10
    ):
        """
        初始化微信通知器

        Args:
            webhook_url: 企业微信机器人 Webhook URL
            enabled: 是否启用通知
            timeout: 请求超时时间（秒）
        """
        self.webhook_url = webhook_url
        self.enabled = enabled and bool(webhook_url)
        self.timeout = timeout

    async def send_message(
        self,
        content: str,
        mentioned_list: Optional[list[str]] = None
    ) -> bool:
        """
        发送文本消息

        Args:
            content: 消息内容
            mentioned_list: @用户列表（手机号或userid）

        Returns:
            bool: 是否发送成功
        """
        if not self.enabled:
            logger.debug("微信通知未启用，跳过发送")
            return False

        payload = {
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": mentioned_list or []
            }
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload
                )
                result = response.json()

                if result.get("errcode") == 0:
                    logger.info(f"微信通知发送成功: {content[:50]}...")
                    return True
                else:
                    logger.error(f"微信通知发送失败: {result}")
                    return False

        except Exception as e:
            logger.error(f"微信通知发送异常: {e}")
            return False

    async def send_markdown(self, content: str) -> bool:
        """
        发送 Markdown 消息

        Args:
            content: Markdown 格式消息内容

        Returns:
            bool: 是否发送成功
        """
        if not self.enabled:
            return False

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.webhook_url,
                    json=payload
                )
                result = response.json()

                if result.get("errcode") == 0:
                    logger.info("微信 Markdown 通知发送成功")
                    return True
                else:
                    logger.error(f"微信通知发送失败: {result}")
                    return False

        except Exception as e:
            logger.error(f"微信通知发送异常: {e}")
            return False

    async def send_backup_report(
        self,
        task_name: str,
        status: str,
        progress: float,
        files_processed: int,
        bytes_processed: int,
        speed: Optional[float] = None,
        error: Optional[str] = None
    ) -> bool:
        """
        发送备份进度汇报

        Args:
            task_name: 任务名称
            status: 任务状态
            progress: 进度百分比 (0-100)
            files_processed: 已处理文件数
            bytes_processed: 已处理字节数
            speed: 处理速度 (MB/s)
            error: 错误信息

        Returns:
            bool: 是否发送成功
        """
        # 格式化字节数
        def format_bytes(b: int) -> str:
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if b < 1024:
                    return f"{b:.2f} {unit}"
                b /= 1024
            return f"{b:.2f} PB"

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        content = f"""## 📦 磁带备份进度汇报

> 时间: {now}

**任务名称**: {task_name}
**当前状态**: {status}
**完成进度**: {progress:.1f}%

**处理统计**:
- 文件数: {files_processed:,}
- 数据量: {format_bytes(bytes_processed)}
- 速度: {speed:.2f} MB/s" + (f" ({speed:.2f} MB/s)" if speed else "")

"""
        if error:
            content += f"\n⚠️ **错误**: {error}\n"

        return await self.send_markdown(content)


# 全局实例（延迟初始化）
_wechat_notifier: Optional[WeChatNotifier] = None


def get_wechat_notifier() -> Optional[WeChatNotifier]:
    """获取微信通知器实例"""
    global _wechat_notifier
    return _wechat_notifier


def init_wechat_notifier(webhook_url: str, enabled: bool = True) -> WeChatNotifier:
    """初始化微信通知器"""
    global _wechat_notifier
    _wechat_notifier = WeChatNotifier(webhook_url=webhook_url, enabled=enabled)
    return _wechat_notifier
