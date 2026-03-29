#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TAF 系统示例脚本 - openEuler 环境
Example Script for TAF System - openEuler Environment

此脚本演示如何使用 TAF 系统的 API 来：
1. 创建磁带记录
2. 创建并执行一次性备份任务

使用方法：
    python scripts/example_openEuler_backup.py

注意：
    - 确保 TAF 系统已启动（python main.py）
    - 确保数据库和磁带设备已正确配置
"""

import asyncio
import httpx
import json
from datetime import datetime
from typing import Optional, Dict, Any

# TAF 系统配置
TAF_BASE_URL = "http://localhost:8081"  # 根据 .env 中的 WEB_PORT 配置
API_PREFIX = "/api/v1"

# 默认配置
DEFAULT_TIMEOUT = 30.0


class TAFClient:
    """TAF API 客户端"""

    def __init__(self, base_url: str = TAF_BASE_URL):
        self.base_url = base_url
        self.api_prefix = API_PREFIX

    def _get_url(self, endpoint: str) -> str:
        """获取完整的 API URL"""
        return f"{self.base_url}{self.api_prefix}{endpoint}"

    async def create_tape(
        self,
        label: Optional[str] = None,
        serial_number: Optional[str] = None,
        capacity_gb: int = 18000,  # 18TB
        retention_months: int = 12,
        media_type: str = "LTO",
        generation: int = 9,
        location: str = "",
        notes: str = "",
        format_tape: bool = False,
        create_year: Optional[int] = None,
        create_month: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        创建磁带记录

        Args:
            label: 磁带卷标（可选，不指定则自动生成）
            serial_number: 序列号（可选，不指定则自动生成）
            capacity_gb: 容量（GB）
            retention_months: 保留月数
            media_type: 介质类型
            generation: 代次
            location: 存放位置
            notes: 备注
            format_tape: 是否格式化磁带（openEuler 下通常为 False）
            create_year: 创建年份
            create_month: 创建月份

        Returns:
            API 响应
        """
        url = self._get_url("/tape/create")

        payload = {
            "capacity_gb": capacity_gb,
            "retention_months": retention_months,
            "media_type": media_type,
            "generation": generation,
            "location": location,
            "notes": notes,
            "format_tape": format_tape
        }

        if label:
            payload["label"] = label
        if serial_number:
            payload["serial_number"] = serial_number
        if create_year:
            payload["create_year"] = create_year
        if create_month:
            payload["create_month"] = create_month

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

    async def create_one_time_backup(
        self,
        source_paths: list,
        tape_id: Optional[str] = None,
        task_name: Optional[str] = None,
        description: Optional[str] = None,
        compression_enabled: bool = True,
        exclude_patterns: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        创建一次性备份任务

        Args:
            source_paths: 源路径列表
            tape_id: 目标磁带ID（可选，不指定则自动选择）
            task_name: 任务名称
            description: 任务描述
            compression_enabled: 是否启用压缩
            exclude_patterns: 排除模式

        Returns:
            API 响应
        """
        url = self._get_url("/backup/one-time")

        payload = {
            "source_paths": source_paths,
            "compression_enabled": compression_enabled
        }

        if tape_id:
            payload["tape_id"] = tape_id
        if task_name:
            payload["task_name"] = task_name
        if description:
            payload["description"] = description
        if exclude_patterns:
            payload["exclude_patterns"] = exclude_patterns

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return response.json()

    async def query_tapes(self, status: Optional[str] = None) -> Dict[str, Any]:
        """
        查询磁带列表

        Args:
            status: 磁带状态过滤（available, used, full, error）

        Returns:
            API 响应
        """
        url = self._get_url("/tape/list")

        params = {}
        if status:
            params["status"] = status

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    async def query_backup_tasks(self, task_id: Optional[int] = None) -> Dict[str, Any]:
        """
        查询备份任务

        Args:
            task_id: 任务ID（可选）

        Returns:
            API 响应
        """
        if task_id:
            url = self._get_url(f"/backup/tasks/{task_id}")
        else:
            url = self._get_url("/backup/tasks")

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    async def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        url = f"{self.base_url}/health"

        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()


async def main():
    """主函数 - 演示如何使用 TAF API"""

    print("=" * 60)
    print("TAF 系统示例脚本 - openEuler 环境")
    print("=" * 60)
    print()

    client = TAFClient()

    # 1. 健康检查
    print("1. 健康检查...")
    try:
        health = await client.health_check()
        print(f"   系统状态: {health.get('status', 'unknown')}")
        print("   ✓ 系统正常运行")
    except Exception as e:
        print(f"   ✗ 系统连接失败: {e}")
        print("   请确保 TAF 系统已启动: python main.py")
        return
    print()

    # 2. 查询现有磁带
    print("2. 查询可用磁带...")
    try:
        tapes = await client.query_tapes(status="available")
        tape_list = tapes.get("tapes", [])
        print(f"   可用磁带数量: {len(tape_list)}")

        if tape_list:
            print("   现有磁带:")
            for tape in tape_list[:5]:  # 只显示前5个
                print(f"     - {tape.get('tape_id')}: {tape.get('label')}, 容量: {tape.get('capacity_bytes', 0) / (1024**3):.1f}GB")
    except Exception as e:
        print(f"   查询磁带失败: {e}")
        tape_list = []
    print()

    # 3. 创建新磁带（如果需要）
    create_new_tape = False  # 设置为 True 以创建新磁带

    if create_new_tape:
        print("3. 创建新磁带...")
        now = datetime.now()
        try:
            result = await client.create_tape(
                create_year=now.year,
                create_month=now.month,
                capacity_gb=18000,  # 18TB
                retention_months=12,
                notes="示例脚本创建的磁带",
                format_tape=False  # openEuler 下不自动格式化
            )
            print(f"   ✓ 磁带创建成功: {result.get('tape_id')}, 卷标: {result.get('label')}")
            tape_id = result.get('tape_id')
        except Exception as e:
            print(f"   ✗ 创建磁带失败: {e}")
            tape_id = tape_list[0].get('tape_id') if tape_list else None
    else:
        tape_id = tape_list[0].get('tape_id') if tape_list else None
        if tape_id:
            print(f"3. 使用现有磁带: {tape_id}")
        else:
            print("3. 没有可用磁带，请先创建磁带")
            print("   提示: 在脚本中设置 create_new_tape = True 以创建新磁带")
    print()

    # 4. 创建一次性备份任务
    if tape_id:
        print("4. 创建一次性备份任务...")

        # 配置备份源路径（根据实际情况修改）
        source_paths = [
            "/mnt/SSD/data",  # 示例路径，请根据实际情况修改
        ]

        try:
            result = await client.create_one_time_backup(
                source_paths=source_paths,
                tape_id=tape_id,
                task_name=f"示例备份_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                description="使用示例脚本创建的一次性备份任务",
                compression_enabled=True
            )
            print(f"   ✓ 备份任务创建成功!")
            print(f"     任务ID: {result.get('task_id')}")
            print(f"     备份集ID: {result.get('set_id')}")
            print(f"     目标磁带: {result.get('tape_id')}")
            print(f"     消息: {result.get('message')}")

            task_id = result.get('task_id')

        except Exception as e:
            print(f"   ✗ 创建备份任务失败: {e}")
    else:
        print("4. 跳过备份任务创建（没有可用磁带）")
    print()

    # 5. 查询任务状态
    if 'task_id' in dir() and task_id:
        print("5. 查询任务状态...")
        try:
            task = await client.query_backup_tasks(task_id=task_id)
            print(f"   任务状态: {task.get('status')}")
            print(f"   进度: {task.get('progress_percent', 0):.1f}%")
        except Exception as e:
            print(f"   查询任务状态失败: {e}")
    print()

    print("=" * 60)
    print("示例脚本执行完成")
    print("=" * 60)
    print()
    print("API 使用说明:")
    print()
    print("1. 创建磁带:")
    print(f"   POST {client._get_url('/tape/create')}")
    print("   Body: {label, serial_number, capacity_gb, ...}")
    print()
    print("2. 一次性备份:")
    print(f"   POST {client._get_url('/backup/one-time')}")
    print("   Body: {source_paths, tape_id, task_name, ...}")
    print()
    print("3. 查询磁带:")
    print(f"   GET {client._get_url('/tape/list')}")
    print()
    print("4. 查询备份任务:")
    print(f"   GET {client._get_url('/backup/tasks')}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
