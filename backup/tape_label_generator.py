#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带标签生成和自动注册模块
Tape Label Generator and Auto-Registration Module

功能:
1. 生成符合规则的磁带卷标 (TPYYYYMMSS)
2. 生成符合规则的序列号 (TPMMSS)
3. 格式化后自动在数据库注册磁带
"""

import re
import logging
from datetime import datetime
from typing import Tuple, Optional
from dataclasses import dataclass

from utils.scheduler.db_utils import get_opengauss_connection
from backup.utils import normalize_volume_label as normalize_tape_label

logger = logging.getLogger(__name__)


@dataclass
class TapeLabelInfo:
    """磁带标签信息"""
    tape_id: str           # 磁带ID (如 TP20250101)
    label: str             # 卷标 (同 tape_id)
    serial_number: str     # 序列号 (如 TP0101)
    year: int              # 年份
    month: int             # 月份
    sequence: int          # 序号


async def get_next_tape_sequence(year: int, month: int) -> int:
    """获取指定年月的下一个磁带序号

    Args:
        year: 年份
        month: 月份

    Returns:
        下一个序号 (1-99)
    """
    try:
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) as count
                FROM tape_cartridges
                WHERE serial_number IS NOT NULL
                AND serial_number LIKE $1
            """, f"TP{month:02d}%")
            count = row['count'] if row else 0
            return min(count + 1, 99)

    except Exception as e:
        logger.warning(f"获取磁带序号失败: {e}，使用默认序号1")
        return 1


async def generate_tape_label_and_serial(
    year: Optional[int] = None,
    month: Optional[int] = None,
    existing_label: Optional[str] = None
) -> TapeLabelInfo:
    """生成磁带卷标和序列号

    Args:
        year: 年份 (默认当前年)
        month: 月份 (默认当前月)
        existing_label: 已有卷标 (优先使用)

    Returns:
        TapeLabelInfo 对象
    """
    now = datetime.now()
    target_year = year or now.year
    target_month = month or now.month
    target_month = max(1, min(12, target_month))

    # 如果有现有卷标，先规范化
    if existing_label and existing_label != "Unknown":
        label = normalize_tape_label(existing_label, target_year, target_month)
    else:
        # 获取下一个序号
        sequence = await get_next_tape_sequence(target_year, target_month)
        label = f"TP{target_year}{target_month:02d}{sequence:02d}"

    # 提取年月和序号
    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})', label)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        sequence = int(match.group(3))
    else:
        # 如果无法解析，使用默认值
        year = target_year
        month = target_month
        sequence = 1

    # 生成序列号 (TPMMSS 格式)
    serial_number = f"TP{month:02d}{sequence:02d}"

    logger.info(f"生成磁带标签: {label}, 序列号: {serial_number}")

    return TapeLabelInfo(
        tape_id=label,
        label=label,
        serial_number=serial_number,
        year=year,
        month=month,
        sequence=sequence
    )


