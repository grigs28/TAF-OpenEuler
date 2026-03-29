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

from utils.scheduler.db_utils import is_opengauss, is_sqlite, is_redis, get_opengauss_connection, get_sqlite_connection
from tape.tape_cartridge import TapeStatus

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


def normalize_tape_label(label: str, target_year: int, target_month: int) -> str:
    """规范化磁带卷标为 TPYYYYMMSS 格式

    Args:
        label: 原始卷标
        target_year: 目标年份
        target_month: 目标月份

    Returns:
        规范化后的卷标 (TPYYYYMMSS)
    """
    if not label:
        return f"TP{target_year}{target_month:02d}01"

    clean_label = label.strip().upper()
    default_seq = "01"

    def build_label(seq: str, suffix: str = "") -> str:
        # 截取前2位序号，确保不超过99
        if seq and seq.isdigit():
            seq_int = int(seq)
            seq = str(min(seq_int, 99)).zfill(2)
        else:
            seq = default_seq
        return f"TP{target_year}{target_month:02d}{seq}{suffix}"

    # 匹配 TPYYYYMMN 格式 (序号可能超过2位) - 必须先匹配，避免被拆分
    match = re.match(r'^TP(\d{4})(\d{2})(\d{3,})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    # 匹配 TPYYYYMMSS 格式 (序号正好2位)
    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    # 匹配 TAPEYYYYMMSS 格式
    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    # 匹配 TAPEYYYYMMN 格式
    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    # 匹配任意 YYYYMMSS 格式
    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    # 默认返回
    return build_label(default_seq)


async def get_next_tape_sequence(year: int, month: int) -> int:
    """获取指定年月的下一个磁带序号

    Args:
        year: 年份
        month: 月份

    Returns:
        下一个序号 (1-99)
    """
    try:
        if is_redis():
            from backup.redis_tape_db import count_serial_numbers_redis
            pattern = f"TP{month:02d}%"
            count = await count_serial_numbers_redis(pattern)
            return min(count + 1, 99)

        elif is_opengauss():
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow("""
                    SELECT COUNT(*) as count
                    FROM tape_cartridges
                    WHERE serial_number IS NOT NULL
                    AND serial_number LIKE $1
                """, f"TP{month:02d}%")
                count = row['count'] if row else 0
                return min(count + 1, 99)

        elif is_sqlite():
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT COUNT(*) as count
                    FROM tape_cartridges
                    WHERE serial_number IS NOT NULL
                    AND serial_number LIKE ?
                """, (f"TP{month:02d}%",))
                row = await cursor.fetchone()
                count = row[0] if row else 0
                return min(count + 1, 99)

        else:
            # PostgreSQL psycopg模式
            from utils.db_connection_helper import get_psycopg_connection_from_url
            from config.settings import get_settings
            settings = get_settings()
            conn, _ = get_psycopg_connection_from_url(settings.DATABASE_URL, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) FROM tape_cartridges
                        WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                    """, (f"TP{month:02d}%",))
                    count = cur.fetchone()[0] or 0
                    return min(count + 1, 99)
            finally:
                conn.close()

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


async def register_tape_in_database(
    tape_id: str,
    label: str,
    serial_number: str,
    generation: int = 9,
    capacity_bytes: int = 18 * 1024**4,  # 18TB
    retention_months: int = 6,
    location: str = ""
) -> Tuple[bool, str]:
    """在数据库中注册磁带

    Args:
        tape_id: 磁带ID
        label: 卷标
        serial_number: 序列号
        generation: LTO代数 (默认LTO-9)
        capacity_bytes: 容量 (默认18TB)
        retention_months: 保留月数 (默认6个月)
        location: 存储位置

    Returns:
        (是否成功, 消息)
    """
    try:
        now = datetime.now()
        expiry_date = datetime(
            now.year + (now.month + retention_months - 1) // 12,
            (now.month + retention_months - 1) % 12 + 1,
            1
        )

        if is_redis():
            from backup.redis_tape_db import create_tape_redis
            result = await create_tape_redis(
                tape_id=tape_id,
                label=label,
                status="available",
                media_type="LTO",
                generation=generation,
                serial_number=serial_number,
                location=location,
                capacity_bytes=capacity_bytes,
                retention_months=retention_months,
                notes=f"格式化后自动注册",
                manufactured_date=now,
                expiry_date=expiry_date,
                auto_erase=True,
                health_score=100
            )
            success = result.get("success", False)
            if success:
                logger.info(f"[Redis] 磁带 {tape_id} 注册成功")
                return True, f"磁带 {tape_id} 注册成功"
            else:
                return False, result.get("error", "注册失败")

        elif is_opengauss():
            async with get_opengauss_connection() as conn:
                # 检查是否已存在
                existing = await conn.fetchrow(
                    "SELECT tape_id FROM tape_cartridges WHERE tape_id = $1",
                    tape_id
                )

                if existing:
                    # 更新
                    await conn.execute("""
                        UPDATE tape_cartridges
                        SET label = $2, serial_number = $3, status = 'available',
                            generation = $4, capacity_bytes = $5,
                            retention_months = $6, location = $7,
                            manufactured_date = $8, expiry_date = $9,
                            auto_erase = true, health_score = 100
                        WHERE tape_id = $1
                    """, tape_id, label, serial_number, generation, capacity_bytes,
                        retention_months, location, now, expiry_date)
                    logger.info(f"[openGauss] 磁带 {tape_id} 更新成功")
                else:
                    # 插入
                    await conn.execute("""
                        INSERT INTO tape_cartridges (
                            tape_id, label, status, media_type, generation,
                            serial_number, location, capacity_bytes, used_bytes,
                            retention_months, notes, manufactured_date, expiry_date,
                            auto_erase, health_score
                        ) VALUES ($1, $2, 'available', 'LTO', $3, $4, $5, $6, 0,
                            $7, '格式化后自动注册', $8, $9, true, 100)
                    """, tape_id, label, generation, serial_number, location,
                        capacity_bytes, retention_months, now, expiry_date)
                    logger.info(f"[openGauss] 磁带 {tape_id} 注册成功")

                return True, f"磁带 {tape_id} 注册成功"

        elif is_sqlite():
            async with get_sqlite_connection() as conn:
                # 检查是否已存在
                cursor = await conn.execute(
                    "SELECT tape_id FROM tape_cartridges WHERE tape_id = ?",
                    (tape_id,)
                )
                existing = await cursor.fetchone()

                if existing:
                    # 更新
                    await conn.execute("""
                        UPDATE tape_cartridges
                        SET label = ?, serial_number = ?, status = 'available',
                            generation = ?, capacity_bytes = ?,
                            retention_months = ?, location = ?,
                            manufactured_date = ?, expiry_date = ?,
                            auto_erase = 1, health_score = 100
                        WHERE tape_id = ?
                    """, label, serial_number, generation, capacity_bytes,
                        retention_months, location, now.isoformat(),
                        expiry_date.isoformat(), tape_id)
                else:
                    # 插入
                    await conn.execute("""
                        INSERT INTO tape_cartridges (
                            tape_id, label, status, media_type, generation,
                            serial_number, location, capacity_bytes, used_bytes,
                            retention_months, notes, manufactured_date, expiry_date,
                            auto_erase, health_score
                        ) VALUES (?, ?, 'available', 'LTO', ?, ?, ?, ?, 0,
                            ?, '格式化后自动注册', ?, ?, 1, 100)
                    """, tape_id, label, generation, serial_number, location,
                        capacity_bytes, retention_months, now.isoformat(),
                        expiry_date.isoformat())

                await conn.commit()
                logger.info(f"[SQLite] 磁带 {tape_id} 注册成功")
                return True, f"磁带 {tape_id} 注册成功"

        else:
            # PostgreSQL psycopg 模式
            from utils.db_connection_helper import get_psycopg_connection_from_url
            from config.settings import get_settings
            settings = get_settings()
            conn, _ = get_psycopg_connection_from_url(settings.DATABASE_URL, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    # 检查是否已存在
                    cur.execute("SELECT tape_id FROM tape_cartridges WHERE tape_id = %s",
                               (tape_id,))
                    existing = cur.fetchone()

                    if existing:
                        # 更新
                        cur.execute("""
                            UPDATE tape_cartridges
                            SET label = %s, serial_number = %s, status = 'available',
                                generation = %s, capacity_bytes = %s,
                                retention_months = %s, location = %s,
                                manufactured_date = %s, expiry_date = %s,
                                auto_erase = true, health_score = 100
                            WHERE tape_id = %s
                        """, (label, serial_number, generation, capacity_bytes,
                              retention_months, location, now, expiry_date, tape_id))
                    else:
                        # 插入
                        cur.execute("""
                            INSERT INTO tape_cartridges
                            (tape_id, label, status, media_type, generation, serial_number,
                             location, capacity_bytes, used_bytes, retention_months, notes,
                             manufactured_date, expiry_date, auto_erase, health_score)
                            VALUES (%s, %s, 'available', 'LTO', %s, %s, %s, %s, 0,
                                    %s, '格式化后自动注册', %s, %s, true, 100)
                        """, (tape_id, label, generation, serial_number, location,
                              capacity_bytes, retention_months, now, expiry_date))

                    conn.commit()
                    logger.info(f"[PostgreSQL] 磁带 {tape_id} 注册成功")
                    return True, f"磁带 {tape_id} 注册成功"
            finally:
                conn.close()

    except Exception as e:
        logger.error(f"注册磁带失败: {e}", exc_info=True)
        return False, f"注册失败: {e}"


async def format_and_register_tape(
    tape_handler,
    existing_label: str = "Unknown",
    force_format: bool = True
) -> Tuple[bool, str, Optional[TapeLabelInfo]]:
    """格式化磁带并自动注册

    Args:
        tape_handler: TapeHandler 实例
        existing_label: 现有卷标 (Unknown 表示无卷标)
        force_format: 是否强制格式化

    Returns:
        (是否成功, 消息, TapeLabelInfo)
    """
    try:
        # 1. 生成卷标和序列号
        label_info = await generate_tape_label_and_serial(existing_label=existing_label)

        logger.info(f"[磁带注册] 开始格式化并注册磁带: {label_info.tape_id}")
        logger.info(f"[磁带注册] 卷标: {label_info.label}, 序列号: {label_info.serial_number}")

        # 2. 格式化磁带
        success, msg = await tape_handler.format_as_ltfs(
            volume_name=label_info.label,
            serial=label_info.serial_number
        )

        if not success:
            return False, f"格式化失败: {msg}", label_info

        logger.info(f"[磁带注册] 格式化成功: {msg}")

        # 3. 注册到数据库
        reg_success, reg_msg = await register_tape_in_database(
            tape_id=label_info.tape_id,
            label=label_info.label,
            serial_number=label_info.serial_number
        )

        if not reg_success:
            return False, f"格式化成功但注册失败: {reg_msg}", label_info

        # 4. 更新 tape_manager 缓存
        if tape_handler.tape_manager:
            from tape.tape_cartridge import TapeCartridge
            now = datetime.now()
            expiry_date = datetime(
                now.year + (now.month + 6 - 1) // 12,
                (now.month + 6 - 1) % 12 + 1,
                1
            )

            new_tape = TapeCartridge(
                tape_id=label_info.tape_id,
                label=label_info.label,
                status=TapeStatus.AVAILABLE,
                generation=9,
                serial_number=label_info.serial_number,
                capacity_bytes=18 * 1024**4,
                created_date=now,
                expiry_date=expiry_date
            )

            tape_handler.tape_manager.tape_cartridges[label_info.tape_id] = new_tape
            tape_handler.tape_manager.current_tape = new_tape
            logger.info(f"[磁带注册] 已更新 tape_manager 缓存")

        return True, f"格式化并注册成功: {label_info.tape_id}", label_info

    except Exception as e:
        logger.error(f"格式化并注册磁带异常: {e}", exc_info=True)
        return False, f"异常: {e}", None
