#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_query
Tape Management API - tape_query
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
from .tape_utils import normalize_tape_label, check_tape_exists_sqlite, count_serial_numbers_sqlite, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection, is_sqlite, is_redis, get_sqlite_connection
from utils.tape_tools import tape_tools_manager
from config.database import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()


def parse_expiry_date_for_inventory(expiry_date):
    """解析过期日期（用于库存统计）"""
    from datetime import date, datetime
    if isinstance(expiry_date, date):
        return expiry_date
    if isinstance(expiry_date, datetime):
        return expiry_date.date()
    if isinstance(expiry_date, str):
        try:
            dt = datetime.fromisoformat(expiry_date.replace('Z', '+00:00'))
            return dt.date()
        except:
            return date.today()
    return date.today()


async def check_tape_exists_sqlite(db_manager, tape_id: str, label: str) -> tuple[bool, bool]:
    """检查磁带是否存在（SQLite版本）"""
    
    async with get_sqlite_connection() as conn:
        # 检查 tape_id
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE tape_id = ?", (tape_id,))
        row = await cursor.fetchone()
        tape_exists = (row[0] > 0) if row else False
        
        # 检查 label
        cursor = await conn.execute("SELECT COUNT(*) FROM tape_cartridges WHERE label = ?", (label,))
        row = await cursor.fetchone()
        label_exists = (row[0] > 0) if row else False
        
        return tape_exists, label_exists


async def count_serial_numbers_sqlite(db_manager, pattern: str) -> int:
    """统计序列号数量（SQLite版本）"""
    from utils.scheduler.db_utils import is_redis
    
    # 检查数据库类型
    if is_redis():
        raise ValueError("Redis模式下不支持磁带管理功能")
    if not is_sqlite():
        raise ValueError("当前数据库类型不支持磁带管理功能")
    
    async with get_sqlite_connection() as conn:
        cursor = await conn.execute("""
            SELECT COUNT(*) FROM tape_cartridges
            WHERE serial_number IS NOT NULL AND serial_number LIKE ?
        """, (pattern,))
        row = await cursor.fetchone()
        return row[0] if row else 0


def normalize_tape_label(label: Optional[str], year: int, month: int) -> str:
    target_year = f"{year:04d}"
    target_month = f"{month:02d}"
    default_seq = "01"
    default_label = f"TP{target_year}{target_month}{default_seq}"

    if not label:
        return default_label

    clean_label = label.strip().upper()

    def build_label(seq: str, suffix: str = "") -> str:
        seq = (seq if seq and seq.isdigit() else default_seq).zfill(2)[:2]
        return f"TP{target_year}{target_month}{seq}{suffix}"

    match = re.match(r'^TP(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TP(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d{2})(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.match(r'^TAPE(\d{4})(\d{2})(\d+)(.*)$', clean_label)
    if match:
        return build_label(match.group(3), match.group(4))

    match = re.search(r'(\d{4})(\d{2})(\d{2})', clean_label)
    if match:
        return build_label(match.group(3))

    return default_label



@router.get("/show/{tape_id}")
async def get_tape(tape_id: str, request: Request):
    """获取磁带详情"""
    try:
        from config.settings import get_settings
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            logger.warning(f"[Redis模式] 获取磁带详情暂未实现: {tape_id}")
            raise HTTPException(status_code=501, detail="Redis模式下暂不支持获取磁带详情功能")
        
        # 检查是否为 SQLite
        if is_sqlite():
            # 使用原生SQL查询 SQLite
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT tape_id, label, status, media_type, generation, serial_number, location,
                           capacity_bytes, used_bytes, retention_months, notes, manufactured_date, 
                           expiry_date, auto_erase, health_score
                    FROM tape_cartridges 
                    WHERE tape_id = ?
                """, (tape_id,))
                
                row = await cursor.fetchone()
                
                if not row:
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                # 构建返回数据
                tape = {
                    "tape_id": row[0],
                    "label": row[1],
                    "status": row[2] if isinstance(row[2], str) else (row[2].value if hasattr(row[2], 'value') else str(row[2])),
                    "media_type": row[3],
                    "generation": row[4],
                    "serial_number": row[5],
                    "location": row[6],
                    "capacity_bytes": row[7],
                    "used_bytes": row[8],
                    "retention_months": row[9],
                    "notes": row[10],
                    "manufactured_date": row[11].isoformat() if row[11] and hasattr(row[11], 'isoformat') else (str(row[11]) if row[11] else None),
                    "expiry_date": row[12].isoformat() if row[12] and hasattr(row[12], 'isoformat') else (str(row[12]) if row[12] else None),
                    "auto_erase": row[13],
                    "health_score": row[14]
                }
                
                return {
                    "success": True,
                    "tape": tape
                }
        
        # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
        from utils.db_connection_helper import get_psycopg_connection_from_url
        
        # 连接数据库
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        
        try:
            with conn.cursor() as cur:
                # 查询磁带信息
                cur.execute("""
                    SELECT tape_id, label, status, media_type, generation, serial_number, location,
                           capacity_bytes, used_bytes, retention_months, notes, manufactured_date, 
                           expiry_date, auto_erase, health_score
                    FROM tape_cartridges 
                    WHERE tape_id = %s
                """, (tape_id,))
                
                row = cur.fetchone()
                
                if not row:
                    return {
                        "success": False,
                        "message": f"磁带 {tape_id} 不存在"
                    }
                
                # 构建返回数据
                tape = {
                    "tape_id": row[0],
                    "label": row[1],
                    "status": row[2],
                    "media_type": row[3],
                    "generation": row[4],
                    "serial_number": row[5],
                    "location": row[6],
                    "capacity_bytes": row[7],
                    "used_bytes": row[8],
                    "retention_months": row[9],
                    "notes": row[10],
                    "manufactured_date": row[11].isoformat() if row[11] else None,
                    "expiry_date": row[12].isoformat() if row[12] else None,
                    "auto_erase": row[13],
                    "health_score": row[14]
                }
                
                return {
                    "success": True,
                    "tape": tape
                }
                
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"获取磁带详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update/{tape_id}")
async def update_tape(tape_id: str, request: UpdateTapeRequest, http_request: Request):
    """更新磁带记录"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "PUT"
    request_url = str(http_request.url)
    
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        from config.settings import get_settings
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            logger.warning(f"[Redis模式] 更新磁带暂未实现: {tape_id}")
            raise HTTPException(status_code=501, detail="Redis模式下暂不支持更新磁带功能")
        
        # 检查是否为 SQLite
        if is_sqlite():
            # SQLite 版本暂不支持更新磁带（需要实现）
            logger.warning(f"[SQLite模式] 更新磁带暂未实现: {tape_id}")
            raise HTTPException(status_code=501, detail="SQLite模式下暂不支持更新磁带功能")

        # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
        from utils.db_connection_helper import get_psycopg_connection_from_url
        
        # 连接数据库
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        
        old_values = {}
        new_values = {}
        changed_fields = []
        
        try:
            with conn.cursor() as cur:
                # 检查磁带是否存在并获取旧值（包括manufactured_date用于序列号生成）
                cur.execute("""
                    SELECT tape_id, label, serial_number, media_type, generation, capacity_bytes, location, notes, manufactured_date
                    FROM tape_cartridges WHERE tape_id = %s
                """, (tape_id,))
                existing = cur.fetchone()
                
                if not existing:
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}",
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
                
                # 保存旧值
                old_values = {
                    "tape_id": existing[0],
                    "label": existing[1],
                    "serial_number": existing[2],
                    "media_type": existing[3],
                    "generation": existing[4],
                    "capacity_bytes": existing[5],
                    "location": existing[6],
                    "notes": existing[7],
                    "manufactured_date": existing[8].isoformat() if existing[8] else None
                }
                
                # 构建更新字段和值
                update_fields = []
                update_values = []
                
                if request.serial_number is not None and request.serial_number != old_values["serial_number"]:
                    update_fields.append("serial_number = %s")
                    update_values.append(request.serial_number)
                    new_values["serial_number"] = request.serial_number
                    changed_fields.append("serial_number")
                if request.media_type is not None and request.media_type != old_values["media_type"]:
                    update_fields.append("media_type = %s")
                    update_values.append(request.media_type)
                    new_values["media_type"] = request.media_type
                    changed_fields.append("media_type")
                if request.generation is not None and request.generation != old_values["generation"]:
                    update_fields.append("generation = %s")
                    update_values.append(request.generation)
                    new_values["generation"] = request.generation
                    changed_fields.append("generation")
                if request.capacity_gb is not None:
                    capacity_bytes = request.capacity_gb * (1024 ** 3)
                    if capacity_bytes != old_values["capacity_bytes"]:
                        update_fields.append("capacity_bytes = %s")
                        update_values.append(capacity_bytes)
                        new_values["capacity_bytes"] = capacity_bytes
                        changed_fields.append("capacity_bytes")
                if request.location is not None and request.location != old_values["location"]:
                    update_fields.append("location = %s")
                    update_values.append(request.location)
                    new_values["location"] = request.location
                    changed_fields.append("location")
                if request.notes is not None and request.notes != old_values["notes"]:
                    update_fields.append("notes = %s")
                    update_values.append(request.notes)
                    new_values["notes"] = request.notes
                    changed_fields.append("notes")
                
                # 如果没有需要更新的字段，返回错误
                if not update_fields:
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        resource_name=existing[1],
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}",
                        category="tape",
                        success=False,
                        error_message="没有提供需要更新的字段",
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=int((datetime.now() - start_time).total_seconds() * 1000)
                    )
                    return {
                        "success": False,
                        "message": "没有提供需要更新的字段"
                    }
                
                # 构建并执行更新SQL
                update_sql = f"""
                    UPDATE tape_cartridges
                    SET {', '.join(update_fields)}
                    WHERE tape_id = %s
                """
                update_values.append(tape_id)
                cur.execute(update_sql, update_values)
                
                conn.commit()
                logger.info(f"更新磁带记录: {tape_id}")
                
                # 如果请求格式化磁盘，执行格式化操作
                format_tape = getattr(request, 'format_tape', False)  # 默认False，需要显式指定
                if format_tape:
                    # 获取卷标和序列号用于格式化
                    # 如果请求中提供了新卷标，使用新卷标；否则使用旧卷标
                    label = getattr(request, 'label', None) or old_values.get("label") or tape_id
                    serial_number = new_values.get("serial_number") or old_values.get("serial_number")
                    
                    # 如果请求中提供了新卷标，需要先更新数据库中的卷标（在格式化之前）
                    if getattr(request, 'label', None) and getattr(request, 'label', None) != old_values.get("label"):
                        # 先更新卷标到数据库
                        cur.execute("UPDATE tape_cartridges SET label = %s WHERE tape_id = %s", 
                                  (getattr(request, 'label'), tape_id))
                        conn.commit()
                        new_values["label"] = getattr(request, 'label')
                        changed_fields.append("label")
                        logger.info(f"更新时将同时更新卷标: {old_values.get('label')} -> {getattr(request, 'label')}")
                        # 更新label变量，用于后续格式化
                        label = getattr(request, 'label')
                    
                    # 如果请求中提供了新序列号，也需要先更新数据库中的序列号（在格式化之前）
                    if serial_number and serial_number != old_values.get("serial_number"):
                        # 如果序列号还没有在update_fields中，需要单独更新
                        if "serial_number" not in changed_fields:
                            cur.execute("UPDATE tape_cartridges SET serial_number = %s WHERE tape_id = %s", 
                                      (serial_number, tape_id))
                            conn.commit()
                            logger.info(f"更新时将同时更新序列号: {old_values.get('serial_number')} -> {serial_number}")
                    
                    # 如果没有序列号，生成一个（使用新规则：TPMMNN）
                    # 序列号生成优先级：1. 从manufactured_date获取创建年月 2. 从卷标中提取 3. 当前年月
                    if not serial_number:
                        # 优先从manufactured_date获取创建年月
                        year = None
                        month = None
                        if old_values.get("manufactured_date"):
                            # manufactured_date 在 old_values 中已经是字符串格式
                            if isinstance(old_values["manufactured_date"], str):
                                manufactured_date = datetime.fromisoformat(old_values["manufactured_date"].replace('Z', '+00:00'))
                            else:
                                # 如果是 datetime 对象，直接使用
                                manufactured_date = old_values["manufactured_date"]
                            year = manufactured_date.year
                            month = manufactured_date.month
                        
                        # 如果manufactured_date中没有年月，尝试从卷标中提取
                        if year is None or month is None:
                            match = re.search(r'(\d{4})(\d{2})', label)
                            if match:
                                year = int(match.group(1))
                                month = int(match.group(2))
                        
                        # 如果仍然没有年月，使用当前年月
                        if year is None or month is None:
                            now = datetime.now()
                            year = now.year
                            month = now.month
                        
                        # 生成序列号（TPMMNN格式：TP + 月份 + 序号）
                        mm = month
                        # 查询当前月份已有多少张磁盘（查询TP + 月份开头的序列号）
                        cur.execute("""
                            SELECT COUNT(*) FROM tape_cartridges 
                            WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                        """, (f"TP{mm:02d}%",))
                        count = cur.fetchone()[0] or 0
                        sequence = count + 1
                        serial_number = f"TP{mm:02d}{sequence:02d}"
                        logger.info(f"更新时自动生成序列号: {serial_number} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={label})")
                    
                    # 验证序列号格式（TPMMNN格式）
                    serial_param = None
                    if serial_number:
                        serial_number_upper = serial_number.strip().upper()
                        # 验证格式：TPMMNN（TP + 2位月份 + 2位序号）
                        if len(serial_number_upper) == 6 and serial_number_upper.startswith('TP') and serial_number_upper[2:4].isdigit() and serial_number_upper[4:6].isdigit():
                            serial_param = serial_number_upper
                        else:
                            logger.warning(f"序列号格式不正确，将不使用: {serial_number}")
                    
                    # 格式化磁盘（后台执行）
                    # 使用BackgroundTasks需要在函数参数中传递，这里使用asyncio.create_task
                    import asyncio
                    
                    # 保存变量供后台任务使用
                    background_label = label
                    background_serial = serial_param
                    background_tape_id = tape_id
                    background_old_serial = old_values.get("serial_number")
                    background_system = system  # 保存system实例供后台任务使用
                    
                    # 在格式化开始前，将状态设置为MAINTENANCE（维护中）
                    cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                              ('MAINTENANCE', tape_id))
                    conn.commit()
                    logger.info(f"格式化开始前，将磁带 {tape_id} 状态设置为 MAINTENANCE")
                    
                    async def format_tape_background():
                        try:
                            from utils.tape_tools import tape_tools_manager
                            # 重新连接数据库（因为原连接已关闭）
                            from utils.db_connection_helper import get_psycopg_connection
                            db_conn, _ = get_psycopg_connection(
                                host=host,
                                port=port,
                                user=username,
                                password=password,
                                database=database,
                                prefer_psycopg3=True
                            )
                            
                            try:
                                # 使用保存的system实例
                                drive_letter = background_system.settings.TAPE_DRIVE_LETTER or "O"
                                if drive_letter.endswith(":"):
                                    drive_letter = drive_letter[:-1]
                                drive_letter = drive_letter.strip().upper()
                                
                                logger.info(f"后台格式化磁盘: 卷标={background_label}, SN={background_serial}, 盘符={drive_letter}")
                                format_result = await tape_tools_manager.format_tape_ltfs(
                                    drive_letter=drive_letter,
                                    volume_label=background_label,
                                    serial=background_serial,
                                    eject_after=False
                                )
                                
                                logger.info(f"格式化命令执行完成 - 成功: {format_result.get('success')}, 返回码: {format_result.get('returncode')}, "
                                          f"stdout长度: {len(format_result.get('stdout', ''))}, stderr长度: {len(format_result.get('stderr', ''))}")
                                
                                if format_result.get("success"):
                                    logger.info(f"磁盘格式化成功: 卷标={background_label}, SN={background_serial}")
                                    
                                    # 格式化成功后，从磁盘读取实际的卷标和SN，然后更新数据库
                                    try:
                                        # 等待一小段时间，确保格式化操作完全完成
                                        import asyncio
                                        await asyncio.sleep(2)
                                        
                                        # 读取磁盘上的实际卷标和序列号（60秒超时）
                                        try:
                                            label_result = await asyncio.wait_for(
                                                tape_tools_manager.read_tape_label_windows(),
                                                timeout=60.0
                                            )
                                        except asyncio.TimeoutError:
                                            logger.warning("读取磁带卷标超时（60秒）")
                                            label_result = {"success": False, "error": "读取卷标超时（60秒）"}
                                        
                                        if label_result.get("success"):
                                            actual_label = label_result.get("volume_name", "").strip()
                                            actual_serial = label_result.get("serial_number", "").strip()
                                            
                                            logger.info(f"从磁盘读取到实际值: 卷标={actual_label}, SN={actual_serial}")
                                            
                                            # 如果读取到的值与预期值不同，更新数据库
                                            if actual_label or actual_serial:
                                                update_fields = []
                                                update_values = []
                                                
                                                # 更新卷标（如果读取到的值与数据库中的不同）
                                                if actual_label and actual_label != background_label:
                                                    update_fields.append("label = %s")
                                                    update_values.append(actual_label)
                                                    logger.info(f"更新数据库卷标: {background_label} -> {actual_label}")
                                                
                                                # 更新序列号（如果读取到的值与数据库中的不同）
                                                if actual_serial and actual_serial != background_serial:
                                                    update_fields.append("serial_number = %s")
                                                    update_values.append(actual_serial)
                                                    logger.info(f"更新数据库序列号: {background_serial} -> {actual_serial}")
                                                
                                                # 如果有需要更新的字段，执行更新
                                                if update_fields:
                                                    update_values.append(background_tape_id)
                                                    update_sql = f"""
                                                        UPDATE tape_cartridges
                                                        SET {', '.join(update_fields)}, updated_at = NOW()
                                                        WHERE tape_id = %s
                                                    """
                                                    with db_conn.cursor() as db_cur:
                                                        db_cur.execute(update_sql, update_values)
                                                        db_conn.commit()
                                                        logger.info(f"已根据磁盘实际值更新数据库: tape_id={background_tape_id}, 更新字段={update_fields}")
                                                else:
                                                    logger.info(f"磁盘实际值与数据库一致，无需更新: 卷标={actual_label}, SN={actual_serial}")
                                            else:
                                                logger.warning("从磁盘读取到的卷标或序列号为空，跳过数据库更新")
                                            
                                            # 格式化成功，将状态改回AVAILABLE（可用）
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成，将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                        else:
                                            logger.warning(f"读取磁盘卷标失败: {label_result.get('error', '未知错误')}")
                                            # 即使读取失败，格式化成功也应该将状态改回AVAILABLE
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成（读取卷标失败），将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                    except Exception as read_error:
                                        logger.error(f"读取磁盘卷标并更新数据库时出错: {str(read_error)}", exc_info=True)
                                        # 即使读取异常，格式化成功也应该将状态改回AVAILABLE
                                        try:
                                            with db_conn.cursor() as db_cur:
                                                db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                            ('AVAILABLE', background_tape_id))
                                                db_conn.commit()
                                                logger.info(f"格式化完成（读取卷标异常），将磁带 {background_tape_id} 状态设置为 AVAILABLE")
                                        except Exception as db_update_error:
                                            logger.error(f"更新状态失败: {str(db_update_error)}", exc_info=True)
                                else:
                                    # 格式化失败，将状态改为ERROR（故障）
                                    error_detail = format_result.get("stderr") or format_result.get("stdout") or "格式化失败"
                                    returncode = format_result.get("returncode", -1)
                                    logger.error(f"格式化磁盘失败 - 返回码: {returncode}, 错误详情: {error_detail}")
                                    
                                    # 格式化失败时，将状态改为ERROR
                                    with db_conn.cursor() as db_cur:
                                        db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                    ('ERROR', background_tape_id))
                                        db_conn.commit()
                                        logger.warning(f"格式化失败，将磁带 {background_tape_id} 状态设置为 ERROR")
                                    
                                    # 发送钉钉通知
                                    try:
                                        if background_system and hasattr(background_system, 'dingtalk_notifier') and background_system.dingtalk_notifier:
                                            await background_system.dingtalk_notifier.send_tape_format_notification(
                                                tape_id=background_tape_id,
                                                status="failed",
                                                error_detail=f"返回码: {returncode}, {error_detail}",
                                                volume_label=background_label,
                                                serial_number=background_serial
                                            )
                                    except Exception as notify_error:
                                        logger.error(f"发送格式化失败钉钉通知异常: {str(notify_error)}", exc_info=True)
                            finally:
                                db_conn.close()
                        except Exception as e:
                            logger.error(f"后台格式化磁盘异常: {str(e)}", exc_info=True)
                            # 异常时也要将状态改为ERROR
                            try:
                                from utils.db_connection_helper import get_psycopg_connection
                                db_conn, _ = get_psycopg_connection(
                                    host=host,
                                    port=port,
                                    user=username,
                                    password=password,
                                    database=database,
                                    prefer_psycopg3=True
                                )
                                with db_conn.cursor() as db_cur:
                                    db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                ('ERROR', background_tape_id))
                                    db_conn.commit()
                                    logger.error(f"格式化异常，将磁带 {background_tape_id} 状态设置为 ERROR")
                                db_conn.close()
                            except Exception as db_error:
                                logger.error(f"更新磁带状态失败: {str(db_error)}", exc_info=True)
                    
                    # 在后台执行格式化任务
                    # FastAPI已经提供了事件循环，直接创建任务即可
                    asyncio.create_task(format_tape_background())
                    logger.info(f"格式化磁盘任务已在后台启动: 卷标={label}, SN={serial_param}")
                    
                    # 返回格式化标志，供前端显示提示
                    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
                    await log_operation(
                        operation_type=OperationType.UPDATE,
                        resource_type="tape",
                        resource_id=tape_id,
                        resource_name=old_values.get("label"),
                        operation_name="更新磁带",
                        operation_description=f"更新磁带 {tape_id}，格式化任务已在后台启动",
                        category="tape",
                        success=True,
                        result_message=f"磁带 {tape_id} 更新成功，格式化任务已在后台启动",
                        old_values=old_values,
                        new_values=new_values,
                        changed_fields=changed_fields,
                        ip_address=ip_address,
                        request_method=request_method,
                        request_url=request_url,
                        duration_ms=duration_ms
                    )
                    
                    return {
                        "success": True,
                        "message": f"磁带 {tape_id} 更新成功，格式化任务已在后台启动",
                        "tape_id": tape_id,
                        "formatted": True
                    }
        
        finally:
            conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=old_values.get("label"),
            operation_name="更新磁带",
            operation_description=f"更新磁带 {tape_id}",
            category="tape",
            success=True,
            result_message=f"磁带 {tape_id} 更新成功",
            old_values=old_values,
            new_values=new_values,
            changed_fields=changed_fields,
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": f"磁带 {tape_id} 更新成功",
            "tape_id": tape_id,
            "formatted": False
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"更新磁带记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id,
            operation_name="更新磁带",
            operation_description=f"更新磁带 {tape_id}",
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
            function="update_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check/{tape_id}")
async def check_tape_exists(tape_id: str, request: Request):
    """检查磁带是否存在"""
    try:
        from config.settings import get_settings
        from datetime import datetime, timezone
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            # Redis模式：使用Redis查询磁带
            from backup.redis_tape_db import get_tape_redis
            from datetime import timezone
            
            tape = await get_tape_redis(tape_id)
            if not tape:
                return {
                    "exists": False
                }
            
            # 检查是否过期（仅比较年月）
            is_expired = False
            expiry_date_val = tape.get('expiry_date')
            if expiry_date_val:
                now = datetime.now(timezone.utc)
                # 如果 expiry_date_val 是字符串，转换为 datetime
                if isinstance(expiry_date_val, str):
                    expiry_date_val = datetime.fromisoformat(expiry_date_val.replace('Z', '+00:00'))
                # 比较年月
                if (now.year > expiry_date_val.year) or (now.year == expiry_date_val.year and now.month >= expiry_date_val.month):
                    is_expired = True
            
            return {
                "exists": True,
                "tape_id": tape.get('tape_id'),
                "label": tape.get('label'),
                "status": tape.get('status'),
                "is_expired": is_expired,
                "expiry_date": expiry_date_val.isoformat() if expiry_date_val and hasattr(expiry_date_val, 'isoformat') else (str(expiry_date_val) if expiry_date_val else None)
            }
        
        # 检查是否为 SQLite
        is_sqlite = database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")
        
        if is_sqlite:
            # 使用原生SQL查询 SQLite
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT tape_id, label, status, expiry_date
                    FROM tape_cartridges
                    WHERE tape_id = ?
                """, (tape_id,))
                row = await cursor.fetchone()
                
                if row:
                    # 检查是否过期（仅比较年月）
                    is_expired = False
                    expiry_date_val = row[3]  # expiry_date
                    if expiry_date_val:
                        now = datetime.now(timezone.utc)
                        # 如果 expiry_date_val 是字符串，转换为 datetime
                        if isinstance(expiry_date_val, str):
                            expiry_date_val = datetime.fromisoformat(expiry_date_val.replace('Z', '+00:00'))
                        # 比较年月
                        if (now.year > expiry_date_val.year) or (now.year == expiry_date_val.year and now.month >= expiry_date_val.month):
                            is_expired = True
                    
                    return {
                        "exists": True,
                        "tape_id": row[0],
                        "label": row[1],
                        "status": row[2] if isinstance(row[2], str) else (row[2].value if hasattr(row[2], 'value') else str(row[2])),
                        "is_expired": is_expired,
                        "expiry_date": expiry_date_val.isoformat() if expiry_date_val and hasattr(expiry_date_val, 'isoformat') else (str(expiry_date_val) if expiry_date_val else None)
                    }
                else:
                    return {
                        "exists": False
                    }
        else:
            # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
            from utils.db_connection_helper import get_psycopg_connection_from_url
            
            # 连接数据库
            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            
            try:
                with conn.cursor() as cur:
                    # 查询磁带是否存在
                    cur.execute("""
                        SELECT tape_id, label, status, expiry_date
                        FROM tape_cartridges
                        WHERE tape_id = %s
                    """, (tape_id,))
                    
                    row = cur.fetchone()
                    
                    if row:
                        # 检查是否过期（仅比较年月）
                        is_expired = False
                        if row[3]:  # expiry_date
                            # 使用timezone-aware datetime进行比较
                            now = datetime.now(timezone.utc)
                            expiry_date = row[3]
                            # 比较年月
                            if (now.year > expiry_date.year) or (now.year == expiry_date.year and now.month >= expiry_date.month):
                                is_expired = True
                        
                        return {
                            "exists": True,
                            "tape_id": row[0],
                            "label": row[1],
                            "status": row[2] if isinstance(row[2], str) else row[2].value,
                            "is_expired": is_expired,
                            "expiry_date": row[3].isoformat() if row[3] else None
                        }
                    else:
                        return {
                            "exists": False
                        }
            
            finally:
                conn.close()
        
    except Exception as e:
        logger.error(f"检查磁带存在性失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_tapes(request: Request):
    """获取所有磁带列表"""
    try:
        from config.settings import get_settings
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            # Redis模式：使用Redis查询磁带列表
            from backup.redis_tape_db import list_tapes_redis
            
            tapes_redis = await list_tapes_redis()
            
            tapes = []
            for tape in tapes_redis:
                capacity_bytes = tape.get('capacity_bytes', 0)
                used_bytes = tape.get('used_bytes', 0)
                usage_percent = (used_bytes / capacity_bytes * 100) if capacity_bytes > 0 else 0
                
                tapes.append({
                    "tape_id": tape.get('tape_id'),
                    "label": tape.get('label'),
                    "status": tape.get('status', 'AVAILABLE'),
                    "media_type": tape.get('media_type', 'LTO'),
                    "generation": tape.get('generation', 8),
                    "serial_number": tape.get('serial_number', ''),
                    "location": tape.get('location', ''),
                    "capacity_bytes": capacity_bytes,
                    "used_bytes": used_bytes,
                    "usage_percent": usage_percent,
                    "write_count": tape.get('write_count', 0),
                    "read_count": tape.get('read_count', 0),
                    "load_count": tape.get('load_count', 0),
                    "health_score": tape.get('health_score', 100),
                    "first_use_date": tape.get('first_use_date').isoformat() if tape.get('first_use_date') and hasattr(tape.get('first_use_date'), 'isoformat') else None,
                    "last_erase_date": tape.get('last_erase_date').isoformat() if tape.get('last_erase_date') and hasattr(tape.get('last_erase_date'), 'isoformat') else None,
                    "expiry_date": tape.get('expiry_date').isoformat() if tape.get('expiry_date') and hasattr(tape.get('expiry_date'), 'isoformat') else None,
                    "retention_months": tape.get('retention_months', 6),
                    "backup_set_count": tape.get('backup_set_count', 0),
                    "notes": tape.get('notes', '')
                })
            
            return {
                "success": True,
                "tapes": tapes,
                "count": len(tapes)
            }
        
        # 检查是否为 SQLite
        is_sqlite = database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")
        
        if is_sqlite:
            # 使用原生SQL查询 SQLite
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT tape_id, label, status, media_type, generation,
                           serial_number, location, capacity_bytes, used_bytes,
                           write_count, read_count, load_count, health_score,
                           first_use_date, last_erase_date, expiry_date,
                           retention_months, backup_set_count, notes
                    FROM tape_cartridges
                    ORDER BY tape_id
                """)
                rows = await cursor.fetchall()
                
                tapes = []
                for row in rows:
                    usage_percent = (row[8] / row[7] * 100) if row[7] and row[7] > 0 else 0
                    tapes.append({
                        "tape_id": row[0],
                        "label": row[1],
                        "status": row[2] if isinstance(row[2], str) else (row[2].value if hasattr(row[2], 'value') else str(row[2])),
                        "media_type": row[3],
                        "generation": row[4],
                        "serial_number": row[5],
                        "location": row[6],
                        "capacity_bytes": row[7],
                        "used_bytes": row[8],
                        "usage_percent": usage_percent,
                        "write_count": row[9],
                        "read_count": row[10],
                        "load_count": row[11],
                        "health_score": row[12],
                        "first_use_date": row[13].isoformat() if row[13] and hasattr(row[13], 'isoformat') else (str(row[13]) if row[13] else None),
                        "last_erase_date": row[14].isoformat() if row[14] and hasattr(row[14], 'isoformat') else (str(row[14]) if row[14] else None),
                        "expiry_date": row[15].isoformat() if row[15] and hasattr(row[15], 'isoformat') else (str(row[15]) if row[15] else None),
                        "retention_months": row[16],
                        "backup_set_count": row[17],
                        "notes": row[18]
                    })

            return {
                "success": True,
                "tapes": tapes,
                "count": len(tapes)
            }
        elif is_opengauss():
            # 使用openGauss连接
            async with get_opengauss_connection() as conn:
                rows = await conn.fetch("""
                    SELECT
                        tape_id, label, status, media_type, generation,
                        serial_number, location, capacity_bytes, used_bytes,
                        write_count, read_count, load_count, health_score,
                        first_use_date, last_erase_date, expiry_date,
                        retention_months, backup_set_count, notes
                    FROM tape_cartridges
                    ORDER BY tape_id
                """)

                tapes = []
                for row in rows:
                    tapes.append({
                        "tape_id": row["tape_id"],
                        "label": row["label"],
                        "status": row["status"].value if hasattr(row["status"], 'value') else str(row["status"]),
                        "media_type": row["media_type"],
                        "generation": row["generation"],
                        "serial_number": row["serial_number"],
                        "location": row["location"],
                        "capacity_bytes": row["capacity_bytes"],
                        "used_bytes": row["used_bytes"],
                        "usage_percent": (row["used_bytes"] / row["capacity_bytes"] * 100) if row["capacity_bytes"] > 0 else 0,
                        "write_count": row["write_count"],
                        "read_count": row["read_count"],
                        "load_count": row["load_count"],
                        "health_score": row["health_score"],
                        "first_use_date": row["first_use_date"].isoformat() if row["first_use_date"] else None,
                        "last_erase_date": row["last_erase_date"].isoformat() if row["last_erase_date"] else None,
                        "expiry_date": row["expiry_date"].isoformat() if row["expiry_date"] else None,
                        "retention_months": row["retention_months"],
                        "backup_set_count": row["backup_set_count"],
                        "notes": row["notes"]
                    })

            return {
                "success": True,
                "tapes": tapes,
                "count": len(tapes)
            }
        else:
            # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）- 用于其他数据库
            from utils.db_connection_helper import get_psycopg_connection_from_url

            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)

            tapes = []
            try:
                with conn.cursor() as cur:
                    # 查询所有磁带
                    cur.execute("""
                        SELECT
                            tape_id, label, status, media_type, generation,
                            serial_number, location, capacity_bytes, used_bytes,
                            write_count, read_count, load_count, health_score,
                            first_use_date, last_erase_date, expiry_date,
                            retention_months, backup_set_count, notes
                        FROM tape_cartridges
                        ORDER BY tape_id
                    """)

                    rows = cur.fetchall()

                    for row in rows:
                        tapes.append({
                            "tape_id": row[0],
                            "label": row[1],
                            "status": row[2] if isinstance(row[2], str) else row[2].value,
                            "media_type": row[3],
                            "generation": row[4],
                            "serial_number": row[5],
                            "location": row[6],
                            "capacity_bytes": row[7],
                            "used_bytes": row[8],
                            "usage_percent": (row[8] / row[7] * 100) if row[7] > 0 else 0,
                            "write_count": row[9],
                            "read_count": row[10],
                            "load_count": row[11],
                            "health_score": row[12],
                            "first_use_date": row[13].isoformat() if row[13] else None,
                            "last_erase_date": row[14].isoformat() if row[14] else None,
                            "expiry_date": row[15].isoformat() if row[15] else None,
                            "retention_months": row[16],
                            "backup_set_count": row[17],
                            "notes": row[18]
                        })
            finally:
                conn.close()
            
        return {
            "success": True,
            "tapes": tapes,
            "count": len(tapes)
        }
        
    except Exception as e:
        logger.error(f"获取磁带列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory")
async def get_tape_inventory(request: Request):
    """获取磁带库存统计（从数据库获取真实数据）"""
    start_time = datetime.now()
    try:
        from config.settings import get_settings
        from datetime import date
        from utils.scheduler.db_utils import is_redis
        
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查是否为 Redis
        if is_redis():
            # Redis模式：使用Redis查询磁带库存
            from backup.redis_tape_db import list_tapes_redis
            
            tapes_redis = await list_tapes_redis()
            
            total_tapes = len(tapes_redis)
            available_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'AVAILABLE')
            in_use_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'IN_USE')
            full_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'FULL')
            # 计算过期磁带数量
            expired_tapes = 0
            for t in tapes_redis:
                expiry = t.get('expiry_date')
                if expiry:
                    try:
                        expiry_date = parse_expiry_date_for_inventory(expiry)
                        if expiry_date and expiry_date < date.today():
                            expired_tapes += 1
                    except Exception:
                        pass
            error_tapes = sum(1 for t in tapes_redis if t.get('status', '').upper() == 'ERROR')
            excellent_tapes = sum(1 for t in tapes_redis if (t.get('health_score') or 0) >= 80)
            good_tapes = sum(1 for t in tapes_redis if 60 <= (t.get('health_score') or 0) < 80)
            warning_tapes = sum(1 for t in tapes_redis if 40 <= (t.get('health_score') or 0) < 60)
            critical_tapes = sum(1 for t in tapes_redis if (t.get('health_score') or 0) < 40)
            total_capacity_bytes = sum(t.get('capacity_bytes', 0) or 0 for t in tapes_redis)
            total_used_bytes = sum(t.get('used_bytes', 0) or 0 for t in tapes_redis)
            total_available_bytes = total_capacity_bytes - total_used_bytes
            
            inventory = {
                "total_tapes": total_tapes,
                "available_tapes": available_tapes,
                "in_use_tapes": in_use_tapes,
                "full_tapes": full_tapes,
                "expired_tapes": expired_tapes,
                "error_tapes": error_tapes,
                "excellent_tapes": excellent_tapes,
                "good_tapes": good_tapes,
                "warning_tapes": warning_tapes,
                "critical_tapes": critical_tapes,
                "total_capacity_bytes": total_capacity_bytes,
                "total_used_bytes": total_used_bytes,
                "total_available_bytes": total_available_bytes,
                "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
            }
            
            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"[Redis模式] 获取磁带库存统计成功: 总计 {total_tapes} 个磁带",
                module="web.api.tape.crud",
                function="get_tape_inventory",
                duration_ms=duration_ms
            )
            return inventory

        # 检查是否为 openGauss
        if is_opengauss():
            # 使用openGauss连接查询磁带库存
            async with get_opengauss_connection() as conn:
                rows = await conn.fetch("SELECT * FROM tape_cartridges")

                # 计算统计信息
                from models.tape import TapeStatus
                from datetime import date

                total_tapes = len(rows)
                available_tapes = sum(1 for t in rows if t.get('status') == TapeStatus.AVAILABLE)
                in_use_tapes = sum(1 for t in rows if t.get('status') == TapeStatus.IN_USE)
                full_tapes = sum(1 for t in rows if t.get('status') == TapeStatus.FULL)
                expired_tapes = sum(1 for t in rows if t.get('expiry_date') and t.get('expiry_date').date() < date.today())
                error_tapes = sum(1 for t in rows if t.get('status') == TapeStatus.ERROR)
                excellent_tapes = sum(1 for t in rows if (t.get('health_score') or 0) >= 80)
                good_tapes = sum(1 for t in rows if 60 <= (t.get('health_score') or 0) < 80)
                warning_tapes = sum(1 for t in rows if 40 <= (t.get('health_score') or 0) < 60)
                critical_tapes = sum(1 for t in rows if (t.get('health_score') or 0) < 40)
                total_capacity_bytes = sum(t.get('capacity_bytes') or 0 for t in rows)
                total_used_bytes = sum(t.get('used_bytes') or 0 for t in rows)
                total_available_bytes = total_capacity_bytes - total_used_bytes

                inventory = {
                    "total_tapes": total_tapes,
                    "available_tapes": available_tapes,
                    "in_use_tapes": in_use_tapes,
                    "full_tapes": full_tapes,
                    "expired_tapes": expired_tapes,
                    "error_tapes": error_tapes,
                    "excellent_tapes": excellent_tapes,
                    "good_tapes": good_tapes,
                    "warning_tapes": warning_tapes,
                    "critical_tapes": critical_tapes,
                    "total_capacity_bytes": total_capacity_bytes,
                    "total_used_bytes": total_used_bytes,
                    "total_available_bytes": total_available_bytes,
                    "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
                }

            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"[openGauss模式] 获取磁带库存统计成功: 总计 {total_tapes} 个磁带",
                module="web.api.tape.crud",
                function="get_tape_inventory",
                duration_ms=duration_ms
            )
            return inventory

        # 检查是否为 SQLite
        is_sqlite = database_url.startswith("sqlite:///") or database_url.startswith("sqlite+aiosqlite:///")

        if is_sqlite:
            # 使用原生SQL查询 SQLite
            
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("SELECT * FROM tape_cartridges")
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                tapes = [dict(zip(columns, row)) for row in rows]
                
                # 计算统计信息
                from models.tape import TapeStatus
                from datetime import date
                
                total_tapes = len(tapes)
                available_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.AVAILABLE.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.AVAILABLE))
                in_use_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.IN_USE.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.IN_USE))
                full_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.FULL.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.FULL))
                expired_tapes = sum(1 for t in tapes if t.get('expiry_date') and (t.get('expiry_date').date() if hasattr(t.get('expiry_date'), 'date') else date.fromisoformat(str(t.get('expiry_date'))[:10])) < date.today())
                error_tapes = sum(1 for t in tapes if (t.get('status') == TapeStatus.ERROR.value if isinstance(t.get('status'), str) else t.get('status') == TapeStatus.ERROR))
                excellent_tapes = sum(1 for t in tapes if (t.get('health_score') or 0) >= 80)
                good_tapes = sum(1 for t in tapes if 60 <= (t.get('health_score') or 0) < 80)
                warning_tapes = sum(1 for t in tapes if 40 <= (t.get('health_score') or 0) < 60)
                critical_tapes = sum(1 for t in tapes if (t.get('health_score') or 0) < 40)
                total_capacity_bytes = sum(t.get('capacity_bytes') or 0 for t in tapes)
                total_used_bytes = sum(t.get('used_bytes') or 0 for t in tapes)
                total_available_bytes = total_capacity_bytes - total_used_bytes
                
                inventory = {
                    "total_tapes": total_tapes,
                    "available_tapes": available_tapes,
                    "in_use_tapes": in_use_tapes,
                    "full_tapes": full_tapes,
                    "expired_tapes": expired_tapes,
                    "error_tapes": error_tapes,
                    "excellent_tapes": excellent_tapes,
                    "good_tapes": good_tapes,
                    "warning_tapes": warning_tapes,
                    "critical_tapes": critical_tapes,
                    "total_capacity_bytes": total_capacity_bytes,
                    "total_used_bytes": total_used_bytes,
                    "total_available_bytes": total_available_bytes,
                    "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
                }

            duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"[SQLite模式] 获取磁带库存统计成功: 总计 {total_tapes} 个磁带",
                module="web.api.tape.crud",
                function="get_tape_inventory",
                duration_ms=duration_ms
            )
            return inventory
        else:
            # 使用统一的连接辅助函数（支持 psycopg2 和 psycopg3）
            from utils.db_connection_helper import get_psycopg_connection_from_url
            
            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            
            try:
                with conn.cursor() as cur:
                    # 从数据库获取真实统计数据
                    # 注意：available_tapes 只统计 AVAILABLE 状态，排除 MAINTENANCE（格式化中）状态
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_tapes,
                            COUNT(*) FILTER (WHERE status = 'AVAILABLE') as available_tapes,
                            COUNT(*) FILTER (WHERE status = 'IN_USE') as in_use_tapes,
                            COUNT(*) FILTER (WHERE status = 'FULL') as full_tapes,
                            COUNT(*) FILTER (WHERE expiry_date < CURRENT_DATE) as expired_tapes,
                            COUNT(*) FILTER (WHERE status = 'ERROR') as error_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 80) as excellent_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 60 AND health_score < 80) as good_tapes,
                            COUNT(*) FILTER (WHERE health_score >= 40 AND health_score < 60) as warning_tapes,
                            COUNT(*) FILTER (WHERE health_score < 40) as critical_tapes,
                            COALESCE(SUM(capacity_bytes), 0) as total_capacity_bytes,
                            COALESCE(SUM(used_bytes), 0) as total_used_bytes
                        FROM tape_cartridges
                    """)
                    
                    row = cur.fetchone()
                    
                    if row:
                        total_capacity_bytes = row[10] or 0
                        total_used_bytes = row[11] or 0
                        total_available_bytes = total_capacity_bytes - total_used_bytes
                        
                        inventory = {
                            "total_tapes": row[0] or 0,
                            "available_tapes": row[1] or 0,
                            "in_use_tapes": row[2] or 0,
                            "full_tapes": row[3] or 0,
                            "expired_tapes": row[4] or 0,
                            "error_tapes": row[5] or 0,
                            "excellent_tapes": row[6] or 0,
                            "good_tapes": row[7] or 0,
                            "warning_tapes": row[8] or 0,
                            "critical_tapes": row[9] or 0,
                            "total_capacity_bytes": total_capacity_bytes,
                            "total_used_bytes": total_used_bytes,
                            "total_available_bytes": total_available_bytes,
                            "usage_percent": (total_used_bytes / total_capacity_bytes * 100) if total_capacity_bytes > 0 else 0
                        }
                    else:
                        # 如果没有数据，返回空统计
                        inventory = {
                            "total_tapes": 0,
                            "available_tapes": 0,
                            "in_use_tapes": 0,
                            "full_tapes": 0,
                            "expired_tapes": 0,
                            "error_tapes": 0,
                            "excellent_tapes": 0,
                            "good_tapes": 0,
                            "warning_tapes": 0,
                            "critical_tapes": 0,
                            "total_capacity_bytes": 0,
                            "total_used_bytes": 0,
                            "total_available_bytes": 0,
                            "usage_percent": 0
                        }
            
            finally:
                conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.TAPE,
            message=f"获取磁带库存统计成功: 总计 {inventory['total_tapes']} 个磁带",
            module="web.api.tape.crud",
            function="get_tape_inventory",
            duration_ms=duration_ms
        )
        
        return inventory

    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取磁带库存失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_tape_inventory",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/current")
async def get_current_tape(request: Request):
    """获取当前磁带信息"""
    start_time = datetime.now()
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        tape_info = await system.tape_manager.get_tape_info()
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        
        if tape_info:
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message=f"获取当前磁带信息成功: {tape_info.get('tape_id', 'N/A')}",
                module="web.api.tape.crud",
                function="get_current_tape",
                duration_ms=duration_ms
            )
            return tape_info
        else:
            await log_system(
                level=LogLevel.INFO,
                category=LogCategory.TAPE,
                message="获取当前磁带信息: 当前没有加载的磁带",
                module="web.api.tape.crud",
                function="get_current_tape",
                duration_ms=duration_ms
            )
            return {"message": "当前没有加载的磁带"}

    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"获取当前磁带信息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.TAPE,
            message=error_msg,
            module="web.api.tape.crud",
            function="get_current_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


