#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API - tape_create
Tape Management API - tape_create
"""

import logging
import traceback
import json
import re
import os
import asyncio
import threading
from typing import List, Dict, Any, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Request, Depends, BackgroundTasks
from pydantic import BaseModel

from .models import CreateTapeRequest, UpdateTapeRequest
from .tape_utils import normalize_tape_label, parse_expiry_date_for_inventory
from models.system_log import OperationType, LogCategory, LogLevel
from utils.log_utils import log_operation, log_system
from utils.scheduler.db_utils import is_opengauss
from utils.tape_tools import tape_tools_manager

logger = logging.getLogger(__name__)
router = APIRouter()





@router.post("/create")
async def create_tape(request: CreateTapeRequest, http_request: Request, background_tasks: BackgroundTasks):
    """创建或更新磁带记录，并使用LtfsCmdFormat.exe格式化磁带"""
    start_time = datetime.now()
    ip_address = http_request.client.host if http_request.client else None
    request_method = "POST"
    request_url = str(http_request.url)
    
    try:
        system = http_request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        from config.settings import get_settings
        from utils.db_connection_helper import get_psycopg_connection_from_url

        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 统一生成卷标与盘符
        current_datetime = datetime.now()
        target_year = request.create_year or current_datetime.year
        target_month = request.create_month or current_datetime.month
        target_month = max(1, min(12, target_month))
        final_label = normalize_tape_label(request.label or request.tape_id, target_year, target_month)
        tape_id_value = final_label

        # Linux系统使用设备路径，Windows使用盘符
        import platform
        is_linux = platform.system() == 'Linux'

        if is_linux:
            # Linux: 使用设备路径（区分大小写）
            drive_letter = (settings.TAPE_DRIVE_LETTER or settings.TAPE_DEVICE_PATH or "/dev/nst0").strip()
        else:
            # Windows: 使用盘符（转大写）
            drive_letter = (settings.TAPE_DRIVE_LETTER or "O").strip().upper()
            if drive_letter.endswith(":"):
                drive_letter = drive_letter[:-1]
            if not drive_letter:
                drive_letter = "O"

        # 检查LTFS工具是否可用
        ltfs_tools_dir = getattr(settings, 'LTFS_TOOLS_DIR', '')
        ltfs_format_tool = os.path.join(ltfs_tools_dir, 'LtfsCmdFormat.exe') if ltfs_tools_dir else ''
        mkltfs_tool = 'mkltfs'  # Linux使用系统命令
        has_ltfs_tools = False

        if is_linux:
            # Linux检查mkltfs是否可用
            import shutil
            has_ltfs_tools = shutil.which(mkltfs_tool) is not None
            if not has_ltfs_tools and ltfs_tools_dir and os.path.exists(os.path.join(ltfs_tools_dir, 'mkltfs')):
                has_ltfs_tools = True
        else:
            # Windows检查LtfsCmdFormat.exe
            has_ltfs_tools = os.path.exists(ltfs_format_tool)
        
        # 检查磁带是否已存在（以卷标为基准）
        tape_exists = False
        label_exists = False

        # 使用 openGauss 连接查询
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        try:
            with conn.cursor() as cur:
                # 检查tape_id是否存在
                cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (tape_id_value,))
                tape_exists = cur.fetchone() is not None
                # 检查label是否存在
                cur.execute("SELECT 1 FROM tape_cartridges WHERE label = %s", (final_label,))
                label_exists = cur.fetchone() is not None
        finally:
            conn.close()
        
        # 如果数据库中没有该卷标，需要格式化磁盘并生成SN
        # 序列号生成优先级：1. 创建年份和月份（request.create_year/create_month） 2. 从卷标中提取 3. 当前年月
        # 如果用户没有提供序列号，自动生成（TPMMNN格式：TP + 月份 + 序号）
        if not request.serial_number:
            # 优先使用创建年份和月份
            year = target_year
            month = target_month
            
            # 如果卷标中包含年月信息，验证是否与创建年月一致
            match = re.search(r'(\d{4})(\d{2})', final_label)
            if match:
                label_year = int(match.group(1))
                label_month = int(match.group(2))
                # 如果卷标中的年月与创建年月不一致，使用卷标中的年月（卷标优先）
                if label_year != year or label_month != month:
                    logger.warning(f"卷标中的年月({label_year}{label_month:02d})与创建年月({year}{month:02d})不一致，使用卷标中的年月")
                    year = label_year
                    month = label_month
            
            # 生成序列号（TPMMNN格式：TP + 月份 + 序号）
            mm = month
            conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    # 查询当前月份已有多少张磁盘（查询TP + 月份开头的序列号）
                    cur.execute("""
                        SELECT COUNT(*) FROM tape_cartridges
                        WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                    """, (f"TP{mm:02d}%",))
                    count = cur.fetchone()[0] or 0
                    sequence = count + 1
                    generated_serial = f"TP{mm:02d}{sequence:02d}"
                    logger.info(f"自动生成序列号: {generated_serial} (创建年份={year}, 创建月份={month}, 序号={sequence}, 卷标={final_label})")
            finally:
                conn.close()
            
            # 使用生成的序列号
            serial_param = generated_serial
            request.serial_number = generated_serial
        else:
            # 用户提供了序列号，验证格式和月份一致性
            candidate = request.serial_number.strip().upper()
            # 验证格式：TPMMNN（TP + 2位月份 + 2位序号）
            if len(candidate) == 6 and candidate.startswith('TP') and candidate[2:4].isdigit() and candidate[4:6].isdigit():
                # 验证序列号中的月份是否与创建月份一致
                serial_month = int(candidate[2:4])
                
                # 优先使用创建年份和月份
                expected_year = target_year
                expected_month = target_month
                
                # 如果卷标中包含年月信息，验证是否与创建年月一致
                match = re.search(r'(\d{4})(\d{2})', final_label)
                if match:
                    label_year = int(match.group(1))
                    label_month = int(match.group(2))
                    # 如果卷标中的年月与创建年月不一致，使用卷标中的年月
                    if label_year != expected_year or label_month != expected_month:
                        expected_year = label_year
                        expected_month = label_month
                
                    # 验证序列号中的月份是否与期望的月份一致
                if serial_month != expected_month:
                    logger.warning(f"序列号中的月份({serial_month:02d})与创建月份({expected_month:02d})不一致，将重新生成")
                    # 重新生成序列号
                    mm = expected_month
                    conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT COUNT(*) FROM tape_cartridges
                                WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                            """, (f"TP{mm:02d}%",))
                            count = cur.fetchone()[0] or 0
                            sequence = count + 1
                            generated_serial = f"TP{mm:02d}{sequence:02d}"
                            serial_param = generated_serial
                            request.serial_number = generated_serial
                            logger.info(f"重新生成序列号: {generated_serial} (创建年份={expected_year}, 创建月份={expected_month}, 序号={sequence})")
                    finally:
                        conn.close()
                else:
                    serial_param = candidate
            else:
                serial_param = None
                logger.warning(f"提供的序列号格式不正确: {request.serial_number}，将自动生成")
                # 如果格式不正确，重新生成
                # 优先使用创建年份和月份
                year = target_year
                month = target_month
                
                # 如果卷标中包含年月信息，验证是否与创建年月一致
                match = re.search(r'(\d{4})(\d{2})', final_label)
                if match:
                    label_year = int(match.group(1))
                    label_month = int(match.group(2))
                    if label_year != year or label_month != month:
                        logger.warning(f"卷标中的年月({label_year}{label_month:02d})与创建年月({year}{month:02d})不一致，使用卷标中的年月")
                        year = label_year
                        month = label_month
                
                mm = month
                conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT COUNT(*) FROM tape_cartridges
                            WHERE serial_number IS NOT NULL AND serial_number LIKE %s
                        """, (f"TP{mm:02d}%",))
                        count = cur.fetchone()[0] or 0
                        sequence = count + 1
                        generated_serial = f"TP{mm:02d}{sequence:02d}"
                        serial_param = generated_serial
                        request.serial_number = generated_serial
                finally:
                    conn.close()

        # LTFS 方式写入磁带：格式化为 LTFS 文件系统
        # 添加新磁带必须格式化
        format_tape = True  # 强制格式化
        tape_prepare_result = {"success": True, "message": "待格式化"}

        logger.info(f"[LTFS] ========== 开始 LTFS 格式化流程 ==========")
        logger.info(f"[LTFS] 磁带ID: {tape_id_value}")
        logger.info(f"[LTFS] 卷名: {final_label}")
        logger.info(f"[LTFS] 序列号: {serial_param}")
        logger.info(f"[LTFS] 设备: {drive_letter}")
        logger.info(f"[LTFS] 提示: 格式化可能需要几分钟到一小时，请耐心等待...")

        # 使用线程执行 LTFS 格式化
        def prepare_tape_thread():
            """线程中执行 LTFS 格式化"""
            from backup.tape_handler import TapeHandler
            import asyncio
            nonlocal tape_prepare_result

            try:
                logger.info(f"[LTFS格式化] 步骤1: 创建事件循环...")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

                try:
                    logger.info(f"[LTFS格式化] 步骤2: 创建 TapeHandler 实例...")
                    handler = TapeHandler(settings=settings)

                    logger.info(f"[LTFS格式化] 步骤3: 执行 mkltfs 格式化命令...")
                    logger.info(f"[LTFS格式化] 命令: mkltfs -d {drive_letter} -n {final_label} -s {serial_param} -f")
                    logger.info(f"[LTFS格式化] 正在格式化，请稍候（此过程可能需要较长时间）...")

                    success, msg = loop.run_until_complete(
                        handler.format_tape(volume_name=final_label, serial=serial_param)
                    )

                    tape_prepare_result = {
                        "success": success,
                        "message": msg
                    }

                    if success:
                        logger.info(f"[LTFS格式化] ✅ 格式化成功!")
                        logger.info(f"[LTFS格式化] 结果: {msg}")
                        logger.info(f"[LTFS格式化] ========== LTFS 格式化完成 ==========")

                        # 格式化成功后，检查并录入 openGauss 数据库
                        logger.info(f"[LTFS格式化] 步骤4: 检查 openGauss 数据库中是否存在磁带记录...")
                        try:
                            from utils.db_connection_helper import get_psycopg_connection_from_url, set_autocommit
                            from config.settings import get_settings

                            settings_obj = get_settings()
                            database_url = settings_obj.DATABASE_URL

                            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                            try:
                                set_autocommit(conn, is_psycopg3, autocommit=True)
                                with conn.cursor() as cur:
                                    # 检查磁带是否已存在
                                    cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (tape_id_value,))
                                    tape_exists = cur.fetchone() is not None

                                    if not tape_exists:
                                        logger.info(f"[LTFS格式化] 数据库中不存在磁带 {tape_id_value}，开始录入...")

                                        # 计算容量与有效期
                                        capacity_bytes_local = request.capacity_gb * (1024 ** 3) if request.capacity_gb else 18 * 1024 * (1024 ** 3)
                                        created_date_local = datetime(target_year, target_month, 1)

                                        expiry_year_local = created_date_local.year
                                        expiry_month_local = created_date_local.month + request.retention_months
                                        while expiry_month_local > 12:
                                            expiry_year_local += 1
                                            expiry_month_local -= 12
                                        expiry_date_local = datetime(expiry_year_local, expiry_month_local, 1)

                                        # 录入数据库
                                        media_type_str = request.media_type.value if hasattr(request.media_type, 'value') else str(request.media_type)
                                        cur.execute(
                                            """
                                            INSERT INTO tape_cartridges
                                            (tape_id, label, status, media_type, generation, serial_number, location,
                                             capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            """,
                                            (
                                                tape_id_value,
                                                final_label,
                                                'available',
                                                media_type_str,
                                                request.generation,
                                                serial_param,
                                                request.location or '',
                                                capacity_bytes_local,
                                                0,
                                                request.retention_months,
                                                request.notes or '完整备份前格式化',
                                                created_date_local,
                                                expiry_date_local,
                                                True,
                                                100
                                            )
                                        )
                                        logger.info(f"[LTFS格式化] ✅ openGauss 录入磁带成功: {tape_id_value}")
                                    else:
                                        logger.info(f"[LTFS格式化] 数据库中已存在磁带 {tape_id_value}，跳过录入")
                            finally:
                                conn.close()

                        except Exception as db_error:
                            logger.error(f"[LTFS格式化] 录入 openGauss 数据库异常: {str(db_error)}", exc_info=True)

                        # 发送钉钉通知
                        try:
                            if system and hasattr(system, 'dingtalk_notifier') and system.dingtalk_notifier:
                                system.dingtalk_notifier.send_tape_format_notification_sync(
                                    tape_id=tape_id_value,
                                    status="success",
                                    volume_label=final_label,
                                    serial_number=serial_param
                                )
                        except Exception as notify_error:
                            logger.error(f"发送磁带格式化成功钉钉通知异常: {str(notify_error)}")
                    else:
                        logger.error(f"[LTFS格式化] ❌ 格式化失败: {msg}")
                        logger.error(f"[LTFS格式化] ========== LTFS 格式化失败 ==========")

                        # 发送失败通知
                        try:
                            if system and hasattr(system, 'dingtalk_notifier') and system.dingtalk_notifier:
                                system.dingtalk_notifier.send_tape_format_notification_sync(
                                    tape_id=tape_id_value,
                                    status="failed",
                                    error_detail=msg,
                                    volume_label=final_label,
                                    serial_number=serial_param
                                )
                        except Exception as notify_error:
                            logger.error(f"发送磁带格式化失败钉钉通知异常: {str(notify_error)}")
                finally:
                    loop.close()

            except Exception as e:
                logger.error(f"[LTFS格式化] ❌ 异常: {str(e)}", exc_info=True)
                logger.error(f"[LTFS格式化] ========== LTFS 格式化异常 ==========")
                tape_prepare_result = {
                    "success": False,
                    "message": f"异常: {str(e)}"
                }

        # 在后台线程执行
        import threading
        prepare_thread = threading.Thread(target=prepare_tape_thread, daemon=True, name=f"FormatLTFS-{tape_id_value}")
        prepare_thread.start()
        logger.info(f"[LTFS] 格式化任务已在后台启动，请查看日志了解进度")

        # 计算容量与有效期
        capacity_bytes = request.capacity_gb * (1024 ** 3) if request.capacity_gb else 18 * 1024 * (1024 ** 3)
        created_date = datetime(target_year, target_month, 1)
        
        expiry_year = created_date.year
        expiry_month = created_date.month + request.retention_months
        while expiry_month > 12:
            expiry_year += 1
            expiry_month -= 12
        expiry_date = datetime(expiry_year, expiry_month, 1)
        
        new_values = {
            "tape_id": tape_id_value,
            "label": final_label,
            "status": "available",  # 枚举值必须是小写
            "media_type": request.media_type,
            "generation": request.generation,
            "serial_number": request.serial_number,
            "location": request.location,
            "capacity_bytes": capacity_bytes,
            "retention_months": request.retention_months,
            "notes": request.notes,
            "manufactured_date": created_date.isoformat(),
            "expiry_date": expiry_date.isoformat()
        }
        
        # 写入或更新数据库（仅在不需要格式化时执行，如果需要格式化则在线程中执行）
        if format_tape:
            # 如果需要格式化，跳过这里的数据库写入，在线程中格式化成功并读取卷标后再写数据库
            logger.info(f"磁带 {tape_id_value} 需要格式化，数据库写入将在格式化成功后在线程中执行")
        else:
            conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    if tape_exists:
                        logger.info("磁带 %s 已存在，%s更新数据库记录",
                                  tape_id_value, "后台格式化任务已启动，" if format_tape else "跳过格式化，直接")
                        cur.execute(
                            """
                            UPDATE tape_cartridges
                            SET label = %s,
                                status = %s,
                                media_type = %s,
                                generation = %s,
                                serial_number = %s,
                                location = %s,
                                capacity_bytes = %s,
                                retention_months = %s,
                                notes = %s,
                                manufactured_date = %s,
                                expiry_date = %s
                            WHERE tape_id = %s
                            """,
                            (
                                final_label,
                                'available',  # 枚举值必须是小写
                                request.media_type,
                                request.generation,
                                request.serial_number,
                                request.location,
                                capacity_bytes,
                                request.retention_months,
                                request.notes,
                                created_date,
                                expiry_date,
                                tape_id_value
                            )
                        )
                    else:
                        logger.info("磁带 %s 不存在，创建新数据库记录", tape_id_value)
                        cur.execute(
                            """
                            INSERT INTO tape_cartridges
                            (tape_id, label, status, media_type, generation, serial_number, location,
                             capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                tape_id_value,
                                final_label,
                                'available',  # 枚举值必须是小写
                                request.media_type,
                                request.generation,
                                request.serial_number,
                                request.location,
                                capacity_bytes,
                                0,
                                request.retention_months,
                                request.notes,
                                created_date,
                                expiry_date,
                                True,
                                100
                            )
                        )
                conn.commit()
            finally:
                conn.close()
        
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        operation_type = OperationType.UPDATE if tape_exists else OperationType.CREATE
        operation_name = "更新磁带" if tape_exists else "创建磁带"
        operation_description = f"磁带 {tape_id_value} {'更新' if tape_exists else '创建'}成功，{'格式化任务已在后台启动' if format_tape else '未格式化（用户选择跳过）'}"
        result_message = f"磁带 {tape_id_value} {'更新' if tape_exists else '创建'}成功，卷标 {final_label}" + ("（格式化任务已在后台执行）" if format_tape else "")
        
        await log_operation(
            operation_type=operation_type,
            resource_type="tape",
            resource_id=tape_id_value,
            resource_name=final_label,
            operation_name=operation_name,
            operation_description=operation_description,
            category="tape",
            success=True,
            result_message=result_message,
            new_values=new_values,
            ip_address=ip_address,
            request_method=request_method,
            request_url=request_url,
            duration_ms=duration_ms
        )
        
        return {
            "success": True,
            "message": result_message,
            "tape_id": tape_id_value,
            "label": final_label,
            "formatted": format_tape,
            "updated": tape_exists
        }
        
    except HTTPException:
        raise
    except Exception as e:
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        error_msg = f"创建/更新磁带失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        await log_operation(
            operation_type=OperationType.UPDATE,
            resource_type="tape",
            resource_id=tape_id_value if 'tape_id_value' in locals() else getattr(request, 'tape_id', None),
            resource_name=final_label if 'final_label' in locals() else getattr(request, 'label', None),
            operation_name="创建/更新磁带",
            operation_description="磁带创建/更新失败",
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
            function="create_tape",
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc(),
            duration_ms=duration_ms
        )
        raise HTTPException(status_code=500, detail=str(e))


