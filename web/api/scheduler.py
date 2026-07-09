#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务管理API
Scheduled Task Management API
"""

import logging
import re
import asyncio
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from datetime import datetime

from models.scheduled_task import ScheduledTask, ScheduledTaskLog, ScheduleType, ScheduledTaskStatus, TaskActionType
from models.system_log import OperationType
from utils.scheduler import TaskScheduler
from utils.log_utils import log_operation, log_system
from models.system_log import LogLevel, LogCategory
from utils.scheduler.task_storage import release_task_locks_by_task, release_all_active_locks
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
from utils.tape_tools import tape_tools_manager

logger = logging.getLogger(__name__)
router = APIRouter()


# ===== 请求/响应模型 =====

class ScheduleConfigBase(BaseModel):
    """调度配置基类"""
    pass


class OnceScheduleConfig(ScheduleConfigBase):
    """一次性任务配置"""
    datetime: str = Field(..., description="执行时间，格式: YYYY-MM-DD HH:MM:SS")


class IntervalScheduleConfig(ScheduleConfigBase):
    """间隔任务配置"""
    interval: int = Field(..., description="间隔数值")
    unit: str = Field(..., description="时间单位: minutes/hours/days")


class DailyScheduleConfig(ScheduleConfigBase):
    """每日任务配置"""
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class WeeklyScheduleConfig(ScheduleConfigBase):
    """每周任务配置"""
    day_of_week: int = Field(..., ge=0, le=6, description="星期几 (0=Monday, 6=Sunday)")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class MonthlyScheduleConfig(ScheduleConfigBase):
    """每月任务配置"""
    day_of_month: int = Field(..., ge=1, le=31, description="每月几号")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class YearlyScheduleConfig(ScheduleConfigBase):
    """每年任务配置"""
    month: int = Field(..., ge=1, le=12, description="月份")
    day: int = Field(..., ge=1, le=31, description="日期")
    time: str = Field(..., description="执行时间，格式: HH:MM:SS")


class CronScheduleConfig(ScheduleConfigBase):
    """Cron表达式配置"""
    cron: str = Field(..., description="Cron表达式")


class ScheduledTaskCreate(BaseModel):
    """创建计划任务请求"""
    task_name: str = Field(..., description="任务名称")
    description: Optional[str] = Field(None, description="任务描述")
    schedule_type: str = Field(..., description="调度类型: once/interval/daily/weekly/monthly/yearly/cron")
    schedule_config: Dict[str, Any] = Field(..., description="调度配置")
    action_type: str = Field(..., description="任务动作类型: backup/recovery/cleanup/health_check/retention_check/custom")
    action_config: Dict[str, Any] = Field(default_factory=dict, description="任务动作配置")
    backup_task_id: Optional[int] = Field(None, description="备份任务模板ID（当action_type=backup时使用）")
    enabled: bool = Field(True, description="是否启用")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    task_metadata: Optional[Dict[str, Any]] = Field(None, description="任务元数据")


class ScheduledTaskUpdate(BaseModel):
    """更新计划任务请求"""
    task_name: Optional[str] = Field(None, description="任务名称")
    description: Optional[str] = Field(None, description="任务描述")
    schedule_type: Optional[str] = Field(None, description="调度类型")
    schedule_config: Optional[Dict[str, Any]] = Field(None, description="调度配置")
    action_type: Optional[str] = Field(None, description="任务动作类型")
    action_config: Optional[Dict[str, Any]] = Field(None, description="任务动作配置")
    enabled: Optional[bool] = Field(None, description="是否启用")
    tags: Optional[List[str]] = Field(None, description="标签列表")
    task_metadata: Optional[Dict[str, Any]] = Field(None, description="任务元数据")


class ScheduledTaskResponse(BaseModel):
    """计划任务响应"""
    id: int
    task_name: str
    description: Optional[str]
    schedule_type: str
    schedule_config: Dict[str, Any]
    action_type: str
    action_config: Dict[str, Any]
    status: str
    enabled: bool
    next_run_time: Optional[datetime]
    last_run_time: Optional[datetime]
    last_success_time: Optional[datetime]
    last_failure_time: Optional[datetime]
    total_runs: int
    success_runs: int
    failure_runs: int
    average_duration: Optional[int]
    last_error: Optional[str]
    tags: Optional[List[str]]
    task_metadata: Optional[Dict[str, Any]]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ScheduledTaskRunRequest(BaseModel):
    """手动运行计划任务的选项"""
    mode: Optional[str] = Field("auto", description="运行模式: auto/resume/restart")
    force_rescan: Optional[bool] = Field(False, description="强制重新扫描/压缩前置文件")


# ===== API端点 =====

@router.get("/tasks", response_model=List[ScheduledTaskResponse])
async def get_scheduled_tasks(
    enabled_only: bool = False,
    request: Request = None
):
    """获取所有计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        # 使用TaskScheduler获取任务
        scheduler: TaskScheduler = system.scheduler
        tasks = await scheduler.get_tasks(enabled_only=enabled_only)
        
        # 确保tasks是列表，避免返回None
        if tasks is None:
            logger.warning("scheduler.get_tasks()返回了None，返回空列表")
            return []
        
        # 确保tasks是可迭代的
        if not isinstance(tasks, (list, tuple)):
            logger.warning(f"scheduler.get_tasks()返回了非列表类型: {type(tasks)}，返回空列表")
            return []
        
        # 安全地构建响应列表
        try:
            result = [ScheduledTaskResponse(**task.to_dict()) for task in tasks]
            return result
        except Exception as e:
            logger.error(f"构建计划任务响应列表失败: {str(e)}", exc_info=True)
            return []
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取计划任务列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=ScheduledTaskResponse)
async def get_scheduled_task(task_id: int, request: Request = None):
    """获取单个计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        task = await scheduler.get_task(task_id)
        
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return ScheduledTaskResponse(**task.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks", response_model=ScheduledTaskResponse)
async def create_scheduled_task(task: ScheduledTaskCreate, request: Request = None):
    """创建计划任务
    
    当action_type=backup时，如果提供了backup_task_id，将从备份任务模板加载配置。
    """
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 验证备份任务模板（如果提供了backup_task_id）
        if task.action_type == "backup" and task.backup_task_id:
            # 使用 openGauss 原生 SQL 验证备份任务模板
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow("""
                    SELECT id, task_name, task_type, source_paths, exclude_patterns,
                           compression_enabled, encryption_enabled, retention_days,
                           description, tape_device, is_template
                    FROM backup_tasks
                    WHERE id = $1 AND is_template = TRUE
                """, task.backup_task_id)

                if not row:
                    raise HTTPException(
                        status_code=404,
                        detail=f"备份任务模板不存在: {task.backup_task_id}"
                    )
            
            # 将backup_task_id保存到task_metadata中
            if task.task_metadata is None:
                task.task_metadata = {}
            task.task_metadata['backup_task_id'] = task.backup_task_id
        
        # 验证调度类型
        try:
            schedule_type = ScheduleType(task.schedule_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的调度类型: {task.schedule_type}")
        
        # 验证任务动作类型
        try:
            action_type = TaskActionType(task.action_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"无效的任务动作类型: {task.action_type}")
        
        # 检查磁带卷标并格式化磁盘（无论数据库中是否存在该卷标）
        # 仅在备份任务且目标为磁带时执行
        if task.action_type == "backup":
            action_config = task.action_config or {}
            volume_label = action_config.get("volume_label") or action_config.get("tape_volume_label")
            backup_target = action_config.get("backup_target", "")
            tape_device = action_config.get("tape_device")
            
            # 如果指定了卷标且备份目标是磁带
            if volume_label and (backup_target == "tape" or tape_device):
                try:
                    await _check_and_format_tape_if_needed(
                        volume_label=volume_label,
                        system=system,
                        request=request
                    )
                except Exception as e:
                    logger.error(f"检查并格式化磁带失败: {str(e)}")
                    # 不阻止任务创建，只记录错误
                    await log_system(
                        level=LogLevel.WARNING,
                        category=LogCategory.SCHEDULER,
                        message=f"创建计划任务时检查磁带卷标失败: {str(e)}",
                        module="web.api.scheduler",
                        function="create_scheduled_task",
                        details={"volume_label": volume_label, "error": str(e)}
                    )
        
        # 创建计划任务对象
        scheduled_task = ScheduledTask(
            task_name=task.task_name,
            description=task.description,
            schedule_type=schedule_type,
            schedule_config=task.schedule_config,
            action_type=action_type,
            action_config=task.action_config,
            enabled=task.enabled,
            status=ScheduledTaskStatus.ACTIVE if task.enabled else ScheduledTaskStatus.INACTIVE,
            tags=task.tags,
            task_metadata=task.task_metadata
        )
        # 同时设置 backup_task_id 列，确保 JOIN 查询能直接匹配
        if task.backup_task_id:
            scheduled_task.backup_task_id = task.backup_task_id

        # 添加任务
        success = await scheduler.add_task(scheduled_task)
        
        if not success:
            raise HTTPException(status_code=500, detail="创建计划任务失败")
        
        # 重新获取任务（包含ID和时间信息）
        created_task = await scheduler.get_task(scheduled_task.id)
        
        if not created_task:
            raise HTTPException(status_code=500, detail="创建计划任务后无法获取任务详情")
        
        # 构建响应对象，确保时间字段是 datetime 对象而不是字符串
        return ScheduledTaskResponse(
            id=created_task.id,
            task_name=created_task.task_name,
            description=created_task.description,
            schedule_type=created_task.schedule_type.value if created_task.schedule_type else None,
            schedule_config=created_task.schedule_config or {},
            action_type=created_task.action_type.value if created_task.action_type else None,
            action_config=created_task.action_config or {},
            status=created_task.status.value if created_task.status else ScheduledTaskStatus.INACTIVE.value,
            enabled=created_task.enabled,
            next_run_time=created_task.next_run_time,
            last_run_time=created_task.last_run_time,
            last_success_time=created_task.last_success_time,
            last_failure_time=created_task.last_failure_time,
            total_runs=getattr(created_task, 'total_runs', 0),
            success_runs=getattr(created_task, 'success_runs', 0),
            failure_runs=getattr(created_task, 'failure_runs', 0),
            average_duration=getattr(created_task, 'average_duration', None),
            last_error=getattr(created_task, 'last_error', None),
            tags=created_task.tags or [],
            task_metadata=created_task.task_metadata or {},
            created_at=created_task.created_at if hasattr(created_task, 'created_at') and created_task.created_at else datetime.now(),
            updated_at=created_task.updated_at if hasattr(created_task, 'updated_at') and created_task.updated_at else datetime.now()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        logger.error(f"创建计划任务失败: {str(e)}")
        logger.error(f"错误详情:\n{error_detail}")
        # 返回更详细的错误信息（仅用于调试，生产环境可以简化）
        error_msg = str(e)
        if "day" in error_msg.lower() or "month" in error_msg.lower() or "date" in error_msg.lower():
            error_msg = f"日期时间计算错误: {error_msg}"
        raise HTTPException(status_code=500, detail=error_msg)


# ===== 辅助函数 =====

async def _check_tape_label_exists(volume_label: str) -> bool:
    """检查数据库中是否存在指定的磁带卷标
    
    Args:
        volume_label: 磁带卷标
        
    Returns:
        如果存在返回True，否则返回False
    """
    try:
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                "SELECT tape_id FROM tape_cartridges WHERE label = $1 LIMIT 1",
                volume_label
            )
            return row is not None
    except Exception as e:
        logger.error(f"检查磁带卷标是否存在失败: {str(e)}")
        return False


async def _generate_serial_number(year: int, month: int) -> str:
    """生成序列号（SN），格式：TPMMNN（TP + 月份2位 + 序号2位）
    
    Args:
        year: 年份（4位）
        month: 月份（1-12）
        
    Returns:
        6位序列号，例如：TP1101（11月第一张磁盘）
    """
    try:
        mm = month

        async with get_opengauss_connection() as conn:
            # 查询序列号以TPMM开头的记录数量（排除NULL）
            count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM tape_cartridges
                WHERE serial_number IS NOT NULL AND serial_number LIKE $1
                """,
                f"TP{mm:02d}%"
            )
            # 序号从01开始
            sequence = (count or 0) + 1

        # 生成6位序列号：TPMMNN
        sn = f"TP{mm:02d}{sequence:02d}"
        logger.info(f"生成序列号: {sn} (年份={year}, 月份={month}, 序号={sequence})")
        return sn
    except Exception as e:
        logger.error(f"生成序列号失败: {str(e)}")
        # 如果失败，使用默认序号01
        mm = month
        return f"TP{mm:02d}01"


async def _format_tape_via_disk_management(
    volume_label: str,
    serial_number: str,
    system_instance,
    request: Request = None
) -> bool:
    """通过调用磁盘管理的格式化功能来格式化磁盘（复用磁盘管理的后台格式化逻辑）
    
    Args:
        volume_label: 卷标名称
        serial_number: 序列号（6位）
        system_instance: 系统实例
        request: HTTP请求对象（用于日志）
        
    Returns:
        成功返回True，失败返回False
    """
    from config.settings import get_settings
    from utils.tape_tools import tape_tools_manager
    from models.system_log import OperationType
    from utils.log_utils import log_operation
    from utils.db_connection_helper import get_psycopg_connection_from_url
    
    try:
        # 获取数据库连接信息
        settings = get_settings()
        database_url = settings.DATABASE_URL
        
        # 检查磁带是否存在
        tape_id = volume_label  # 使用卷标作为tape_id
        conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
        tape_exists = False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (tape_id,))
                tape_exists = cur.fetchone() is not None
        finally:
            conn.close()
        
        # 如果不存在，先创建记录（状态为MAINTENANCE）
        if not tape_exists:
            conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO tape_cartridges (tape_id, label, serial_number, status, capacity_bytes, used_bytes, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                        """,
                        (tape_id, volume_label, serial_number, 'MAINTENANCE', 0, 0)
                    )
                    conn.commit()
                    logger.info(f"创建磁带记录（状态为MAINTENANCE）: tape_id={tape_id}, label={volume_label}, SN={serial_number}")
            finally:
                conn.close()
        else:
            # 如果存在，更新状态为MAINTENANCE
            conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tape_cartridges SET status = %s, label = %s, serial_number = %s, updated_at = NOW() WHERE tape_id = %s",
                        ('MAINTENANCE', volume_label, serial_number, tape_id)
                    )
                    conn.commit()
                    logger.info(f"更新磁带记录（状态为MAINTENANCE）: tape_id={tape_id}, label={volume_label}, SN={serial_number}")
            finally:
                conn.close()
        
        # 后台执行格式化任务（与磁盘管理页面一致）
        async def format_tape_background():
            """后台格式化任务（复用磁盘管理的格式化逻辑）"""
            try:
                # 重新连接数据库
                db_conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                
                try:
                    # 获取盘符
                    drive_letter = system_instance.settings.TAPE_DRIVE_LETTER or "O"
                    if drive_letter.endswith(":"):
                        drive_letter = drive_letter[:-1]
                    drive_letter = drive_letter.strip().upper()
                    
                    logger.info(f"后台格式化磁盘（计划任务）: 卷标={volume_label}, SN={serial_number}, 盘符={drive_letter}")
                    # 格式化操作设置2小时超时（7200秒）
                    try:
                        format_result = await asyncio.wait_for(
                            tape_tools_manager.format_tape_ltfs(
                                drive_letter=drive_letter,
                                volume_label=volume_label,
                                serial=serial_number,
                                eject_after=False
                            ),
                            timeout=7200.0  # 2小时超时
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"计划任务中格式化磁带超时（2小时）: 卷标={volume_label}, SN={serial_number}")
                        format_result = {
                            "success": False,
                            "returncode": -1,
                            "stdout": "",
                            "stderr": "格式化操作超时（2小时），操作未完成"
                        }
                    
                    logger.info(f"格式化命令执行完成 - 成功: {format_result.get('success')}, 返回码: {format_result.get('returncode')}, "
                              f"stdout长度: {len(format_result.get('stdout', ''))}, stderr长度: {len(format_result.get('stderr', ''))}")
                    
                    if format_result.get("success"):
                        logger.info(f"磁盘格式化成功: 卷标={volume_label}, SN={serial_number}")
                        
                        # 格式化成功后，从磁盘读取实际的卷标和SN，然后更新数据库
                        try:
                            # 等待一小段时间，确保格式化操作完全完成
                            await asyncio.sleep(2)
                            
                            # 读取磁盘上的实际卷标和序列号（60秒超时）
                            try:
                                label_result = await asyncio.wait_for(
                                    tape_tools_manager.read_tape_label_windows(),
                                    timeout=60.0
                                )
                            except asyncio.TimeoutError:
                                logger.warning("计划任务中读取磁带卷标超时（60秒）")
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
                                    if actual_label and actual_label != volume_label:
                                        update_fields.append("label = %s")
                                        update_values.append(actual_label)
                                        logger.info(f"更新数据库卷标: {volume_label} -> {actual_label}")
                                    
                                    # 更新序列号（如果读取到的值与数据库中的不同）
                                    if actual_serial and actual_serial != serial_number:
                                        update_fields.append("serial_number = %s")
                                        update_values.append(actual_serial)
                                        logger.info(f"更新数据库序列号: {serial_number} -> {actual_serial}")
                                    
                                    # 如果有需要更新的字段，执行更新
                                    if update_fields:
                                        update_values.append(tape_id)
                                        update_sql = f"""
                                            UPDATE tape_cartridges
                                            SET {', '.join(update_fields)}, updated_at = NOW()
                                            WHERE tape_id = %s
                                        """
                                        with db_conn.cursor() as db_cur:
                                            db_cur.execute(update_sql, update_values)
                                            db_conn.commit()
                                            logger.info(f"已根据磁盘实际值更新数据库: tape_id={tape_id}, 更新字段={update_fields}")
                                    else:
                                        logger.info(f"磁盘实际值与数据库一致，无需更新: 卷标={actual_label}, SN={actual_serial}")
                                else:
                                    logger.warning("从磁盘读取到的卷标或序列号为空，跳过数据库更新")
                                
                                # 格式化成功，将状态改回AVAILABLE（可用）
                                with db_conn.cursor() as db_cur:
                                    db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                ('AVAILABLE', tape_id))
                                    db_conn.commit()
                                    logger.info(f"格式化完成，将磁带 {tape_id} 状态设置为 AVAILABLE")
                            else:
                                logger.warning(f"读取磁盘卷标失败: {label_result.get('error', '未知错误')}")
                                # 即使读取失败，格式化成功也应该将状态改回AVAILABLE
                                with db_conn.cursor() as db_cur:
                                    db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                ('AVAILABLE', tape_id))
                                    db_conn.commit()
                                    logger.info(f"格式化完成（读取卷标失败），将磁带 {tape_id} 状态设置为 AVAILABLE")
                        except Exception as read_error:
                            logger.error(f"读取磁盘卷标并更新数据库时出错: {str(read_error)}", exc_info=True)
                            # 即使读取异常，格式化成功也应该将状态改回AVAILABLE
                            try:
                                with db_conn.cursor() as db_cur:
                                    db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                                ('AVAILABLE', tape_id))
                                    db_conn.commit()
                                    logger.info(f"格式化完成（读取卷标异常），将磁带 {tape_id} 状态设置为 AVAILABLE")
                            except Exception as db_update_error:
                                logger.error(f"更新状态失败: {str(db_update_error)}", exc_info=True)
                    else:
                        # 格式化失败或超时，将状态改为ERROR（故障）
                        error_detail = format_result.get("stderr") or format_result.get("stdout") or "格式化失败"
                        returncode = format_result.get("returncode", -1)
                        is_timeout = "超时" in error_detail or "超时" in (format_result.get("stderr") or "")
                        
                        if is_timeout:
                            logger.error(f"格式化磁盘超时（2小时）- 卷标: {volume_label}, SN: {serial_number}, 错误详情: {error_detail}")
                        else:
                            logger.error(f"格式化磁盘失败 - 返回码: {returncode}, 错误详情: {error_detail}")
                        
                        # 格式化失败或超时时，将状态改为ERROR
                        with db_conn.cursor() as db_cur:
                            db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                        ('ERROR', tape_id))
                            db_conn.commit()
                            logger.warning(f"格式化失败/超时，将磁带 {tape_id} 状态设置为 ERROR")
                        
                        # 发送钉钉通知（失败或超时都发送）
                        try:
                            if system_instance and hasattr(system_instance, 'dingtalk_notifier') and system_instance.dingtalk_notifier:
                                await system_instance.dingtalk_notifier.send_tape_format_notification(
                                    tape_id=tape_id,
                                    status="failed",
                                    error_detail=f"{'超时（2小时）' if is_timeout else f'返回码: {returncode}'}, {error_detail}",
                                    volume_label=volume_label,
                                    serial_number=serial_number
                                )
                        except Exception as notify_error:
                            logger.error(f"发送格式化失败/超时钉钉通知异常: {str(notify_error)}", exc_info=True)
                finally:
                    db_conn.close()
            except Exception as e:
                logger.error(f"后台格式化磁盘异常: {str(e)}", exc_info=True)
                # 异常时也要将状态改为ERROR
                try:
                    db_conn, _ = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                    with db_conn.cursor() as db_cur:
                        db_cur.execute("UPDATE tape_cartridges SET status = %s WHERE tape_id = %s", 
                                    ('ERROR', tape_id))
                        db_conn.commit()
                        logger.error(f"格式化异常，将磁带 {tape_id} 状态设置为 ERROR")
                    db_conn.close()
                except Exception as db_error:
                    logger.error(f"更新磁带状态失败: {str(db_error)}", exc_info=True)
        
        # 在后台执行格式化任务（不阻塞API）
        asyncio.create_task(format_tape_background())
        logger.info(f"格式化磁盘任务已在后台启动（计划任务）: 卷标={volume_label}, SN={serial_number}")
        
        # 记录操作日志
        await log_operation(
            operation_type=OperationType.TAPE_FORMAT,
            resource_type="tape",
            resource_id=tape_id,
            resource_name=volume_label,
            operation_name="计划任务创建时格式化磁盘",
            operation_description=f"格式化磁盘并添加卷标和SN: {volume_label} / {serial_number}（后台执行）",
            category="scheduler",
            success=True,
            result_message=f"格式化任务已启动，卷标={volume_label}, SN={serial_number}",
            request_method="POST",
            request_url="/api/scheduler/tasks",
            ip_address=request.client.host if request and request.client else None
        )
        
        return True
    except Exception as e:
        logger.error(f"调用磁盘管理格式化功能失败: {str(e)}", exc_info=True)
        return False


async def _check_and_format_tape_if_needed(
    volume_label: str,
    system,
    request: Request = None
):
    """格式化磁盘并添加/更新卷标和SN记录（无论数据库中是否存在该卷标）
    
    先判断当前磁带卷标是否为当月，如果是当月，根据当前系统时间更新卷标。
    然后调用磁盘管理的格式化功能（后台异步执行）。
    
    Args:
        volume_label: 磁带卷标
        system: 系统实例
        request: HTTP请求对象（用于日志）
    """
    from web.api.tape.crud import _normalize_tape_label
    
    # 获取当前系统时间
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    
    # 从卷标中提取年月信息
    label_year = None
    label_month = None
    match = re.search(r'(\d{4})(\d{2})', volume_label)
    if match:
        label_year = int(match.group(1))
        label_month = int(match.group(2))
        logger.info(f"从卷标提取年月: {label_year}年{label_month}月")
    
    # 判断当前磁带卷标是否为当月
    is_current_month = False
    if label_year is not None and label_month is not None:
        is_current_month = (label_year == current_year and label_month == current_month)
        logger.info(f"卷标年月({label_year}年{label_month}月) vs 当前年月({current_year}年{current_month}月): {'是当月' if is_current_month else '不是当月'}")
    
    # 如果是当月，根据当前系统时间更新卷标
    if is_current_month:
        # 使用当前系统时间规范化卷标（格式：TPYYYYMMNN）
        updated_label = _normalize_tape_label(volume_label, current_year, current_month)
        if updated_label != volume_label:
            logger.info(f"卷标是当月，根据当前系统时间更新卷标: {volume_label} -> {updated_label}")
            volume_label = updated_label
        else:
            logger.info(f"卷标已是当前系统时间格式，无需更新: {volume_label}")
    else:
        # 如果不是当月，使用当前系统时间生成新卷标
        updated_label = _normalize_tape_label(volume_label, current_year, current_month)
        logger.info(f"卷标不是当月，根据当前系统时间生成新卷标: {volume_label} -> {updated_label}")
        volume_label = updated_label
    
    # 检查数据库中是否存在该卷标
    exists = await _check_tape_label_exists(volume_label)
    
    if exists:
        logger.info(f"磁带卷标已存在于数据库，将调用磁盘管理的格式化功能: {volume_label}")
    else:
        logger.info(f"磁带卷标不存在于数据库，将调用磁盘管理的格式化功能创建新记录: {volume_label}")
    
    # 从卷标中提取年月信息，用于生成序列号
    match = re.search(r'(\d{4})(\d{2})', volume_label)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
    else:
        year = current_year
        month = current_month
    
    # 生成序列号（TPMMNN格式）
    serial_number = await _generate_serial_number(year, month)
    
    # 调用磁盘管理的格式化功能（复用后台格式化逻辑）
    success = await _format_tape_via_disk_management(
        volume_label=volume_label,
        serial_number=serial_number,
        system_instance=system,
        request=request
    )
    
    if not success:
        raise RuntimeError(f"格式化磁盘失败: {volume_label}")


# 更具体的路径必须在通用路径之前定义，以确保路由匹配正确
@router.post("/tasks/{task_id}/run")
async def run_scheduled_task(
    task_id: int,
    run_request: Optional[ScheduledTaskRunRequest] = None,
    request: Request = None
):
    """立即运行计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 获取任务信息（用于日志）
        task = await scheduler.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        run_options = {"mode": "auto"}
        if run_request:
            run_options = run_request.dict(exclude_none=True)
            if 'mode' not in run_options or not run_options['mode']:
                run_options['mode'] = "auto"
        
        try:
            success = await scheduler.run_task(task_id, run_options=run_options)
            
            if not success:
                raise HTTPException(status_code=404, detail="计划任务不存在")
        except RuntimeError as e:
            # 捕获任务锁获取失败的错误
            if "任务已在执行中" in str(e):
                raise HTTPException(status_code=409, detail=str(e))
            raise
        
        # 记录操作日志
        await log_operation(
            operation_type=OperationType.SCHEDULER_RUN,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task.task_name,
            operation_name="手动运行计划任务",
            operation_description=f"手动运行计划任务: {task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务已开始执行 (ID: {task_id})",
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/run",
            ip_address=request.client.host if request and request.client else None
        )
        
        return {
            "success": True,
            "message": "计划任务已开始执行",
            "run_options": run_options
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"运行计划任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_RUN,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="手动运行计划任务",
            operation_description=f"手动运行计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/run",
            ip_address=request.client.host if request and request.client else None
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/stop")
async def stop_scheduled_task(task_id: int, request: Request = None):
    """停止正在运行的计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 获取任务信息（用于日志）
        task = await scheduler.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        success = await scheduler.stop_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在或未在运行")
        
        # 记录操作日志
        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task.task_name,
            operation_name="停止计划任务",
            operation_description=f"停止计划任务: {task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务已停止 (ID: {task_id})",
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/stop",
            ip_address=request.client.host if request and request.client else None
        )
        
        return {"success": True, "message": "计划任务已停止"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"停止计划任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="停止计划任务",
            operation_description=f"停止计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/stop",
            ip_address=request.client.host if request and request.client else None
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/enable")
async def enable_scheduled_task(task_id: int, request: Request = None):
    """启用计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 获取任务信息（用于日志）
        task = await scheduler.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        success = await scheduler.enable_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        # 记录操作日志
        await log_operation(
            operation_type=OperationType.SCHEDULER_ENABLE,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task.task_name,
            operation_name="启用计划任务",
            operation_description=f"启用计划任务: {task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务已启用 (ID: {task_id})",
            old_values={"enabled": False},
            new_values={"enabled": True},
            changed_fields=["enabled"],
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/enable",
            ip_address=request.client.host if request and request.client else None
        )
        
        return {"success": True, "message": "计划任务已启用"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启用计划任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_ENABLE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="启用计划任务",
            operation_description=f"启用计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/enable",
            ip_address=request.client.host if request and request.client else None
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/disable")
async def disable_scheduled_task(task_id: int, request: Request = None):
    """禁用计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 获取任务信息（用于日志）
        task = await scheduler.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        success = await scheduler.disable_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        # 记录操作日志
        await log_operation(
            operation_type=OperationType.SCHEDULER_DISABLE,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task.task_name,
            operation_name="禁用计划任务",
            operation_description=f"禁用计划任务: {task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"计划任务已禁用 (ID: {task_id})",
            old_values={"enabled": True},
            new_values={"enabled": False},
            changed_fields=["enabled"],
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/disable",
            ip_address=request.client.host if request and request.client else None
        )
        
        return {"success": True, "message": "计划任务已禁用"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"禁用计划任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_DISABLE,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="禁用计划任务",
            operation_description=f"禁用计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/disable",
            ip_address=request.client.host if request and request.client else None
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/{task_id}/unlock")
async def unlock_scheduled_task(task_id: int, request: Request = None):
    """解锁指定计划任务的所有活跃锁"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        scheduler: TaskScheduler = system.scheduler
        task = await scheduler.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="计划任务不存在")

        # 记录解锁操作日志
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.SYSTEM,
            message=f"开始解锁计划任务: {task.task_name} (ID: {task_id})",
            module="web.api.scheduler",
            function="unlock_scheduled_task",
            task_id=task_id,
            details={"task_name": task.task_name, "task_id": task_id}
        )
        
        # 先停止正在运行的任务执行器（如果存在）
        if task_id in scheduler._running_executions:
            logger.info(f"检测到任务 {task_id} 正在运行，先停止执行器...")
            try:
                await scheduler.stop_task(task_id)
                logger.info(f"已停止任务 {task_id} 的执行器")
            except Exception as stop_error:
                logger.warning(f"停止任务 {task_id} 的执行器失败: {str(stop_error)}")
        
        # 解锁任务并重置状态
        from utils.scheduler.task_unlocker import unlock_task_and_reset_status
        success = await unlock_task_and_reset_status(task_id)
        if not success:
            logger.warning(f"解锁任务 {task_id} 可能失败，但继续执行")

        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id=str(task_id),
            resource_name=task.task_name,
            operation_name="解锁计划任务",
            operation_description=f"解锁计划任务所有活跃锁: {task.task_name}",
            category="scheduler",
            success=True,
            result_message=f"已解锁 (ID: {task_id})",
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/unlock",
            ip_address=request.client.host if request and request.client else None
        )
        
        # 记录系统日志（成功）
        await log_system(
            level=LogLevel.INFO,
            category=LogCategory.SYSTEM,
            message=f"解锁计划任务成功: {task.task_name} (ID: {task_id})",
            module="web.api.scheduler",
            function="unlock_scheduled_task",
            task_id=task_id,
            details={"task_name": task.task_name, "task_id": task_id}
        )

        return {"success": True, "message": "已解锁该任务的所有锁"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"解锁计划任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id=str(task_id),
            operation_name="解锁计划任务",
            operation_description=f"解锁计划任务失败 (ID: {task_id})",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url=f"/api/scheduler/tasks/{task_id}/unlock",
            ip_address=request.client.host if request and request.client else None
        )
        
        # 记录系统日志（失败）
        import traceback
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.SYSTEM,
            message=f"解锁计划任务失败: {str(e)}",
            module="web.api.scheduler",
            function="unlock_scheduled_task",
            task_id=task_id,
            details={"task_id": task_id, "error": str(e)},
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tasks/unlock-all")
async def unlock_all_tasks(request: Request = None):
    """解锁所有活跃的任务锁（谨慎使用）"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")

        # 记录解锁操作日志
        await log_system(
            level=LogLevel.WARNING,
            category=LogCategory.SYSTEM,
            message="开始解锁所有活跃任务锁（谨慎操作）",
            module="web.api.scheduler",
            function="unlock_all_tasks",
            details={"operation": "unlock_all"}
        )
        
        # 解锁所有任务并重置状态
        from utils.scheduler.task_unlocker import unlock_all_tasks_and_reset_status
        count = await unlock_all_tasks_and_reset_status()
        logger.info(f"已解锁所有任务并重置了 {count} 个 RUNNING 状态的任务")

        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id="*",
            operation_name="解锁所有任务",
            operation_description="解锁所有活跃的任务锁",
            category="scheduler",
            success=True,
            result_message="已解锁所有活跃任务锁",
            request_method="POST",
            request_url="/api/scheduler/tasks/unlock-all",
            ip_address=request.client.host if request and request.client else None
        )
        
        # 记录系统日志（成功）
        await log_system(
            level=LogLevel.WARNING,
            category=LogCategory.SYSTEM,
            message="解锁所有活跃任务锁完成",
            module="web.api.scheduler",
            function="unlock_all_tasks",
            details={"operation": "unlock_all", "result": "success"}
        )

        return {"success": True, "message": "已解锁所有活跃任务锁"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"解锁所有任务失败: {str(e)}")
        
        # 记录操作日志（失败）
        await log_operation(
            operation_type=OperationType.SCHEDULER_STOP,
            resource_type="scheduler",
            resource_id="*",
            operation_name="解锁所有任务",
            operation_description="解锁所有任务失败",
            category="scheduler",
            success=False,
            error_message=str(e),
            request_method="POST",
            request_url="/api/scheduler/tasks/unlock-all",
            ip_address=request.client.host if request and request.client else None
        )
        
        # 记录系统日志（失败）
        import traceback
        await log_system(
            level=LogLevel.ERROR,
            category=LogCategory.SYSTEM,
            message=f"解锁所有任务失败: {str(e)}",
            module="web.api.scheduler",
            function="unlock_all_tasks",
            details={"operation": "unlock_all", "error": str(e)},
            exception_type=type(e).__name__,
            stack_trace=traceback.format_exc()
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/tasks/{task_id}", response_model=ScheduledTaskResponse)
async def update_scheduled_task(
    task_id: int,
    task: ScheduledTaskUpdate,
    request: Request = None
):
    """更新计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        # 构建更新字典
        updates = {}
        
        if task.task_name is not None:
            updates['task_name'] = task.task_name
        if task.description is not None:
            updates['description'] = task.description
        if task.schedule_type is not None:
            try:
                updates['schedule_type'] = ScheduleType(task.schedule_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的调度类型: {task.schedule_type}")
        if task.schedule_config is not None:
            updates['schedule_config'] = task.schedule_config
        if task.action_type is not None:
            try:
                updates['action_type'] = TaskActionType(task.action_type)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"无效的任务动作类型: {task.action_type}")
        if task.action_config is not None:
            updates['action_config'] = task.action_config
        if task.enabled is not None:
            updates['enabled'] = task.enabled
            # 更新状态
            if task.enabled:
                updates['status'] = ScheduledTaskStatus.ACTIVE
            else:
                updates['status'] = ScheduledTaskStatus.INACTIVE
        if task.tags is not None:
            updates['tags'] = task.tags
        if task.task_metadata is not None:
            updates['task_metadata'] = task.task_metadata
        
        # 更新任务
        success = await scheduler.update_task(task_id, updates)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        # 重新获取任务
        updated_task = await scheduler.get_task(task_id)
        
        return ScheduledTaskResponse(**updated_task.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tasks/{task_id}")
async def delete_scheduled_task(task_id: int, request: Request = None):
    """删除计划任务"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        success = await scheduler.delete_task(task_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="计划任务不存在")
        
        return {"success": True, "message": "计划任务已删除"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除计划任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: int,
    limit: int = 100,
    offset: int = 0,
    request: Request = None
):
    """获取计划任务执行日志"""
    try:
        async with get_opengauss_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM scheduled_task_logs
                WHERE scheduled_task_id = $1
                ORDER BY started_at DESC
                LIMIT $2 OFFSET $3
                """,
                task_id, limit, offset
            )

            total_row = await conn.fetchrow(
                "SELECT COUNT(*) as cnt FROM scheduled_task_logs WHERE scheduled_task_id = $1",
                task_id
            )
            total = total_row['cnt'] if total_row else 0

            logs = []
            for row in rows:
                log_dict = dict(row)
                # 序列化 datetime
                for k, v in log_dict.items():
                    if hasattr(v, 'isoformat'):
                        log_dict[k] = v.isoformat()
                logs.append(log_dict)

            return {
                "logs": logs,
                "total": total,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"获取任务日志失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/scheduler/status")
async def get_scheduler_status(request: Request = None):
    """获取调度器状态"""
    try:
        system = request.app.state.system
        if not system:
            raise HTTPException(status_code=500, detail="系统未初始化")
        
        scheduler: TaskScheduler = system.scheduler
        
        return {
            "running": scheduler.running,
            "total_tasks": len(scheduler.tasks),
            "enabled_tasks": len([t for t in scheduler.tasks.values() if t.get('task', {}).enabled]),
            "running_executions": len(scheduler._running_executions)
        }
        
    except Exception as e:
        logger.error(f"获取调度器状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

