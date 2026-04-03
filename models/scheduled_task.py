#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务数据模型
Scheduled Task Data Models
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, Boolean, Enum, ForeignKey
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel


class ScheduleType(enum.Enum):
    """调度类型"""
    ONCE = "once"                    # 一次性任务（某月某日某时）
    INTERVAL = "interval"            # 间隔任务（每N分钟/小时/天）
    DAILY = "daily"                  # 每日任务
    WEEKLY = "weekly"                # 每周任务
    MONTHLY = "monthly"              # 每月任务
    YEARLY = "yearly"                # 每年任务
    CRON = "cron"                    # Cron表达式


class ScheduledTaskStatus(enum.Enum):
    """计划任务状态"""
    ACTIVE = "active"                # 活跃（已启用）
    INACTIVE = "inactive"            # 未激活（已禁用）
    RUNNING = "running"              # 运行中
    PAUSED = "paused"                # 已暂停
    ERROR = "error"                  # 错误状态


class TaskActionType(enum.Enum):
    """任务动作类型"""
    BACKUP = "backup"                # 备份任务
    RECOVERY = "recovery"            # 恢复任务
    CLEANUP = "cleanup"              # 清理任务
    HEALTH_CHECK = "health_check"    # 健康检查
    RETENTION_CHECK = "retention_check"  # 保留期检查
    VERIFY = "verify"                    # 验证任务
    CUSTOM = "custom"                # 自定义任务


class ScheduledTask(BaseModel):
    """计划任务表"""

    __tablename__ = "scheduled_tasks"

    # 基本信息
    task_name = Column(String(200), nullable=False, unique=True, comment="任务名称")
    description = Column(Text, comment="任务描述")
    status = Column(Enum(ScheduledTaskStatus), default=ScheduledTaskStatus.INACTIVE, comment="任务状态")
    
    # 调度配置
    schedule_type = Column(Enum(ScheduleType), nullable=False, comment="调度类型")
    schedule_config = Column(JSON, comment="调度配置（根据schedule_type不同而不同）")
    
    # schedule_config 结构说明：
    # 1. ONCE（一次性）: {"datetime": "2024-12-25 14:30:00"}
    # 2. INTERVAL（间隔）: {"interval": 30, "unit": "minutes"}  # unit: minutes/hours/days
    # 3. DAILY（每日）: {"time": "02:00:00"}  # 每天02:00执行
    # 4. WEEKLY（每周）: {"day_of_week": 0, "time": "02:00:00"}  # 0=Monday, 6=Sunday
    # 5. MONTHLY（每月）: {"day_of_month": 1, "time": "02:00:00"}  # 每月1号02:00
    # 6. YEARLY（每年）: {"month": 1, "day": 1, "time": "02:00:00"}  # 每年1月1日02:00
    # 7. CRON（Cron表达式）: {"cron": "0 2 * * *"}
    
    # 任务动作配置
    action_type = Column(Enum(TaskActionType), nullable=False, comment="任务动作类型")
    action_config = Column(JSON, comment="任务动作配置（根据action_type不同而不同）")
    
    # action_config 结构说明：
    # BACKUP: {"source_paths": [], "task_type": "full", ...}
    # RECOVERY: {"backup_set_id": "...", "target_path": "...", ...}
    # CLEANUP: {"retention_days": 180, ...}
    # HEALTH_CHECK: {}
    # RETENTION_CHECK: {}
    # CUSTOM: {"command": "...", "args": [...]}
    
    # 时间信息
    next_run_time = Column(DateTime(timezone=True), comment="下次执行时间")
    last_run_time = Column(DateTime(timezone=True), comment="上次执行时间")
    last_success_time = Column(DateTime(timezone=True), comment="上次成功执行时间")
    last_failure_time = Column(DateTime(timezone=True), comment="上次失败执行时间")
    
    # 执行统计
    total_runs = Column(Integer, default=0, comment="总执行次数")
    success_runs = Column(Integer, default=0, comment="成功次数")
    failure_runs = Column(Integer, default=0, comment="失败次数")
    average_duration = Column(Integer, comment="平均执行时长（秒）")
    
    # 错误信息
    last_error = Column(Text, comment="最后一次错误信息")
    
    # 启用/禁用
    enabled = Column(Boolean, default=True, comment="是否启用")
    
    # 任务元数据
    task_metadata = Column(JSON, comment="任务元数据")
    tags = Column(JSON, comment="标签列表")
    
    # 关联信息（当action_type=backup时，关联的备份任务模板ID）
    backup_task_id = Column(Integer, ForeignKey("backup_tasks.id"), nullable=True, comment="关联的备份任务模板ID")
    
    def __repr__(self):
        return f"<ScheduledTask(id={self.id}, name={self.task_name}, type={self.schedule_type.value}, status={self.status.value})>"
    
    def to_dict(self):
        """转换为字典"""
        result = super().to_dict()
        # 处理枚举类型
        if hasattr(self, 'schedule_type') and self.schedule_type:
            result['schedule_type'] = self.schedule_type.value
        if hasattr(self, 'status') and self.status:
            result['status'] = self.status.value
        if hasattr(self, 'action_type') and self.action_type:
            result['action_type'] = self.action_type.value
        return result


class ScheduledTaskLog(BaseModel):
    """计划任务执行日志表"""

    __tablename__ = "scheduled_task_logs"

    # 关联信息
    scheduled_task_id = Column(Integer, ForeignKey("scheduled_tasks.id"), nullable=False, comment="计划任务ID")
    
    # 执行信息
    execution_id = Column(String(100), unique=True, nullable=False, comment="执行ID")
    started_at = Column(DateTime(timezone=True), nullable=False, comment="开始时间")
    completed_at = Column(DateTime(timezone=True), comment="完成时间")
    duration = Column(Integer, comment="执行时长（秒）")
    
    # 执行状态
    status = Column(String(20), nullable=False, comment="执行状态: success/failed/running/cancelled")
    error_message = Column(Text, comment="错误信息")
    
    # 执行结果
    result = Column(JSON, comment="执行结果")
    
    # 关联关系
    scheduled_task = relationship("ScheduledTask", backref="execution_logs")
    
    def __repr__(self):
        return f"<ScheduledTaskLog(id={self.id}, task_id={self.scheduled_task_id}, status={self.status})>"

