#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份数据模型
Backup Data Models
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, String, DateTime, Text, BigInteger, Enum, Float, JSON, Boolean, ForeignKey
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel


class BackupTaskType(enum.Enum):
    """备份任务类型"""
    FULL = "full"                # 完整备份
    INCREMENTAL = "incremental"  # 增量备份
    DIFFERENTIAL = "differential"  # 差异备份
    MONTHLY_FULL = "monthly_full"  # 月度完整备份
    VERIFY = "verify"              # 验证任务


class BackupTaskStatus(enum.Enum):
    """备份任务状态"""
    PENDING = "pending"          # 等待中
    RUNNING = "running"          # 运行中
    COMPLETED = "completed"      # 已完成
    FAILED = "failed"           # 失败
    CANCELLED = "cancelled"      # 已取消
    PAUSED = "paused"           # 暂停


class BackupTask(BaseModel):
    """备份任务表"""

    __tablename__ = "backup_tasks"

    # 基本信息
    task_name = Column(String(200), nullable=False, comment="任务名称")
    task_type = Column(Enum(BackupTaskType), nullable=False, comment="任务类型")
    description = Column(Text, comment="任务描述")
    status = Column(Enum(BackupTaskStatus), default=BackupTaskStatus.PENDING, comment="任务状态")
    is_template = Column(Boolean, default=False, comment="是否为模板（配置）")
    template_id = Column(Integer, ForeignKey("backup_tasks.id"), nullable=True, comment="模板ID（如果是执行记录）")

    # 备份配置
    source_paths = Column(JSON, comment="源路径列表")
    exclude_patterns = Column(JSON, comment="排除模式")
    compression_enabled = Column(Boolean, default=True, comment="是否启用压缩")
    encryption_enabled = Column(Boolean, default=False, comment="是否启用加密")
    retention_days = Column(Integer, default=180, comment="保留天数")
    enable_simple_scan = Column(Boolean, default=True, comment="是否启用简洁扫描（默认开启）")

    # 时间信息
    scheduled_time = Column(DateTime(timezone=True), comment="计划执行时间")
    started_at = Column(DateTime(timezone=True), comment="实际开始时间")
    completed_at = Column(DateTime(timezone=True), comment="完成时间")

    # 执行信息
    executed_by = Column(String(100), comment="执行者")
    worker_id = Column(String(100), comment="Worker ID")

    # 统计信息
    total_files = Column(Integer, default=0, comment="总文件数")
    processed_files = Column(Integer, default=0, comment="已处理文件数")
    total_bytes = Column(BigInteger, default=0, comment="总字节数")
    processed_bytes = Column(BigInteger, default=0, comment="已处理字节数")
    compressed_bytes = Column(BigInteger, default=0, comment="压缩后字节数")

    # 磁带信息
    tape_device = Column(String(200), comment="目标磁带机设备（模板配置）")
    tape_id = Column(String(50), comment="使用的磁带ID（执行记录）")
    backup_set_id = Column(String(50), comment="备份集ID")

    # 结果信息
    progress_percent = Column(Float, default=0.0, comment="进度百分比")
    error_message = Column(Text, comment="错误信息")
    result_summary = Column(JSON, comment="结果摘要")
    scan_status = Column(String(50), default="pending", comment="扫描状态")
    scan_completed_at = Column(DateTime(timezone=True), comment="扫描完成时间")
    operation_stage = Column(String(50), comment="操作阶段（scan/compress/copy/finalize）")

    # 关联关系
    backup_sets = relationship("BackupSet", back_populates="backup_task", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<BackupTask(id={self.id}, name={self.task_name}, status={self.status.value})>"


class BackupSetStatus(enum.Enum):
    """备份集状态"""
    ACTIVE = "active"      # 活跃
    ARCHIVED = "archived"  # 已归档
    CORRUPTED = "corrupted"  # 已损坏
    DELETED = "deleted"    # 已删除


class BackupSet(BaseModel):
    """备份集表"""

    __tablename__ = "backup_sets"

    # 基本信息
    set_id = Column(String(50), unique=True, nullable=False, comment="备份集ID")
    set_name = Column(String(200), nullable=False, comment="备份集名称")
    backup_group = Column(String(20), nullable=False, comment="备份组(YYYY-MM)")
    status = Column(Enum(BackupSetStatus), default=BackupSetStatus.ACTIVE, comment="状态")

    # 关联信息
    backup_task_id = Column(Integer, ForeignKey("backup_tasks.id"), comment="关联的备份任务ID")
    tape_id = Column(String(50), comment="存储磁带ID")

    # 备份信息
    backup_type = Column(Enum(BackupTaskType), nullable=False, comment="备份类型")
    backup_time = Column(DateTime(timezone=True), nullable=False, comment="备份时间")
    source_info = Column(JSON, comment="源信息")

    # 存储信息
    total_files = Column(Integer, default=0, comment="总文件数")
    total_bytes = Column(BigInteger, default=0, comment="总字节数")
    compressed_bytes = Column(BigInteger, default=0, comment="压缩后字节数")
    compression_ratio = Column(Float, comment="压缩比")
    chunk_count = Column(Integer, default=0, comment="数据块数量")

    # 验证信息
    checksum = Column(String(128), comment="校验和")
    verified = Column(Boolean, default=False, comment="是否已验证")
    verified_at = Column(DateTime(timezone=True), comment="验证时间")

    # 保留信息
    retention_until = Column(DateTime(timezone=True), comment="保留至")
    auto_delete = Column(Boolean, default=True, comment="是否自动删除")

    # 关联关系
    backup_task = relationship("BackupTask", back_populates="backup_sets")
    backup_files = relationship("BackupFile", back_populates="backup_set", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<BackupSet(id={self.id}, set_id={self.set_id}, group={self.backup_group})>"


class BackupFileType(enum.Enum):
    """备份文件类型"""
    FILE = "file"      # 文件
    DIRECTORY = "directory"  # 目录
    SYMLINK = "symlink"  # 符号链接


class BackupFile(BaseModel):
    """备份文件表"""

    __tablename__ = "backup_files"

    # 关联信息
    backup_set_id = Column(Integer, ForeignKey("backup_sets.id"), nullable=False, comment="备份集ID")

    # 文件信息
    file_path = Column(Text, nullable=False, comment="文件路径")  # 使用TEXT类型，无长度限制
    file_name = Column(Text, nullable=False, comment="文件名")  # 使用TEXT类型，无长度限制
    directory_path = Column(Text, comment="目录路径")  # 使用TEXT类型，无长度限制
    display_name = Column(Text, comment="展示名称")  # 使用TEXT类型，无长度限制
    file_type = Column(Enum(BackupFileType), nullable=False, comment="文件类型")
    file_size = Column(BigInteger, nullable=False, comment="文件大小")
    compressed_size = Column(BigInteger, comment="压缩后大小")

    # 属性信息
    file_permissions = Column(String(20), comment="文件权限")
    file_owner = Column(String(100), comment="文件所有者")
    file_group = Column(String(100), comment="文件组")
    created_time = Column(DateTime(timezone=True), comment="文件创建时间")
    modified_time = Column(DateTime(timezone=True), comment="文件修改时间")
    accessed_time = Column(DateTime(timezone=True), comment="文件访问时间")

    # 存储信息
    tape_block_start = Column(BigInteger, comment="磁带块起始位置")
    tape_block_count = Column(Integer, comment="磁带块数量")
    compressed = Column(Boolean, default=False, comment="是否压缩")
    encrypted = Column(Boolean, default=False, comment="是否加密")
    checksum = Column(String(128), comment="文件校验和")
    is_copy_success = Column(Boolean, default=False, comment="是否复制成功")
    copy_status_at = Column(DateTime(timezone=True), comment="复制状态更新时间")

    # 备份信息
    backup_time = Column(DateTime(timezone=True), nullable=False, comment="备份时间")
    chunk_number = Column(Integer, comment="数据块编号")
    version = Column(Integer, default=1, comment="版本号")

    # 元数据
    file_metadata = Column(JSON, comment="文件元数据")
    tags = Column(JSON, comment="标签")

    # 关联关系
    backup_set = relationship("BackupSet", back_populates="backup_files")

    def __repr__(self):
        return f"<BackupFile(id={self.id}, path={self.file_path}, size={self.file_size})>"