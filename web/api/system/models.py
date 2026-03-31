#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API数据模型
System Management API Models
"""

from typing import Optional
from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    """数据库配置模型"""
    db_type: str = Field(..., description="数据库类型: sqlite（内存数据库）、opengauss")
    db_host: Optional[str] = Field(None, description="数据库主机")
    db_port: Optional[int] = Field(None, description="数据库端口")
    db_user: Optional[str] = Field(None, description="数据库用户名")
    db_password: Optional[str] = Field(None, description="数据库密码")
    db_database: Optional[str] = Field(None, description="数据库名称")
    db_path: Optional[str] = Field(None, description="SQLite数据库路径")
    db_index: Optional[int] = Field(0, description="Redis数据库编号(0-15)")
    config_file_path: Optional[str] = Field(None, description="Redis配置文件路径")
    pool_size: int = Field(10, description="连接池大小")
    max_overflow: int = Field(20, description="最大溢出连接数")


class SystemConfigRequest(BaseModel):
    """系统配置请求模型"""
    retention_months: int = 6
    auto_erase_expired: bool = True
    monthly_backup_cron: str = "0 2 1 * *"
    dingtalk_api_url: str = ""
    dingtalk_api_key: str = ""
    dingtalk_default_phone: str = ""
    database_config: Optional[DatabaseConfig] = None


class TapeConfig(BaseModel):
    """磁带机配置模型"""
    tape_device_path: str = Field("/dev/nst0", description="磁带设备路径（如 /dev/nst0）")
    ltfs_device_path: str = Field("/dev/sg2", description="LTFS SCSI设备路径（如 /dev/sg2）")
    default_block_size: int = Field(262144, description="默认块大小(字节)")
    max_volume_size: int = Field(322122547200, description="最大卷大小(字节)")
    tape_pool_size: int = Field(12, description="磁带池大小")
    tape_check_interval: int = Field(3600, description="状态检查间隔(秒)")
    auto_tape_cleanup: bool = Field(True, description="自动清理过期磁带")
    # 工具路径配置
    itdt_path: Optional[str] = Field(None, description="ITDT可执行文件路径")
    ltfs_tools_dir: Optional[str] = Field(None, description="LTFS工具目录")
    default_device_address: Optional[str] = Field(None, description="默认驱动器地址(SCSI格式)")


class DingTalkConfig(BaseModel):
    """钉钉通知配置模型"""
    dingtalk_api_url: str = Field(..., description="钉钉API地址")
    dingtalk_api_key: str = Field(..., description="钉钉API密钥")
    dingtalk_default_phone: str = Field(..., description="默认手机号")


class NotificationUser(BaseModel):
    """通知人员模型"""
    phone: str = Field(..., description="手机号")
    name: str = Field(..., description="姓名")
    remark: Optional[str] = Field(None, description="备注")
    enabled: bool = Field(True, description="是否启用")


class NotificationEvents(BaseModel):
    """通知事件配置模型"""
    notify_backup_success: bool = Field(True, description="备份成功")
    notify_backup_started: bool = Field(True, description="备份开始")
    notify_backup_failed: bool = Field(True, description="备份失败")
    notify_recovery_success: bool = Field(True, description="恢复成功")
    notify_recovery_failed: bool = Field(True, description="恢复失败")
    notify_tape_change: bool = Field(True, description="磁带更换")
    notify_tape_expired: bool = Field(True, description="磁带过期")
    notify_tape_error: bool = Field(True, description="磁带错误")
    notify_capacity_warning: bool = Field(True, description="容量预警")
    notify_system_error: bool = Field(True, description="系统错误")
    notify_system_started: bool = Field(True, description="系统启动")

