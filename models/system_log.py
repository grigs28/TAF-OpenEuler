#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统日志数据模型
System Log Data Models
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, ForeignKey, Enum, Float, Boolean
from sqlalchemy.orm import relationship
import enum

from .base import BaseModel


class LogLevel(enum.Enum):
    """日志级别"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class LogCategory(enum.Enum):
    """日志分类"""
    SYSTEM = "system"         # 系统日志
    BACKUP = "backup"         # 备份日志
    RECOVERY = "recovery"     # 恢复日志
    TAPE = "tape"            # 磁带日志
    USER = "user"            # 用户操作日志
    SECURITY = "security"     # 安全日志
    PERFORMANCE = "performance"  # 性能日志
    API = "api"              # API日志
    WEB = "web"              # Web日志
    DATABASE = "database"     # 数据库日志


class SystemLog(BaseModel):
    """系统日志表"""

    __tablename__ = "system_logs"

    # 日志信息
    log_level = Column(Enum(LogLevel), nullable=False, comment="日志级别")
    category = Column(Enum(LogCategory), nullable=False, comment="日志分类")
    message = Column(Text, nullable=False, comment="日志消息")
    details = Column(JSON, comment="详细信息")

    # 时间信息
    log_time = Column(DateTime(timezone=True), nullable=False, comment="日志时间")
    timestamp = Column(Integer, comment="时间戳(毫秒)")

    # 来源信息
    module = Column(String(100), comment="模块名")
    function = Column(String(100), comment="函数名")
    line_number = Column(Integer, comment="行号")
    file_path = Column(String(500), comment="文件路径")

    # 上下文信息
    thread_id = Column(String(50), comment="线程ID")
    process_id = Column(Integer, comment="进程ID")
    request_id = Column(String(100), comment="请求ID")
    session_id = Column(String(100), comment="会话ID")

    # 环境信息
    hostname = Column(String(100), comment="主机名")
    environment = Column(String(50), comment="环境(dev/test/prod)")
    version = Column(String(50), comment="版本号")

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), comment="用户ID")
    task_id = Column(String(100), comment="任务ID")
    correlation_id = Column(String(100), comment="关联ID")

    # 性能信息
    duration_ms = Column(Integer, comment="持续时间(毫秒)")
    memory_usage_mb = Column(Float, comment="内存使用(MB)")
    cpu_usage_percent = Column(Float, comment="CPU使用率")

    # 索引字段
    exception_type = Column(String(200), comment="异常类型")
    stack_trace = Column(Text, comment="堆栈跟踪")

    def __repr__(self):
        return f"<SystemLog(id={self.id}, level={self.log_level.value}, category={self.category.value})>"


class OperationType(enum.Enum):
    """操作类型"""
    # 基础操作
    CREATE = "create"         # 创建
    UPDATE = "update"         # 更新
    DELETE = "delete"         # 删除
    READ = "read"            # 读取
    EXECUTE = "execute"       # 执行
    CONFIG = "config"        # 配置
    EXPORT = "export"        # 导出
    IMPORT = "import"        # 导入
    
    # 用户认证操作
    LOGIN = "login"          # 登录
    LOGOUT = "logout"        # 登出
    REGISTER = "register"    # 注册
    PASSWORD_CHANGE = "password_change"  # 密码修改
    PASSWORD_RESET = "password_reset"    # 密码重置
    
    # 磁带操作
    TAPE_LOAD = "tape_load"          # 加载磁带
    TAPE_UNLOAD = "tape_unload"      # 卸载磁带
    TAPE_EJECT = "tape_eject"        # 弹出磁带
    TAPE_SCAN = "tape_scan"          # 扫描磁带
    TAPE_READ_LABEL = "tape_read_label"  # 读取磁带标签
    TAPE_WRITE_LABEL = "tape_write_label"  # 写入磁带标签
    TAPE_ERASE = "tape_erase"        # 擦除磁带
    TAPE_FORMAT = "tape_format"      # 格式化磁带
    TAPE_MOUNT = "tape_mount"        # 挂载磁带
    TAPE_UNMOUNT = "tape_unmount"    # 卸载磁带
    TAPE_VERIFY = "tape_verify"      # 验证磁带
    TAPE_REWIND = "tape_rewind"      # 回绕磁带
    TAPE_POSITION = "tape_position"  # 定位磁带
    
    # 备份操作
    BACKUP_START = "backup_start"       # 备份开始
    BACKUP_COMPLETE = "backup_complete"  # 备份完成
    BACKUP_FAILED = "backup_failed"     # 备份失败
    BACKUP_CANCEL = "backup_cancel"     # 备份取消
    BACKUP_PAUSE = "backup_pause"       # 备份暂停
    BACKUP_RESUME = "backup_resume"     # 备份恢复
    
    # 恢复操作
    RECOVERY_START = "recovery_start"       # 恢复开始
    RECOVERY_COMPLETE = "recovery_complete"  # 恢复完成
    RECOVERY_FAILED = "recovery_failed"     # 恢复失败
    RECOVERY_CANCEL = "recovery_cancel"     # 恢复取消
    RECOVERY_VERIFY = "recovery_verify"     # 恢复验证
    
    # 计划任务操作
    SCHEDULER_CREATE = "scheduler_create"   # 创建计划任务
    SCHEDULER_UPDATE = "scheduler_update"   # 更新计划任务
    SCHEDULER_DELETE = "scheduler_delete"   # 删除计划任务
    SCHEDULER_ENABLE = "scheduler_enable"   # 启用计划任务
    SCHEDULER_DISABLE = "scheduler_disable"  # 禁用计划任务
    SCHEDULER_RUN = "scheduler_run"         # 运行计划任务
    SCHEDULER_STOP = "scheduler_stop"        # 停止计划任务
    
    # 系统操作
    SYSTEM_START = "system_start"           # 系统启动
    SYSTEM_STOP = "system_stop"            # 系统停止
    SYSTEM_RESTART = "system_restart"      # 系统重启
    SYSTEM_CONFIG = "system_config"         # 系统配置
    SYSTEM_BACKUP = "system_backup"         # 系统备份
    SYSTEM_RESTORE = "system_restore"       # 系统恢复
    
    # 维护操作
    MAINTENANCE_START = "maintenance_start"  # 维护开始
    MAINTENANCE_COMPLETE = "maintenance_complete"  # 维护完成
    CLEANUP = "cleanup"                     # 清理
    ARCHIVE = "archive"                     # 归档


class OperationLog(BaseModel):
    """操作日志表"""

    __tablename__ = "operation_logs"

    # 关联信息
    user_id = Column(Integer, ForeignKey("users.id"), comment="用户ID")
    username = Column(String(100), comment="用户名（冗余字段，便于查询）")

    # 操作信息
    operation_type = Column(Enum(OperationType), nullable=False, comment="操作类型")
    resource_type = Column(String(100), comment="资源类型（tape/backup/recovery/scheduler/user/system）")
    resource_id = Column(String(100), comment="资源ID")
    resource_name = Column(String(200), comment="资源名称（冗余字段，便于查询）")
    operation_name = Column(String(200), comment="操作名称")
    operation_description = Column(Text, comment="操作描述")
    
    # 分类信息
    category = Column(String(50), comment="操作分类（login/modify/backup/recovery/tape/maintenance）")

    # 时间信息
    operation_time = Column(DateTime(timezone=True), nullable=False, comment="操作时间")
    duration_ms = Column(Integer, comment="持续时间(毫秒)")

    # 请求信息
    request_method = Column(String(10), comment="请求方法")
    request_url = Column(String(1000), comment="请求URL")
    request_params = Column(JSON, comment="请求参数")
    request_body = Column(JSON, comment="请求体")

    # 响应信息
    response_status = Column(Integer, comment="响应状态码")
    response_body = Column(JSON, comment="响应体")
    response_size = Column(Integer, comment="响应大小(字节)")

    # 结果信息
    success = Column(Boolean, nullable=False, comment="是否成功")
    result_message = Column(Text, comment="结果消息")
    error_code = Column(String(50), comment="错误代码")
    error_message = Column(Text, comment="错误消息")

    # 客户端信息
    ip_address = Column(String(45), comment="IP地址")
    user_agent = Column(Text, comment="用户代理")
    referer = Column(String(1000), comment="来源页面")

    # 审计信息
    old_values = Column(JSON, comment="修改前值")
    new_values = Column(JSON, comment="修改后值")
    changed_fields = Column(JSON, comment="变更字段")

    # 关联关系
    user = relationship("User", back_populates="operation_logs")

    def __repr__(self):
        return f"<OperationLog(id={self.id}, user_id={self.user_id}, operation={self.operation_type.value})>"

