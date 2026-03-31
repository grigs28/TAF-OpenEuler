#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统配置管理模块
System Configuration Management Module
"""

import os
import re
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator

try:
    from pydantic_settings import BaseSettings
except ImportError:
    # 如果 pydantic-settings 没有安装，提供错误信息
    raise ImportError(
        "需要安装 pydantic-settings 包。请运行: pip install pydantic-settings\n"
        "Pydantic v2 将 BaseSettings 移动到了单独的包中。"
    )


def _read_version_from_changelog() -> str:
    """从CHANGELOG.md读取最新版本号"""
    try:
        changelog_path = Path("CHANGELOG.md")
        if changelog_path.exists():
            with open(changelog_path, "r", encoding="utf-8") as f:
                content = f.read()
                # 匹配 ## [版本号] 格式
                match = re.search(r'^##\s+\[([\d.]+)\]', content, re.MULTILINE)
                if match:
                    return match.group(1)
    except Exception:
        pass
    # 默认版本
    return "0.0.2"


class Settings(BaseSettings):
    """系统配置类"""

    # 应用配置
    APP_NAME: str = "企业级磁带备份系统"
    APP_VERSION: str = Field(default_factory=_read_version_from_changelog, description="版本号从CHANGELOG.md自动读取")
    DEBUG: bool = False
    WEB_PORT: int = 8080

    # 数据库配置
    DATABASE_URL: str = "opengauss://username:password@localhost:5432/backup_db"
    # 连接池大小：进一步调大，适应扫描 + 压缩 + 其他任务的并发
    DB_POOL_SIZE: int = 40          # 基础连接数（可通过 .env 覆盖）
    DB_MAX_OVERFLOW: int = 80       # 允许的额外临时连接数
    DB_POOL_TIMEOUT: float = 30.0  # 连接池连接超时时间（秒）
    DB_COMMAND_TIMEOUT: float = 60.0  # 命令超时时间（秒）
    DB_ACQUIRE_TIMEOUT: float = 10.0  # 从连接池获取连接的超时时间（秒）
    DB_MAX_INACTIVE_CONNECTION_LIFETIME: float = 600.0  # 非活跃连接的最大生命周期（秒，默认10分钟）
    DB_QUERY_DOP: int = 16  # openGauss 查询并行度（1-64，默认16，用于优化查询性能）
    OG_HEARTBEAT_INTERVAL: int = 30  # openGauss 心跳间隔（秒）
    OG_HEARTBEAT_TIMEOUT: float = 5.0  # 单次心跳超时时间
    OG_OPERATION_TIMEOUT: float = 45.0  # 默认数据库操作超时
    OG_OPERATION_WARN_THRESHOLD: float = 5.0  # 操作耗时告警阈值
    OG_OPERATION_FAILURE_THRESHOLD: int = 3  # 连续失败次数触发告警
    OG_MAX_HEARTBEAT_FAILURES: int = 3  # 心跳失败重试次数
    OG_ALERT_COOLDOWN: int = 600  # 告警冷却时间（秒）

    # 数据库配置（兼容格式）
    DB_HOST: Optional[str] = "localhost"
    DB_PORT: Optional[int] = 5432
    DB_USER: Optional[str] = "username"
    DB_PASSWORD: Optional[str] = "password"
    DB_DATABASE: Optional[str] = "backup_db"

    @field_validator('DB_PORT', mode='before')
    @classmethod
    def validate_db_port(cls, v):
        """验证DB_PORT，将空字符串转换为None"""
        if v == '':
            return None
        return v

    # 安全配置
    SECRET_KEY: str = "your-jwt-secret-key-change-in-production"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30

    # 磁带配置 (Linux)
    DEFAULT_BLOCK_SIZE: int = 262144  # 256KB
    MAX_VOLUME_SIZE: int = 322122547200  # 300GB
    # 磁带设备路径（用于 mt 命令，Linux 下为 /dev/nst0）
    TAPE_DRIVE_LETTER: str = "/dev/nst0"
    # 是否在完整备份前自动格式化磁带
    ENABLE_TAPE_FORMAT_BEFORE_FULL: bool = True
    # ITDT 接口配置
    TAPE_INTERFACE_TYPE: str = "linux"  # linux 使用 mt 命令
    ITDT_PATH: str = "/usr/local/itdt/itdt"
    ITDT_LOG_LEVEL: str = "Information"  # Errors|Warnings|Information|Debug
    ITDT_LOG_PATH: str = "output"

    # LTFS 工具目录配置
    LTFS_TOOLS_DIR: str = "/usr/local/ltfs"
    MKLTFS_PATH: str = "/usr/local/bin/mkltfs"  # mkltfs 命令路径
    LTFS_PATH: str = "/usr/local/bin/ltfs"  # ltfs 挂载命令路径
    ITDT_RESULT_PATH: str = "output"
    ITDT_DEVICE_PATH: str | None = None
    ITDT_FORCE_GENERIC_DD: bool = True  # 允许在无专用驱动时强制使用通用驱动
    ITDT_SCAN_SHOW_ALL_PATHS: bool = True  # 扫描时显示所有路径

    # 压缩配置
    COMPRESSION_LEVEL: int = 9
    SOLID_BLOCK_SIZE: int = 67108864  # 64MB
    MAX_FILE_SIZE: int = 12 * 1024 * 1024 * 1024  # 12GB (默认值，可通过.env中的MAX_FILE_SIZE覆盖)
    COMPRESSION_DICTIONARY_SIZE: str = "256m"  # 7-Zip字典大小（固定256M）
    COMPRESS_DIRECTLY_TO_TAPE: bool = True  # 是否直接压缩到磁带机（默认True，跳过temp/final目录）

    # 磁盘空间检查配置
    DISK_CHECK_INTERVAL: int = 30  # 磁盘空间检查间隔（秒）
    DISK_CHECK_MAX_WAIT_MINUTES: int = 60  # 最大等待时间（分钟）
    DISK_CHECK_MIN_FREE_MULTIPLIER: int = 3  # 最小剩余空间倍数（3 * MAX_FILE_SIZE）

    # 计划任务配置
    SCHEDULER_ENABLED: bool = True
    MONTHLY_BACKUP_CRON: str = "0 2 1 * *"  # 每月1号02:00
    RETENTION_CHECK_CRON: str = "0 3 * * *"  # 每天03:00

    # 系统配置
    DEFAULT_RETENTION_MONTHS: int = 6
    AUTO_ERASE_EXPIRED: bool = True

    # 日志配置
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/application.log"

    # 钉钉通知配置
    DINGTALK_API_URL: str = "http://localhost:5555"
    DINGTALK_API_KEY: str = "your-dingtalk-api-key"
    DINGTALK_DEFAULT_PHONE: str = "13800000000"

    # 微信通知配置（企业微信机器人）
    WECHAT_WEBHOOK_URL: str = ""  # 企业微信机器人 Webhook URL
    WECHAT_ENABLED: bool = False  # 是否启用微信通知
    WECHAT_REPORT_INTERVAL: int = 30  # 备份进度汇报间隔（分钟）

    # SMB/CIFS 网络路径配置
    SMB_USERNAME: str = ""  # SMB 用户名（如：administrator）
    SMB_PASSWORD: str = ""  # SMB 密码
    SMB_DOMAIN: str = ""  # SMB 域（如：DOMAIN 或 nt08）
    SMB_MOUNT_BASE: str = "/mnt/smb"  # SMB 挂载基础目录

    # 备份配置
    BACKUP_TEMP_DIR: str = "temp/backup"
    RECOVERY_TEMP_DIR: str = "temp/recovery"
    BACKUP_COMPRESS_DIR: str = "temp/compress"  # 压缩文件临时目录（先压缩到这里，再移动到磁带机）
    COMPRESS_OUTPUT_DIR: str = "temp/output"  # 压缩输出目录（用于存放最终压缩文件）
    COMPRESSION_THREADS: int = 3  # Python压缩线程数（py7zr/PGZip）
    # 压缩方法配置
    COMPRESSION_METHOD: str = "zstd"  # 压缩方法: "pgzip"、"zstd" 或 "tar"
    # 注意：COMPRESSION_COMMAND_THREADS 默认使用 WEB_WORKERS 的值，在代码中动态获取
    PGZIP_BLOCK_SIZE: str = "1M"  # PGZip块大小（默认1M，可通过.env中的PGZIP_BLOCK_SIZE覆盖）
    PGZIP_THREADS: int = 3  # PGZip线程数
    ZSTD_THREADS: int = 3  # Zstandard压缩线程数
    ZSTD_WRITE_SIZE: int = 1048576  # Zstandard压缩写入缓冲区大小（字节），默认1MB（1048576字节）

    # 扫描进度更新配置
    SCAN_UPDATE_INTERVAL: int = 2000  # 后台扫描每处理多少个文件更新一次数据库（total_files/total_bytes）
    # 优化：从500增加到2000，减少数据库写入频率，提升扫描速度
    # 如需更快速度，可增加到5000（需要更多内存，但写入速度更快）
    SCAN_LOG_INTERVAL_SECONDS: int = 60  # 后台扫描进度日志输出的时间间隔（秒）
    SCAN_WAIT_TIMEOUT: int = 300  # 等待后台扫描写入文件记录的超时时间（秒），默认300秒（5分钟）
    # 压缩并行批次配置
    COMPRESSION_PARALLEL_BATCHES: int = 3  # 压缩并行批次数量（默认3），预读取程序队列数为该值+1
    COMPRESSION_BATCHES_REDUCTION: int = 1  # 扫描时减少的并行批次数（默认1），0表示不减少

    # 扫描方法配置
    SCAN_METHOD: str = "default"  # 扫描方法: "default" (默认)
    
    # 简洁扫描配置（已移除开关，始终使用 SimpleScanner）

    # 纯内存扫描模式配置
    SCAN_MEMORY_ONLY: bool = False  # 纯内存扫描模式（跳过扫描阶段的数据库写入，压缩后才写入数据库）
    # 启用后：扫描器将文件记录写入内存，预取器从内存读取，压缩完成后一次性写入数据库
    # 优点：减少扫描阶段数据库I/O，提升性能；崩溃时无孤儿记录
    # 注意：需要足够内存存放文件记录（100万文件约500MB）

    # 检查点配置
    USE_CHECKPOINT: bool = False  # 是否启用检查点文件，默认不启用

    # 磁带管理配置
    TAPE_POOL_SIZE: int = 12  # 磁带池大小
    TAPE_CHECK_INTERVAL: int = 3600  # 磁带状态检查间隔（秒）
    AUTO_TAPE_CLEANUP: bool = True

    # Web界面配置
    WEB_STATIC_DIR: str = "web/static"
    WEB_TEMPLATE_DIR: str = "web/templates"
    MAX_UPLOAD_SIZE: int = 1073741824  # 1GB

    # 监控配置
    METRICS_ENABLED: bool = True
    HEALTH_CHECK_INTERVAL: int = 300  # 5分钟

    # 高级配置
    ENVIRONMENT: str = "production"
    WEB_HOST: str = "0.0.0.0"
    WEB_WORKERS: int = 4  # Web服务器工作进程数，同时作为7-Zip命令行线程数（-mmt参数）的默认值
    ENABLE_CORS: bool = True
    CORS_ORIGINS: str = "*"
    ENABLE_GZIP: bool = True
    TAPE_DEVICE_PATH: str = "/dev/nst0"  # mt 命令使用的磁带设备
    LTFS_DEVICE_PATH: str = "/dev/sg2"   # LTFS 挂载使用的 SCSI generic 设备
    LOG_BACKUP_COUNT: int = 30
    ASYNC_POOL_SIZE: int = 20
    ASYNC_MAX_OVERFLOW: int = 40
    ENABLE_QUERY_CACHE: bool = True
    QUERY_CACHE_TTL: int = 300
    WEBSOCKET_HEARTBEAT: int = 30
    SESSION_TIMEOUT: int = 3600
    
    # 数据目录配置
    DATA_DIR: str = "data"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # 允许额外的字段


# 全局配置实例
settings = Settings()


def get_settings() -> Settings:
    """获取配置实例"""
    return settings


def reload_settings() -> Settings:
    """重新加载配置"""
    global settings
    settings = Settings()
    return settings