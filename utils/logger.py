#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志管理模块
Logging Management Module
"""

import os
import logging
import logging.handlers
from datetime import datetime
from pathlib import Path

from config.settings import get_settings


def setup_logging():
    """设置日志系统"""
    settings = get_settings()

    # 创建日志目录
    log_dir = Path(settings.LOG_FILE).parent
    log_dir.mkdir(exist_ok=True)

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))

    # 清除现有处理器
    root_logger.handlers.clear()

    # 创建格式器（包含更详细的信息）
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 详细格式器（用于错误日志，包含堆栈跟踪）
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(pathname)s - %(message)s\n%(exc_info)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台处理器：与系统日志级别保持一致，保证屏幕与日志文件输出一致
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 文件处理器（按大小轮转，最大10MB，保留30个备份文件）
    file_handler = logging.handlers.RotatingFileHandler(
        filename=settings.LOG_FILE,
        maxBytes=settings.LOG_MAX_BYTES,
        backupCount=settings.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # 错误日志文件处理器（按大小轮转，最大10MB，保留30个备份文件）
    # 所有警告及以上级别的日志都写入error.log
    error_log_file = log_dir / 'error.log'
    error_handler = logging.handlers.RotatingFileHandler(
        filename=error_log_file,
        maxBytes=settings.LOG_MAX_BYTES,
        backupCount=settings.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.WARNING)  # 改为WARNING级别，包含所有警告及以上日志
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)
    
    # 设置错误日志格式，包含异常堆栈
    error_handler.formatter = detailed_formatter
    
    # 压缩日志文件处理器（压缩过程中的所有警告及以上级别日志）
    # 使用TimedRotatingFileHandler按日期命名，匹配现有日志文件格式（compression_YYYYMMDD.log）
    compression_log_dir = log_dir / 'compression'
    compression_log_dir.mkdir(exist_ok=True)
    # 使用基础文件名，TimedRotatingFileHandler会在轮转时自动添加日期后缀
    compression_log_file = compression_log_dir / "compression.log"
    compression_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(compression_log_file),
        when='midnight',
        interval=1,
        backupCount=30,  # 保留30天的日志文件
        encoding='utf-8'
    )
    compression_handler.setLevel(logging.WARNING)
    compression_handler.setFormatter(formatter)
    
    # 添加过滤器：只记录压缩相关模块的日志
    class CompressionLogFilter(logging.Filter):
        def filter(self, record):
            # 压缩相关模块：backup.compressor, backup.compression_worker
            module_name = record.name
            return (module_name.startswith('backup.compressor') or 
                    module_name.startswith('backup.compression_worker'))
    
    compression_handler.addFilter(CompressionLogFilter())
    root_logger.addHandler(compression_handler)
    
    # 文件移动日志文件处理器（文件移动过程中的所有警告及以上级别日志）
    # 使用TimedRotatingFileHandler按日期命名，匹配现有日志文件格式（file_move_YYYYMMDD.log）
    file_move_log_dir = log_dir / 'file_move'
    file_move_log_dir.mkdir(exist_ok=True)
    # 使用基础文件名，TimedRotatingFileHandler会在轮转时自动添加日期后缀
    file_move_log_file = file_move_log_dir / "file_move.log"
    file_move_handler = logging.handlers.TimedRotatingFileHandler(
        filename=str(file_move_log_file),
        when='midnight',
        interval=1,
        backupCount=30,  # 保留30天的日志文件
        encoding='utf-8'
    )
    file_move_handler.setLevel(logging.WARNING)
    file_move_handler.setFormatter(formatter)
    
    # 添加过滤器：只记录文件移动相关模块的日志
    class FileMoveLogFilter(logging.Filter):
        def filter(self, record):
            # 文件移动相关模块：backup.file_move_worker, backup.tape_file_mover
            module_name = record.name
            return (module_name.startswith('backup.file_move_worker') or 
                    module_name.startswith('backup.tape_file_mover'))
    
    file_move_handler.addFilter(FileMoveLogFilter())
    root_logger.addHandler(file_move_handler)

    # 获取logger（在添加处理器之前先定义）
    logger = logging.getLogger(__name__)
    
    # 添加系统日志处理器（将备份相关的警告及以上级别日志写入系统日志表）
    try:
        from utils.system_log_handler import SystemLogHandler
        system_log_handler = SystemLogHandler()
        system_log_handler.setFormatter(formatter)
        root_logger.addHandler(system_log_handler)
        logger.info("系统日志处理器已添加（备份相关警告及以上级别日志将自动写入系统日志表）")
    except Exception as e:
        logger.warning(f"添加系统日志处理器失败: {str(e)}")

    # 设置第三方库日志级别
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
    logging.getLogger('hypercorn').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)
    # 将 psycopg3 连接池的警告日志级别设置为 ERROR，避免在 INFO 级别时显示连接重置警告
    # 这些警告是正常的连接池清理行为，不影响功能
    logging.getLogger('psycopg.pool').setLevel(logging.ERROR)

    # 记录启动日志
    logger.info("=" * 60)
    logger.info("企业级磁带备份系统 - 日志系统初始化完成")
    logger.info(f"日志级别: {settings.LOG_LEVEL}")
    logger.info(f"日志文件: {settings.LOG_FILE}")
    logger.info(f"错误日志文件: {error_log_file} (警告及以上级别)")
    logger.info(f"压缩日志文件: {compression_log_file} (压缩相关警告及以上级别)")
    logger.info(f"文件移动日志文件: {file_move_log_file} (文件移动相关警告及以上级别)")
    logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


def get_logger(name: str) -> logging.Logger:
    """获取日志器"""
    return logging.getLogger(name)