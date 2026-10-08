#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统日志处理器
System Log Handler - 自动将日志写入系统日志表
"""

import logging
import asyncio
import traceback
from typing import Optional
from datetime import datetime

from models.system_log import LogLevel, LogCategory
from utils.log_utils import log_system


class SystemLogHandler(logging.Handler):
    """系统日志处理器 - 将日志自动写入系统日志表"""
    
    # 需要记录到系统日志的模块前缀
    BACKUP_MODULES = [
        'backup',
        'compressor',
        'file_scanner',
        'backup_engine',
        'backup_scanner',
        'backup_task_manager',
        'backup_db',
        'tape_handler',
        'backup_notifier',
        'utils.scheduler',
        'recovery',
        'tape',
    ]
    
    # 需要记录的日志级别（WARNING及以上）
    MIN_LEVEL = logging.WARNING
    
    def __init__(self):
        super().__init__()
        self.setLevel(self.MIN_LEVEL)
    
    def emit(self, record: logging.LogRecord):
        """处理日志记录"""
        try:
            # 只处理 WARNING 及以上级别的日志
            if record.levelno < self.MIN_LEVEL:
                return
            
            # 检查是否来自备份相关模块
            module_name = record.name
            if not any(module_name.startswith(prefix) for prefix in self.BACKUP_MODULES):
                return
            
            # 将日志级别映射到 LogLevel 枚举
            level_map = {
                logging.WARNING: LogLevel.WARNING,
                logging.ERROR: LogLevel.ERROR,
                logging.CRITICAL: LogLevel.CRITICAL,
            }
            log_level = level_map.get(record.levelno, LogLevel.WARNING)
            
            # 确定日志分类
            category = LogCategory.BACKUP
            if 'compressor' in module_name or '压缩' in record.getMessage():
                category = LogCategory.BACKUP
            elif 'tape' in module_name:
                category = LogCategory.TAPE
            elif 'scanner' in module_name or '扫描' in record.getMessage():
                category = LogCategory.BACKUP
            else:
                category = LogCategory.BACKUP
            
            # 提取模块和函数信息
            module = record.module if hasattr(record, 'module') else module_name
            function = record.funcName if hasattr(record, 'funcName') else None
            file_path = record.pathname if hasattr(record, 'pathname') else None
            line_number = record.lineno if hasattr(record, 'lineno') else None
            
            # 构建消息
            message = record.getMessage()
            
            # 提取异常信息
            exception_type = None
            stack_trace = None
            if record.exc_info:
                exception_type = record.exc_info[0].__name__ if record.exc_info[0] else None
                stack_trace = ''.join(traceback.format_exception(*record.exc_info))
            
            # 提取任务ID（如果存在）
            task_id = None
            if hasattr(record, 'task_id'):
                task_id = record.task_id
            
            # 构建详细信息
            details = {
                'logger_name': module_name,
                'level_name': record.levelname,
                'module': module,
                'function': function,
                'file_path': file_path,
                'line_number': line_number,
            }
            
            # 如果有异常信息，添加到details
            if exception_type:
                details['exception_type'] = exception_type
            
            # 异步记录到系统日志表（使用 run_coroutine_threadsafe 避免事件循环问题）
            try:
                # 尝试获取正在运行的事件循环
                loop = None
                try:
                    # 优先使用 get_running_loop()，它只返回当前正在运行的事件循环
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # 如果没有运行中的事件循环，尝试获取当前事件循环
                    try:
                        loop = asyncio.get_event_loop()
                        # 如果事件循环没有运行，无法使用，设置为None
                        if not loop.is_running():
                            loop = None
                    except RuntimeError:
                        # 完全没有事件循环，无法记录日志
                        loop = None
                
                if loop is not None and loop.is_running():
                    # 有运行中的事件循环，使用 run_coroutine_threadsafe 在正确的事件循环中执行
                    # 这样可以避免 "Task got Future attached to a different loop" 错误
                    try:
                        asyncio.run_coroutine_threadsafe(
                            log_system(
                                level=log_level,
                                category=category,
                                message=message,
                                module=module,
                                function=function,
                                file_path=file_path,
                                line_number=line_number,
                                task_id=task_id,
                                details=details,
                                exception_type=exception_type,
                                stack_trace=stack_trace
                            ),
                            loop
                        )
                    except RuntimeError as loop_error:
                        # 如果事件循环已关闭或无效，静默忽略
                        # 这通常发生在系统关闭时
                        if "closed" not in str(loop_error).lower() and "different loop" not in str(loop_error).lower():
                            import sys
                            print(f"SystemLogHandler: 记录系统日志失败（事件循环问题）: {str(loop_error)}", file=sys.stderr)
                else:
                    # 没有运行中的事件循环，无法异步记录
                    # 静默忽略，因为日志记录失败不应该影响主程序
                    pass
            except Exception as e:
                # 如果记录失败，静默忽略（避免影响主程序）
                # 只在调试模式下输出错误信息
                import sys
                error_msg = str(e)
                # 忽略事件循环相关的错误（这些是预期的，不影响功能）
                if "different loop" not in error_msg.lower() and "closed" not in error_msg.lower():
                    print(f"SystemLogHandler: 记录系统日志失败: {error_msg}", file=sys.stderr)
                
        except Exception as e:
            # 处理日志记录时的异常（避免影响主程序）
            import sys
            print(f"SystemLogHandler: 处理日志记录时发生异常: {str(e)}", file=sys.stderr)
            self.handleError(record)

