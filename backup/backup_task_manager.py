#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份任务管理模块
Backup Task Manager Module

负责备份任务的创建、状态查询和取消
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import List, Optional, Dict
from pathlib import Path

from config.settings import get_settings
from models.backup import BackupTask, BackupTaskStatus, BackupTaskType
from utils.network_path import validate_network_path
from utils.scheduler.db_utils import get_opengauss_connection

logger = logging.getLogger(__name__)


class BackupTaskManager:
    """备份任务管理器"""
    
    def __init__(self, settings=None):
        """初始化任务管理器
        
        Args:
            settings: 系统设置对象
        """
        self.settings = settings or get_settings()
        self._current_task: Optional[BackupTask] = None
    
    async def create_backup_task(self, task_name: str, source_paths: List[str],
                                  task_type: BackupTaskType = BackupTaskType.FULL,
                                  **kwargs) -> Optional[BackupTask]:
        """创建备份任务
        
        支持网络路径（UNC路径）：
        - \\192.168.0.79 - 自动列出所有共享
        - \\192.168.0.79\yz - 指定共享路径
        
        Args:
            task_name: 任务名称
            source_paths: 源路径列表
            task_type: 任务类型
            **kwargs: 其他参数
            
        Returns:
            BackupTask: 创建的备份任务对象，如果失败返回None
        """
        try:
            # 检查参数
            if not task_name or not source_paths:
                raise ValueError("任务名称和源路径不能为空")

            # 验证源路径（支持 UNC 网络路径）
            expanded_source_paths = []
            for path in source_paths:
                # 验证路径
                validation_result = validate_network_path(path)
                
                if not validation_result['valid']:
                    # 对于 UNC 路径，如果无法访问，给出更详细的错误信息
                    if validation_result['is_unc']:
                        error_msg = f"无法访问网络路径: {path}"
                        if validation_result['error']:
                            error_msg += f" ({validation_result['error']})"
                        raise ValueError(error_msg)
                    else:
                        raise ValueError(f"源路径不存在: {path}")
                
                # 如果是 UNC 路径且已展开，使用展开后的路径
                if validation_result['is_unc'] and validation_result['expanded_paths']:
                    expanded_source_paths.extend(validation_result['expanded_paths'])
                else:
                    expanded_source_paths.append(path)
            
            # 使用展开后的路径列表
            if expanded_source_paths:
                source_paths = expanded_source_paths
                logger.info(f"路径已展开，共 {len(source_paths)} 个路径")

            # 创建备份任务
            backup_task = BackupTask(
                task_name=task_name,
                task_type=task_type,
                source_paths=source_paths,
                exclude_patterns=kwargs.get('exclude_patterns', []),
                compression_enabled=kwargs.get('compression_enabled', True),
                encryption_enabled=kwargs.get('encryption_enabled', False),
                retention_days=kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                description=kwargs.get('description', ''),
                scheduled_time=kwargs.get('scheduled_time'),
                created_by=kwargs.get('created_by', 'system')
            )

            # 保存到数据库 - 使用原生 openGauss SQL
            async with get_opengauss_connection() as conn:
                # 插入备份任务
                task_id = await conn.fetchval(
                    """
                    INSERT INTO backup_tasks
                    (task_name, task_type, description, status, source_paths, exclude_patterns,
                     compression_enabled, encryption_enabled, retention_days, scheduled_time,
                     created_by, created_at, updated_at)
                    VALUES ($1, $2::backuptasktype, $3, $4::backuptaskstatus, $5::jsonb, $6::jsonb,
                            $7, $8, $9, $10, $11, $12, $13)
                    RETURNING id
                    """,
                    task_name,
                    task_type.value,
                    kwargs.get('description', ''),
                    'PENDING',  # BackupTaskStatus.PENDING
                    json.dumps(source_paths) if source_paths else None,
                    json.dumps(kwargs.get('exclude_patterns', [])) if kwargs.get('exclude_patterns') else None,
                    kwargs.get('compression_enabled', True),
                    kwargs.get('encryption_enabled', False),
                    kwargs.get('retention_days', self.settings.DEFAULT_RETENTION_MONTHS * 30),
                    kwargs.get('scheduled_time'),
                    kwargs.get('created_by', 'system'),
                    datetime.now(),
                    datetime.now()
                )
                backup_task.id = task_id

                # ========= 多表方案：为该任务创建 backup_files 分组和物理表 =========
                # 物理表名采用固定前缀 + 任务ID，避免 SQL 注入
                table_name = f"backup_files_{task_id:06d}"

                # 1. 在 backup_files_groups 中创建元数据记录
                backup_files_group_id = await conn.fetchval(
                    """
                    INSERT INTO backup_files_groups (table_name, task_id)
                    VALUES ($1, $2)
                    RETURNING id
                    """,
                    table_name,
                    task_id,
                )

                # 2. 更新 backup_tasks 表，写入 group_id 和 table_name
                await conn.execute(
                    """
                    UPDATE backup_tasks
                    SET backup_files_group_id = $1,
                        backup_files_table = $2
                    WHERE id = $3
                    """,
                    backup_files_group_id,
                    table_name,
                    task_id,
                )

                # 3. 为该任务创建物理表（基于 backup_files_template 结构）
                # 注意：表名不能用参数占位符，只能通过受控字符串拼接
                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    LIKE backup_files_template INCLUDING ALL
                )
                """
                await conn.execute(create_sql)

                # openGauss / psycopg3 binary protocol 需要显式提交事务
                actual_conn = conn._conn if hasattr(conn, "_conn") else conn
                try:
                    await actual_conn.commit()
                except Exception as commit_err:
                    logger.warning(f"[备份任务] 提交创建任务及其 backup_files 分组/表事务失败（可能已自动提交）: {commit_err}")

            logger.info(f"创建备份任务成功: {task_name}")
            return backup_task

        except Exception as e:
            logger.error(f"创建备份任务失败: {str(e)}")
            return None
    
    async def get_task_status(self, task_id: int) -> Optional[Dict]:
        """获取任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict: 任务状态字典，如果任务不存在返回None
        """
        try:
            # 尝试从运行中的任务对象获取 current_compression_progress（从内存中的压缩程序获取）
            current_compression_progress = None
            if self._current_task and self._current_task.id == task_id:
                if hasattr(self._current_task, 'current_compression_progress') and self._current_task.current_compression_progress:
                    current_compression_progress = self._current_task.current_compression_progress

            # 如果从_current_task获取不到，尝试从系统实例获取
            if not current_compression_progress:
                try:
                    from web.api.backup.utils import get_system_instance
                    system = get_system_instance(None)  # 尝试不传request
                    if system and system.backup_engine:
                        backup_engine = system.backup_engine
                        # 优先从 compression_worker 获取聚合进度（包含 task_progress_list）
                        if hasattr(backup_engine, '_current_compression_worker') and backup_engine._current_compression_worker:
                            compression_worker = backup_engine._current_compression_worker
                            if compression_worker.backup_task.id == task_id:
                                # 从 compression_worker 获取聚合进度（包含所有并行任务的进度和 task_progress_list）
                                aggregated_progress = compression_worker.get_aggregated_compression_progress()
                                if aggregated_progress:
                                    current_compression_progress = aggregated_progress
                                    logger.debug(f"[get_task_status] 从 compression_worker 获取聚合进度，包含 {len(aggregated_progress.get('task_progress_list', []))} 个任务进度")
                                # 如果 get_aggregated_compression_progress 返回 None，尝试从 backup_task 获取
                                elif hasattr(compression_worker.backup_task, 'current_compression_progress'):
                                    current_compression_progress = compression_worker.backup_task.current_compression_progress
                except Exception as e:
                    logger.debug(f"从压缩工作线程获取进度失败: {str(e)}")
            
            from utils.scheduler.db_utils import get_opengauss_connection

            # 优先从内存中的任务对象获取所有实时统计（全部使用内存数据）
            # 尝试从多个来源获取：task_manager._current_task 或 backup_engine._current_task
            in_memory_total_files = None
            in_memory_total_bytes = None
            in_memory_processed_files = None
            in_memory_processed_bytes = None
            in_memory_compressed_bytes = None
            in_memory_scan_status = None
            in_memory_compression_completed = None

            # 1. 从 task_manager._current_task 获取
            if self._current_task and self._current_task.id == task_id:
                in_memory_total_files = getattr(self._current_task, 'total_files', None)
                in_memory_total_bytes = getattr(self._current_task, 'total_bytes', None)
                in_memory_processed_files = getattr(self._current_task, 'processed_files', None)
                in_memory_processed_bytes = getattr(self._current_task, 'processed_bytes', None)
                in_memory_compressed_bytes = getattr(self._current_task, 'compressed_bytes', None)
                in_memory_scan_status = getattr(self._current_task, 'scan_status', None)
                in_memory_compression_completed = getattr(self._current_task, 'compression_completed', None)

            # 2. 如果获取不到，尝试从 backup_engine._current_task 获取
            if (in_memory_total_files is None or in_memory_total_bytes is None or
                in_memory_processed_files is None or in_memory_processed_bytes is None or
                in_memory_scan_status is None):
                try:
                    from web.api.backup.utils import get_system_instance
                    system = get_system_instance(None)  # 尝试不传request
                    if system and system.backup_engine:
                        backup_engine = system.backup_engine
                        if backup_engine._current_task and backup_engine._current_task.id == task_id:
                            if in_memory_total_files is None:
                                in_memory_total_files = getattr(backup_engine._current_task, 'total_files', None)
                            if in_memory_total_bytes is None:
                                in_memory_total_bytes = getattr(backup_engine._current_task, 'total_bytes', None)
                            if in_memory_processed_files is None:
                                in_memory_processed_files = getattr(backup_engine._current_task, 'processed_files', None)
                            if in_memory_processed_bytes is None:
                                in_memory_processed_bytes = getattr(backup_engine._current_task, 'processed_bytes', None)
                            if in_memory_compressed_bytes is None:
                                in_memory_compressed_bytes = getattr(backup_engine._current_task, 'compressed_bytes', None)
                            if in_memory_scan_status is None:
                                in_memory_scan_status = getattr(backup_engine._current_task, 'scan_status', None)
                            if in_memory_compression_completed is None:
                                in_memory_compression_completed = getattr(backup_engine._current_task, 'compression_completed', None)
                except Exception:
                    pass

            # 使用连接池
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, task_name, task_type, status, progress_percent,
                           total_files, total_bytes, processed_files, processed_bytes, compressed_bytes,
                           scan_status, started_at, completed_at, error_message, result_summary,
                           source_paths, tape_device, tape_id, description
                    FROM backup_tasks
                    WHERE id = $1
                    """,
                    task_id
                )

                if row:
                    # 解析 source_paths
                    source_paths = None
                    if row['source_paths']:
                        try:
                            if isinstance(row['source_paths'], str):
                                source_paths = json.loads(row['source_paths'])
                            else:
                                source_paths = row['source_paths']
                        except:
                            source_paths = None

                    # 计算压缩率
                    compression_ratio = 0.0
                    if row['processed_bytes'] and row['processed_bytes'] > 0 and row['compressed_bytes']:
                        compression_ratio = float(row['compressed_bytes']) / float(row['processed_bytes'])

                    # 所有统计字段：优先使用内存中的实时统计，其次回退到数据库字段
                    total_files_value = in_memory_total_files if in_memory_total_files is not None else (row['total_files'] or 0)
                    total_bytes_value = in_memory_total_bytes if in_memory_total_bytes is not None else (row['total_bytes'] or 0)
                    processed_files_value = in_memory_processed_files if in_memory_processed_files is not None else (row['processed_files'] or 0)
                    processed_bytes_value = in_memory_processed_bytes if in_memory_processed_bytes is not None else (row['processed_bytes'] or 0)
                    compressed_bytes_value = in_memory_compressed_bytes if in_memory_compressed_bytes is not None else (row['compressed_bytes'] or 0)
                    scan_status_value = in_memory_scan_status if in_memory_scan_status is not None else row.get('scan_status')

                    # 计算进度百分比（基于内存统计）
                    progress_percent_value = 0.0
                    if total_files_value > 0 and processed_files_value >= 0:
                        # 有总文件数，计算进度百分比（即使 processed_files 为 0 也要计算）
                        progress_percent_value = min(100.0, (processed_files_value / total_files_value) * 100.0)
                    elif row.get('progress_percent') is not None:
                        # 没有总文件数，使用数据库中的进度百分比（包括 0 值）
                        progress_percent_value = float(row['progress_percent'])
                    # 如果 total_files 为 0，说明扫描刚开始，进度为 0% 是正常的，但仍需要返回 0.0

                    return {
                        'id': row['id'],
                        'task_name': row['task_name'],
                        'task_type': row['task_type'],
                        'status': row['status'],
                        'progress_percent': progress_percent_value,
                        'total_files': total_files_value,  # 优先使用内存统计
                        'total_bytes': total_bytes_value,  # 优先使用内存统计
                        'processed_files': processed_files_value,  # 优先使用内存统计
                        'processed_bytes': processed_bytes_value,  # 优先使用内存统计
                        'compressed_bytes': compressed_bytes_value,  # 优先使用内存统计
                        'compression_ratio': compression_ratio,
                        'scan_status': scan_status_value,  # 优先使用内存统计
                        'started_at': row['started_at'],
                        'completed_at': row['completed_at'],
                        'error_message': row['error_message'],
                        'result_summary': row['result_summary'],
                        'source_paths': source_paths,
                        'tape_device': row['tape_device'],
                        'tape_id': row['tape_id'],
                        'description': row['description'],
                        'current_compression_progress': current_compression_progress,
                        'compression_completed': bool(in_memory_compression_completed) if in_memory_compression_completed is not None else None
                    }

            return None
        except Exception as e:
            logger.error(f"获取任务状态失败: {str(e)}")
            return None
    
    async def cancel_task(self, task_id: int) -> bool:
        """取消任务

        Args:
            task_id: 任务ID

        Returns:
            bool: 如果取消成功返回True，否则返回False
        """
        try:
            from utils.scheduler.db_utils import get_opengauss_connection

            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 更新任务状态为取消
                await conn.execute(
                    """
                    UPDATE backup_tasks
                    SET status = $1::backuptaskstatus, updated_at = $2
                    WHERE id = $3 AND status = $4::backuptaskstatus
                    """,
                    'CANCELLED',  # BackupTaskStatus.CANCELLED
                    datetime.now(),
                    task_id,
                    'RUNNING'  # BackupTaskStatus.RUNNING
                )

                # 检查是否更新成功
                row = await conn.fetchrow(
                    """
                    SELECT id FROM backup_tasks WHERE id = $1 AND status = $2::backuptaskstatus
                    """,
                    task_id,
                    'CANCELLED'
                )

                if row:
                    logger.info(f"任务已取消: {task_id}")
                    return True

            return False
        except Exception as e:
            logger.error(f"取消任务失败: {str(e)}")
            return False

