#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恢复引擎模块
Recovery Engine Module
"""

import os
import io
import gzip
import zipfile
import tarfile
import asyncio
import logging
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import py7zr

from config.settings import get_settings
from models.backup import BackupSetStatus, BackupTaskType, BackupFileType
from models.system_log import OperationLog, OperationType
from tape.tape_manager import TapeManager
from utils.dingtalk_notifier import DingTalkNotifier
from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
import json

try:
    import zstandard as zstd
except ImportError:
    zstd = None

logger = logging.getLogger(__name__)


class RecoveryEngine:
    """恢复引擎"""

    def __init__(self):
        self.settings = get_settings()
        self.tape_manager: Optional[TapeManager] = None
        self.dingtalk_notifier: Optional[DingTalkNotifier] = None
        self._initialized = False
        self._current_recovery: Optional[Dict] = None
        self._progress_callbacks: List[Callable] = []

    async def initialize(self):
        """初始化恢复引擎"""
        try:
            # 创建恢复临时目录
            Path(self.settings.RECOVERY_TEMP_DIR).mkdir(parents=True, exist_ok=True)

            self._initialized = True
            logger.info("恢复引擎初始化完成")

        except Exception as e:
            logger.error(f"恢复引擎初始化失败: {str(e)}")
            raise

    def set_dependencies(self, tape_manager: TapeManager, dingtalk_notifier: DingTalkNotifier):
        """设置依赖组件"""
        self.tape_manager = tape_manager
        self.dingtalk_notifier = dingtalk_notifier

    def add_progress_callback(self, callback: Callable):
        """添加进度回调"""
        self._progress_callbacks.append(callback)

    async def search_backup_sets(self, filters: Dict[str, Any] = None) -> List[Dict]:
        """搜索备份集（从数据库查询真实数据）"""
        try:
            backup_sets = []
            filters = filters or {}

            # openGauss 原生SQL查询
            # 使用连接池
            async with get_opengauss_connection() as conn:
                # 构建WHERE子句
                where_clauses = []
                params = []
                param_index = 1

                # 只查询活跃状态的备份集
                where_clauses.append("LOWER(status::text) = LOWER('ACTIVE')")

                # 应用过滤条件
                if 'backup_group' in filters and filters['backup_group']:
                    where_clauses.append(f"backup_group = ${param_index}")
                    params.append(filters['backup_group'])
                    param_index += 1

                if 'tape_id' in filters and filters['tape_id']:
                    where_clauses.append(f"tape_id = ${param_index}")
                    params.append(filters['tape_id'])
                    param_index += 1

                if 'date_from' in filters and filters['date_from']:
                    where_clauses.append(f"backup_time >= ${param_index}")
                    params.append(filters['date_from'])
                    param_index += 1

                if 'date_to' in filters and filters['date_to']:
                    where_clauses.append(f"backup_time <= ${param_index}")
                    params.append(filters['date_to'])
                    param_index += 1

                where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

                # 查询备份集
                sql = f"""
                    SELECT id, set_id, set_name, backup_group, backup_type, backup_time,
                           total_files, total_bytes, compressed_bytes, compression_ratio,
                           tape_id, status, created_at
                    FROM backup_sets
                    WHERE {where_sql}
                    ORDER BY backup_time DESC
                    LIMIT 100
                """
                rows = await conn.fetch(sql, *params)

                # 转换为字典格式
                for row in rows:
                    backup_set = {
                        'id': row['id'],
                        'set_id': row['set_id'],
                        'set_name': row['set_name'],
                        'backup_group': row['backup_group'],
                        'backup_type': row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                        'backup_time': row['backup_time'].isoformat() if isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                        'total_files': row['total_files'] or 0,
                        'total_bytes': row['total_bytes'] or 0,
                        'compressed_bytes': row['compressed_bytes'] or 0,
                        'compression_ratio': float(row['compression_ratio']) if row['compression_ratio'] else None,
                        'tape_id': row['tape_id'],
                        'status': row['status'].value if hasattr(row['status'], 'value') else str(row['status']),
                        'created_at': row['created_at'].isoformat() if isinstance(row['created_at'], datetime) else str(row['created_at'])
                    }
                    backup_sets.append(backup_set)

            logger.info(f"查询到 {len(backup_sets)} 个备份集")
            return backup_sets

        except Exception as e:
            logger.error(f"搜索备份集失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def get_backup_set_files(self, backup_set_id: str) -> List[Dict]:
        """获取备份集文件列表（从数据库查询真实数据）"""
        try:
            files = []

            # openGauss 原生SQL查询
            # 使用连接池
            async with get_opengauss_connection() as conn:
                try:
                    # 首先根据 set_id 查找备份集的 id
                    backup_set_row = await conn.fetchrow(
                        "SELECT id FROM backup_sets WHERE set_id = $1",
                        backup_set_id
                    )
                except Exception as query_error:
                    logger.error(f"[openGauss] 查询备份集ID失败: backup_set_id={backup_set_id}, 错误: {str(query_error)}", exc_info=True)
                    return []

                if not backup_set_row:
                    logger.warning(f"[openGauss] 备份集不存在: {backup_set_id}")
                    return []

                backup_set_db_id = backup_set_row['id']

                # 多表方案：根据 backup_set_db_id 决定物理表名
                from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)

                # 查询该备份集的所有文件（恢复时应该显示所有已扫描的文件，不管是否已复制到磁带）
                sql = f"""
                    SELECT id, file_path, file_name, directory_path, display_name,
                           file_type, file_size, compressed_size,
                           file_permissions, created_time, modified_time, accessed_time,
                           compressed, checksum, backup_time, chunk_number
                    FROM {table_name}
                    WHERE backup_set_id = $1
                    ORDER BY file_path ASC
                """
                try:
                    rows = await conn.fetch(sql, backup_set_db_id)
                except Exception as query_error:
                    logger.error(f"[openGauss] 查询备份集文件列表失败: backup_set_id={backup_set_id}, backup_set_db_id={backup_set_db_id}, 表: {table_name}, 错误: {str(query_error)}", exc_info=True)
                    return []

                # 转换为字典格式
                for row in rows:
                    file_info = {
                        'id': row['id'],
                        'file_path': row['file_path'],
                        'file_name': row['file_name'],
                        'directory_path': row['directory_path'],
                        'display_name': row['display_name'],
                        'file_type': row['file_type'].value if hasattr(row['file_type'], 'value') else str(row['file_type']),
                        'file_size': row['file_size'] or 0,
                        'compressed_size': row['compressed_size'] or 0,
                        'file_permissions': row['file_permissions'],
                        'created_time': row['created_time'].isoformat() if row['created_time'] and isinstance(row['created_time'], datetime) else (str(row['created_time']) if row['created_time'] else None),
                        'modified_time': row['modified_time'].isoformat() if row['modified_time'] and isinstance(row['modified_time'], datetime) else (str(row['modified_time']) if row['modified_time'] else None),
                        'accessed_time': row['accessed_time'].isoformat() if row['accessed_time'] and isinstance(row['accessed_time'], datetime) else (str(row['accessed_time']) if row['accessed_time'] else None),
                        'compressed': row['compressed'] or False,
                        'checksum': row['checksum'],
                        'backup_time': row['backup_time'].isoformat() if row['backup_time'] and isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                        'chunk_number': row['chunk_number']
                    }
                    files.append(file_info)

            logger.info(f"查询到 {len(files)} 个文件 (备份集: {backup_set_id})")
            return files

        except Exception as e:
            logger.error(f"获取备份集文件列表失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def get_top_level_directories(self, backup_set_id: str) -> List[Dict]:
        """获取备份集的顶层目录结构（只返回顶层目录和文件，不返回所有文件）

        用于优化性能，避免一次性加载所有文件导致前端卡顿

        Args:
            backup_set_id: 备份集ID

        Returns:
            顶层目录和文件列表，每个元素包含：
            - name: 目录或文件名
            - type: 'directory' 或 'file'
            - path: 完整路径
            - file: 如果是文件，包含文件信息；如果是目录，为None
            - has_children: 是否有子节点（仅目录）
        """
        try:
            directories = {}
            files = []

            # 使用连接池
            async with get_opengauss_connection() as conn:
                    # 首先根据 set_id 查找备份集的 id
                    backup_set_row = await conn.fetchrow(
                        "SELECT id FROM backup_sets WHERE set_id = $1",
                        backup_set_id
                    )
                    
                    if not backup_set_row:
                        logger.warning(f"备份集不存在: {backup_set_id}")
                        return []
                    
                    backup_set_db_id = backup_set_row['id']

                    # 多表方案：根据 backup_set_db_id 决定物理表名
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                    
                    # 使用SQL查询直接在数据库中提取唯一的顶层目录和文件
                    # 使用字符串函数提取第一级路径，并使用GROUP BY去重
                    # 这样可以避免在Python中遍历所有文件路径
                    sql = f"""
                        WITH normalized_paths AS (
                            SELECT 
                                file_path,
                                REPLACE(file_path, '\\', '/') as normalized_path
                            FROM {table_name}
                            WHERE backup_set_id = $1
                        ),
                        first_levels AS (
                            SELECT 
                                CASE 
                                    WHEN POSITION('/' IN normalized_path) > 0 
                                    THEN SUBSTRING(normalized_path FROM 1 FOR POSITION('/' IN normalized_path) - 1)
                                    ELSE normalized_path
                                END as first_level,
                                file_path,
                                CASE 
                                    WHEN POSITION('/' IN normalized_path) > 0 THEN 'directory'
                                    ELSE 'file'
                                END as item_type
                            FROM normalized_paths
                        )
                        SELECT DISTINCT
                            first_level,
                            item_type,
                            MIN(file_path) as sample_path
                        FROM first_levels
                        GROUP BY first_level, item_type
                        ORDER BY first_level ASC
                    """
                    # 添加重试机制，避免缓冲区错误
                    rows = None
                    retry_count = 0
                    max_retries = 3
                    while retry_count < max_retries:
                        try:
                            rows = await conn.fetch(sql, backup_set_db_id)
                            break  # 成功，退出重试循环
                        except AssertionError as e:
                            error_msg = str(e)
                            if "insufficient data in buffer" in error_msg:
                                retry_count += 1
                                if retry_count >= max_retries:
                                    logger.error(
                                        f"查询顶层目录结构失败（已重试 {max_retries} 次）: {e}，"
                                        f"备份集ID: {backup_set_db_id}"
                                    )
                                    raise  # 主查询失败，抛出异常
                                else:
                                    logger.warning(
                                        f"查询顶层目录结构时缓冲区错误（第 {retry_count} 次重试）: {e}，"
                                        f"备份集ID: {backup_set_db_id}"
                                    )
                                    await asyncio.sleep(1.0 * retry_count)  # 指数退避
                                    continue
                            else:
                                # 其他 AssertionError，也重试
                                retry_count += 1
                                if retry_count >= max_retries:
                                    logger.error(
                                        f"查询顶层目录结构失败（已重试 {max_retries} 次）: {e}，"
                                        f"备份集ID: {backup_set_db_id}"
                                    )
                                    raise
                                await asyncio.sleep(1.0 * retry_count)
                                continue
                        except Exception as e:
                            # 其他异常，直接抛出
                            logger.error(
                                f"查询顶层目录结构时发生异常: {e}，备份集ID: {backup_set_db_id}"
                            )
                            raise
                    
                    if rows is None:
                        logger.error(f"查询顶层目录结构失败，返回空结果，备份集ID: {backup_set_db_id}")
                        return []
                    
                    logger.info(f"通过SQL查询提取到 {len(rows)} 个唯一的顶层项")
                    
                    # 处理查询结果
                    for row in rows:
                        first_level = row['first_level']
                        item_type = row['item_type']
                        sample_path = row['sample_path']
                        
                        if not first_level:
                            continue
                        
                        if item_type == 'file':
                            # 顶层文件，查询文件详细信息
                            # 添加重试机制，避免缓冲区错误
                            file_row = None
                            retry_count = 0
                            max_retries = 3
                            while retry_count < max_retries:
                                try:
                                    # 简化查询，去掉不必要的类型转换
                                    file_row = await conn.fetchrow(
                                        f"""
                                        SELECT id, file_path, file_name, file_type, file_size, compressed_size,
                                               file_permissions, created_time, modified_time, accessed_time,
                                               compressed, checksum, backup_time, chunk_number
                                        FROM {table_name}
                                        WHERE backup_set_id = $1 AND file_path = $2
                                        LIMIT 1
                                        """,
                                        backup_set_db_id,
                                        sample_path
                                    )
                                    break  # 成功，退出重试循环
                                except AssertionError as e:
                                    error_msg = str(e)
                                    if "insufficient data in buffer" in error_msg:
                                        retry_count += 1
                                        if retry_count >= max_retries:
                                            logger.warning(
                                                f"查询文件详细信息失败（已重试 {max_retries} 次）: {sample_path}, "
                                                f"错误: {e}，跳过此文件"
                                            )
                                            file_row = None
                                            break
                                        else:
                                            logger.debug(
                                                f"查询文件详细信息时缓冲区错误（第 {retry_count} 次重试）: "
                                                f"{sample_path}, 错误: {e}"
                                            )
                                            await asyncio.sleep(0.5 * retry_count)  # 指数退避
                                            continue
                                    else:
                                        # 其他 AssertionError，也重试
                                        retry_count += 1
                                        if retry_count >= max_retries:
                                            logger.warning(
                                                f"查询文件详细信息失败（已重试 {max_retries} 次）: {sample_path}, "
                                                f"错误: {e}，跳过此文件"
                                            )
                                            file_row = None
                                            break
                                        await asyncio.sleep(0.5 * retry_count)
                                        continue
                                except Exception as e:
                                    # 其他异常，记录并跳过
                                    logger.warning(
                                        f"查询文件详细信息时发生异常: {sample_path}, "
                                        f"错误: {e}，跳过此文件"
                                    )
                                    file_row = None
                                    break
                            
                            if file_row:
                                file_info = {
                                    'id': file_row['id'],
                                    'file_path': file_row['file_path'],
                                    'file_name': file_row['file_name'],
                                    'directory_path': file_row['directory_path'],
                                    'display_name': file_row['display_name'],
                                    'file_type': file_row['file_type'].value if hasattr(file_row['file_type'], 'value') else str(file_row['file_type']),
                                    'file_size': file_row['file_size'] or 0,
                                    'compressed_size': file_row['compressed_size'] or 0,
                                    'file_permissions': file_row['file_permissions'],
                                    'created_time': file_row['created_time'].isoformat() if file_row['created_time'] and isinstance(file_row['created_time'], datetime) else (str(file_row['created_time']) if file_row['created_time'] else None),
                                    'modified_time': file_row['modified_time'].isoformat() if file_row['modified_time'] and isinstance(file_row['modified_time'], datetime) else (str(file_row['modified_time']) if file_row['modified_time'] else None),
                                    'accessed_time': file_row['accessed_time'].isoformat() if file_row['accessed_time'] and isinstance(file_row['accessed_time'], datetime) else (str(file_row['accessed_time']) if file_row['accessed_time'] else None),
                                    'compressed': file_row['compressed'] or False,
                                    'checksum': file_row['checksum'],
                                    'backup_time': file_row['backup_time'].isoformat() if file_row['backup_time'] and isinstance(file_row['backup_time'], datetime) else str(file_row['backup_time']),
                                    'chunk_number': file_row['chunk_number']
                                }
                                files.append({
                                    'name': first_level,
                                    'type': 'file',
                                    'path': sample_path,
                                    'file': file_info,
                                    'has_children': False
                                })
                        else:
                            # 顶层目录，检查是否有子项
                            # 优化：使用 EXISTS 代替 COUNT，减少数据传输量
                            # 添加重试机制，避免缓冲区错误
                            like_pattern1 = first_level + '/%'
                            like_pattern2 = first_level + '\\%'
                            has_children = True  # 默认值
                            
                            retry_count = 0
                            max_retries = 3
                            while retry_count < max_retries:
                                try:
                                    # 使用 EXISTS 代替 COUNT，只返回布尔值，减少数据传输
                                    # 多表方案：根据 backup_set_id 决定物理表名，避免直接访问基础表 backup_files
                                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                                    has_children_sql = f"""
                                        SELECT EXISTS(
                                            SELECT 1
                                            FROM {table_name}
                                            WHERE backup_set_id = $1 
                                              AND (
                                                  REPLACE(file_path, '\\\\', '/') LIKE $2 
                                                  OR REPLACE(file_path, '\\\\', '/') LIKE $3
                                              )
                                              AND REPLACE(file_path, '\\\\', '/') != $4
                                              LIMIT 1
                                        ) as has_children
                                    """
                                    count_row = await conn.fetchrow(
                                        has_children_sql, 
                                        backup_set_db_id, 
                                        like_pattern1, 
                                        like_pattern2, 
                                        first_level
                                    )
                                    has_children = count_row['has_children'] if count_row else True
                                    break  # 成功，退出重试循环
                                except AssertionError as e:
                                    error_msg = str(e)
                                    if "insufficient data in buffer" in error_msg:
                                        retry_count += 1
                                        if retry_count >= max_retries:
                                            logger.warning(
                                                f"检查目录是否有子项失败（已重试 {max_retries} 次）: {first_level}, "
                                                f"错误: {e}，使用默认值 has_children=True"
                                            )
                                            has_children = True  # 使用默认值
                                            break
                                        else:
                                            logger.debug(
                                                f"检查目录是否有子项时缓冲区错误（第 {retry_count} 次重试）: "
                                                f"{first_level}, 错误: {e}"
                                            )
                                            await asyncio.sleep(0.5 * retry_count)  # 指数退避
                                            continue
                                    else:
                                        # 其他 AssertionError，也重试
                                        retry_count += 1
                                        if retry_count >= max_retries:
                                            logger.warning(
                                                f"检查目录是否有子项失败（已重试 {max_retries} 次）: {first_level}, "
                                                f"错误: {e}，使用默认值 has_children=True"
                                            )
                                            has_children = True
                                            break
                                        await asyncio.sleep(0.5 * retry_count)
                                        continue
                                except Exception as e:
                                    # 其他异常，记录并使用默认值
                                    logger.warning(
                                        f"检查目录是否有子项时发生异常: {first_level}, "
                                        f"错误: {e}，使用默认值 has_children=True"
                                    )
                                    has_children = True
                                    break
                            
                            directories[first_level] = {
                                'name': first_level,
                                'type': 'directory',
                                'path': first_level,
                                'file': None,
                                'has_children': has_children
                            }

            # 合并目录和文件，目录在前
            result = list(directories.values()) + files
            logger.info(f"查询到 {len(result)} 个顶层目录项 (备份集: {backup_set_id})，其中目录: {len(directories)}, 文件: {len(files)}")
            
            # 如果顶层目录项过多，记录警告并显示前10个示例
            if len(result) > 1000:
                logger.warning(f"顶层目录项过多 ({len(result)} 个)，可能存在路径格式问题")
                sample_paths = [item['path'] for item in result[:10]]
                logger.warning(f"前10个顶层项示例: {sample_paths}")
            
            return result
            
        except Exception as e:
            logger.error(f"获取顶层目录结构失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def get_directory_contents(self, backup_set_id: str, directory_path: str) -> List[Dict]:
        """获取指定目录下的文件和子目录列表

        Args:
            backup_set_id: 备份集ID
            directory_path: 目录路径（如 'folder1' 或 'folder1/subfolder'）

        Returns:
            目录内容列表，每个元素包含：
            - name: 目录或文件名
            - type: 'directory' 或 'file'
            - path: 完整路径
            - file: 如果是文件，包含文件信息；如果是目录，为None
            - has_children: 是否有子节点（仅目录）
        """
        try:
            # 规范化路径（移除前导和尾随斜杠）
            directory_path = directory_path.strip('/').strip('\\')

            directories = {}
            files = []

            # 使用连接池
            async with get_opengauss_connection() as conn:
                try:
                    # 首先根据 set_id查找备份集的id
                    backup_set_row = await conn.fetchrow(
                        "SELECT id FROM backup_sets WHERE set_id = $1",
                        backup_set_id
                    )
                except Exception as query_error:
                    logger.error(f"[openGauss] 查询备份集ID失败: backup_set_id={backup_set_id}, 错误: {str(query_error)}", exc_info=True)
                    return []

                if not backup_set_row:
                    logger.warning(f"[openGauss] 备份集不存在: {backup_set_id}")
                    return []

                backup_set_db_id = backup_set_row['id']

                # 查询该目录下的所有文件
                if directory_path:
                    # 规范化目录路径，统一使用正斜杠
                    normalized_dir = directory_path.replace('\\', '/')
                    # 构建匹配模式：支持 D:/ 和 D:\ 两种格式
                    # 使用 REPLACE 规范化路径后再匹配
                    sql = """
                        SELECT id, file_path, file_name, directory_path, display_name,
                               file_type, file_size, compressed_size,
                               file_permissions, created_time, modified_time, accessed_time,
                               compressed, checksum, backup_time, chunk_number
                        FROM backup_files
                        WHERE backup_set_id = $1
                          AND (
                              REPLACE(file_path, '\\', '/') = $2
                              OR REPLACE(file_path, '\\', '/') LIKE $3
                              OR REPLACE(file_path, '\\', '/') LIKE $4
                          )
                        ORDER BY file_path ASC
                    """
                    # 匹配模式：D:/ 或 D:\ 开头的路径
                    like_pattern1 = normalized_dir + '/%'
                    like_pattern2 = normalized_dir + '\\%'
                    try:
                        rows = await conn.fetch(sql, backup_set_db_id, normalized_dir, like_pattern1, like_pattern2)
                    except Exception as query_error:
                        logger.error(f"[openGauss] 查询目录内容失败: backup_set_id={backup_set_id}, directory_path={directory_path}, 错误: {str(query_error)}", exc_info=True)
                        return []
                else:
                    # 根目录
                    from utils.scheduler.db_utils import get_backup_files_table_by_set_id
                    table_name = await get_backup_files_table_by_set_id(conn, backup_set_db_id)
                    sql = f"""
                        SELECT id, file_path, file_name, directory_path, display_name,
                               file_type, file_size, compressed_size,
                               file_permissions, created_time, modified_time, accessed_time,
                               compressed, checksum, backup_time, chunk_number
                        FROM {table_name}
                        WHERE backup_set_id = $1
                        ORDER BY file_path ASC
                    """
                    try:
                        rows = await conn.fetch(sql, backup_set_db_id)
                    except Exception as query_error:
                        logger.error(f"[openGauss] 查询根目录内容失败: backup_set_id={backup_set_id}, 错误: {str(query_error)}", exc_info=True)
                        return []

                # 处理查询结果
                normalized_dir = directory_path.replace('\\', '/') if directory_path else ''

                for row in rows:
                    file_path = row['file_path']

                    # 规范化文件路径
                    normalized_file_path = file_path.replace('\\', '/')

                    # 计算相对路径
                    if directory_path:
                        # 检查路径是否匹配（支持 / 和 \ 两种分隔符）
                        if normalized_file_path == normalized_dir:
                            # 这是目录本身，跳过
                            continue
                        if not (normalized_file_path.startswith(normalized_dir + '/') or
                               normalized_file_path.startswith(normalized_dir + '\\')):
                            continue

                        # 提取相对路径（跳过目录路径和分隔符）
                        if normalized_file_path.startswith(normalized_dir + '/'):
                            relative_path = normalized_file_path[len(normalized_dir + '/'):]
                        elif normalized_file_path.startswith(normalized_dir + '\\'):
                            relative_path = normalized_file_path[len(normalized_dir + '\\'):]
                        else:
                            continue
                    else:
                        relative_path = normalized_file_path

                    # 分割路径
                    path_parts = relative_path.split('/')
                    path_parts = [p for p in path_parts if p]

                    if not path_parts:
                        continue

                    # 获取当前层级的内容（第一个部分）
                    first_part = path_parts[0]

                    if len(path_parts) == 1:
                        # 这是当前目录下的文件
                        file_info = {
                            'id': row['id'],
                            'file_path': row['file_path'],
                            'file_name': row['file_name'],
                            'directory_path': row['directory_path'],
                            'display_name': row['display_name'],
                            'file_type': row['file_type'].value if hasattr(row['file_type'], 'value') else str(row['file_type']),
                            'file_size': row['file_size'] or 0,
                            'compressed_size': row['compressed_size'] or 0,
                            'file_permissions': row['file_permissions'],
                            'created_time': row['created_time'].isoformat() if row['created_time'] and isinstance(row['created_time'], datetime) else (str(row['created_time']) if row['created_time'] else None),
                            'modified_time': row['modified_time'].isoformat() if row['modified_time'] and isinstance(row['modified_time'], datetime) else (str(row['modified_time']) if row['modified_time'] else None),
                            'accessed_time': row['accessed_time'].isoformat() if row['accessed_time'] and isinstance(row['accessed_time'], datetime) else (str(row['accessed_time']) if row['accessed_time'] else None),
                            'compressed': row['compressed'] or False,
                            'checksum': row['checksum'],
                            'backup_time': row['backup_time'].isoformat() if row['backup_time'] and isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                            'chunk_number': row['chunk_number']
                        }
                        files.append({
                            'name': first_part,
                            'type': 'file',
                            'path': file_path,
                            'file': file_info,
                            'has_children': False
                        })
                    else:
                        # 这是子目录
                        if first_part not in directories:
                            # 构建子目录路径，保持与原始路径格式一致
                            if directory_path:
                                # 检查原始路径格式，使用相同的分隔符
                                if '\\' in directory_path:
                                    child_path = directory_path + '\\' + first_part
                                else:
                                    child_path = directory_path + '/' + first_part
                            else:
                                child_path = first_part

                            directories[first_part] = {
                                'name': first_part,
                                'type': 'directory',
                                'path': child_path,
                                'file': None,
                                'has_children': True
                            }

            # 合并目录和文件，目录在前
            result = list(directories.values()) + files
            logger.info(f"查询到 {len(result)} 个目录项 (路径: {directory_path})")
            return result

        except Exception as e:
            logger.error(f"获取目录内容失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    async def search_files(self, backup_set_id: str, search_term: str,
                         file_type: str = None) -> List[Dict]:
        """搜索文件"""
        try:
            all_files = await self.get_backup_set_files(backup_set_id)
            filtered_files = []

            search_term_lower = search_term.lower()

            for file_info in all_files:
                # 搜索文件名或路径
                if (search_term_lower in file_info['file_name'].lower() or
                    search_term_lower in file_info['file_path'].lower()):

                    # 文件类型过滤
                    if file_type and file_info['file_type'] != file_type:
                        continue

                    filtered_files.append(file_info)

            return filtered_files

        except Exception as e:
            logger.error(f"搜索文件失败: {str(e)}")
            return []

    async def create_recovery_task(self, backup_set_id: str, files: List[Dict],
                                 target_path: str, **kwargs) -> Optional[str]:
        """创建恢复任务"""
        try:
            if not backup_set_id or not files or not target_path:
                raise ValueError("备份集ID、文件列表和目标路径不能为空")

            # 验证目标路径
            target_dir = Path(target_path)
            target_dir.mkdir(parents=True, exist_ok=True)

            # 生成恢复任务ID
            recovery_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            # 创建恢复任务信息
            recovery_info = {
                'recovery_id': recovery_id,
                'backup_set_id': backup_set_id,
                'files': files,
                'target_path': target_path,
                'status': 'pending',
                'created_at': datetime.now(),
                'started_at': None,
                'completed_at': None,
                'progress_percent': 0.0,
                'processed_files': 0,
                'total_files': len(files),
                'total_bytes': sum(f.get('file_size', 0) for f in files),
                'processed_bytes': 0,
                'error_message': None,
                'created_by': kwargs.get('created_by', 'system')
            }

            self._current_recovery = recovery_info

            logger.info(f"创建恢复任务成功: {recovery_id}")
            return recovery_id

        except Exception as e:
            logger.error(f"创建恢复任务失败: {str(e)}")
            return None

    async def execute_recovery(self, recovery_id: str) -> bool:
        """执行恢复操作"""
        try:
            if not self._initialized:
                raise RuntimeError("恢复引擎未初始化")

            if not self._current_recovery or self._current_recovery['recovery_id'] != recovery_id:
                raise RuntimeError("恢复任务不存在")

            recovery_info = self._current_recovery
            logger.info(f"开始执行恢复任务: {recovery_id}")

            # 更新状态
            recovery_info['status'] = 'running'
            recovery_info['started_at'] = datetime.now()

            # 发送开始通知
            if self.dingtalk_notifier:
                await self.dingtalk_notifier.send_recovery_notification(
                    recovery_id,
                    "started"
                )

            # 执行恢复流程
            success = await self._perform_recovery(recovery_info)

            # 更新完成状态
            recovery_info['completed_at'] = datetime.now()
            if success:
                recovery_info['status'] = 'completed'
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_recovery_notification(
                        recovery_id,
                        "success",
                        {
                            'file_count': recovery_info['processed_files'],
                            'size': self._format_bytes(recovery_info['processed_bytes']),
                            'duration': str(recovery_info['completed_at'] - recovery_info['started_at'])
                        }
                    )
            else:
                recovery_info['status'] = 'failed'
                if self.dingtalk_notifier:
                    await self.dingtalk_notifier.send_recovery_notification(
                        recovery_id,
                        "failed",
                        {'error': recovery_info['error_message']}
                    )

            logger.info(f"恢复任务执行完成: {recovery_id}, 成功: {success}")
            return success

        except Exception as e:
            logger.error(f"执行恢复任务失败: {str(e)}")
            if self._current_recovery:
                self._current_recovery['error_message'] = str(e)
                self._current_recovery['status'] = 'failed'
            return False
        finally:
            self._current_recovery = None

    async def _perform_recovery(self, recovery_info: Dict) -> bool:
        """执行恢复流程"""
        try:
            backup_set_id = recovery_info['backup_set_id']
            target_path = Path(recovery_info['target_path'])
            files = recovery_info['files']

            # 1. 获取备份集信息
            backup_set_info = await self._get_backup_set_info(backup_set_id)
            if not backup_set_info:
                raise RuntimeError(f"备份集不存在: {backup_set_id}")

            # 2. 获取磁带信息
            tape_id = backup_set_info['tape_id']
            logger.info(f"需要加载磁带: {tape_id}")

            # 3. 加载磁带
            if not await self.tape_manager.load_tape(tape_id):
                raise RuntimeError(f"加载磁带失败: {tape_id}")

            # 4. 读取并恢复文件
            processed_files = 0
            processed_bytes = 0

            for file_info in files:
                try:
                    # 读取文件数据
                    file_data = await self._read_file_from_tape(file_info)
                    if not file_data:
                        logger.warning(f"无法读取文件: {file_info['file_path']}")
                        continue

                    # 解压文件（如果需要）
                    if file_info.get('compressed_size', 0) > 0:
                        file_data = await self._decompress_file_data(file_data, file_info)

                    # 写入目标位置
                    target_file_path = target_path / Path(file_info['file_path']).name
                    target_file_path.parent.mkdir(parents=True, exist_ok=True)

                    with open(target_file_path, 'wb') as f:
                        f.write(file_data)

                    # 验证文件完整性
                    if await self._verify_file_integrity(target_file_path, file_info):
                        processed_files += 1
                        processed_bytes += len(file_data)
                        logger.info(f"文件恢复成功: {file_info['file_path']}")
                    else:
                        logger.error(f"文件完整性验证失败: {file_info['file_path']}")

                    # 更新进度
                    recovery_info['processed_files'] = processed_files
                    recovery_info['processed_bytes'] = processed_bytes
                    recovery_info['progress_percent'] = (processed_files / recovery_info['total_files']) * 100

                    # 通知进度更新
                    await self._notify_progress(recovery_info)

                except Exception as e:
                    logger.error(f"恢复文件失败 {file_info['file_path']}: {str(e)}")
                    continue

            # 5. 卸载磁带
            await self.tape_manager.unload_tape()

            return processed_files > 0

        except Exception as e:
            logger.error(f"恢复流程执行失败: {str(e)}")
            recovery_info['error_message'] = str(e)
            return False

    async def _get_backup_set_info(self, backup_set_id: str) -> Optional[Dict]:
        """获取备份集信息（从数据库查询真实数据）"""
        try:
            # openGauss 原生SQL查询
            # 使用连接池
            async with get_opengauss_connection() as conn:
                try:
                    row = await conn.fetchrow(
                        """
                        SELECT id, set_id, set_name, backup_group, backup_type, backup_time,
                               total_files, total_bytes, compressed_bytes, tape_id, status
                        FROM backup_sets
                        WHERE set_id = $1
                        """,
                        backup_set_id
                    )
                except Exception as query_error:
                    logger.error(f"[openGauss] 查询备份集信息失败: backup_set_id={backup_set_id}, 错误: {str(query_error)}", exc_info=True)
                    raise

                if not row:
                    logger.debug(f"[openGauss] 备份集不存在: backup_set_id={backup_set_id}")
                    return None

                try:
                    return {
                        'id': row['id'],
                        'set_id': row['set_id'],
                        'set_name': row['set_name'],
                        'backup_group': row['backup_group'],
                        'backup_type': row['backup_type'].value if hasattr(row['backup_type'], 'value') else str(row['backup_type']),
                        'backup_time': row['backup_time'].isoformat() if isinstance(row['backup_time'], datetime) else str(row['backup_time']),
                        'total_files': row['total_files'] or 0,
                        'total_bytes': row['total_bytes'] or 0,
                        'compressed_bytes': row['compressed_bytes'] or 0,
                        'tape_id': row['tape_id'],
                        'status': row['status'].value if hasattr(row['status'], 'value') else str(row['status'])
                    }
                except Exception as parse_error:
                    logger.error(f"[openGauss] 解析备份集信息失败: backup_set_id={backup_set_id}, 错误: {str(parse_error)}", exc_info=True)
                    raise
        except Exception as e:
            logger.error(f"获取备份集信息失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    async def _read_file_from_tape(self, file_info: Dict) -> Optional[bytes]:
        """从磁带读取文件数据"""
        try:
            # 这里应该根据文件信息从磁带读取数据
            # 暂时返回示例数据
            return b"sample file data"
        except Exception as e:
            logger.error(f"从磁带读取文件失败: {str(e)}")
            return None

    async def _decompress_file_data(self, compressed_data: bytes, file_info: Dict) -> bytes:
        """根据文件后缀自动选择解压方式"""
        if not compressed_data:
            return compressed_data

        metadata = file_info.get('file_metadata') or {}
        archive_hint = metadata.get('tape_file_path') or file_info.get('file_path') or file_info.get('file_name') or ''
        archive_hint = archive_hint.lower()
        target_name = metadata.get('original_path') or file_info.get('file_path') or file_info.get('file_name') or ''
        target_name = Path(target_name).name

        try:
            if archive_hint.endswith(('.tar.gz', '.tgz')):
                return self._extract_from_tar_archive(compressed_data, target_name, mode='r:gz')
            if archive_hint.endswith(('.tar.zst', '.tzst')):
                return self._extract_from_tar_zst_archive(compressed_data, target_name)
            if archive_hint.endswith('.tar'):
                return self._extract_from_tar_archive(compressed_data, target_name, mode='r:')
            if archive_hint.endswith('.zip'):
                return self._extract_from_zip_archive(compressed_data, target_name)
            if archive_hint.endswith('.7z'):
                return self._extract_from_7z_archive(compressed_data, target_name)
            if archive_hint.endswith('.gz'):
                # 普通单文件GZip
                return gzip.decompress(compressed_data)
            if archive_hint.endswith('.zst'):
                return self._decompress_zstd_blob(compressed_data)
        except Exception as e:
            logger.error(f"根据后缀解压失败: {str(e)}", exc_info=True)
            return compressed_data

        # 未匹配到已知后缀，直接返回原数据
        return compressed_data

    def _select_archive_entry(self, candidate_names: List[str], target_name: str) -> Optional[str]:
        """优先匹配目标文件名，找不到时返回第一个条目"""
        if not candidate_names:
            return None
        normalized_target = (target_name or '').replace('\\', '/').lower()
        if normalized_target:
            for name in candidate_names:
                normalized = name.replace('\\', '/').lower()
                if normalized == normalized_target or normalized.endswith('/' + normalized_target):
                    return name
        return candidate_names[0]

    def _extract_from_tar_archive(self, compressed_data: bytes, target_name: str, mode: str) -> bytes:
        with tarfile.open(fileobj=io.BytesIO(compressed_data), mode=mode) as tar:
            members = [m for m in tar.getmembers() if m.isfile()]
            if not members:
                return compressed_data
            selected_name = self._select_archive_entry([m.name for m in members], target_name)
            member = next((m for m in members if m.name == selected_name), None)
            if not member:
                return compressed_data
            extracted = tar.extractfile(member)
            if not extracted:
                return compressed_data
            return extracted.read()

    def _extract_from_tar_zst_archive(self, compressed_data: bytes, target_name: str) -> bytes:
        if zstd is None:
            logger.warning("无法解压 .tar.zst 文件，因为未安装 zstandard 库")
            return compressed_data
        try:
            decompressed = self._decompress_zstd_blob(compressed_data)
            return self._extract_from_tar_archive(decompressed, target_name, mode='r:')
        except Exception as e:
            logger.error(f"解压 .tar.zst 文件失败: {e}", exc_info=True)
            return compressed_data

    def _extract_from_zip_archive(self, compressed_data: bytes, target_name: str) -> bytes:
        with zipfile.ZipFile(io.BytesIO(compressed_data)) as zf:
            names = zf.namelist()
            if not names:
                return compressed_data
            selected_name = self._select_archive_entry(names, target_name)
            with zf.open(selected_name) as fh:
                return fh.read()

    def _extract_from_7z_archive(self, compressed_data: bytes, target_name: str) -> bytes:
        with py7zr.SevenZipFile(io.BytesIO(compressed_data)) as archive:
            names = archive.getnames()
            if not names:
                return compressed_data
            selected_name = self._select_archive_entry(names, target_name)
            data_map = archive.read([selected_name])
            stream = data_map.get(selected_name)
            if hasattr(stream, 'read'):
                return stream.read()
            if isinstance(stream, bytes):
                return stream
            return compressed_data

    def _decompress_zstd_blob(self, compressed_data: bytes) -> bytes:
        if zstd is None:
            logger.warning("无法解压 .zst 文件，因为未安装 zstandard 库")
            return compressed_data
        try:
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(compressed_data)
        except Exception as e:
            logger.error(f"Zstandard 解压失败: {e}", exc_info=True)
            return compressed_data

    async def _verify_file_integrity(self, file_path: Path, file_info: Dict) -> bool:
        """验证文件完整性"""
        try:
            if not file_path.exists():
                return False

            # 检查文件大小
            actual_size = file_path.stat().st_size
            expected_size = file_info.get('file_size', 0)
            if actual_size != expected_size:
                logger.warning(f"文件大小不匹配: 期望 {expected_size}, 实际 {actual_size}")
                return False

            # 检查校验和
            if file_info.get('checksum'):
                actual_checksum = self._calculate_file_checksum(file_path)
                if actual_checksum != file_info['checksum']:
                    logger.warning(f"文件校验和不匹配: {file_path}")
                    return False

            return True

        except Exception as e:
            logger.error(f"验证文件完整性失败: {str(e)}")
            return False

    def _calculate_file_checksum(self, file_path: Path) -> str:
        """计算文件校验和"""
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    async def _notify_progress(self, recovery_info: Dict):
        """通知进度更新"""
        try:
            for callback in self._progress_callbacks:
                if asyncio.iscoroutinefunction(callback):
                    await callback(recovery_info)
                else:
                    callback(recovery_info)
        except Exception as e:
            logger.error(f"进度通知失败: {str(e)}")

    def _format_bytes(self, bytes_size: int) -> str:
        """格式化字节大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_size < 1024.0:
                return f"{bytes_size:.2f} {unit}"
            bytes_size /= 1024.0
        return f"{bytes_size:.2f} PB"

    async def get_recovery_status(self, recovery_id: str) -> Optional[Dict]:
        """获取恢复状态"""
        try:
            if self._current_recovery and self._current_recovery['recovery_id'] == recovery_id:
                recovery_info = self._current_recovery.copy()
                # 转换datetime对象为字符串
                for key in ['created_at', 'started_at', 'completed_at']:
                    if recovery_info.get(key):
                        recovery_info[key] = recovery_info[key].isoformat()
                return recovery_info
            return None
        except Exception as e:
            logger.error(f"获取恢复状态失败: {str(e)}")
            return None

    async def cancel_recovery(self, recovery_id: str) -> bool:
        """取消恢复任务"""
        try:
            if self._current_recovery and self._current_recovery['recovery_id'] == recovery_id:
                self._current_recovery['status'] = 'cancelled'
                self._current_recovery = None
                logger.info(f"恢复任务已取消: {recovery_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"取消恢复任务失败: {str(e)}")
            return False

    async def get_backup_groups(self) -> List[str]:
        """获取备份组列表（从数据库查询真实数据）"""
        try:
            groups = []

            # openGauss 原生SQL查询
            # 使用连接池
            async with get_opengauss_connection() as conn:
                try:
                    # 查询所有不重复的备份组，按时间倒序
                    sql = """
                        SELECT DISTINCT backup_group
                        FROM backup_sets
                        WHERE LOWER(status::text) = LOWER('ACTIVE')
                        ORDER BY backup_group DESC
                        LIMIT 12
                    """
                    rows = await conn.fetch(sql)
                    groups = [row['backup_group'] for row in rows]
                except Exception as query_error:
                    logger.error(f"[openGauss] 查询备份组列表失败: 错误: {str(query_error)}", exc_info=True)
                    groups = []

            # 如果没有查询到数据，返回最近6个月的默认组            if not groups:
                from utils.datetime_utils import recent_months
                groups = recent_months(6)

            logger.info(f"查询到 {len(groups)} 个备份组")
            return groups
        except Exception as e:
            logger.error(f"获取备份组列表失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # 返回默认的最近6个月
            from utils.datetime_utils import recent_months
            groups = recent_months(6)
            return groups