#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 任务查询
Backup Management API - Task Query
"""

import logging
import json
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Request
from models.backup import BackupTaskType, BackupTaskStatus
from utils.scheduler.db_utils import is_opengauss, is_redis, get_opengauss_connection, is_sqlite, get_sqlite_connection
from .models import BackupTaskResponse
from .utils import _normalize_status_value, _build_stage_info

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/tasks", response_model=List[BackupTaskResponse])
async def get_backup_tasks(
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务列表（执行记录）
    
    此接口返回所有备份任务的执行记录，包括：
    - 通过计划任务模块创建的备份任务
    - 通过备份管理模块立即执行的备份任务
    """
    try:
        # 在函数开头导入所有需要的函数，避免在条件分支中导入导致作用域问题
        from utils.scheduler.db_utils import is_redis as _is_redis
        
        if is_opengauss():
            def _decode_json_field(value, default=None):
                """openGauss driver可能返回str/memoryview/bytes，统一解码为Python对象"""
                if value is None:
                    return default
                if isinstance(value, memoryview):
                    try:
                        value = value.tobytes()
                    except Exception:
                        return default
                if isinstance(value, (bytes, bytearray)):
                    try:
                        value = value.decode('utf-8')
                    except Exception:
                        return default
                if isinstance(value, str):
                    try:
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return default
                if isinstance(value, (list, dict)):
                    return value
                return default
            # 使用原生SQL查询（使用连接池）
            # 构建WHERE子句
            where_clauses = []
            params = []
            param_index = 1
            
            # 默认返回所有记录（模板+执行记录）；当 status/task_type 为 'all' 或空时不加过滤
            normalized_status = (status or '').lower()
            include_not_run = normalized_status in ('not_run', '未运行')
            if status and normalized_status not in ('all', 'not_run', '未运行'):
                # 以文本方式匹配，确保大小写不敏感
                # 注意：openGauss中status是枚举类型，需要转换为文本进行比较
                # 同时确保传入的status值也转换为小写进行匹配
                # 使用bt.status明确指定backup_tasks表的status字段，避免与scheduled_tasks.status冲突
                status_lower = normalized_status
                where_clauses.append(f"LOWER(bt.status::text) = LOWER(${param_index}::text)")
                params.append(status_lower)  # 使用小写值
                param_index += 1
            # 未运行：仅限从 backup_tasks 侧筛选"未启动"的pending记录
            if include_not_run:
                where_clauses.append("(started_at IS NULL) AND LOWER(bt.status::text)=LOWER('pending')")
            
            normalized_type = (task_type or '').lower()
            if task_type and normalized_type != 'all':
                # 以文本方式匹配，避免依赖枚举类型存在
                # 使用bt.task_type明确指定backup_tasks表的task_type字段
                where_clauses.append(f"LOWER(bt.task_type::text) = LOWER(${param_index})")
                params.append(task_type)
                param_index += 1

            if q and q.strip():
                # 使用bt.task_name明确指定backup_tasks表的task_name字段
                where_clauses.append(f"bt.task_name ILIKE ${param_index}")
                params.append(f"%{q.strip()}%")
                param_index += 1
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # 构建查询（包含模板与执行记录）- 不在SQL层做分页，合并后在内存分页
            # 注释掉运行中/失败任务的模板过滤，允许显示模板任务的运行状态
            # if status and normalized_status in ('running', 'failed'):
            #     where_sql = f"({where_sql}) AND is_template = false"
            
            sql = f"""
                SELECT bt.id, bt.task_name, bt.task_type,
                       CASE
                           WHEN bt.status::text = 'RUNNING' THEN 'running'
                           WHEN bt.status::text = 'PENDING' THEN 'pending'
                           WHEN bt.status::text = 'COMPLETED' THEN 'completed'
                           WHEN bt.status::text = 'FAILED' THEN 'failed'
                           WHEN bt.status::text = 'CANCELLED' THEN 'cancelled'
                           WHEN bt.status::text = 'PAUSED' THEN 'paused'
                           ELSE LOWER(bt.status::text)
                       END as status,
                       bt.progress_percent, bt.total_files,
                       bt.processed_files, bt.total_bytes, bt.processed_bytes, bt.compressed_bytes,
                       bt.created_at, bt.started_at, bt.completed_at, bt.error_message, bt.is_template,
                       bt.tape_device, bt.source_paths, bt.description, bt.result_summary, bt.scan_status, bt.operation_stage,
                       bt.backup_set_id,
                       CASE WHEN st.id IS NOT NULL THEN true ELSE false END as from_scheduler,
                       st.enabled as scheduler_enabled,
                       st.id as scheduler_task_id
                FROM backup_tasks bt
                LEFT JOIN scheduled_tasks st ON st.backup_task_id = bt.id AND st.action_type = 'backup'
                WHERE {where_sql}
                ORDER BY bt.created_at DESC
            """
            
            try:
                async with get_opengauss_connection() as conn:
                    # 添加调试日志
                    logger.debug(f"[任务查询] openGauss查询SQL: {sql}")
                    logger.debug(f"[任务查询] 查询参数: {params}")
                    logger.debug(f"[任务查询] 状态过滤: status={status}, normalized_status={normalized_status}")
                    
                    rows = await conn.fetch(sql, *params)
                    
                    # 确保rows不是None
                    if rows is None:
                        logger.warning("openGauss查询返回了None，返回空列表")
                        return []
                    
                    logger.debug(f"[任务查询] 查询结果数量: {len(rows)}")
                    if rows:
                        logger.debug(f"[任务查询] 第一个任务的状态: {rows[0].get('status')}, is_template: {rows[0].get('is_template')}")
                    
                    tasks = []
                    for row in rows:
                        # 解析JSON字段
                        source_paths = _decode_json_field(row.get("source_paths"), default=[])
                        
                        # 计算压缩率
                        compression_ratio = 0.0
                        total_bytes_actual = 0
                        if row["processed_bytes"] and row["processed_bytes"] > 0 and row["compressed_bytes"]:
                            compression_ratio = float(row["compressed_bytes"]) / float(row["processed_bytes"])
                        
                        # 解析result_summary获取预计的压缩包总数
                        estimated_archive_count = None
                        result_summary_dict = _decode_json_field(row.get("result_summary"), default={})
                        if isinstance(result_summary_dict, dict):
                            estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                            total_bytes_actual = (
                                result_summary_dict.get('total_scanned_bytes')
                                or result_summary_dict.get('total_bytes_actual')
                                or 0
                            )
                        # 规范化状态值：确保是字符串类型
                        raw_status = row["status"]
                        task_id = row["id"]
                        task_name = row.get("task_name", "")
                        
                        # 添加调试日志（对所有任务，特别是最近的任务）
                        task_name_lower = task_name.lower()
                        is_recent_task = (
                            "计划备份-20251123_234825" in task_name or
                            "计划备份-20251123_222248" in task_name or
                            task_id >= 26  # 最近的任务ID
                        )
                        
                        if is_recent_task:
                            logger.debug(f"[任务查询] 任务{task_id} ({task_name}): 原始状态={raw_status}, 类型={type(raw_status)}, repr={repr(raw_status)}, started_at={row.get('started_at')}")
                        
                        status_value = _normalize_status_value(raw_status)
                        
                        # 如果规范化后还不是字符串，强制转换为字符串并转为小写
                        if not isinstance(status_value, str):
                            status_value = str(status_value).lower()
                        else:
                            status_value = status_value.lower()
                        
                        # 添加调试日志（对最近的任务或状态异常的任务）
                        is_running = status_value == 'running'
                        has_started = row.get('started_at') is not None
                        status_mismatch = has_started and status_value == 'pending'
                        
                        if is_recent_task or status_mismatch:
                            logger.debug(
                                f"[任务查询] 任务{task_id} ({task_name}): "
                                f"原始状态={raw_status}, 类型={type(raw_status)}, "
                                f"规范化后={status_value}, is_template={row.get('is_template')}, "
                                f"started_at={row.get('started_at')}, "
                                f"状态异常={status_mismatch}"
                            )
                        
                        # 对于运行中的任务，尝试从内存中的压缩程序/任务状态获取实时进度和统计（全部使用内存数据）
                        current_compression_progress = None
                        in_memory_total_files = None
                        in_memory_total_bytes = None
                        in_memory_processed_files = None
                        in_memory_processed_bytes = None
                        in_memory_compressed_bytes = None
                        in_memory_scan_status = None
                        task_status = None
                        
                        if status_value == 'running':
                            try:
                                from web.api.backup.utils import get_system_instance
                                system = get_system_instance(http_request)
                                logger.debug(f"[任务查询] 获取系统实例成功: {system is not None}, backup_engine: {system.backup_engine is not None if system else False}")
                                if system and system.backup_engine:
                                    # 从BackupEngine的compression_worker获取聚合的压缩进度
                                    backup_engine = system.backup_engine
                                    if hasattr(backup_engine, '_current_compression_worker') and backup_engine._current_compression_worker:
                                        compression_worker = backup_engine._current_compression_worker
                                        if compression_worker.backup_task.id == row["id"]:
                                            # 从compression_worker获取聚合的压缩进度（包含所有并行任务的进度）
                                            aggregated_progress = compression_worker.get_aggregated_compression_progress()
                                            if aggregated_progress:
                                                # 有新的聚合进度，使用它
                                                current_compression_progress = aggregated_progress
                                                logger.debug(f"[任务查询] 任务 {row['id']} 从内存压缩程序获取聚合进度: {aggregated_progress}")
                                            else:
                                                # get_aggregated_compression_progress 返回 None（可能任务刚启动或暂时没有活跃任务）
                                                # 保留上一次的 current_compression_progress，避免交替显示
                                                if not current_compression_progress:
                                                    # 如果还没有 current_compression_progress，尝试从 backup_task 获取
                                                    if hasattr(compression_worker.backup_task, 'current_compression_progress') and compression_worker.backup_task.current_compression_progress:
                                                        current_compression_progress = compression_worker.backup_task.current_compression_progress
                                                        logger.debug(f"[任务查询] 任务 {row['id']} get_aggregated_compression_progress 返回 None，使用 backup_task 中的上一次进度")
                                    # 从get_task_status获取所有内存统计（优先使用内存数据）
                                    task_status = await system.backup_engine.get_task_status(row["id"])
                                    logger.debug(f"[任务查询] 任务 {row['id']} 状态: {task_status}")
                                    if task_status:
                                        # 压缩进度：只有在 current_compression_progress 还没有设置时才使用 task_status 中的值
                                        # 这样可以避免覆盖从 compression_worker 获取的实时数据
                                        if not current_compression_progress and 'current_compression_progress' in task_status:
                                            current_compression_progress = task_status['current_compression_progress']
                                            logger.debug(f"[任务查询] 任务 {row['id']} 从get_task_status获取压缩进度: {current_compression_progress}")
                                        # 所有统计字段（优先使用内存中的值）
                                        if 'total_files' in task_status:
                                            try:
                                                in_memory_total_files = int(task_status.get('total_files') or 0)
                                            except (ValueError, TypeError):
                                                in_memory_total_files = None
                                        if 'total_bytes' in task_status:
                                            try:
                                                in_memory_total_bytes = int(task_status.get('total_bytes') or 0)
                                            except (ValueError, TypeError):
                                                in_memory_total_bytes = None
                                        if 'processed_files' in task_status:
                                            try:
                                                in_memory_processed_files = int(task_status.get('processed_files') or 0)
                                            except (ValueError, TypeError):
                                                in_memory_processed_files = None
                                        if 'processed_bytes' in task_status:
                                            try:
                                                in_memory_processed_bytes = int(task_status.get('processed_bytes') or 0)
                                            except (ValueError, TypeError):
                                                in_memory_processed_bytes = None
                                        if 'compressed_bytes' in task_status:
                                            try:
                                                in_memory_compressed_bytes = int(task_status.get('compressed_bytes') or 0)
                                            except (ValueError, TypeError):
                                                in_memory_compressed_bytes = None
                                        if 'scan_status' in task_status:
                                            in_memory_scan_status = task_status.get('scan_status')
                            except Exception as e:
                                logger.error(f"[任务查询] 获取任务 {row['id']} 压缩进度失败: {str(e)}", exc_info=True)
                        
                        # 构建阶段信息，传入current_compression_progress用于构建operation_status
                        # scan_status 优先使用内存中的值，其次回退到数据库字段
                        scan_status_value = in_memory_scan_status if in_memory_scan_status is not None else row.get("scan_status")

                        # 预分组完成状态由预分组任务在完成时更新description字段来标记，不需要查询数据库
                        stage_info = _build_stage_info(
                            row.get("description"),
                            scan_status_value,
                            status_value,
                            row.get("operation_stage"),  # 优先使用数据库中的 operation_stage 字段
                            current_compression_progress  # 传入从内存获取的压缩进度
                        )
                        
                        # 所有统计字段：优先使用内存中的实时统计，其次回退到数据库字段
                        total_files_value = in_memory_total_files if in_memory_total_files is not None else (row["total_files"] or 0)
                        total_bytes_value = in_memory_total_bytes if in_memory_total_bytes is not None else (row["total_bytes"] or 0)
                        processed_files_value = in_memory_processed_files if in_memory_processed_files is not None else (row["processed_files"] or 0)
                        processed_bytes_value = in_memory_processed_bytes if in_memory_processed_bytes is not None else (row["processed_bytes"] or 0)
                        compressed_bytes_value = in_memory_compressed_bytes if in_memory_compressed_bytes is not None else (row["compressed_bytes"] or 0)

                        # 计算进度百分比（基于内存统计）
                        # 优先使用内存统计计算，如果内存统计不可用，使用数据库中的 progress_percent
                        progress_percent_value = 0.0
                        if total_files_value > 0 and processed_files_value >= 0:
                            # 有总文件数，计算进度百分比
                            progress_percent_value = min(100.0, (processed_files_value / total_files_value) * 100.0)
                        elif row.get("progress_percent") is not None:
                            # 没有总文件数，使用数据库中的进度百分比（包括 0 值）
                            progress_percent_value = float(row["progress_percent"])
                        # 如果 total_files 为 0，说明扫描刚开始，进度为 0% 是正常的，但仍需要返回 0.0 而不是 None
                        
                        tasks.append({
                            "task_id": row["id"],
                            "task_name": row["task_name"],
                            "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                            "status": status_value,
                            "progress_percent": progress_percent_value,  # 基于内存统计计算
                            "total_files": total_files_value,  # 总文件数（优先使用内存中的实时统计）
                            "processed_files": processed_files_value,  # 已处理文件数（优先使用内存中的实时统计）
                            "total_bytes": total_bytes_value,  # 总字节数（优先使用内存中的实时统计）
                            "total_bytes_actual": total_bytes_actual,
                            "processed_bytes": processed_bytes_value,  # 已处理容量（优先使用内存中的实时统计）
                            "compressed_bytes": compressed_bytes_value,  # 压缩后大小（优先使用内存中的实时统计）
                            "compression_ratio": compression_ratio,
                            "estimated_archive_count": estimated_archive_count,  # 压缩包数量（从 result_summary.estimated_archive_count 读取）
                            "created_at": row["created_at"],
                            "started_at": row["started_at"],
                            "completed_at": row["completed_at"],
                            "error_message": row["error_message"],
                            "is_template": row["is_template"] or False,
                            "tape_device": row["tape_device"],
                            "source_paths": source_paths or [],
                            "description": row["description"] or "",
                            "from_scheduler": row.get("from_scheduler", False),  # 从JOIN查询中获取正确的值
                            "enabled": row.get("scheduler_enabled", True),  # 计划任务的启用状态
                            "scheduler_task_id": row.get("scheduler_task_id"),  # scheduled_tasks.id，用于启用/禁用操作
                            "operation_status": stage_info["operation_status"],
                            "operation_stage": stage_info["operation_stage"],
                            "operation_stage_label": stage_info["operation_stage_label"],
                            "stage_steps": stage_info["stage_steps"],
                            "current_compression_progress": current_compression_progress
                        })
                    
                    # 追加计划任务（未运行模板）
                    # 仅当无状态过滤或过滤为pending/all时返回
                    include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
                    if include_sched:
                        sched_where = ["LOWER(action_type::text)=LOWER('BACKUP')"]
                        sched_params = []
                        if q and q.strip():
                            sched_where.append("task_name ILIKE $1")
                            sched_params.append(f"%{q.strip()}%")
                        # 任务类型筛选
                        if task_type and normalized_type != 'all':
                            # 从 action_config->task_type 里匹配（字符串包含）
                            # openGauss json 提取可后续增强，这里简化为 ILIKE 检测
                            if sched_params:
                                sched_where.append("(action_config::text) ILIKE $2")
                                sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                            else:
                                sched_where.append("(action_config::text) ILIKE $1")
                                sched_params.append(f"%\"task_type\": \"{task_type}\"%")
                        # 未运行：计划任务自然视作未运行
                        sched_sql = f"""
                            SELECT id, task_name, status, enabled, created_at, action_config, task_metadata
                            FROM scheduled_tasks
                            WHERE {' AND '.join(sched_where)}
                            ORDER BY created_at DESC
                        """
                        sched_rows = await conn.fetch(sched_sql, *sched_params)
                        template_ids = set()
                        parsed_sched_rows = []
                        for srow in sched_rows:
                            action_cfg = _decode_json_field(srow.get("action_config"), default={})
                            task_metadata = _decode_json_field(srow.get("task_metadata"), default={})
                            backup_template_id = None
                            if isinstance(task_metadata, dict):
                                backup_template_id = task_metadata.get("backup_task_id")
                                if backup_template_id:
                                    template_ids.add(int(backup_template_id))
                            parsed_sched_rows.append((srow, action_cfg, task_metadata, backup_template_id))

                        template_info_map = {}
                        if template_ids:
                            template_rows = await conn.fetch(
                                """
                                SELECT id, source_paths, tape_device
                                FROM backup_tasks
                                WHERE id = ANY($1::int[])
                                """,
                                list(template_ids)
                            )
                            for trow in template_rows:
                                t_source_paths = _decode_json_field(trow.get('source_paths'), default=[])
                                template_info_map[trow['id']] = {
                                    "source_paths": t_source_paths or [],
                                    "tape_device": trow.get('tape_device')
                                }
                        for srow, acfg, metadata, template_id in parsed_sched_rows:
                            # 从action_config中提取task_type/tape_device/source_paths
                            atype = 'full'
                            tdev = None
                            spaths: Optional[List[str]] = None
                            try:
                                if isinstance(acfg, dict):
                                    atype = acfg.get('task_type') or atype
                                    tdev = acfg.get('tape_device')
                                    cfg_paths = acfg.get('source_paths')
                                    if isinstance(cfg_paths, list):
                                        spaths = [str(p) for p in cfg_paths if p]
                                    elif isinstance(cfg_paths, str) and cfg_paths.strip():
                                        spaths = [cfg_paths.strip()]
                            except Exception as parse_error:
                                logger.debug(f"解析计划任务 action_config 失败: {parse_error}")
                            template_fallback = template_info_map.get(int(template_id)) if template_id else None
                            if (not spaths) and template_fallback:
                                spaths = template_fallback.get("source_paths") or []
                            if (not tdev) and template_fallback:
                                tdev = template_fallback.get("tape_device")
                            stage_info = _build_stage_info("", None, "pending")
                            tasks.append({
                                "task_id": srow["id"],
                                "task_name": srow["task_name"],
                                "task_type": atype,
                                "status": "pending",  # 计划任务视为未运行
                                "progress_percent": 0.0,
                                "total_files": 0,
                                "processed_files": 0,
                                "total_bytes": 0,
                                "total_bytes_actual": 0,
                                "processed_bytes": 0,
                                "compressed_bytes": 0,
                                "compression_ratio": 0.0,
                                "created_at": srow["created_at"],
                                "started_at": None,
                                "completed_at": None,
                                "error_message": None,
                                "is_template": True,
                                "tape_device": tdev,
                                "source_paths": spaths or [],
                                "from_scheduler": True,
                                "enabled": srow.get("enabled", True),
                                "scheduler_task_id": srow.get("id"),  # scheduled_tasks.id，用于启用/禁用操作
                                "description": "",
                                "estimated_archive_count": None,
                                "operation_status": stage_info["operation_status"],
                                "operation_stage": stage_info["operation_stage"],
                                "operation_stage_label": stage_info["operation_stage_label"],
                                "stage_steps": stage_info["stage_steps"]
                            })
                    
                    # 合并后排序与分页（统一为时间戳，避免aware/naive比较异常）
                    def _ts(val):
                        try:
                            if not val:
                                return 0.0
                            if isinstance(val, (int, float)):
                                return float(val)
                            # datetime
                            return val.timestamp()
                        except Exception:
                            return 0.0

                    # 修复任务显示优先级：执行记录优先于模板任务
                    # 模板任务 (is_template=True) 应该排在执行记录后面
                    def _sort_key(x):
                        created_ts = _ts(x.get('created_at'))
                        # 模板任务排在后面：添加一个很大的惩罚值
                        is_template_penalty = 1000000000 if x.get('is_template') else 0
                        # 计划任务 (from_scheduler=True) 也稍作调整，让执行记录优先
                        scheduler_penalty = 100000 if x.get('from_scheduler') and not x.get('is_template') else 0
                        return created_ts - is_template_penalty - scheduler_penalty

                    tasks.sort(key=_sort_key, reverse=True)
                    # 确保tasks是列表，避免返回None
                    if tasks is None:
                        logger.warning("openGauss路径中tasks为None，返回空列表")
                        return []
                    return tasks[offset:offset+limit]
            except Exception as e:
                error_msg = str(e)
                # 如果表不存在，返回空列表
                if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(e).__name__):
                    logger.warning(
                        f"backup_tasks 表不存在，返回空列表（可能是数据库未初始化）: {error_msg}"
                    )
                    return []
                # 其他错误记录并返回空列表
                logger.error(f"查询备份任务列表失败: {error_msg}", exc_info=True)
                return []
        else:
            # 检查是否为Redis数据库
            if _is_redis():
                # Redis版本
                from backup.redis_backup_db import get_backup_tasks_redis
                result = await get_backup_tasks_redis(status, task_type, q, limit, offset)
                # 确保返回的是列表，避免返回None
                if result is None:
                    logger.warning("get_backup_tasks_redis()返回了None，返回空列表")
                    return []
                return result
            
            # 检查是否为SQLite（确保只对SQLite使用SQLite连接）
            if not _is_sqlite():
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.warning(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务列表，返回空列表")
                return []
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                # 构建WHERE子句
                where_clauses = ["is_template = 0"]
                params = []
                
                # 状态过滤
                normalized_status = (status or '').lower()
                include_not_run = normalized_status in ('not_run', '未运行')
                if status and normalized_status not in ('all', 'not_run', '未运行'):
                    # 使用LOWER进行大小写不敏感匹配
                    where_clauses.append("LOWER(status) = LOWER(?)")
                    params.append(status)
                # 未运行：仅限未启动的pending记录
                if include_not_run:
                    where_clauses.append("(started_at IS NULL) AND LOWER(status) = LOWER('pending')")
                
                if task_type and task_type.lower() != 'all':
                    where_clauses.append("LOWER(task_type) = LOWER(?)")
                    params.append(task_type)
                
                if q and q.strip():
                    where_clauses.append("task_name LIKE ?")
                    params.append(f"%{q.strip()}%")
                
                where_sql = " AND ".join(where_clauses)
                
                # 构建查询
                sql = f"""
                    SELECT bt.id, bt.task_name, bt.task_type, bt.status, bt.progress_percent, bt.total_files,
                           bt.processed_files, bt.total_bytes, bt.processed_bytes, bt.compressed_bytes,
                           bt.created_at, bt.started_at, bt.completed_at, bt.error_message, bt.is_template,
                           bt.tape_device, bt.source_paths, bt.description, bt.result_summary, bt.scan_status, bt.operation_stage,
                           bt.backup_set_id,
                           CASE WHEN st.id IS NOT NULL THEN 1 ELSE 0 END as from_scheduler,
                           st.enabled as scheduler_enabled,
                           st.id as scheduler_task_id
                    FROM backup_tasks bt
                    LEFT JOIN scheduled_tasks st ON st.backup_task_id = bt.id AND st.action_type = 'backup'
                    WHERE {where_sql}
                    ORDER BY bt.created_at DESC
                """
                cursor = await conn.execute(sql, params)
                rows = await cursor.fetchall()
                
                # 转换为响应格式
                tasks = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = []
                    try:
                        if row[16]:  # source_paths
                            if isinstance(row[16], str):
                                source_paths = json.loads(row[16])
                            elif isinstance(row[16], (list, dict)):
                                source_paths = row[16]
                    except Exception:
                        source_paths = []
                    
                    # 计算压缩率
                    compression_ratio = 0.0
                    total_bytes_actual = 0
                    processed_bytes = row[8] or 0  # processed_bytes
                    compressed_bytes = row[9] or 0  # compressed_bytes
                    if processed_bytes > 0 and compressed_bytes:
                        compression_ratio = float(compressed_bytes) / float(processed_bytes)
                    
                    # 解析result_summary获取预计的压缩包总数
                    estimated_archive_count = None
                    result_summary_dict = {}
                    try:
                        if row[18]:  # result_summary
                            if isinstance(row[18], str):
                                result_summary_dict = json.loads(row[18])
                            elif isinstance(row[18], dict):
                                result_summary_dict = row[18]
                        if isinstance(result_summary_dict, dict):
                            estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                            total_bytes_actual = (
                                result_summary_dict.get('total_scanned_bytes')
                                or result_summary_dict.get('total_bytes_actual')
                                or 0
                            )
                    except Exception:
                        pass
                    
                    # 处理状态值（将小写转换为枚举值）
                    status_raw = row[3]  # status
                    status_value = ""
                    if status_raw:
                        # 如果是字符串，尝试转换为枚举值
                        if isinstance(status_raw, str):
                            # 尝试直接匹配枚举值（不区分大小写）
                            status_lower = status_raw.lower()
                            try:
                                # 枚举值本身就是小写，所以可以直接匹配
                                status_enum = BackupTaskStatus(status_lower)
                                status_value = status_enum.value
                            except (ValueError, AttributeError):
                                # 如果转换失败，尝试匹配枚举名称（大写）
                                try:
                                    # 尝试将小写转换为大写枚举名称
                                    status_upper = status_raw.upper()
                                    status_enum = BackupTaskStatus[status_upper]
                                    status_value = status_enum.value
                                except (KeyError, ValueError, AttributeError):
                                    # 如果都失败，保持原值
                                    status_value = status_raw
                        else:
                            # 如果不是字符串，使用 _normalize_status_value
                            status_value = _normalize_status_value(status_raw)
                    
                    # 构建阶段信息
                    # 预分组完成状态由预分组任务在完成时更新description字段来标记，不需要查询数据库
                    scan_status = row[19]  # scan_status
                    stage_info = _build_stage_info(
                        row[17] or "",  # description
                        scan_status,  # scan_status
                        status_value,
                        row[20] if len(row) > 20 else None,  # operation_stage
                        None  # current_compression_progress
                    )
                    
                    # 处理task_type
                    task_type_value = row[2]  # task_type
                    if isinstance(task_type_value, str) and task_type_value.islower():
                        try:
                            task_type_enum = BackupTaskType(task_type_value)
                            task_type_value = task_type_enum.value
                        except (ValueError, AttributeError):
                            pass
                    
                    tasks.append({
                        "task_id": row[0],  # id
                        "task_name": row[1],  # task_name
                        "task_type": task_type_value,
                        "status": status_value,
                        "progress_percent": float(row[4]) if row[4] else 0.0,  # progress_percent
                        "total_files": row[5] or 0,  # total_files
                        "processed_files": row[6] or 0,  # processed_files
                        "total_bytes": row[7] or 0,  # total_bytes
                        "total_bytes_actual": total_bytes_actual,
                        "processed_bytes": processed_bytes,
                        "compressed_bytes": compressed_bytes,
                        "compression_ratio": compression_ratio,
                        "estimated_archive_count": estimated_archive_count,
                        "created_at": row[10],  # created_at
                        "started_at": row[11],  # started_at
                        "completed_at": row[12],  # completed_at
                        "error_message": row[13],  # error_message
                        "is_template": bool(row[14]) if row[14] is not None else False,  # is_template
                        "tape_device": row[15],  # tape_device
                        "source_paths": source_paths or [],
                        "description": row[17] or "",  # description
                        "from_scheduler": bool(row[20]) if len(row) > 20 and row[20] is not None else False,  # from_scheduler
                        "enabled": row[21] if len(row) > 21 and row[21] is not None else True,  # scheduler_enabled
                        "scheduler_task_id": row[22] if len(row) > 22 and row[22] is not None else None,  # scheduler_task_id
                        "operation_status": stage_info["operation_status"],
                        "operation_stage": stage_info["operation_stage"],
                        "operation_stage_label": stage_info["operation_stage_label"],
                        "stage_steps": stage_info["stage_steps"]
                    })
                
                # 追加计划任务（未运行模板）
                # 仅当无状态过滤或过滤为pending/all时返回
                normalized_status = (status or '').lower()
                include_sched = (not status) or (normalized_status in ("all", "pending", 'not_run', '未运行'))
                if include_sched:
                    sched_where = ["action_type = 'BACKUP'"]
                    sched_params = []
                    if q and q.strip():
                        sched_where.append("task_name LIKE ?")
                        sched_params.append(f"%{q.strip()}%")
                    
                    sched_sql = f"""
                        SELECT id, task_name, status, enabled, created_at, action_config, task_metadata
                        FROM scheduled_tasks
                        WHERE {' AND '.join(sched_where)}
                        ORDER BY created_at DESC
                    """
                    sched_cursor = await conn.execute(sched_sql, sched_params)
                    sched_rows = await sched_cursor.fetchall()
                    
                    template_ids = set()
                    parsed_sched_rows = []
                    for srow in sched_rows:
                        task_metadata = {}
                        try:
                            if srow[6]:  # task_metadata
                                if isinstance(srow[6], str):
                                    task_metadata = json.loads(srow[6])
                                elif isinstance(srow[6], dict):
                                    task_metadata = srow[6]
                        except Exception:
                            pass
                        backup_template_id = task_metadata.get("backup_task_id") if isinstance(task_metadata, dict) else None
                        if backup_template_id:
                            template_ids.add(int(backup_template_id))
                        parsed_sched_rows.append((srow, task_metadata, backup_template_id))
                    
                    template_info_map = {}
                    if template_ids:
                        placeholders = ','.join('?' * len(template_ids))
                        template_sql = f"""
                            SELECT id, source_paths, tape_device
                            FROM backup_tasks
                            WHERE id IN ({placeholders})
                        """
                        template_cursor = await conn.execute(template_sql, list(template_ids))
                        template_rows = await template_cursor.fetchall()
                        for trow in template_rows:
                            t_source_paths = []
                            try:
                                if trow[1]:  # source_paths
                                    if isinstance(trow[1], str):
                                        t_source_paths = json.loads(trow[1])
                                    elif isinstance(trow[1], (list, dict)):
                                        t_source_paths = trow[1]
                            except Exception:
                                pass
                            template_info_map[trow[0]] = {
                                "source_paths": t_source_paths or [],
                                "tape_device": trow[2]
                            }
                    
                    for srow, metadata, template_id in parsed_sched_rows:
                        action_cfg = {}
                        try:
                            if srow[5]:  # action_config
                                if isinstance(srow[5], str):
                                    action_cfg = json.loads(srow[5])
                                elif isinstance(srow[5], dict):
                                    action_cfg = srow[5]
                        except Exception:
                            pass
                        
                        # 从action_config中提取task_type/tape_device/source_paths
                        atype = 'full'
                        tdev = None
                        spaths: Optional[List[str]] = None
                        try:
                            if isinstance(action_cfg, dict):
                                atype = action_cfg.get('task_type') or atype
                                tdev = action_cfg.get('tape_device')
                                cfg_paths = action_cfg.get('source_paths')
                                if isinstance(cfg_paths, list):
                                    spaths = [str(p) for p in cfg_paths if p]
                                elif isinstance(cfg_paths, str) and cfg_paths.strip():
                                    spaths = [cfg_paths.strip()]
                        except Exception as parse_error:
                            logger.debug(f"解析计划任务 action_config 失败: {parse_error}")
                        
                        template_fallback = template_info_map.get(int(template_id)) if template_id else None
                        if (not spaths) and template_fallback:
                            spaths = template_fallback.get("source_paths") or []
                        if (not tdev) and template_fallback:
                            tdev = template_fallback.get("tape_device")
                        
                        stage_info = _build_stage_info("", None, "pending")
                        tasks.append({
                            "task_id": srow[0],  # id
                            "task_name": srow[1],  # task_name
                            "task_type": atype,
                            "status": "pending",  # 计划任务视为未运行
                            "progress_percent": 0.0,
                            "total_files": 0,
                            "processed_files": 0,
                            "total_bytes": 0,
                            "total_bytes_actual": 0,
                            "processed_bytes": 0,
                            "compressed_bytes": 0,
                            "compression_ratio": 0.0,
                            "created_at": srow[4],  # created_at
                            "started_at": None,
                            "completed_at": None,
                            "error_message": None,
                            "is_template": True,
                            "tape_device": tdev,
                            "source_paths": spaths or [],
                            "from_scheduler": True,
                            "enabled": bool(srow[3]) if srow[3] is not None else True,  # enabled
                            "description": "",
                            "estimated_archive_count": None,
                            "operation_status": stage_info["operation_status"],
                            "operation_stage": stage_info["operation_stage"],
                            "operation_stage_label": stage_info["operation_stage_label"],
                            "stage_steps": stage_info["stage_steps"]
                        })
                
                # 合并后排序与分页（统一为时间戳，避免aware/naive比较异常）
                def _ts(val):
                    try:
                        if not val:
                            return 0.0
                        if isinstance(val, (int, float)):
                            return float(val)
                        # datetime
                        return val.timestamp()
                    except Exception:
                        return 0.0

                # 修复任务显示优先级：执行记录优先于模板任务
                # 模板任务 (is_template=True) 应该排在执行记录后面
                def _sort_key(x):
                    created_ts = _ts(x.get('created_at'))
                    # 模板任务排在后面：添加一个很大的惩罚值
                    is_template_penalty = 1000000000 if x.get('is_template') else 0
                    # 计划任务 (from_scheduler=True) 也稍作调整，让执行记录优先
                    scheduler_penalty = 100000 if x.get('from_scheduler') and not x.get('is_template') else 0
                    return created_ts - is_template_penalty - scheduler_penalty

                tasks.sort(key=_sort_key, reverse=True)
                # 确保tasks是列表，避免返回None
                if tasks is None:
                    logger.warning("SQLite路径中tasks为None，返回空列表")
                    return []
                return tasks[offset:offset+limit]

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int, http_request: Request):
    """获取备份任务详情"""
    try:
        if is_opengauss():
            # 使用原生SQL查询（使用连接池）
            async with get_opengauss_connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, task_name, task_type, status, progress_percent, total_files, 
                           processed_files, total_bytes, processed_bytes, compressed_bytes,
                           created_at, started_at, completed_at, error_message, is_template,
                           tape_device, source_paths, description, result_summary, scan_status, operation_stage, enabled
                    FROM backup_tasks
                    WHERE id = $1
                    """,
                    task_id
                )
                
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                # 解析JSON字段
                import json
                source_paths = None
                if row["source_paths"]:
                    try:
                        if isinstance(row["source_paths"], str):
                            source_paths = json.loads(row["source_paths"])
                        else:
                            source_paths = row["source_paths"]
                    except:
                        source_paths = None
                
                total_bytes_actual = 0
                estimated_archive_count = None
                try:
                    result_summary = row.get("result_summary")
                    result_summary_dict = None
                    if result_summary:
                        if isinstance(result_summary, str):
                            result_summary_dict = json.loads(result_summary)
                        elif isinstance(result_summary, dict):
                            result_summary_dict = result_summary
                    if isinstance(result_summary_dict, dict):
                        total_bytes_actual = result_summary_dict.get('total_scanned_bytes') or 0
                        estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                except Exception:
                    total_bytes_actual = 0
                compression_ratio = 0.0
                compressed_bytes = row.get("compressed_bytes") or 0
                try:
                    if row["processed_bytes"] and row["processed_bytes"] > 0 and compressed_bytes:
                        compression_ratio = float(compressed_bytes) / float(row["processed_bytes"])
                except Exception:
                    compression_ratio = 0.0
                
                status_value = _normalize_status_value(row["status"])
                
                # 构建阶段信息
                # 预分组完成状态由预分组任务在完成时更新description字段来标记，不需要查询数据库
                scan_status = row.get("scan_status")
                stage_info = _build_stage_info(
                    row.get("description"),
                    scan_status,
                    status_value,
                    row.get("operation_stage"),  # 优先使用数据库中的 operation_stage 字段
                    None  # current_compression_progress
                )
                # 对于运行中的任务，尝试获取 current_compression_progress
                current_compression_progress = None
                if status_value and status_value.lower() == 'running':
                    try:
                        from web.api.backup.utils import get_system_instance
                        system = get_system_instance(http_request)
                        if system and system.backup_engine:
                            task_status = await system.backup_engine.get_task_status(row["id"])
                            if task_status and 'current_compression_progress' in task_status:
                                current_compression_progress = task_status['current_compression_progress']
                    except Exception as e:
                        logger.debug(f"获取任务压缩进度失败: {str(e)}")
                
                return {
                    "task_id": row["id"],
                    "task_name": row["task_name"],
                    "task_type": row["task_type"].value if hasattr(row["task_type"], "value") else str(row["task_type"]),
                    "status": status_value,
                    "progress_percent": float(row["progress_percent"]) if row["progress_percent"] else 0.0,
                    "total_files": row["total_files"] or 0,
                    "processed_files": row["processed_files"] or 0,
                    "total_bytes": row["total_bytes"] or 0,
                    "total_bytes_actual": total_bytes_actual,
                    "processed_bytes": row["processed_bytes"] or 0,
                    "compressed_bytes": compressed_bytes,
                    "compression_ratio": compression_ratio,
                    "estimated_archive_count": estimated_archive_count,
                    "created_at": row["created_at"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "error_message": row["error_message"],
                    "description": row["description"] or "",
                    "is_template": row["is_template"] or False,
                    "tape_device": row["tape_device"],
                    "source_paths": source_paths or [],
                    "enabled": row.get("enabled", True),
                    "from_scheduler": False,
                    "operation_status": stage_info["operation_status"],
                    "operation_stage": stage_info["operation_stage"],
                    "operation_stage_label": stage_info["operation_stage_label"],
                    "stage_steps": stage_info["stage_steps"],
                    "current_compression_progress": current_compression_progress
                }
        else:
            # 检查是否为Redis数据库
            from utils.scheduler.db_utils import is_redis
            
            if is_redis():
                logger.warning("[Redis模式] 查询备份任务详情暂未实现，抛出HTTPException")
                raise HTTPException(status_code=404, detail="Redis模式下查询备份任务详情暂未实现，请使用Redis相关API")
            
            if not is_sqlite():
                from utils.scheduler.db_utils import is_opengauss
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.warning(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务详情，抛出HTTPException")
                raise HTTPException(status_code=400, detail=f"{db_type}模式下不支持使用SQLite连接查询备份任务详情")
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT id, task_name, task_type, status, progress_percent, total_files,
                           processed_files, total_bytes, processed_bytes, compressed_bytes,
                           created_at, started_at, completed_at, error_message, is_template,
                           tape_device, source_paths, description, result_summary, scan_status, operation_stage, backup_set_id
                    FROM backup_tasks
                    WHERE id = ?
                """, (task_id,))
                row = await cursor.fetchone()
                
                if not row:
                    raise HTTPException(status_code=404, detail="备份任务不存在")
                
                # 解析JSON字段
                source_paths = []
                try:
                    if row[16]:  # source_paths
                        if isinstance(row[16], str):
                            source_paths = json.loads(row[16])
                        elif isinstance(row[16], (list, dict)):
                            source_paths = row[16]
                except Exception:
                    source_paths = []
                
                # 解析result_summary
                total_bytes_actual = 0
                estimated_archive_count = None
                try:
                    if row[18]:  # result_summary
                        result_summary_dict = {}
                        if isinstance(row[18], str):
                            result_summary_dict = json.loads(row[18])
                        elif isinstance(row[18], dict):
                            result_summary_dict = row[18]
                        if isinstance(result_summary_dict, dict):
                            total_bytes_actual = result_summary_dict.get('total_scanned_bytes') or 0
                            estimated_archive_count = result_summary_dict.get('estimated_archive_count')
                except Exception:
                    pass
                
                # 计算压缩率
                compression_ratio = 0.0
                processed_bytes = row[8] or 0  # processed_bytes (索引8)
                compressed_bytes = row[9] or 0  # compressed_bytes (索引9)
                if processed_bytes > 0 and compressed_bytes:
                    compression_ratio = float(compressed_bytes) / float(processed_bytes)
                
                # 处理状态值
                status_raw = row[3]  # status
                status_value = ""
                if status_raw:
                    if isinstance(status_raw, str):
                        status_lower = status_raw.lower()
                        try:
                            status_enum = BackupTaskStatus(status_lower)
                            status_value = status_enum.value
                        except (ValueError, AttributeError):
                            try:
                                status_enum = BackupTaskStatus[status_raw.upper()]
                                status_value = status_enum.value
                            except (KeyError, ValueError, AttributeError):
                                status_value = status_raw
                    else:
                        status_value = _normalize_status_value(status_raw)
                
                # 处理task_type
                task_type_value = row[2]  # task_type
                if isinstance(task_type_value, str) and task_type_value.islower():
                    try:
                        task_type_enum = BackupTaskType(task_type_value)
                        task_type_value = task_type_enum.value
                    except (ValueError, AttributeError):
                        pass
                
                # 构建阶段信息
                # 预分组完成状态由预分组任务在完成时更新description字段来标记，不需要查询数据库
                scan_status = row[19]  # scan_status
                stage_info = _build_stage_info(
                    row[17] or "",  # description
                    scan_status,  # scan_status
                    status_value,
                    row[20] if len(row) > 20 else None,  # operation_stage
                    None  # current_compression_progress
                )
                
                return {
                    "task_id": row[0],  # id
                    "task_name": row[1],  # task_name
                    "task_type": task_type_value,
                    "status": status_value,
                    "progress_percent": float(row[4]) if row[4] else 0.0,  # progress_percent
                    "total_files": row[5] or 0,  # total_files
                    "processed_files": row[6] or 0,  # processed_files
                    "total_bytes": row[7] or 0,  # total_bytes
                    "total_bytes_actual": total_bytes_actual,
                    "processed_bytes": processed_bytes,
                    "compressed_bytes": compressed_bytes,
                    "compression_ratio": compression_ratio,
                    "estimated_archive_count": estimated_archive_count,
                    "created_at": row[10],  # created_at
                    "started_at": row[11],  # started_at
                    "completed_at": row[12],  # completed_at
                    "error_message": row[13],  # error_message
                    "description": row[17] or "",  # description
                    "is_template": bool(row[14]) if row[14] is not None else False,  # is_template
                    "tape_device": row[15],  # tape_device
                    "source_paths": source_paths or [],
                    "operation_status": stage_info["operation_status"],
                    "operation_stage": stage_info["operation_stage"],
                    "operation_stage_label": stage_info["operation_stage_label"],
                    "stage_steps": stage_info["stage_steps"],
                    "enabled": True,  # SQLite中没有enabled字段，默认为True
                    "from_scheduler": False
                }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取备份任务详情失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=List[Dict[str, Any]])
async def get_backup_templates(
    limit: int = 50,
    offset: int = 0,
    http_request: Request = None
):
    """获取备份任务模板列表（配置）
    
    返回所有备份任务配置模板，供计划任务模块选择。
    """
    try:
        if is_opengauss():
            # 使用原生SQL查询（openGauss），严禁SQLAlchemy解析openGauss
            async with get_opengauss_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, task_name, task_type, description, source_paths, tape_device,
                           compression_enabled, encryption_enabled, retention_days, exclude_patterns, created_at
                    FROM backup_tasks
                    WHERE is_template = TRUE
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                    """,
                    limit, offset
                )
                
                # 确保rows不是None
                if rows is None:
                    logger.warning("openGauss查询模板返回了None，返回空列表")
                    return []
                
                template_list = []
                for row in rows:
                    # 解析 source_paths 和 exclude_patterns（JSON格式）
                    source_paths = row['source_paths'] if row['source_paths'] else []
                    exclude_patterns = row['exclude_patterns'] if row['exclude_patterns'] else []
                    
                    # 如果 source_paths 是字符串，尝试解析为 JSON
                    if isinstance(source_paths, str):
                        try:
                            import json
                            source_paths = json.loads(source_paths)
                        except:
                            source_paths = []
                    
                    # 如果 exclude_patterns 是字符串，尝试解析为 JSON
                    if isinstance(exclude_patterns, str):
                        try:
                            import json
                            exclude_patterns = json.loads(exclude_patterns)
                        except:
                            exclude_patterns = []
                    
                    template_list.append({
                        "task_id": row['id'],
                        "task_name": row['task_name'],
                        "task_type": row['task_type'].value if hasattr(row['task_type'], 'value') else str(row['task_type']),
                        "description": row['description'],
                        "source_paths": source_paths,
                        "tape_device": row['tape_device'],
                        "compression_enabled": row['compression_enabled'],
                        "encryption_enabled": row['encryption_enabled'],
                        "retention_days": row['retention_days'],
                        "exclude_patterns": exclude_patterns,
                        "created_at": row['created_at']
                    })
                
                return template_list
        else:
            # 检查是否为Redis数据库
            from utils.scheduler.db_utils import is_redis
            
            if is_redis():
                # Redis模式下返回空列表（暂未实现Redis查询备份任务模板）
                logger.debug("[Redis模式] 查询备份任务模板暂未实现，返回空列表")
                return []
            
            if not is_sqlite():
                from utils.scheduler.db_utils import is_opengauss
                db_type = "openGauss" if is_opengauss() else "未知类型"
                logger.debug(f"[{db_type}模式] 当前数据库类型不支持使用SQLite连接查询备份任务模板，返回空列表")
                return []
            
            # 使用原生SQL查询（SQLite）
            async with get_sqlite_connection() as conn:
                cursor = await conn.execute("""
                    SELECT id, task_name, task_type, description, source_paths, tape_device,
                           compression_enabled, encryption_enabled, retention_days, exclude_patterns, created_at
                    FROM backup_tasks
                    WHERE is_template = 1
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """, (limit, offset))
                rows = await cursor.fetchall()
                
                template_list = []
                for row in rows:
                    # 解析JSON字段
                    source_paths = []
                    exclude_patterns = []
                    try:
                        if row[4]:  # source_paths
                            if isinstance(row[4], str):
                                source_paths = json.loads(row[4])
                            elif isinstance(row[4], (list, dict)):
                                source_paths = row[4]
                    except Exception:
                        pass
                    try:
                        if row[9]:  # exclude_patterns
                            if isinstance(row[9], str):
                                exclude_patterns = json.loads(row[9])
                            elif isinstance(row[9], (list, dict)):
                                exclude_patterns = row[9]
                    except Exception:
                        pass
                    
                    # 处理task_type
                    task_type_value = row[2]  # task_type
                    if isinstance(task_type_value, str) and task_type_value.islower():
                        try:
                            task_type_enum = BackupTaskType(task_type_value)
                            task_type_value = task_type_enum.value
                        except (ValueError, AttributeError):
                            pass
                    
                    template_list.append({
                        "task_id": row[0],  # id
                        "task_name": row[1],  # task_name
                        "task_type": task_type_value,
                        "description": row[3] or "",  # description
                        "source_paths": source_paths or [],
                        "tape_device": row[5],  # tape_device
                        "compression_enabled": bool(row[6]) if row[6] is not None else False,  # compression_enabled
                        "encryption_enabled": bool(row[7]) if row[7] is not None else False,  # encryption_enabled
                        "retention_days": row[8],  # retention_days
                        "exclude_patterns": exclude_patterns or [],
                        "created_at": row[10]  # created_at
                    })
                
                return template_list

    except Exception as e:
        logger.error(f"获取备份任务模板列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

