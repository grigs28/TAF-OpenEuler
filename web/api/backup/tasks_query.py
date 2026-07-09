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
from utils.scheduler.db_utils import get_opengauss_connection
from .models import BackupTaskResponse  # noqa: F401 - used by /tasks/{task_id}
from .utils import _normalize_status_value, _build_stage_info

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/tasks")
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
                   bs.set_id as backup_set_str_id,
                   CASE WHEN st.id IS NOT NULL THEN true ELSE false END as from_scheduler,
                   st.enabled as scheduler_enabled,
                   st.id as scheduler_task_id
            FROM backup_tasks bt
            LEFT JOIN backup_sets bs ON bt.backup_set_id = bs.id
            LEFT JOIN scheduled_tasks st ON st.action_type = 'backup' AND (
                st.backup_task_id = bt.id
                OR st.backup_task_id = bt.template_id
                OR (st.backup_task_id IS NULL AND bt.template_id IS NOT NULL
                    AND (st.task_metadata->>'backup_task_id')::int = bt.template_id)
            )
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
                    in_memory_compression_completed = None
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

                                        # 获取预取器循环次数和运行状态（用于预分组徽章判断）
                                        in_memory_prefetch_loop_count = 0
                                        in_memory_prefetcher_running = True
                                        if hasattr(compression_worker, 'file_group_prefetcher') and compression_worker.file_group_prefetcher:
                                            fp = compression_worker.file_group_prefetcher
                                            in_memory_prefetch_loop_count = getattr(fp, 'prefetch_loop_count', 0) or 0
                                            in_memory_prefetcher_running = getattr(fp, '_running', True)

                                        # 获取 compression_completed 状态
                                        if in_memory_compression_completed is None:
                                            in_memory_compression_completed = getattr(compression_worker.backup_task, 'compression_completed', None)

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
                                    if 'compression_completed' in task_status:
                                        in_memory_compression_completed = task_status.get('compression_completed')
                        except Exception as e:
                            logger.error(f"[任务查询] 获取任务 {row['id']} 压缩进度失败: {str(e)}", exc_info=True)

                    # 构建阶段信息，传入current_compression_progress用于构建operation_status
                    # scan_status 优先使用内存中的值，其次回退到数据库字段
                    scan_status_value = in_memory_scan_status if in_memory_scan_status is not None else row.get("scan_status")

                    # 检查 final 目录是否有待写入磁带的文件（用于 copy 步骤状态判断）
                    final_dir_has_files = None
                    if status_value == 'running':
                        try:
                            if system and system.backup_engine:
                                if hasattr(system.backup_engine, 'final_dir_monitor') and system.backup_engine.final_dir_monitor:
                                    _set_id = row.get("backup_set_str_id")
                                    # 数据库中 backup_set_str_id 为空时，从内存中的 compression_worker 回退获取
                                    if not _set_id:
                                        _cw = getattr(system.backup_engine, '_current_compression_worker', None)
                                        if _cw and hasattr(_cw, 'backup_set') and _cw.backup_set:
                                            _set_id = getattr(_cw.backup_set, 'set_id', None)
                                    if _set_id:
                                        final_dir_has_files = not system.backup_engine.final_dir_monitor.is_final_dir_empty(_set_id)
                        except Exception:
                            pass

                    # 预分组完成：预取器执行过（loop_count > 0）且已停止（_running == False）
                    prefetch_done = False
                    try:
                        prefetch_done = in_memory_prefetch_loop_count > 0 and not in_memory_prefetcher_running
                    except NameError:
                        pass

                    # 预分组完成状态由预分组任务在完成时更新description字段来标记，不需要查询数据库
                    stage_info = _build_stage_info(
                        row.get("description"),
                        scan_status_value,
                        status_value,
                        row.get("operation_stage"),  # 优先使用数据库中的 operation_stage 字段
                        current_compression_progress,  # 传入从内存获取的压缩进度
                        final_dir_has_files,  # 传入 final 目录状态
                        prefetch_done,  # 传入预取器是否完成
                        in_memory_compression_completed,  # 传入压缩是否完成
                        task_type=row.get("task_type")  # 传入任务类型，验证任务使用两阶段流
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
                    sched_where = ["LOWER(action_type::text) IN (LOWER('BACKUP'), LOWER('VERIFY'))"]
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
                        # 检查是否为验证类型计划任务
                        raw_action_type = srow.get('action_type', '')
                        if raw_action_type and str(raw_action_type).lower() == 'verify':
                            atype = 'verify'
                        tdev = None
                        spaths: Optional[List[str]] = None
                        try:
                            if isinstance(acfg, dict):
                                if atype != 'verify':  # 验证任务不从action_config取task_type
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
                        stage_info = _build_stage_info("", None, "pending", task_type=atype)
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
                total_count = len(tasks)
                return {
                    "tasks": tasks[offset:offset+limit],
                    "total": total_count
                }
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

    except Exception as e:
        logger.error(f"获取备份任务列表失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", response_model=BackupTaskResponse)
async def get_backup_task(task_id: int, http_request: Request):
    """获取备份任务详情"""
    try:
        # 使用原生SQL查询（使用连接池）
        async with get_opengauss_connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT bt.id, bt.task_name, bt.task_type, bt.status, bt.progress_percent, bt.total_files,
                       bt.processed_files, bt.total_bytes, bt.processed_bytes, bt.compressed_bytes,
                       bt.created_at, bt.started_at, bt.completed_at, bt.error_message, bt.is_template,
                       bt.tape_device, bt.source_paths, bt.description, bt.result_summary, bt.scan_status, bt.operation_stage,
                       bt.backup_set_id, bs.set_id as backup_set_str_id
                FROM backup_tasks bt
                LEFT JOIN backup_sets bs ON bt.backup_set_id = bs.id
                WHERE bt.id = $1
                """,
                task_id
            )

            if not row:
                raise HTTPException(status_code=404, detail="备份任务不存在")

            # 解析JSON字段
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
            scan_status = row.get("scan_status")

            # 对于运行中的任务，同时获取 current_compression_progress 和 prefetch_done
            current_compression_progress = None
            prefetch_done = False
            single_compression_completed = None
            if status_value and status_value.lower() == 'running':
                try:
                    from web.api.backup.utils import get_system_instance
                    system = get_system_instance(http_request)
                    if system and system.backup_engine:
                        # 获取 current_compression_progress
                        task_status = await system.backup_engine.get_task_status(row["id"])
                        if task_status and 'current_compression_progress' in task_status:
                            current_compression_progress = task_status['current_compression_progress']
                        # 获取 compression_completed（从 task_status）
                        if task_status and 'compression_completed' in task_status:
                            single_compression_completed = task_status.get('compression_completed')
                        # 获取 prefetch_done
                        cw = getattr(system.backup_engine, '_current_compression_worker', None)
                        if cw and hasattr(cw, 'file_group_prefetcher') and cw.file_group_prefetcher:
                            fp = cw.file_group_prefetcher
                            prefetch_loop_count = getattr(fp, 'prefetch_loop_count', 0) or 0
                            prefetcher_running = getattr(fp, '_running', True)
                            prefetch_done = prefetch_loop_count > 0 and not prefetcher_running
                        # 获取 compression_completed（从 compression_worker.backup_task）
                        if cw and hasattr(cw, 'backup_task') and single_compression_completed is None:
                            single_compression_completed = getattr(cw.backup_task, 'compression_completed', None)
                except Exception as e:
                    logger.debug(f"获取任务压缩进度失败: {str(e)}")

            # 检查 final 目录是否有待写入磁带的文件（用于 copy 步骤状态判断）
            final_dir_has_files = None
            if status_value and status_value.lower() == 'running':
                try:
                    from web.api.backup.utils import get_system_instance
                    system = get_system_instance(http_request)
                    if system and system.backup_engine:
                        if hasattr(system.backup_engine, 'final_dir_monitor') and system.backup_engine.final_dir_monitor:
                            _set_id = row.get("backup_set_str_id")
                            # 数据库中 backup_set_str_id 为空时，从内存中的 compression_worker 回退获取
                            if not _set_id:
                                _cw = getattr(system.backup_engine, '_current_compression_worker', None)
                                if _cw and hasattr(_cw, 'backup_set') and _cw.backup_set:
                                    _set_id = getattr(_cw.backup_set, 'set_id', None)
                            if _set_id:
                                final_dir_has_files = not system.backup_engine.final_dir_monitor.is_final_dir_empty(_set_id)
                except Exception:
                    pass

            # 构建阶段信息
            stage_info = _build_stage_info(
                row.get("description"),
                scan_status,
                status_value,
                row.get("operation_stage"),  # 优先使用数据库中的 operation_stage 字段
                current_compression_progress,
                final_dir_has_files,  # 传入 final 目录状态
                prefetch_done,  # 传入预取器是否完成
                single_compression_completed,  # 传入压缩是否完成
                task_type=str(row.get("task_type", ""))  # 传入任务类型
            )

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
        # 使用原生SQL查询（openGauss）
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
                        source_paths = json.loads(source_paths)
                    except:
                        source_paths = []

                # 如果 exclude_patterns 是字符串，尝试解析为 JSON
                if isinstance(exclude_patterns, str):
                    try:
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

    except Exception as e:
        logger.error(f"获取备份任务模板列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
