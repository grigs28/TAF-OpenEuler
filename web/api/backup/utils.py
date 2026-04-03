#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API - 工具函数
Backup Management API - Utility Functions
"""

import logging

import re
from typing import Dict, Any, Optional
from fastapi import Request

logger = logging.getLogger(__name__)

# 阶段流程定义
STAGE_FLOW_DEFINITION = [
    ("scan", "扫描文件"),
    ("prefetch", "预分组"),
    ("compress", "压缩/打包"),
    ("copy", "写入磁带"),
    ("finalize", "完成"),
]
STAGE_INDEX = {code: idx for idx, (code, _) in enumerate(STAGE_FLOW_DEFINITION)}
STAGE_LABELS = {
    "scan": "扫描文件",
    "prefetch": "预分组",
    "compress": "压缩/打包",
    "copy": "写入磁带",
    "finalize": "完成备份",
    "waiting": "等待批次",
    "cancelled": "任务已取消",
    "failed": "任务失败",
    "format": "格式化磁带",
}
OP_STATUS_PATTERN = re.compile(r'\[([^\]]+)\]')
OP_STAGE_KEYWORDS = [
    ("扫描", "scan"),
    ("准备压缩", "compress"),
    ("压缩", "compress"),
    ("等待下一批", "compress"),
    ("复制", "copy"),
    ("写入", "copy"),
    ("完成", "finalize"),
    ("格式化", "format"),
    ("取消", "cancelled"),
    ("失败", "failed"),
]


def _normalize_status_value(value: Any) -> str:
    """标准化状态值"""
    if value is None:
        return ""
    
    # 如果是枚举类型，获取其value属性
    if hasattr(value, "value"):
        result = str(value.value).lower()
        return result
    
    # 如果是字符串，直接转为小写
    if isinstance(value, str):
        return value.lower()
    
    # 其他类型，转换为字符串并转为小写
    return str(value).lower()


def _build_stage_info(description: Optional[str], scan_status: Optional[str], status_value: str, operation_stage: Optional[str] = None, current_compression_progress: Optional[Dict[str, Any]] = None, final_dir_has_files: Optional[bool] = None, prefetch_done: bool = False, compression_completed: Optional[bool] = None) -> Dict[str, Any]:
    """构建阶段信息

    Args:
        description: 任务描述（包含操作状态信息，不再从中解析压缩进度）
        scan_status: 扫描状态
        status_value: 任务状态值
        operation_stage: 操作阶段代码（优先使用，如果提供则直接使用）
        current_compression_progress: 当前压缩进度信息（从内存中的压缩程序获取，包含所有并行任务的聚合进度）
        final_dir_has_files: final目录是否有待写入磁带的文件（None表示未知/不适用，True=有文件，False=无文件）
        prefetch_done: 预取器是否已完成（预取器执行过且已停止，表示所有文件已分组）
        compression_completed: 压缩是否已完成（从backup_task.compression_completed获取）
    """
    # 优先使用数据库中的 operation_stage 字段
    stage_code = operation_stage
    
    desc = description or ""
    matches = OP_STATUS_PATTERN.findall(desc)
    operation_status = matches[-1] if matches else None
    
    # 提取完整的操作状态，包括方括号后的进度信息
    # 例如: "[压缩文件中...] 814/1637 个文件 (49.7%)" -> "压缩文件中 814/1637 个文件 (49.7%)"
    if operation_status:
        operation_status = operation_status.replace("...", "")
        # 检查方括号后是否有进度信息
        last_bracket_pos = desc.rfind(']')
        if last_bracket_pos >= 0 and last_bracket_pos + 1 < len(desc):
            remaining_text = desc[last_bracket_pos + 1:].strip()
            if remaining_text:
                # 如果方括号后有文本，将其追加到 operation_status
                operation_status = operation_status + " " + remaining_text

    # 如果提供了current_compression_progress（从内存中的压缩程序获取），使用它构建压缩阶段的operation_status
    if current_compression_progress and stage_code == 'compress':
        current_file_index = current_compression_progress.get('current_file_index', 0)
        total_files_in_group = current_compression_progress.get('total_files_in_group', 0)
        percent = current_compression_progress.get('percent', 0.0)
        running_count = current_compression_progress.get('running_count', 1)
        
        # 构建operation_status：压缩文件中 X/Y 个文件 (Z%)
        if total_files_in_group > 0:
            operation_status = f"压缩文件中 {current_file_index}/{total_files_in_group} 个文件 ({percent:.1f}%)"
            if running_count > 1:
                operation_status += f" [并行{running_count}个任务]"
            logger.debug(f"[状态构建] 从内存压缩程序构建operation_status: {operation_status}")
        else:
            operation_status = "压缩文件中..."
    elif stage_code == 'compress' and not current_compression_progress:
        # 压缩阶段但没有进度信息，使用默认状态
        operation_status = "压缩文件中..."

    # 如果没有从数据库获取到 stage_code，则从 operation_status 中解析
    if not stage_code and operation_status:
        lowered = operation_status.lower()
        for keyword, code in OP_STAGE_KEYWORDS:
            if keyword.lower() in lowered:
                stage_code = code
                break

    normalized_status = (status_value or "").lower()
    normalized_scan = (scan_status or "").lower()

    if not stage_code:
        if normalized_status in ("failed",):
            stage_code = "failed"
            operation_status = operation_status or "任务失败"
        elif normalized_status in ("cancelled",):
            stage_code = "cancelled"
            operation_status = operation_status or "任务已取消"
        elif normalized_status in ("completed",):
            # 只有当任务状态为 completed 且 operation_stage 为 finalize 时，才显示"备份完成"
            # 避免扫描完成时误显示为整个任务完成
            if operation_stage and operation_stage.lower() == "finalize":
                stage_code = "finalize"
                operation_status = operation_status or "备份完成"
            else:
                # 如果任务状态是 completed 但 operation_stage 不是 finalize，说明可能只是某个阶段完成
                # 根据 scan_status 判断当前阶段
                stage_code = normalized_scan if normalized_scan in STAGE_INDEX else "scan"
                operation_status = operation_status or "正在处理"
        elif normalized_status in ("running",):
            stage_code = normalized_scan if normalized_scan in STAGE_INDEX else "scan"
            operation_status = operation_status or "正在处理"
        else:
            stage_code = "waiting"
            operation_status = operation_status or "等待开始"

    stage_label = STAGE_LABELS.get(stage_code, "未知阶段")
    stage_steps = []

    # 备份引擎采用并发流水线架构：
    # 扫描、预分组、压缩、写入磁带是并发任务
    # 每个阶段有独立的状态变量，不依赖线性顺序

    normalized_scan = (scan_status or "").lower()
    normalized_status = (status_value or "").lower()
    is_success = normalized_status == "completed"
    is_failed = normalized_status in ("failed", "cancelled")
    is_running = normalized_status == "running"

    # 每个阶段独立的完成标志
    scan_done = normalized_scan == "completed"
    prefetch_done_flag = scan_done and prefetch_done
    compress_done_flag = scan_done and prefetch_done_flag and bool(compression_completed)
    # copy_done: final_dir 无文件 + 上游全部完成（由任务完成逻辑判定，这里只判断显示状态）

    for idx, (code, label) in enumerate(STAGE_FLOW_DEFINITION):
        if code == "scan":
            if is_success or scan_done:
                label = "扫描完成"
                step_status = "completed"
            elif is_failed:
                step_status = "completed" if scan_done else "failed"
            elif is_running:
                label = "扫描文件"
                step_status = "active"
            else:
                step_status = "pending"

        elif code == "prefetch":
            if is_success or prefetch_done_flag:
                label = "分组完成"
                step_status = "completed"
            elif is_failed:
                step_status = "completed" if prefetch_done_flag else "failed"
            elif is_running and not prefetch_done:
                # 预取器还在循环（等待扫描产出或正在分组）
                label = "预分组中"
                step_status = "active"
            else:
                label = "预分组"
                step_status = "pending"

        elif code == "compress":
            if is_success or compress_done_flag:
                label = "压缩完成"
                step_status = "completed"
            elif is_failed:
                step_status = "completed" if compress_done_flag else "failed"
            elif is_running and compression_completed:
                # 压缩已完成但上游依赖尚未传播（竞态），仍显示完成
                label = "压缩完成"
                step_status = "completed"
            elif is_running:
                label = "压缩/打包"
                step_status = "active"
            else:
                step_status = "pending"

        elif code == "copy":
            if is_success:
                label = "写入磁带完成"
                step_status = "completed"
            elif is_failed:
                step_status = "failed"
            elif is_running and final_dir_has_files:
                # final 目录有文件，正在写入磁带
                label = "写入磁带中"
                step_status = "active"
            elif is_running:
                label = "写入磁带"
                step_status = "pending"
            else:
                step_status = "pending"

        elif code == "finalize":
            if is_success:
                label = "备份完成"
                step_status = "completed"
            elif is_failed:
                label = "任务失败" if normalized_status == "failed" else "已取消"
                step_status = "failed"
            else:
                step_status = "pending"

        else:
            step_status = "pending"

        step = {
            "code": code,
            "label": label,
            "status": step_status
        }
        stage_steps.append(step)

    return {
        "operation_status": operation_status,
        "operation_stage": stage_code,
        "operation_stage_label": stage_label,
        "stage_steps": stage_steps
    }


def get_system_instance(request: Request):
    """获取系统实例"""
    return request.app.state.system
