#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带恢复 API
Tape Recovery API

提供从 LTFS 磁带直接恢复的 API 端点，不依赖数据库。
"""

import asyncio
import io
import json
import logging
from pathlib import Path
from typing import Optional, List, Dict
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================
# 请求模型
# ============================================================

class MountTapeRequest(BaseModel):
    max_retries: int = 3
    retry_interval: int = 30


class RestoreRequest(BaseModel):
    """恢复请求"""
    set_id: str
    target_path: str
    overwrite: str = "skip"           # skip / overwrite
    preserve_permissions: bool = True
    verify_integrity: bool = True
    selected_archives: Optional[List[str]] = None
    # 文件级选择: key=归档文件名, value=文件路径列表; null/空=恢复该归档全部文件
    selected_files: Optional[Dict[str, Optional[List[str]]]] = None


# ============================================================
# 辅助函数
# ============================================================

def _get_engine(request: Request):
    """获取磁带恢复引擎实例"""
    system = request.app.state.system
    if not system:
        raise HTTPException(status_code=500, detail="系统未初始化")
    engine = getattr(system, 'tape_recovery_engine', None)
    if not engine:
        raise HTTPException(status_code=500, detail="磁带恢复引擎未初始化")
    return engine


# ============================================================
# 端点
# ============================================================

@router.get("/tape-status")
async def get_tape_status(request: Request):
    """获取磁带状态（是否加载、是否挂载 LTFS）"""
    engine = _get_engine(request)
    return await engine.get_tape_status()


@router.post("/mount-tape")
async def mount_tape(req: MountTapeRequest, request: Request):
    """挂载 LTFS 文件系统"""
    engine = _get_engine(request)
    return await engine.mount_tape(
        max_retries=req.max_retries,
        retry_interval=req.retry_interval,
    )


@router.post("/mount-tape-stream")
async def mount_tape_stream(req: MountTapeRequest, request: Request):
    """挂载 LTFS 文件系统（SSE 流式输出，实时显示命令日志）"""
    engine = _get_engine(request)

    async def _stream():
        try:
            async for event in engine.mount_tape_streaming(
                max_retries=req.max_retries,
                retry_interval=req.retry_interval,
            ):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'log', 'message': f'挂载异常: {str(e)}'}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'result', 'success': False, 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/unmount-tape")
async def unmount_tape(request: Request):
    """卸载 LTFS 文件系统"""
    engine = _get_engine(request)
    return await engine.unmount_tape()


@router.post("/unmount-tape-stream")
async def unmount_tape_stream(request: Request):
    """卸载 LTFS 文件系统（SSE 流式输出，实时显示命令日志）"""
    engine = _get_engine(request)

    async def _stream():
        try:
            async for event in engine.unmount_tape_streaming():
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'log', 'message': f'卸载异常: {str(e)}'}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'result', 'success': False, 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/remount-tape")
async def remount_tape(request: Request):
    """先卸载再重新挂载 LTFS"""
    engine = _get_engine(request)

    # 先卸载
    unmount_result = await engine.unmount_tape()
    if not unmount_result["success"]:
        return {"success": False, "message": f"卸载失败: {unmount_result['message']}", "step": "unmount"}

    # 再挂载
    mount_result = await engine.mount_tape()
    mount_result["step"] = "mount"
    return mount_result


@router.post("/remount-tape-stream")
async def remount_tape_stream(request: Request):
    """先卸载再重新挂载 LTFS（SSE 流式输出，实时显示命令日志）"""
    engine = _get_engine(request)

    async def _stream():
        # 步骤1：卸载（流式）
        try:
            async for event in engine.unmount_tape_streaming():
                # 给 unmount 的 result 事件添加 step 标识，前端依赖此字段判断
                if event.get('type') == 'result' and 'step' not in event:
                    event['step'] = 'unmount'
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event.get('type') == 'result' and not event.get('success'):
                    return
        except Exception as e:
            yield f"data: {json.dumps({'type': 'log', 'message': f'卸载异常: {str(e)}'}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'result', 'success': False, 'message': str(e), 'step': 'unmount'}, ensure_ascii=False)}\n\n"
            return

        # 步骤1完成
        yield f"data: {json.dumps({'type': 'step_done', 'step': 0}, ensure_ascii=False)}\n\n"

        # 步骤2：挂载（流式）
        try:
            async for event in engine.mount_tape_streaming():
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'log', 'message': f'挂载异常: {str(e)}'}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'result', 'success': False, 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/eject-tape")
async def eject_tape(request: Request):
    """卸载并弹出磁带"""
    engine = _get_engine(request)
    return await engine.eject_tape()


@router.post("/eject-tape-stream")
async def eject_tape_stream(request: Request):
    """卸载并弹出磁带（SSE 流式输出，实时显示命令日志）"""
    engine = _get_engine(request)

    async def _stream():
        # eject_tape_streaming() 内部已包含卸载步骤，直接调用即可
        try:
            async for event in engine.eject_tape_streaming():
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'log', 'message': f'弹出异常: {str(e)}'}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'result', 'success': False, 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/scan-tape")
async def scan_tape(request: Request):
    """扫描磁带内容，发现备份集"""
    engine = _get_engine(request)
    return await engine.scan_tape_contents()


@router.post("/scan-tape-stream")
async def scan_tape_stream(request: Request):
    """流式扫描磁带内容（SSE），逐个归档实时推送"""
    engine = _get_engine(request)

    async def _stream():
        try:
            async for event in engine.scan_tape_contents_streaming():
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.get("/backup-sets/{set_id}/contents")
async def get_backup_set_contents(set_id: str, request: Request):
    """获取备份集的完整文件列表（合并所有归档）"""
    engine = _get_engine(request)
    return await engine.list_backup_set_contents(set_id)


@router.get("/backup-sets/{set_id}/archive-contents")
async def get_archive_contents(set_id: str, archive: str, request: Request,
                                          page: int = 1, page_size: int = 100,
                                          use_cache: bool = True):
    """获取单个归档文件的内部文件列表（支持分页和数据库缓存）"""
    engine = _get_engine(request)

    if use_cache:
        # 尝试从数据库缓存读取
        tape_status = await engine.get_tape_status()
        tape_label = tape_status.get("tape_label") or "unknown"

        cache_info = await engine.check_archive_cache(tape_label, set_id, archive)
        if cache_info and cache_info.get("cached"):
            return await engine.get_cached_archive_contents(
                tape_label, set_id, archive, page, page_size,
            )

    # 缓存未命中或 use_cache=False，走原始解压
    result = await engine.list_archive_contents(set_id, archive)

    # 如果缓存未命中，异步保存到数据库
    if use_cache and result.get("entries"):
        tape_status_info = await engine.get_tape_status()
        tape_label = tape_status_info.get("tape_label") or "unknown"
        archive_path = engine._get_mount_point() / set_id / archive
        archive_size = archive_path.stat().st_size if archive_path.exists() else None
        asyncio.create_task(
            engine.save_archive_contents(
                tape_label, set_id, archive,
                result["entries"], archive_size,
            )
        )

    return result


@router.get("/backup-sets/{set_id}/expand-all-stream")
async def expand_all_stream(set_id: str, tape_label: str, request: Request):
    """展开全部归档文件列表（SSE 流式，优先查数据库缓存）

    查询参数:
        tape_label: 磁带卷标（用作缓存键）
    """
    engine = _get_engine(request)

    # 如果没有传 tape_label，尝试获取
    if not tape_label:
        status = await engine.get_tape_status()
        tape_label = status.get("tape_label") or "unknown"

    async def _stream():
        try:
            async for event in engine.expand_all_cached(tape_label, set_id):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.get("/backup-sets/{set_id}/cached-archive-contents")
async def get_cached_archive_contents(set_id: str, archive: str, request: Request,
                                             page: int = 1, page_size: int = 100):
    """从数据库缓存读取归档文件列表（分页）"""
    engine = _get_engine(request)
    tape_status = await engine.get_tape_status()
    tape_label = tape_status.get("tape_label") or "unknown"
    return await engine.get_cached_archive_contents(
        tape_label, set_id, archive, page, page_size,
    )


@router.get("/backup-sets/{set_id}/search-files-stream")
async def search_files_stream(set_id: str, q: str, request: Request):
    """在备份集所有归档中搜索文件名（SSE 流式，逐个归档检索）"""
    engine = _get_engine(request)

    async def _stream():
        try:
            async for event in engine.search_files_streaming(set_id, q):
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@router.post("/restore")
async def start_restore(
    req: RestoreRequest,
    background_tasks: BackgroundTasks,
    request: Request,
):
    """创建并启动恢复任务"""
    engine = _get_engine(request)

    try:
        recovery_id = await engine.create_recovery_task(
            set_id=req.set_id,
            target_path=req.target_path,
            overwrite=req.overwrite,
            preserve_permissions=req.preserve_permissions,
            verify_integrity=req.verify_integrity,
            selected_archives=req.selected_archives,
            selected_files=req.selected_files,
        )

        # 后台执行恢复
        background_tasks.add_task(engine.execute_recovery, recovery_id)

        return {
            "success": True,
            "recovery_id": recovery_id,
            "message": "恢复任务已创建",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"创建恢复任务失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recovery-status/{recovery_id}")
async def get_recovery_status(recovery_id: str, request: Request):
    """获取恢复任务状态和进度"""
    engine = _get_engine(request)
    status = await engine.get_recovery_status(recovery_id)
    if not status:
        raise HTTPException(status_code=404, detail="恢复任务不存在")
    return status


@router.post("/cancel/{recovery_id}")
async def cancel_recovery(recovery_id: str, request: Request):
    """取消恢复任务"""
    engine = _get_engine(request)
    success = await engine.cancel_recovery(recovery_id)
    if not success:
        raise HTTPException(status_code=404, detail="恢复任务不存在")
    return {"success": True, "message": "已请求取消恢复任务"}


# ============================================================
# 下载端点
# ============================================================

class ZipDownloadRequest(BaseModel):
    """ZIP 下载请求"""
    set_id: str
    selected_files: Optional[Dict[str, Optional[List[str]]]] = None


@router.post("/download-zip")
async def download_zip(req: ZipDownloadRequest, request: Request):
    """将选中归档中的文件打包为 ZIP 流式下载"""
    import zipfile

    engine = _get_engine(request)
    mount_point = engine._get_mount_point()
    set_dir = mount_point / req.set_id

    if not set_dir.exists():
        raise HTTPException(status_code=404, detail=f"备份集不存在: {req.set_id}")

    selected_files = req.selected_files or {}

    async def _zip_stream():
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for archive_name, file_list in selected_files.items():
                archive_path = set_dir / archive_name
                if not archive_path.exists():
                    continue

                # 获取文件过滤列表
                files_filter = file_list if file_list else None
                try:
                    entries = await asyncio.to_thread(
                        engine._extract_to_memory, archive_path, files_filter
                    )
                    for rel_path, data in entries:
                        zf.writestr(rel_path, data)
                except Exception as e:
                    logger.error(f"[ZIP下载] 处理归档 {archive_name} 失败: {e}")

        buffer.seek(0)
        return buffer.getvalue()

    try:
        data = await _zip_stream()
        return StreamingResponse(
            io.BytesIO(data),
            media_type="application/zip",
            headers={
                "Content-Disposition": f'attachment; filename="{req.set_id}_recovery.zip"'
            },
        )
    except Exception as e:
        logger.error(f"[ZIP下载] 打包失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"打包失败: {str(e)}")


@router.get("/download-raw")
async def download_raw(
    set_id: str,
    archive: str,
    request: Request,
):
    """直接下载原始归档文件"""
    engine = _get_engine(request)
    mount_point = engine._get_mount_point()

    # 防止路径穿越
    if '..' in set_id or '..' in archive or '/' in archive or '\\' in archive:
        raise HTTPException(status_code=400, detail="非法路径")

    archive_path = mount_point / set_id / archive

    # 确保路径在挂载点内
    try:
        archive_path.resolve().relative_to(mount_point.resolve())
    except ValueError:
        raise HTTPException(status_code=400, detail="非法路径")

    if not archive_path.exists():
        raise HTTPException(status_code=404, detail=f"归档文件不存在: {archive}")

    file_size = archive_path.stat().st_size

    def _iter_file():
        with open(str(archive_path), 'rb') as f:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        _iter_file(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{archive}"',
            "Content-Length": str(file_size),
        },
    )
