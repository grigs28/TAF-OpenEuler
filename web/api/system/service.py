#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - 服务管理（systemd）
System Management API - Service Management (systemd)
"""

import logging
import os
import shutil
import subprocess
from pathlib import Path
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

SERVICE_NAME = "taf"
SERVICE_FILE_SRC = Path(__file__).parent.parent.parent.parent / "deploy" / "taf.service"
SERVICE_FILE_DST = Path(f"/etc/systemd/system/{SERVICE_NAME}.service")


def _is_root() -> bool:
    return os.geteuid() == 0


def _run_systemctl(*args) -> subprocess.CompletedProcess:
    """执行 systemctl 命令"""
    cmd = ["systemctl"] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=30)


def _is_systemd_running() -> bool:
    """检测当前进程是否由 systemd 管理"""
    try:
        # 检查 INVOCATION_ID 环境变量（systemd 设置）
        if os.environ.get("INVOCATION_ID"):
            return True
        # 检查是否在 cgroup 中
        with open("/proc/self/cgroup", "r") as f:
            content = f.read()
            if f"/{SERVICE_NAME}.service" in content:
                return True
    except Exception:
        pass
    return False


@router.get("/service/status")
async def get_service_status():
    """获取服务状态"""
    try:
        result = _run_systemctl("status", SERVICE_NAME, "--no-pager")
        is_installed = SERVICE_FILE_DST.exists()
        is_systemd = _is_systemd_running()

        # 解析状态
        status = "unknown"
        active_line = ""
        if result.returncode != 4:  # 4 = unit not found
            for line in result.stdout.splitlines():
                if line.strip().startswith("Active:"):
                    active_line = line.strip()
                    if "active (running)" in line:
                        status = "running"
                    elif "inactive (dead)" in line:
                        status = "stopped"
                    elif "activating" in line:
                        status = "starting"
                    elif "deactivating" in line:
                        status = "stopping"
                    elif "failed" in line:
                        status = "failed"
                    break

        return {
            "success": True,
            "service_name": SERVICE_NAME,
            "is_installed": is_installed,
            "is_systemd": is_systemd,
            "status": status,
            "active_line": active_line,
            "is_root": _is_root(),
            "output": result.stdout[:2000] if result.stdout else "",
        }
    except Exception as e:
        logger.error(f"获取服务状态失败: {e}")
        return {
            "success": True,
            "service_name": SERVICE_NAME,
            "is_installed": False,
            "is_systemd": False,
            "status": "error",
            "active_line": "",
            "is_root": _is_root(),
            "output": str(e),
        }


@router.post("/service/install")
async def install_service():
    """安装 systemd 服务"""
    if not _is_root():
        raise HTTPException(status_code=403, detail="需要 root 权限安装服务，请使用 sudo 运行或手动执行: sudo cp deploy/taf.service /etc/systemd/system/ && sudo systemctl daemon-reload")

    if not SERVICE_FILE_SRC.exists():
        raise HTTPException(status_code=404, detail=f"服务文件不存在: {SERVICE_FILE_SRC}")

    try:
        # 复制服务文件
        shutil.copy2(str(SERVICE_FILE_SRC), str(SERVICE_FILE_DST))
        # 重载 systemd
        _run_systemctl("daemon-reload")
        # 启用开机自启
        _run_systemctl("enable", SERVICE_NAME)

        logger.info(f"[服务管理] systemd 服务已安装并启用: {SERVICE_NAME}")
        return {
            "success": True,
            "message": f"服务已安装并启用开机自启 ({SERVICE_NAME}.service)",
        }
    except Exception as e:
        logger.error(f"安装服务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/service/uninstall")
async def uninstall_service():
    """卸载 systemd 服务"""
    if not _is_root():
        raise HTTPException(status_code=403, detail="需要 root 权限卸载服务")

    try:
        # 停止服务
        _run_systemctl("stop", SERVICE_NAME)
        # 禁用开机自启
        _run_systemctl("disable", SERVICE_NAME)
        # 删除服务文件
        if SERVICE_FILE_DST.exists():
            SERVICE_FILE_DST.unlink()
        # 重载 systemd
        _run_systemctl("daemon-reload")

        logger.info(f"[服务管理] systemd 服务已卸载: {SERVICE_NAME}")
        return {
            "success": True,
            "message": f"服务已卸载 ({SERVICE_NAME}.service)",
        }
    except Exception as e:
        logger.error(f"卸载服务失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/service/start")
async def start_service():
    """启动服务（由 systemd 管理，先停当前进程再启动）"""
    try:
        result = _run_systemctl("start", SERVICE_NAME)
        if result.returncode == 0:
            return {"success": True, "message": "服务启动命令已发送"}
        else:
            return {"success": False, "message": f"启动失败: {result.stderr[:500]}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/service/stop")
async def stop_service():
    """停止服务（发送 SIGINT，由应用自行优雅关闭）"""
    try:
        result = _run_systemctl("stop", SERVICE_NAME)
        if result.returncode == 0:
            return {"success": True, "message": "服务停止命令已发送"}
        else:
            return {"success": False, "message": f"停止失败: {result.stderr[:500]}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/service/restart")
async def restart_service():
    """重启服务（先停再启动，触发优雅关闭流程）"""
    try:
        # 先停（触发 SIGINT → 优雅关闭）
        _run_systemctl("stop", SERVICE_NAME)
        import time
        time.sleep(3)
        # 再启动
        result = _run_systemctl("start", SERVICE_NAME)
        if result.returncode == 0:
            return {"success": True, "message": "服务重启命令已发送"}
        else:
            return {"success": False, "message": f"重启失败: {result.stderr[:500]}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
