#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SMB 到磁带备份 API
SMB to Tape Backup API

创建从 SMB 共享备份到磁带的一次性任务，支持 zstd 压缩
"""

import asyncio
import subprocess
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from utils.wechat_notifier import get_wechat_notifier
from config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter(tags=["SMB到磁带备份"])


class OneTimeBackupRequest(BaseModel):
    """SMB 到磁带备份请求"""
    smb_path: str  # SMB 路径，如 \\192.168.0.79\bak\sysbak\192.168.0.50\D
    tape_device: str = "/dev/nst0"  # 磁带设备
    task_name: Optional[str] = None  # 任务名称
    smb_username: Optional[str] = "grigs"
    smb_password: Optional[str] = "Slnwg123$"
    mount_point: Optional[str] = None  # 自动生成：/mnt/smb_backup/{task_id}
    use_compression: bool = True  # 是否使用 zstd 压缩
    compression_level: int = 3  # zstd 压缩级别 (1-19)
    compression_threads: int = 4  # zstd 压缩线程数
    temp_dir: str = "/mnt/SSD/compress"  # 临时压缩目录
    notify_wechat: bool = True  # 是否微信通知


class BackupStatus(BaseModel):
    """备份状态"""
    task_id: str
    task_name: str
    status: str  # pending, running, completed, failed
    progress: float = 0
    files_processed: int = 0
    bytes_processed: int = 0
    speed: Optional[float] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None


# 活动任务存储
active_tasks: dict[str, BackupStatus] = {}


async def send_wechat_notification(status: BackupStatus):
    """发送微信通知"""
    notifier = get_wechat_notifier()
    if not notifier:
        return
    
    await notifier.send_backup_report(
        task_name=status.task_name,
        status=status.status,
        progress=status.progress,
        files_processed=status.files_processed,
        bytes_processed=status.bytes_processed,
        speed=status.speed,
        error=status.error
    )


async def run_backup_task(
    task_id: str,
    request: OneTimeBackupRequest
):
    """
    执行备份任务

    流程:
    1. 挂载 SMB 共享
    2. 定位磁带
    3. 使用 tar + zstd 写入磁带（如果启用压缩）
    4. 卸载 SMB 共享
    5. 发送完成通知
    """
    status = active_tasks[task_id]
    status.status = "running"
    status.start_time = datetime.now()

    try:
        # 1. 为每个任务创建独立挂载点（使用 sudo）
        task_mount = Path(f"/mnt/smb_{task_id}")
        # 使用 sudo 创建目录
        subprocess.run(["sudo", "mkdir", "-p", str(task_mount)], check=True)
        subprocess.run(["sudo", "chown", "grigs:grigs", str(task_mount)], check=True)

        # 2. 挂载 SMB 共享（只读）
        logger.info(f"挂载 SMB 共享（只读）: {request.smb_path}")
        # 处理 SMB 路径格式 - 解析出共享根和子路径
        # 输入: \\192.168.0.79\bak\sysbak\192.168.0.50\D
        # 共享根: //192.168.0.79/bak
        # 子路径: sysbak/192.168.0.50/D
        smb_path = request.smb_path.replace("\\", "/")
        if not smb_path.startswith("//"):
            smb_path = "//" + smb_path.lstrip("/")
        
        # 解析共享根（前4段: //server/share）和子路径
        parts = smb_path.split("/")
        if len(parts) < 4:
            raise Exception(f"SMB 路径格式错误，需要包含共享名: {request.smb_path}")
        
        smb_share = "/".join(parts[:4])  # //192.168.0.79/bak
        smb_subpath = "/".join(parts[4:])  # sysbak/192.168.0.50/D
        
        # 挂载共享根（只读）
        mount_cmd = ["sudo", "mount", "-t", "cifs", smb_share, str(task_mount)]
        if request.smb_username and request.smb_password:
            mount_cmd.extend([
                "-o",
                f"username={request.smb_username},password={request.smb_password},vers=3.0,ro"
            ])
        else:
            # 使用 guest 访问（只读）
            mount_cmd.extend(["-o", "guest,ro,vers=3.0"])

        result = subprocess.run(mount_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"SMB 挂载失败: {result.stderr}")
        
        # 确定实际备份路径
        if smb_subpath:
            backup_source = task_mount / smb_subpath
        else:
            backup_source = task_mount

        status.progress = 10
        await send_wechat_notification(status)

        # 3. 定位磁带到末尾
        logger.info(f"定位磁带: {request.tape_device}")
        mt_cmd = ["mt", "-f", request.tape_device, "eod"]
        subprocess.run(mt_cmd, capture_output=True, check=True)

        status.progress = 15
        await send_wechat_notification(status)

        # 4. 获取要备份的文件统计
        logger.info("统计文件...")
        files = list(backup_source.rglob("*"))
        total_files = len([f for f in files if f.is_file()])
        total_size = sum(f.stat().st_size for f in files if f.is_file())

        status.progress = 20
        await send_wechat_notification(status)

        # 5. 执行备份
        if request.use_compression:
            # 使用 zstd 压缩后写入磁带
            logger.info(f"开始 tar + zstd 压缩备份到磁带 (级别={request.compression_level}, 线程={request.compression_threads})...")
            status.status = "running"

            # 创建临时 tar 文件
            temp_dir = Path(request.temp_dir)
            temp_dir.mkdir(parents=True, exist_ok=True)
            temp_tar = temp_dir / f"{task_id}.tar"
            temp_zst = temp_dir / f"{task_id}.tar.zst"

            try:
                # 5.1 创建 tar 文件 - 使用 backup_source 而不是 mount_point
                tar_cmd = [
                    "tar", "-cf", str(temp_tar),
                    "-C", str(backup_source), "."
                ]

                logger.info(f"执行 tar 打包: {' '.join(tar_cmd)}")
                tar_process = subprocess.run(tar_cmd, capture_output=True, text=True)
                if tar_process.returncode != 0:
                    raise Exception(f"tar 打包失败: {tar_process.stderr}")

                status.progress = 50
                status.bytes_processed = temp_tar.stat().st_size if temp_tar.exists() else 0
                await send_wechat_notification(status)

                # 5.2 使用 zstd 压缩
                zstd_cmd = [
                    "zstd",
                    f"-{request.compression_level}",
                    f"-T{request.compression_threads}",
                    "-f",
                    "-o", str(temp_zst),
                    str(temp_tar)
                ]

                logger.info(f"执行 zstd 压缩: {' '.join(zstd_cmd)}")
                zstd_process = subprocess.run(zstd_cmd, capture_output=True, text=True)
                if zstd_process.returncode != 0:
                    raise Exception(f"zstd 压缩失败: {zstd_process.stderr}")

                status.progress = 70
                compressed_size = temp_zst.stat().st_size if temp_zst.exists() else 0
                status.bytes_processed = compressed_size
                await send_wechat_notification(status)

                # 5.3 写入磁带
                logger.info(f"写入磁带: {request.tape_device}")
                dd_cmd = ["dd", f"if={temp_zst}", f"of={request.tape_device}", "bs=256K"]
                dd_process = subprocess.run(dd_cmd, capture_output=True, text=True)
                if dd_process.returncode != 0:
                    raise Exception(f"写入磁带失败: {dd_process.stderr}")

                # 写入文件标记
                subprocess.run(["mt", "-f", request.tape_device, "weof", "1"], capture_output=True)

                status.progress = 95
                status.files_processed = total_files
                status.bytes_processed = compressed_size

            finally:
                # 清理临时文件
                try:
                    if temp_tar.exists():
                        temp_tar.unlink()
                    if temp_zst.exists():
                        temp_zst.unlink()
                except Exception as cleanup_err:
                    logger.warning(f"清理临时文件失败: {cleanup_err}")
        else:
            # 直接使用 tar 写入磁带（无压缩）
            logger.info(f"开始 tar 备份到磁带（无压缩）...")
            status.status = "running"

            tar_cmd = [
                "tar", "-cvf", request.tape_device,
                "-C", str(backup_source), "."
            ]

            process = subprocess.Popen(
                tar_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # 监控进度
            start_time = datetime.now()
            files_processed = 0

            while True:
                line = process.stderr.readline()
                if not line and process.poll() is not None:
                    break

                if line:
                    files_processed += 1
                    # 更新进度
                    if total_files > 0:
                        status.progress = 20 + (files_processed / total_files) * 70
                    status.files_processed = files_processed

                    # 每 1000 个文件更新一次
                    if files_processed % 1000 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        if elapsed > 0:
                            status.speed = files_processed / elapsed  # files/s
                        await send_wechat_notification(status)

            # 等待进程完成
            return_code = process.wait()
            if return_code != 0:
                raise Exception(f"tar 备份失败: {process.stderr.read()}")

            status.progress = 95
            status.files_processed = files_processed
            status.bytes_processed = total_size

        # 6. 卸载 SMB 共享
        logger.info("卸载 SMB 共享...")
        subprocess.run(["sudo", "umount", str(task_mount)], capture_output=True)
        # 清理挂载点目录
        try:
            task_mount.rmdir()
        except:
            pass

        # 7. 完成
        status.status = "completed"
        status.progress = 100
        status.end_time = datetime.now()

        logger.info(f"备份任务完成: {task_id}")
        await send_wechat_notification(status)

    except Exception as e:
        logger.error(f"备份任务失败: {e}", exc_info=True)
        status.status = "failed"
        status.error = str(e)
        status.end_time = datetime.now()

        # 尝试卸载
        try:
            task_mount = Path(f"/mnt/smb_{task_id}")
            subprocess.run(["sudo", "umount", str(task_mount)], capture_output=True)
            task_mount.rmdir()
        except:
            pass

        await send_wechat_notification(status)


@router.post("/smb-to-tape", response_model=BackupStatus)
async def create_smb_to_tape_backup(
    request: OneTimeBackupRequest,
    background_tasks: BackgroundTasks
):
    """
    创建 SMB 到磁带的备份任务

    将 SMB 共享目录备份到磁带，支持 zstd 压缩

    参数:
    - smb_path: SMB 路径，如 \\\\192.168.0.79\\bak\\sysbak\\192.168.0.50\\D
    - tape_device: 磁带设备，默认 /dev/nst0
    - task_name: 任务名称（可选）
    - smb_username: SMB 用户名（可选）
    - smb_password: SMB 密码（可选）
    - use_compression: 是否使用 zstd 压缩，默认 True
    - compression_level: zstd 压缩级别，默认 3
    - compression_threads: zstd 压缩线程数，默认 4
    """
    # 生成任务 ID
    task_id = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 创建任务状态
    task_name = request.task_name or f"备份 {request.smb_path}"
    status = BackupStatus(
        task_id=task_id,
        task_name=task_name,
        status="pending"
    )
    active_tasks[task_id] = status

    # 启动后台任务
    background_tasks.add_task(run_backup_task, task_id, request)

    logger.info(f"创建 SMB 到磁带备份任务: {task_id}")

    return status


@router.get("/smb-to-tape/status/{task_id}", response_model=BackupStatus)
async def get_smb_to_tape_status(task_id: str):
    """获取 SMB 到磁带备份任务状态"""
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")
    return active_tasks[task_id]


@router.get("/smb-to-tape/list")
async def list_smb_to_tape_tasks():
    """列出所有 SMB 到磁带备份任务"""
    return list(active_tasks.values())


@router.delete("/smb-to-tape/cancel/{task_id}")
async def cancel_smb_to_tape_backup(task_id: str):
    """取消 SMB 到磁带备份任务"""
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    status = active_tasks[task_id]
    if status.status == "running":
        # TODO: 实现任务取消逻辑
        status.status = "cancelled"
        status.error = "用户取消"
        return {"success": True, "message": "任务已取消"}
    else:
        return {"success": False, "message": "任务不在运行中"}
