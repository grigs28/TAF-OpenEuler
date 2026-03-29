#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带工具管理API - Linux版本
Tape Tools Management API - Linux Version
"""

import os
import shutil
import logging
import asyncio
import subprocess
from typing import Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, Field

from models.system_log import OperationType, LogCategory, LogLevel
from config.database import get_db
from config.settings import get_settings
from utils.linux_tape import get_linux_tape_operator, LinuxTapeOperator

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tools", tags=["tools"])


# ===== Pydantic 模型 =====
class MkltfsRequest(BaseModel):
    """mkltfs 格式化请求"""
    device_path: str = Field(..., description="设备路径，如：/dev/sg3")
    volume_label: Optional[str] = Field(None, description="卷标名称")
    serial_number: Optional[str] = Field(None, description="序列号（6位字母数字）")


class LtfsMountRequest(BaseModel):
    """LTFS 挂载请求"""
    device_path: str = Field(..., description="设备路径，如：/dev/sg3")
    mount_point: str = Field(default="/mnt/ltfs", description="挂载点")
    sync_mode: bool = Field(default=False, description="同步写入模式")


class LtfsUnmountRequest(BaseModel):
    """LTFS 卸载请求"""
    mount_point: str = Field(default="/mnt/ltfs", description="挂载点")
    eject_after: bool = Field(default=False, description="卸载后弹出磁带")


class PrepareTapeRequest(BaseModel):
    """准备磁带请求"""
    device_path: str = Field(..., description="磁带设备路径，如：/dev/nst0")
    ltfs_device: Optional[str] = Field(None, description="LTFS设备路径，如：/dev/sg3")
    volume_label: Optional[str] = Field(None, description="卷标名称")
    force_erase: bool = Field(default=False, description="强制格式化")


class CompressionRequest(BaseModel):
    """压缩设置请求"""
    enable: bool = Field(default=True, description="是否启用压缩")


class SetblkRequest(BaseModel):
    """设置块大小请求"""
    block_size: int = Field(default=0, description="块大小，0表示变长块")


# 记录操作日志的辅助函数
async def log_tool_operation(
    db,
    operation_type: OperationType,
    operation_name: str,
    success: bool,
    details: dict = None,
    error_message: str = None
):
    """记录工具操作日志"""
    try:
        from models.system_log import OperationLog

        log_entry = OperationLog(
            operation_type=operation_type,
            resource_type="tool",
            operation_name=operation_name,
            operation_description=f"工具管理: {operation_name}",
            category="tape",
            operation_time=datetime.now(),
            success=success,
            request_params=details or {},
            error_message=error_message
        )
        db.add(log_entry)
        await db.commit()
    except Exception as e:
        logger.error(f"记录工具操作日志失败: {str(e)}")


def get_tape_operator() -> LinuxTapeOperator:
    """获取磁带操作器实例"""
    return get_linux_tape_operator()


# ===== 工具检查API =====
@router.get("/check")
async def check_tools_availability():
    """检查工具可用性"""
    try:
        settings = get_settings()

        # 检查 mt 命令
        mt_path = shutil.which('mt') or '/usr/bin/mt'
        mt_available = os.path.exists(mt_path)

        # 检查 LTFS 工具
        ltfs_tools = {}
        for tool in ['mkltfs', 'ltfs']:
            path = shutil.which(tool)
            if not path:
                # 尝试默认路径
                path = f'/usr/local/bin/{tool}'
            ltfs_tools[tool] = {
                "available": os.path.exists(path) if path else False,
                "path": path or f"/usr/local/bin/{tool}"
            }

        # 检查磁带设备
        tape_device = getattr(settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
        tape_device_exists = os.path.exists(tape_device)

        return {
            "mt_available": mt_available,
            "mt_path": mt_path if mt_available else "mt 命令未找到",
            "ltfs_tools": ltfs_tools,
            "tape_device": tape_device if tape_device_exists else None,
            "tape_device_exists": tape_device_exists
        }
    except Exception as e:
        logger.error(f"检查工具可用性失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== Linux mt 命令API =====
@router.get("/linux/status")
async def get_tape_status():
    """获取磁带状态"""
    try:
        operator = get_tape_operator()
        result = operator.status_sync()
        return result
    except Exception as e:
        logger.error(f"获取磁带状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/rewind")
async def rewind_tape():
    """倒带"""
    try:
        operator = get_tape_operator()
        result = operator.rewind_sync()
        return result
    except Exception as e:
        logger.error(f"倒带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/eject")
async def eject_tape():
    """弹出磁带"""
    try:
        operator = get_tape_operator()
        # 使用 mt eject 命令
        settings = get_settings()
        device = getattr(settings, 'TAPE_DEVICE_PATH', '/dev/nst0')

        result = subprocess.run(
            ['mt', '-f', device, 'eject'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=120,
            text=False
        )

        stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
        stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

        return {
            "success": result.returncode == 0,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "stderr": "命令超时", "returncode": -1}
    except Exception as e:
        logger.error(f"弹出磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/eod")
async def seek_eod():
    """定位到数据末尾"""
    try:
        tape_op = await get_linux_tape_operator()
        success = await tape_op.eod()
        return {"success": success}
    except Exception as e:
        logger.error(f"定位到数据末尾失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/linux/tell")
async def tell_position():
    """获取当前位置"""
    try:
        tape_op = await get_linux_tape_operator()
        block = await tape_op.tell()
        return {
            "success": block is not None,
            "block": block
        }
    except Exception as e:
        logger.error(f"获取当前位置失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/compression")
async def set_compression(request: CompressionRequest):
    """设置压缩"""
    try:
        tape_op = await get_linux_tape_operator()
        success = await tape_op.setcompression(request.enable)
        return {"success": success}
    except Exception as e:
        logger.error(f"设置压缩失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"设置压缩失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/setblk")
async def set_block_size(request: SetblkRequest):
    """设置块大小"""
    try:
        operator = get_tape_operator()
        result = operator.setblk_sync(block_size=request.block_size)
        return result
    except Exception as e:
        logger.error(f"设置块大小失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== LTFS 工具API =====
@router.get("/linux/devices")
async def scan_tape_devices():
    """扫描磁带设备"""
    try:
        devices = await LinuxTapeOperator.scan_devices()
        return {
            "success": True,
            "devices": devices,
            "count": len(devices)
        }
    except Exception as e:
        logger.error(f"扫描磁带设备失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/mkltfs")
async def mkltfs_format(request: MkltfsRequest, db = Depends(get_db)):
    """使用 mkltfs 格式化磁带"""
    try:
        # 查找 mkltfs 路径
        mkltfs_path = shutil.which('mkltfs')
        if not mkltfs_path:
            mkltfs_path = '/usr/local/bin/mkltfs'
            if not os.path.exists(mkltfs_path):
                return {
                    "success": False,
                    "stderr": "mkltfs 命令未找到，请安装 LTFS 工具",
                    "returncode": -1
                }

        # 构建命令
        cmd = [mkltfs_path, '-d', request.device_path, '-f']

        # 添加序列号
        if request.serial_number and len(request.serial_number) == 6 and request.serial_number.isalnum():
            cmd.extend(['-s', request.serial_number.upper()])

        # 添加卷标
        if request.volume_label:
            cmd.extend(['-n', request.volume_label])

        logger.info(f"执行命令: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=3600,
            text=False
        )

        stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
        stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

        # 记录日志
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "LTFS格式化",
            result.returncode == 0,
            {"device_path": request.device_path, "volume_label": request.volume_label},
            stderr if result.returncode != 0 else None
        )

        return {
            "success": result.returncode == 0,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode,
            "command": ' '.join(cmd)
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "stderr": "命令超时（3600秒）", "returncode": -1}
    except Exception as e:
        logger.error(f"mkltfs 格式化失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/ltfs-mount")
async def mount_ltfs(request: LtfsMountRequest, db = Depends(get_db)):
    """挂载 LTFS 文件系统"""
    try:
        # 查找 ltfs 路径
        ltfs_path = shutil.which('ltfs')
        if not ltfs_path:
            ltfs_path = '/usr/local/bin/ltfs'
            if not os.path.exists(ltfs_path):
                return {
                    "success": False,
                    "stderr": "ltfs 命令未找到，请安装 LTFS 工具",
                    "returncode": -1
                }

        # 确保挂载点存在
        os.makedirs(request.mount_point, exist_ok=True)

        # 检查并清空挂载点目录（FUSE 要求挂载点为空）
        if os.path.ismount(request.mount_point):
            logger.info(f"目录已挂载，跳过清空: {request.mount_point}")
        else:
            try:
                import shutil
                for item in os.listdir(request.mount_point):
                    item_path = os.path.join(request.mount_point, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                logger.info(f"已清空挂载点目录: {request.mount_point}")
            except Exception as e:
                logger.warning(f"清空挂载点目录失败: {e}")

        # 构建命令
        cmd = [ltfs_path, request.mount_point, '-o', f'devname={request.device_path}']

        # 同步模式
        if request.sync_mode:
            cmd.append('-o')
            cmd.append('sync')

        logger.info(f"执行命令: {' '.join(cmd)}")

        # LTFS 挂载是前台进程，需要后台运行
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=60,  # 挂载超时60秒
            text=False
        )

        stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
        stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

        # 记录日志
        await log_tool_operation(
            db, OperationType.TAPE_MOUNT, "LTFS挂载",
            result.returncode == 0,
            {"device_path": request.device_path, "mount_point": request.mount_point},
            stderr if result.returncode != 0 else None
        )

        return {
            "success": result.returncode == 0,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode,
            "command": ' '.join(cmd)
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "stderr": "挂载超时", "returncode": -1}
    except Exception as e:
        logger.error(f"LTFS 挂载失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/linux/ltfs-unmount")
async def unmount_ltfs(request: LtfsUnmountRequest, db = Depends(get_db)):
    """卸载 LTFS 文件系统"""
    try:
        # 尝试使用 fusermount（FUSE）
        fusermount_path = shutil.which('fusermount')

        if fusermount_path:
            cmd = [fusermount_path, '-u', request.mount_point]
        else:
            # 使用 umount
            cmd = ['umount', request.mount_point]

        logger.info(f"执行命令: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=60,
            text=False
        )

        stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
        stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

        success = result.returncode == 0

        # 如果卸载成功且需要弹出
        if success and request.eject_after:
            settings = get_settings()
            device = getattr(settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
            eject_result = subprocess.run(
                ['mt', '-f', device, 'eject'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=120,
                text=False
            )
            if eject_result.returncode != 0:
                stderr += f"\n弹出磁带失败: {eject_result.stderr.decode('utf-8', errors='ignore')}"

        # 记录日志
        await log_tool_operation(
            db, OperationType.TAPE_UNMOUNT, "LTFS卸载",
            success,
            {"mount_point": request.mount_point, "eject_after": request.eject_after},
            stderr if not success else None
        )

        return {
            "success": success,
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "stderr": "卸载超时", "returncode": -1}
    except Exception as e:
        logger.error(f"LTFS 卸载失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/linux/ltfs-check")
async def check_ltfs_mount():
    """检查 LTFS 挂载状态"""
    try:
        settings = get_settings()
        mount_point = getattr(settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')

        # 检查挂载点是否存在且是挂载点
        if not os.path.exists(mount_point):
            return {
                "success": True,
                "mounted": False,
                "mount_point": mount_point
            }

        # 使用 mount 命令检查
        result = subprocess.run(
            ['mountpoint', '-q', mount_point],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        is_mounted = result.returncode == 0

        # 如果已挂载，尝试读取卷标
        volume_name = None
        if is_mounted:
            # 尝试从 LTFS 挂载点读取卷标
            label_file = os.path.join(mount_point, '.ltfs_label')
            if os.path.exists(label_file):
                try:
                    with open(label_file, 'r') as f:
                        volume_name = f.read().strip()
                except Exception:
                    pass

        return {
            "success": True,
            "mounted": is_mounted,
            "mount_point": mount_point,
            "volume_name": volume_name
        }
    except Exception as e:
        logger.error(f"检查 LTFS 挂载状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ===== 组合流程API =====
@router.post("/linux/prepare")
async def prepare_tape(request: PrepareTapeRequest, db = Depends(get_db)):
    """准备磁带（检查状态、格式化、倒带）"""
    try:
        operator = get_tape_operator()

        # 确定LTFS设备路径
        ltfs_device = request.ltfs_device
        if not ltfs_device:
            settings = get_settings()
            ltfs_device = getattr(settings, 'LTFS_DEVICE_PATH', None)

        # 执行准备流程
        result = operator.check_and_prepare_tape_sync(
            device_path=request.device_path,
            force_erase=request.force_erase,
            progress_callback=None,
            ltfs_label=request.volume_label
        )

        # 如果指定了不同的LTFS设备，使用它来格式化
        if ltfs_device and result.get("was_erased"):
            # 使用指定的LTFS设备进行格式化
            format_result = operator.erase_sync(
                device_path=ltfs_device,
                ltfs_label=request.volume_label,
                timeout=300
            )
            result["ltfs_format_result"] = format_result

        # 记录日志
        await log_tool_operation(
            db, OperationType.TAPE_FORMAT, "准备磁带",
            result.get("success", False),
            {
                "device_path": request.device_path,
                "ltfs_device": ltfs_device,
                "force_erase": request.force_erase,
                "was_erased": result.get("was_erased")
            },
            result.get("message") if not result.get("success") else None
        )

        return result
    except Exception as e:
        logger.error(f"准备磁带失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/linux/label")
async def read_tape_label():
    """读取磁带卷标"""
    try:
        settings = get_settings()
        mount_point = getattr(settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')

        result = {
            "success": False,
            "volume_name": None,
            "serial_number": None,
            "barcode": None
        }

        # 检查是否已挂载
        mount_check = subprocess.run(
            ['mountpoint', '-q', mount_point],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        if mount_check.returncode == 0:
            # 已挂载，尝试从 LTFS 文件系统读取
            # 读取卷标
            label_file = os.path.join(mount_point, '.ltfs_label')
            if os.path.exists(label_file):
                try:
                    with open(label_file, 'r') as f:
                        result["volume_name"] = f.read().strip()
                except Exception:
                    pass

            # 尝试读取 LTFS 配置
            ltfs_conf = os.path.join(mount_point, 'LTFSConf.xml')
            if os.path.exists(ltfs_conf):
                try:
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(ltfs_conf)
                    root = tree.getroot()
                    # 提取卷标和序列号
                    ns = {'ltfs': 'http://www.linustech.org/ltfs/1.2'}
                    volname = root.find('.//ltfs:volumeName', ns)
                    if volname is not None:
                        result["volume_name"] = volname.text
                    serial = root.find('.//ltfs:serialNumber', ns)
                    if serial is not None:
                        result["serial_number"] = serial.text
                except Exception:
                    pass

            result["success"] = True
            result["raw_output"] = f"挂载点: {mount_point}"
        else:
            # 未挂载，使用 mt status 获取基本信息
            device = getattr(settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
            mt_result = subprocess.run(
                ['mt', '-f', device, 'status'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=30,
                text=False
            )

            stdout = mt_result.stdout.decode('utf-8', errors='ignore') if mt_result.stdout else ""
            result["raw_output"] = stdout
            result["success"] = mt_result.returncode == 0

            if not result["success"]:
                result["error"] = "磁带未挂载，无法读取卷标。请先挂载 LTFS。"

        return result
    except subprocess.TimeoutExpired:
        return {"success": False, "error": "命令超时"}
    except Exception as e:
        logger.error(f"读取磁带卷标失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
