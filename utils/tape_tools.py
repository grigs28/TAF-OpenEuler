#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带工具模块 - Linux版本
Tape Tools Module - Linux LTFS Mode
"""

import os
import asyncio
import subprocess
import logging
import shutil
from typing import Optional, Dict, Any, List
from pathlib import Path
from datetime import datetime

from config.settings import get_settings
from utils.linux_tape import get_linux_tape_operator, LinuxTapeOperator

logger = logging.getLogger(__name__)


class TapeToolsManager:
    """磁带工具管理器 - Linux LTFS 模式"""

    def __init__(self):
        self.settings = get_settings()
        self.device_path = getattr(self.settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
        self.ltfs_device = getattr(self.settings, 'LTFS_DEVICE_PATH', '/dev/sg3')
        self.ltfs_mount_point = getattr(self.settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')
        self.mkltfs_path = self._find_mkltfs()
        self.ltfs_path = self._find_ltfs()

    def _find_mkltfs(self) -> str:
        """查找 mkltfs 路径"""
        path = shutil.which('mkltfs')
        if path:
            return path
        for p in ['/usr/local/bin/mkltfs', '/usr/bin/mkltfs']:
            if os.path.exists(p):
                return p
        return 'mkltfs'

    def _find_ltfs(self) -> str:
        """查找 ltfs 路径"""
        path = shutil.which('ltfs')
        if path:
            return path
        for p in ['/usr/local/bin/ltfs', '/usr/bin/ltfs']:
            if os.path.exists(p):
                return p
        return 'ltfs'

    async def run_command(self, cmd: List[str], timeout: int = 300) -> Dict[str, Any]:
        """运行命令并返回结果"""
        cmd_str = ' '.join(cmd)
        logger.info(f"[Linux] 执行命令: {cmd_str}")

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout
            )

            stdout_str = stdout.decode('utf-8', errors='ignore') if stdout else ''
            stderr_str = stderr.decode('utf-8', errors='ignore') if stderr else ''

            success = process.returncode == 0

            return {
                "success": success,
                "returncode": process.returncode,
                "stdout": stdout_str,
                "stderr": stderr_str,
                "command": cmd_str
            }
        except asyncio.TimeoutError:
            logger.error(f"[Linux] 命令执行超时 ({timeout}s)")
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": f"命令执行超时 ({timeout}s)",
                "command": cmd_str
            }
        except Exception as e:
            logger.error(f"[Linux] 命令执行异常: {str(e)}")
            return {
                "success": False,
                "returncode": -1,
                "stdout": "",
                "stderr": str(e),
                "command": cmd_str
            }

    # ===== LTFS 格式化 =====
    async def format_tape_ltfs(
        self,
        drive_letter: Optional[str] = None,
        volume_label: Optional[str] = None,
        serial: Optional[str] = None,
        eject_after: bool = False
    ) -> Dict[str, Any]:
        """使用 mkltfs 格式化磁带"""
        logger.info(f"使用 mkltfs 格式化磁带: {self.ltfs_device}")

        cmd = [self.mkltfs_path, '-d', self.ltfs_device, '-f']

        if volume_label:
            cmd.extend(['-n', volume_label])

        if serial and len(serial) == 6 and serial.isalnum():
            cmd.extend(['-s', serial.upper()])

        result = await self.run_command(cmd, timeout=600)

        if result.get("success") and eject_after:
            operator = get_linux_tape_operator()
            await operator.eject()

        return result

    def format_tape_ltfs_sync(
        self,
        drive_letter: Optional[str] = None,
        volume_label: Optional[str] = None,
        serial: Optional[str] = None,
        eject_after: bool = False
    ) -> Dict[str, Any]:
        """使用 mkltfs 格式化磁带（同步版本）"""
        logger.info(f"[同步] 使用 mkltfs 格式化磁带: {self.ltfs_device}")

        cmd = [self.mkltfs_path, '-d', self.ltfs_device, '-f']

        if volume_label:
            cmd.extend(['-n', volume_label])

        if serial and len(serial) == 6 and serial.isalnum():
            cmd.extend(['-s', serial.upper()])

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=600,
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
            return {
                "success": False,
                "stdout": "",
                "stderr": "命令执行超时",
                "returncode": -1
            }
        except Exception as e:
            return {
                "success": False,
                "stdout": "",
                "stderr": str(e),
                "returncode": -1
            }

    def format_tape_mkltfs_sync(
        self,
        device_path: str,
        volume_label: Optional[str] = None,
        serial_number: Optional[str] = None,
        force: bool = True
    ) -> Dict[str, Any]:
        """使用 mkltfs 格式化磁带（同步版本，指定设备路径）"""
        logger.info(f"[同步] 使用 mkltfs 格式化磁带: {device_path}")

        cmd = [self.mkltfs_path, '-d', device_path, '-f']

        if volume_label:
            cmd.extend(['-n', volume_label])

        if serial_number and len(serial_number) == 6 and serial_number.isalnum():
            cmd.extend(['-s', serial_number.upper()])

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=600,
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
            return {
                "success": False,
                "stdout": "",
                "stderr": "命令执行超时",
                "returncode": -1
            }
        except Exception as e:
            return {
                "success": False,
                "stdout": "",
                "stderr": str(e),
                "returncode": -1
            }

    # ===== 卷标读取 =====
    async def read_tape_label_windows(self) -> Dict[str, Any]:
        """读取磁带卷标（兼容Windows命名）"""
        return await self.read_tape_label()

    async def read_tape_label(self) -> Dict[str, Any]:
        """读取磁带卷标"""
        logger.info("读取磁带卷标...")

        if os.path.ismount(self.ltfs_mount_point):
            result = {
                "success": True,
                "volume_name": None,
                "serial_number": None,
                "file_system": "LTFS"
            }

            ltfs_conf = os.path.join(self.ltfs_mount_point, 'LTFSConf.xml')
            if os.path.exists(ltfs_conf):
                try:
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(ltfs_conf)
                    root = tree.getroot()
                    ns = {'ltfs': 'http://www.linustech.org/ltfs/1.2'}
                    volname = root.find('.//ltfs:volumeName', ns)
                    if volname is not None:
                        result["volume_name"] = volname.text
                    serial = root.find('.//ltfs:serialNumber', ns)
                    if serial is not None:
                        result["serial_number"] = serial.text
                except Exception as e:
                    logger.warning(f"解析 LTFS 配置失败: {e}")

            return result
        else:
            try:
                process = await asyncio.create_subprocess_exec(
                    'mt', '-f', self.device_path, 'status',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)

                stdout_str = stdout.decode('utf-8', errors='ignore') if stdout else ""

                return {
                    "success": process.returncode == 0,
                    "volume_name": None,
                    "serial_number": None,
                    "file_system": "Unknown",
                    "raw_output": stdout_str,
                    "error": "磁带未挂载，请先挂载 LTFS" if process.returncode != 0 else None
                }
            except Exception as e:
                return {
                    "success": False,
                    "volume_name": None,
                    "serial_number": None,
                    "error": str(e)
                }

    def read_tape_label_windows_sync(self) -> Dict[str, Any]:
        """读取磁带卷标（同步版本）"""
        logger.info("[同步] 读取磁带卷标...")

        if os.path.ismount(self.ltfs_mount_point):
            result = {
                "success": True,
                "volume_name": None,
                "serial_number": None,
                "file_system": "LTFS"
            }

            ltfs_conf = os.path.join(self.ltfs_mount_point, 'LTFSConf.xml')
            if os.path.exists(ltfs_conf):
                try:
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(ltfs_conf)
                    root = tree.getroot()
                    ns = {'ltfs': 'http://www.linustech.org/ltfs/1.2'}
                    volname = root.find('.//ltfs:volumeName', ns)
                    if volname is not None:
                        result["volume_name"] = volname.text
                    serial = root.find('.//ltfs:serialNumber', ns)
                    if serial is not None:
                        result["serial_number"] = serial.text
                except Exception as e:
                    logger.warning(f"解析 LTFS 配置失败: {e}")

            return result
        else:
            try:
                result = subprocess.run(
                    ['mt', '-f', self.device_path, 'status'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    timeout=30,
                    text=False
                )

                stdout_str = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""

                return {
                    "success": result.returncode == 0,
                    "volume_name": None,
                    "serial_number": None,
                    "file_system": "Unknown",
                    "raw_output": stdout_str
                }
            except Exception as e:
                return {
                    "success": False,
                    "volume_name": None,
                    "serial_number": None,
                    "error": str(e)
                }

    # ===== LTFS 挂载/卸载 =====
    async def mount_ltfs(self, mount_point: str = None, sync_mode: bool = False) -> Dict[str, Any]:
        """挂载 LTFS 文件系统"""
        mount_point = mount_point or self.ltfs_mount_point
        logger.info(f"挂载 LTFS: {self.ltfs_device} -> {mount_point}")

        os.makedirs(mount_point, exist_ok=True)

        # 检查并清空挂载点目录（FUSE 要求挂载点为空）
        if os.path.ismount(mount_point):
            logger.info(f"目录已挂载，跳过清空: {mount_point}")
        else:
            # 清空目录内容
            try:
                for item in os.listdir(mount_point):
                    item_path = os.path.join(mount_point, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                logger.info(f"已清空挂载点目录: {mount_point}")
            except Exception as e:
                logger.warning(f"清空挂载点目录失败: {e}")

        cmd = [self.ltfs_path, mount_point, '-o', f'devname={self.ltfs_device}']

        if sync_mode:
            cmd.append('sync')

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            await asyncio.sleep(2)

            if os.path.ismount(mount_point):
                return {
                    "success": True,
                    "mount_point": mount_point,
                    "message": "LTFS 挂载成功"
                }
            else:
                try:
                    stdout, stderr = await asyncio.wait_for(
                        process.communicate(),
                        timeout=5
                    )
                    return {
                        "success": False,
                        "stderr": stderr.decode('utf-8', errors='ignore') if stderr else "挂载失败",
                        "stdout": stdout.decode('utf-8', errors='ignore') if stdout else ""
                    }
                except asyncio.TimeoutError:
                    return {
                        "success": False,
                        "stderr": "挂载超时"
                    }
        except Exception as e:
            return {
                "success": False,
                "stderr": str(e)
            }

    async def unmount_ltfs(self, mount_point: str = None) -> Dict[str, Any]:
        """卸载 LTFS 文件系统"""
        mount_point = mount_point or self.ltfs_mount_point
        logger.info(f"卸载 LTFS: {mount_point}")

        try:
            process = await asyncio.create_subprocess_exec(
                'fusermount', '-u', mount_point,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode == 0:
                return {
                    "success": True,
                    "message": "LTFS 卸载成功"
                }
        except FileNotFoundError:
            pass

        try:
            process = await asyncio.create_subprocess_exec(
                'umount', mount_point,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            return {
                "success": process.returncode == 0,
                "stderr": stderr.decode('utf-8', errors='ignore') if process.returncode != 0 else None
            }
        except Exception as e:
            return {
                "success": False,
                "stderr": str(e)
            }

    # ===== 设备扫描 =====
    async def list_drives_ltfs(self) -> Dict[str, Any]:
        """列出 LTFS 驱动器"""
        logger.info("列出磁带设备...")

        devices = await LinuxTapeOperator.scan_devices()

        return {
            "success": True,
            "drives": devices,
            "drive_count": len(devices)
        }

    # ===== 工具检查 =====
    def check_tools_availability(self) -> Dict[str, Any]:
        """检查工具可用性"""
        result = {
            "mt_available": shutil.which('mt') is not None,
            "mt_path": shutil.which('mt') or '/usr/bin/mt',
            "ltfs_tools": {}
        }

        for tool in ['mkltfs', 'ltfs']:
            path = shutil.which(tool)
            if not path:
                for p in [f'/usr/local/bin/{tool}', f'/usr/bin/{tool}']:
                    if os.path.exists(p):
                        path = p
                        break
            result["ltfs_tools"][tool] = {
                "available": path is not None and os.path.exists(path) if path else False,
                "path": path or f"/usr/local/bin/{tool}"
            }

        return result

    # ===== 组合流程 =====
    async def mount_tape_complete(
        self,
        drive_id: str,
        volume_label: Optional[str] = None,
        format_tape: bool = True
    ) -> Dict[str, Any]:
        """完整的磁带挂载流程"""
        logger.info(f"开始完整磁带挂载流程: {drive_id}")

        steps = []

        if format_tape:
            logger.info("步骤1: 格式化磁带...")
            if not volume_label:
                volume_label = self._generate_default_label()

            format_result = await self.format_tape_ltfs(volume_label=volume_label)
            steps.append({
                "step": "format",
                "success": format_result.get("success", False),
                "message": f"格式化磁带 (卷标: {volume_label})"
            })

            if not format_result.get("success"):
                return {
                    "success": False,
                    "error": "格式化失败",
                    "steps": steps
                }

        logger.info(f"步骤2: 挂载 LTFS...")
        mount_result = await self.mount_ltfs()
        steps.append({
            "step": "mount",
            "success": mount_result.get("success", False),
            "message": f"挂载到 {self.ltfs_mount_point}"
        })

        if not mount_result.get("success"):
            return {
                "success": False,
                "error": "挂载失败",
                "steps": steps
            }

        logger.info("完整挂载流程成功！")
        return {
            "success": True,
            "message": f"磁带已成功挂载到 {self.ltfs_mount_point}",
            "steps": steps
        }

    async def unmount_tape_complete(self, drive_id: str) -> Dict[str, Any]:
        """完整的磁带卸载流程"""
        logger.info(f"开始完整磁带卸载流程: {drive_id}")

        steps = []

        logger.info("步骤1: 卸载 LTFS...")
        unmount_result = await self.unmount_ltfs()
        steps.append({
            "step": "unmount",
            "success": unmount_result.get("success", False),
            "message": "卸载 LTFS"
        })

        logger.info("步骤2: 弹出磁带...")
        operator = get_linux_tape_operator()
        eject_success = await operator.eject()
        steps.append({
            "step": "eject",
            "success": eject_success,
            "message": "弹出磁带"
        })

        overall_success = all(step["success"] for step in steps)

        return {
            "success": overall_success,
            "message": "磁带已完全卸载" if overall_success else "卸载过程部分失败",
            "steps": steps
        }

    def _generate_default_label(self) -> str:
        """生成默认卷标（基于日期）"""
        now = datetime.now()
        return f"TP{now.strftime('%Y%m%d')}"


# 全局工具管理器实例
tape_tools_manager = TapeToolsManager()
