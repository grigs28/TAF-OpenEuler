#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带直接恢复引擎
Tape-based Recovery Engine

直接从 LTFS 磁带文件系统恢复，不依赖数据库。

磁带文件系统布局（LTFS 挂载点）：
  /mnt/ltfs/{backup_set_id}/backup_{set_id}_{timestamp}_{seq}.tar.zst

恢复流程：
  1. 检测磁带状态
  2. 挂载 LTFS
  3. 扫描磁带发现备份集
  4. 列出备份集/归档内容
  5. 执行恢复（提取文件到目标路径）
"""

import asyncio
import json
import subprocess
import logging
import os
import re
import shutil
import tarfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional, Set, Any

from config.settings import get_settings

logger = logging.getLogger(__name__)

# 可选依赖
try:
    import zstandard as zstd
except ImportError:
    zstd = None

try:
    import py7zr
except ImportError:
    py7zr = None


class TapeRecoveryEngine:
    """磁带直接恢复引擎 - 从 LTFS 文件系统直接恢复，不依赖数据库"""

    # 系统目录（扫描时忽略）
    SYSTEM_DIRS = {
        '.LTFS', 'lost+found', '.Trash', '.Trashes', '.Spotlight-V100',
        '$RECYCLE.BIN', 'System Volume Information', 'RECYCLER',
    }

    # 归档文件名解析正则
    # backup_2025-10_000001_20251001_020000.tar.zst
    # backup_2025-10_000001_20251001_020000_0001.tar.zst
    ARCHIVE_PATTERN = re.compile(
        r'^backup_(.+?)_(\d{8}_\d{6})(?:_(\d{4}))?'
        r'\.(tar\.zst|tar\.gz|tgz|tar|7z|zip)$'
    )

    # 备份集目录名格式: YYYY-MM_NNNNNN
    SET_ID_PATTERN = re.compile(r'^\d{4}-\d{2}_\d{6}$')

    # 压缩后缀 -> 类型映射
    SUFFIX_MAP = {
        '.tar.zst': 'zstd',
        '.tar.gz': 'gzip',
        '.tgz': 'gzip',
        '.tar': 'tar',
        '.7z': '7z',
        '.zip': 'zip',
    }

    def __init__(self):
        self.settings = get_settings()
        self.tape_handler = None
        self.dingtalk_notifier = None
        self._initialized = False

        # 恢复任务存储（内存）
        self._recovery_tasks: Dict[str, Dict] = {}
        self._cancel_requested: Set[str] = set()

    def set_dependencies(self, tape_handler, dingtalk_notifier=None):
        """注入依赖"""
        self.tape_handler = tape_handler
        self.dingtalk_notifier = dingtalk_notifier
        logger.info("[磁带恢复] 依赖已注入")

    async def initialize(self):
        """初始化恢复引擎"""
        if self._initialized:
            return
        recovery_tmp = Path(self.settings.BACKUP_COMPRESS_DIR) / "recovery_tmp"
        recovery_tmp.mkdir(parents=True, exist_ok=True)
        self._recovery_tmp = recovery_tmp
        self._initialized = True
        logger.info("[磁带恢复] 引擎初始化完成")

    # ================================================================
    # Step 1: 磁带检测
    # ================================================================

    async def get_tape_status(self) -> Dict[str, Any]:
        """检查磁带状态（是否加载、是否已挂载 LTFS）"""
        result = {
            "tape_loaded": False,
            "tape_label": None,
            "device": "",
            "ltfs_mounted": False,
            "mount_point": None,
            "status_message": "未知",
        }

        try:
            if self.tape_handler:
                in_drive, drive_msg = await self.tape_handler.check_tape_in_drive()
                result["tape_loaded"] = in_drive
                result["status_message"] = drive_msg
                result["device"] = self.tape_handler._get_tape_device()
            else:
                result["device"] = getattr(self.settings, 'TAPE_DRIVE_LETTER', '/dev/nst0')
                proc = await asyncio.create_subprocess_exec(
                    'mt', '-f', result["device"], 'status',
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
                output = (stdout + stderr).decode('utf-8', errors='replace')
                if 'ONLINE' in output:
                    result["tape_loaded"] = True
                    result["status_message"] = "磁带已加载"
                elif 'DR_OPEN' in output or 'No tape' in output:
                    result["status_message"] = "驱动器中没有磁带"
                else:
                    result["status_message"] = "状态未知"

            mount_point = self._get_mount_point()
            result["mount_point"] = str(mount_point)
            if mount_point.exists() and mount_point.is_mount():
                result["ltfs_mounted"] = True
                result["tape_label"] = await self._read_tape_label(mount_point)

        except Exception as e:
            logger.error(f"[磁带恢复] 检测磁带状态失败: {e}")
            result["status_message"] = f"检测失败: {str(e)}"

        return result

    async def _read_tape_label(self, mount_point: Path) -> Optional[str]:
        """从 LTFS 元数据或 tape_handler 读取磁带卷标"""
        try:
            if self.tape_handler and self.tape_handler.tape_manager:
                tape_ops = self.tape_handler.tape_manager.tape_operations
                if tape_ops and hasattr(tape_ops, '_read_tape_label'):
                    label_info = await tape_ops._read_tape_label()
                    if label_info and label_info.get('tape_id'):
                        return label_info['tape_id']
        except Exception:
            pass
        return None

    async def mount_tape(self, max_retries: int = 3, retry_interval: int = 30) -> Dict[str, Any]:
        """挂载 LTFS 文件系统"""
        try:
            if self.tape_handler:
                success, msg = await self.tape_handler.mount_with_retry(
                    max_retries=max_retries,
                    retry_interval=retry_interval,
                )
                mount_point = str(self.tape_handler._get_ltfs_mount_point()) if success else None
                return {"success": success, "message": msg, "mount_point": mount_point}
            else:
                return {"success": False, "message": "TapeHandler 未初始化", "mount_point": None}
        except Exception as e:
            logger.error(f"[磁带恢复] 挂载失败: {e}")
            return {"success": False, "message": str(e), "mount_point": None}

    async def unmount_tape(self) -> Dict[str, Any]:
        """卸载 LTFS 文件系统"""
        try:
            mount_point = self._get_mount_point()
            if not mount_point.exists() or not mount_point.is_mount():
                return {"success": True, "message": "LTFS 未挂载，无需卸载"}

            if self.tape_handler:
                result = await self.tape_handler.unmount_ltfs()
                # tape_handler.unmount_ltfs() 返回 bool
                if isinstance(result, bool):
                    return {"success": result, "message": "卸载成功" if result else "卸载失败"}
                # tape_tools.unmount_ltfs() 返回 dict
                return {"success": result.get("success", False), "message": result.get("message", result.get("stderr", "卸载完成"))}
            else:
                # 回退：直接使用系统命令卸载
                proc = await asyncio.create_subprocess_exec(
                    'umount', str(mount_point),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
                if proc.returncode == 0:
                    return {"success": True, "message": "卸载成功"}
                else:
                    err = stderr.decode('utf-8', errors='replace').strip()
                    return {"success": False, "message": f"卸载失败: {err}"}
        except Exception as e:
            logger.error(f"[磁带恢复] 卸载失败: {e}")
            return {"success": False, "message": str(e)}

    async def eject_tape(self) -> Dict[str, Any]:
        """弹出磁带（仅执行弹出操作，卸载需在前一步完成）"""
        try:
            if self.tape_handler and self.tape_handler.tape_manager:
                tape_ops = self.tape_handler.tape_manager.tape_operations
                if tape_ops and hasattr(tape_ops, 'eject'):
                    await tape_ops.eject()
                    await self._send_eject_notification(tape_id="unknown", success=True)
                    return {"success": True, "message": "磁带已弹出"}

            # 回退：使用 mt 命令弹出
            device = getattr(self.settings, 'TAPE_DRIVE_LETTER', '/dev/nst0')
            proc = await asyncio.create_subprocess_exec(
                'mt', '-f', device, 'eject',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
            if proc.returncode == 0:
                await self._send_eject_notification(tape_id="unknown", success=True)
                return {"success": True, "message": "磁带已弹出"}
            else:
                err = stderr.decode('utf-8', errors='replace').strip()
                await self._send_eject_notification(tape_id="unknown", success=False, error=err)
                return {"success": False, "message": f"弹出失败: {err}"}
        except asyncio.TimeoutError:
            await self._send_eject_notification(tape_id="unknown", success=False, error="弹出超时（120秒）")
            return {"success": False, "message": "弹出超时（120秒）"}
        except Exception as e:
            await self._send_eject_notification(tape_id="unknown", success=False, error=str(e))
            return {"success": False, "message": str(e)}

    # ================================================================
    # 流式操作（实时输出命令日志）
    # ================================================================

    async def mount_tape_streaming(self, max_retries: int = 3, retry_interval: int = 30) -> AsyncGenerator[Dict, None]:
        """挂载 LTFS，实时输出命令日志（async generator）"""
        tape_device = self._get_tape_device_path()
        mount_point = self._get_mount_point()
        ltfs_bin = self._find_ltfs_bin()

        yield {"type": "log", "message": f"设备: {tape_device}"}
        yield {"type": "log", "message": f"挂载点: {mount_point}"}

        if not ltfs_bin:
            yield {"type": "log", "message": "错误: 未找到 ltfs 命令"}
            yield {"type": "result", "success": False, "message": "未找到 ltfs 命令"}
            return

        # 清理旧挂载
        if mount_point.is_mount():
            yield {"type": "log", "message": "检测到旧挂载，先卸载..."}
            try:
                async for msg in self.unmount_tape_streaming():
                    yield msg
                await asyncio.sleep(2)
            except Exception as e:
                yield {"type": "log", "message": f"卸载旧挂载异常: {e}"}

        mount_point.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, max_retries + 1):
            yield {"type": "log", "message": f"--- 第 {attempt}/{max_retries} 次挂载尝试 ---"}

            # 清空挂载点
            try:
                for item in mount_point.iterdir():
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
            except Exception as e:
                yield {"type": "log", "message": f"清理挂载点: {e}"}

            cmd_str = f"{ltfs_bin} -o devname={tape_device} {mount_point}"
            yield {"type": "log", "message": f"$ {cmd_str}"}

            try:
                process = await asyncio.create_subprocess_exec(
                    ltfs_bin, '-o', f'devname={tape_device}', str(mount_point),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                # 使用队列收集 stderr 输出
                output_queue = asyncio.Queue()

                async def _read_stderr():
                    """逐行读取 stderr"""
                    try:
                        while True:
                            line = await process.stderr.readline()
                            if not line:
                                break
                            text = line.decode('utf-8', errors='replace').rstrip()
                            if text:
                                await output_queue.put(text)
                    except Exception:
                        pass

                read_task = asyncio.create_task(_read_stderr())
                mounted = False

                # 等待挂载（最多60秒检查 + 30秒二次检查）
                for wait_time in range(60):
                    # 先 yield 队列中的输出
                    while not output_queue.empty():
                        try:
                            text = output_queue.get_nowait()
                            yield {"type": "log", "message": text}
                        except asyncio.QueueEmpty:
                            break

                    await asyncio.sleep(1)

                    # 进程已退出
                    if process.returncode is not None:
                        break

                    if mount_point.is_mount():
                        mounted = True
                        break

                # 如果还没挂载，再等30秒
                if not mounted and process.returncode is None:
                    yield {"type": "log", "message": "初始等待未检测到挂载，继续等待..."}
                    for wait_time in range(30):
                        while not output_queue.empty():
                            try:
                                text = output_queue.get_nowait()
                                yield {"type": "log", "message": text}
                            except asyncio.QueueEmpty:
                                break

                        await asyncio.sleep(1)

                        if process.returncode is not None:
                            break

                        if mount_point.is_mount():
                            mounted = True
                            break

                # 排空剩余输出
                try:
                    await asyncio.wait_for(read_task, timeout=5.0)
                except asyncio.TimeoutError:
                    pass

                while not output_queue.empty():
                    try:
                        text = output_queue.get_nowait()
                        yield {"type": "log", "message": text}
                    except asyncio.QueueEmpty:
                        break

                # 读取 stdout（通常为空）
                if process.returncode is not None:
                    try:
                        stdout_data = await asyncio.wait_for(process.stdout.read(), timeout=2.0)
                        if stdout_data:
                            for line in stdout_data.decode('utf-8', errors='replace').splitlines():
                                if line.strip():
                                    yield {"type": "log", "message": line.strip()}
                    except (asyncio.TimeoutError, Exception):
                        pass

                if mounted:
                    self._update_tape_handler_state(mounted=True, mount_point=mount_point, process=process)
                    yield {"type": "log", "message": f"挂载成功（第{attempt}次尝试）"}
                    yield {"type": "result", "success": True, "message": f"挂载成功（第{attempt}次尝试）", "mount_point": str(mount_point)}
                    return

                # 挂载失败
                if process.returncode is not None:
                    yield {"type": "log", "message": f"LTFS 进程已退出，返回码: {process.returncode}"}
                else:
                    yield {"type": "log", "message": "挂载超时，终止 LTFS 进程"}
                    process.kill()
                    await process.wait()

                if attempt < max_retries:
                    yield {"type": "log", "message": f"等待 {retry_interval} 秒后重试..."}
                    await asyncio.sleep(retry_interval)

            except Exception as e:
                yield {"type": "log", "message": f"挂载异常: {e}"}
                if attempt < max_retries:
                    yield {"type": "log", "message": f"等待 {retry_interval} 秒后重试..."}
                    await asyncio.sleep(retry_interval)

        yield {"type": "result", "success": False, "message": f"挂载失败（{max_retries}次尝试均失败）"}

    async def unmount_tape_streaming(self) -> AsyncGenerator[Dict, None]:
        """卸载 LTFS，实时输出命令日志（async generator）"""
        mount_point = self._get_mount_point()

        if not mount_point.exists() or not mount_point.is_mount():
            yield {"type": "log", "message": "LTFS 未挂载，无需卸载"}
            yield {"type": "result", "success": True, "message": "LTFS 未挂载，无需卸载"}
            return

        yield {"type": "log", "message": f"卸载挂载点: {mount_point}"}

        # 尝试 fusermount
        cmd = ['fusermount', '-u', str(mount_point)]
        yield {"type": "log", "message": f"$ {' '.join(cmd)}"}

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)

            if stdout:
                for line in stdout.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}
            if stderr:
                for line in stderr.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}

            if process.returncode == 0:
                self._update_tape_handler_state(mounted=False)
                yield {"type": "log", "message": "卸载成功"}
                yield {"type": "result", "success": True, "message": "卸载成功"}
                return
            else:
                yield {"type": "log", "message": f"fusermount 返回码: {process.returncode}，尝试 umount..."}
        except asyncio.TimeoutError:
            yield {"type": "log", "message": "fusermount 超时，尝试 umount..."}
        except FileNotFoundError:
            yield {"type": "log", "message": "fusermount 不可用，尝试 umount..."}

        # 回退到 umount
        cmd = ['umount', str(mount_point)]
        yield {"type": "log", "message": f"$ {' '.join(cmd)}"}

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)

            if stdout:
                for line in stdout.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}
            if stderr:
                for line in stderr.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}

            if process.returncode == 0:
                self._update_tape_handler_state(mounted=False)
                yield {"type": "log", "message": "卸载成功"}
                yield {"type": "result", "success": True, "message": "卸载成功"}
            else:
                err = stderr.decode('utf-8', errors='replace').strip() if stderr else f"返回码: {process.returncode}"
                yield {"type": "log", "message": f"卸载失败: {err}"}
                yield {"type": "result", "success": False, "message": f"卸载失败: {err}"}
        except Exception as e:
            yield {"type": "log", "message": f"卸载异常: {e}"}
            yield {"type": "result", "success": False, "message": str(e)}

    async def eject_tape_streaming(self) -> AsyncGenerator[Dict, None]:
        """弹出磁带，实时输出命令日志（async generator）"""
        device = self._get_tape_device_path()
        yield {"type": "log", "message": f"弹出磁带（设备: {device}）"}

        cmd = ['mt', '-f', device, 'eject']
        yield {"type": "log", "message": f"$ {' '.join(cmd)}"}

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=120)

            if stdout:
                for line in stdout.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}
            if stderr:
                for line in stderr.decode('utf-8', errors='replace').splitlines():
                    if line.strip():
                        yield {"type": "log", "message": line.strip()}

            if process.returncode == 0:
                await self._send_eject_notification(tape_id="unknown", success=True)
                yield {"type": "log", "message": "磁带已弹出"}
                yield {"type": "result", "success": True, "message": "磁带已弹出"}
            else:
                err = stderr.decode('utf-8', errors='replace').strip() if stderr else f"返回码: {process.returncode}"
                await self._send_eject_notification(tape_id="unknown", success=False, error=err)
                yield {"type": "log", "message": f"弹出失败: {err}"}
                yield {"type": "result", "success": False, "message": f"弹出失败: {err}"}
        except asyncio.TimeoutError:
            await self._send_eject_notification(tape_id="unknown", success=False, error="弹出超时（120秒）")
            yield {"type": "log", "message": "弹出超时（120秒）"}
            yield {"type": "result", "success": False, "message": "弹出超时（120秒）"}
        except Exception as e:
            await self._send_eject_notification(tape_id="unknown", success=False, error=str(e))
            yield {"type": "log", "message": f"弹出异常: {e}"}
            yield {"type": "result", "success": False, "message": str(e)}

    def _get_tape_device_path(self) -> str:
        """获取磁带设备路径"""
        if self.tape_handler:
            return self.tape_handler._get_ltfs_device()
        return getattr(self.settings, 'TAPE_DRIVE_LETTER', '/dev/nst0')

    def _find_ltfs_bin(self) -> str:
        """查找 ltfs 二进制路径"""
        # 优先从 tape_handler 获取
        if self.tape_handler:
            from backup.tape_handler import LTFS_BIN
            if os.path.exists(LTFS_BIN):
                return LTFS_BIN
        # 使用 which 查找
        ltfs_path = shutil.which('ltfs')
        if ltfs_path:
            return ltfs_path
        # 默认路径
        default = '/usr/local/bin/ltfs'
        if os.path.exists(default):
            return default
        return None

    def _update_tape_handler_state(self, mounted: bool, mount_point: Path = None, process=None):
        """更新 tape_handler 的挂载状态"""
        if not self.tape_handler:
            return
        self.tape_handler._ltfs_mounted = mounted
        if mounted:
            self.tape_handler._ltfs_mount_point = mount_point
            self.tape_handler._ltfs_process = process
        else:
            self.tape_handler._ltfs_process = None

    async def _send_eject_notification(self, tape_id: str, success: bool, error: str = None):
        """发送弹出磁带通知（异步）"""
        if not self.dingtalk_notifier:
            logger.debug(f"[磁带恢复] 钉钉通知器未配置，跳过弹出通知")
            return
        try:
            if success:
                await self.dingtalk_notifier.send_tape_notification(tape_id=tape_id, action="eject")
            else:
                await self.dingtalk_notifier.send_tape_notification(
                    tape_id=tape_id, action="eject_failed",
                    details={"error": error or "未知错误"}
                )
        except Exception as e:
            logger.warning(f"发送弹出通知失败: {e}")

    # ================================================================
    # Step 2: 扫描磁带内容
    # ================================================================

    async def scan_tape_contents(self) -> Dict[str, Any]:
        """扫描 LTFS 挂载点，发现所有备份集"""
        mount_point = self._get_mount_point()

        if not mount_point.exists() or not mount_point.is_mount():
            return {
                "success": False,
                "backup_sets": [],
                "total_backup_sets": 0,
                "total_archives": 0,
                "message": "LTFS 未挂载，请先挂载磁带",
            }

        try:
            backup_sets = []
            total_archives = 0

            for item in mount_point.iterdir():
                if not item.is_dir():
                    continue
                if item.name in self.SYSTEM_DIRS or item.name.startswith('.'):
                    continue

                set_id = item.name
                archives = []

                for archive_file in sorted(item.iterdir(), key=lambda f: f.name):
                    if not archive_file.is_file():
                        continue

                    file_size = archive_file.stat().st_size
                    parsed = self._parse_archive_filename(archive_file.name)

                    archive_info = {
                        "filename": archive_file.name,
                        "size_bytes": file_size,
                        "size_display": self._format_bytes(file_size),
                    }

                    if parsed:
                        archive_info.update({
                            "compression_type": parsed["compression_type"],
                            "timestamp": parsed["timestamp"],
                            "timestamp_display": self._format_timestamp(parsed["timestamp"]),
                            "sequence": parsed["sequence"],
                        })
                    else:
                        archive_info.update({
                            "compression_type": self._detect_compression_type(archive_file.name),
                            "timestamp": None,
                            "timestamp_display": None,
                            "sequence": None,
                        })

                    archives.append(archive_info)

                if not archives:
                    continue

                total_size = sum(a["size_bytes"] for a in archives)
                timestamps = [a["timestamp"] for a in archives if a.get("timestamp")]

                backup_sets.append({
                    "set_id": set_id,
                    "archive_count": len(archives),
                    "total_size_bytes": total_size,
                    "total_size_display": self._format_bytes(total_size),
                    "archives": archives,
                    "earliest_timestamp": self._format_timestamp(min(timestamps)) if timestamps else None,
                    "latest_timestamp": self._format_timestamp(max(timestamps)) if timestamps else None,
                })
                total_archives += len(archives)

            # 按时间排序（最新的在前）
            backup_sets.sort(key=lambda s: s.get("latest_timestamp") or "", reverse=True)

            return {
                "success": True,
                "backup_sets": backup_sets,
                "total_backup_sets": len(backup_sets),
                "total_archives": total_archives,
                "message": f"发现 {len(backup_sets)} 个备份集, {total_archives} 个归档文件",
            }

        except Exception as e:
            logger.error(f"[磁带恢复] 扫描磁带失败: {e}", exc_info=True)
            return {
                "success": False,
                "backup_sets": [],
                "total_backup_sets": 0,
                "total_archives": 0,
                "message": f"扫描失败: {str(e)}",
            }

    def _parse_archive_filename(self, filename: str) -> Optional[Dict]:
        """解析归档文件名，提取元数据"""
        import re as _re
        m = _re.match(
            r'^backup_(.+?)_(\d{8}_\d{6})(?:_(\d{4}))?'
            r'\.(tar\.zst|tar\.gz|tgz|tar|7z|zip)$',
            filename,
        )
        if not m:
            return None
        set_id, timestamp, seq, suffix = m.groups()
        return {
            "set_id": set_id,
            "timestamp": timestamp,
            "sequence": int(seq) if seq else 0,
            "suffix": f".{suffix}",
            "compression_type": self._detect_compression_type(filename),
        }

    def _detect_compression_type(self, filename: str) -> str:
        """根据文件名后缀检测压缩类型"""
        lower = filename.lower()
        for suffix in sorted(self.SUFFIX_MAP.keys(), key=len, reverse=True):
            if lower.endswith(suffix):
                return self.SUFFIX_MAP[suffix]
        return "unknown"

    @staticmethod
    def _format_timestamp(ts_str: str) -> Optional[str]:
        """将 '20251001_020000' 转为 '2025-10-01 02:00:00'"""
        if not ts_str:
            return None
        try:
            dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            return ts_str

    # ================================================================
    # Step 3: 归档内容列表
    # ================================================================

    async def list_archive_contents(self, set_id: str, archive_filename: str) -> Dict[str, Any]:
        """列出单个归档文件的内部文件列表（不提取）"""
        archive_path = self._get_mount_point() / set_id / archive_filename
        if not archive_path.exists():
            return {"archive": archive_filename, "entries": [], "total_entries": 0,
                    "total_files": 0, "total_dirs": 0, "error": "文件不存在"}

        try:
            entries = await asyncio.to_thread(self._list_archive_sync, archive_path)
            files = [e for e in entries if not e["is_dir"]]
            dirs = [e for e in entries if e["is_dir"]]
            return {
                "archive": archive_filename,
                "entries": entries,
                "total_entries": len(entries),
                "total_files": len(files),
                "total_dirs": len(dirs),
            }
        except Exception as e:
            logger.error(f"[磁带恢复] 列出归档内容失败: {e}")
            return {"archive": archive_filename, "entries": [], "total_entries": 0,
                    "total_files": 0, "total_dirs": 0, "error": str(e)}

    async def list_backup_set_contents(self, set_id: str) -> Dict[str, Any]:
        """列出备份集中所有归档的文件列表（按归档分组，保留同名文件）"""
        set_dir = self._get_mount_point() / set_id
        if not set_dir.exists():
            return {"set_id": set_id, "entries": [], "total_entries": 0,
                    "total_files": 0, "total_dirs": 0, "archives": [], "error": "目录不存在"}

        try:
            all_entries = []
            archive_files = sorted(
                [f for f in set_dir.iterdir() if f.is_file()],
                key=lambda f: f.name,
            )

            # 按归档分组返回，每个条目带有 source_archive 标识
            archive_summaries = []
            for archive_file in archive_files:
                entries = await asyncio.to_thread(self._list_archive_sync, archive_file)
                file_entries = [e for e in entries if not e["is_dir"]]
                dir_entries = [e for e in entries if e["is_dir"]]

                for entry in entries:
                    entry["source_archive"] = archive_file.name
                    all_entries.append(entry)

                archive_summaries.append({
                    "filename": archive_file.name,
                    "size_bytes": archive_file.stat().st_size,
                    "file_count": len(file_entries),
                    "dir_count": len(dir_entries),
                    "total_size": sum(e.get("size", 0) for e in file_entries),
                })

            files = [e for e in all_entries if not e["is_dir"]]
            dirs = [e for e in all_entries if e["is_dir"]]

            return {
                "set_id": set_id,
                "entries": all_entries,
                "archives": archive_summaries,
                "total_entries": len(all_entries),
                "total_files": len(files),
                "total_dirs": len(dirs),
            }
        except Exception as e:
            logger.error(f"[磁带恢复] 列出备份集内容失败: {e}")
            return {"set_id": set_id, "entries": [], "total_entries": 0,
                    "total_files": 0, "total_dirs": 0, "archives": [], "error": str(e)}

    def _list_archive_sync(self, archive_path: Path) -> List[Dict]:
        """同步列出归档内容（在线程池中运行）"""
        name = archive_path.name.lower()

        if name.endswith('.tar.zst'):
            return self._list_tar_zst(archive_path)
        elif name.endswith(('.tar.gz', '.tgz')):
            return self._list_tar(archive_path, 'r:gz')
        elif name.endswith('.tar'):
            return self._list_tar(archive_path, 'r:')
        elif name.endswith('.7z'):
            return self._list_7z(archive_path)
        elif name.endswith('.zip'):
            return self._list_zip(archive_path)
        else:
            return [{"name": archive_path.name, "size": archive_path.stat().st_size,
                     "is_dir": False, "modified_time": None}]

    def _list_tar(self, path: Path, mode: str) -> List[Dict]:
        """列出 tar 归档内容"""
        entries = []
        try:
            with tarfile.open(str(path), mode) as tar:
                for member in tar.getmembers():
                    entries.append({
                        "name": member.name,
                        "size": member.size if member.isfile() else 0,
                        "is_dir": member.isdir(),
                        "modified_time": datetime.fromtimestamp(member.mtime).isoformat() if member.mtime else None,
                    })
        except Exception as e:
            logger.error(f"[磁带恢复] 读取tar归档失败 {path.name}: {e}")
        return entries

    def _list_tar_zst(self, path: Path) -> List[Dict]:
        """列出 .tar.zst 归档内容（使用 zstandard 库流式解压）"""
        entries = []
        try:
            if zstd is not None:
                dctx = zstd.ZstdDecompressor()
                with open(str(path), 'rb') as fh:
                    with dctx.stream_reader(fh) as reader:
                        with tarfile.open(fileobj=reader, mode='r|') as tar:
                            for member in tar:
                                entries.append({
                                    "name": member.name,
                                    "size": member.size if member.isfile() else 0,
                                    "is_dir": member.isdir(),
                                    "modified_time": datetime.fromtimestamp(member.mtime).isoformat() if member.mtime else None,
                                })
            else:
                # 回退到 CLI
                proc = subprocess.Popen(
                    ['zstd', '-d', str(path), '--stdout'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
                    for member in tar:
                        entries.append({
                            "name": member.name,
                            "size": member.size if member.isfile() else 0,
                            "is_dir": member.isdir(),
                            "modified_time": datetime.fromtimestamp(member.mtime).isoformat() if member.mtime else None,
                        })
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        except Exception as e:
            logger.error(f"[磁带恢复] 读取tar.zst归档失败 {path.name}: {e}")
        return entries

    def _list_7z(self, path: Path) -> List[Dict]:
        """列出 .7z 归档内容"""
        if py7zr is None:
            logger.warning("[磁带恢复] py7zr 库未安装，无法读取 .7z")
            return []
        entries = []
        try:
            with py7zr.SevenZipFile(str(path), mode='r') as archive:
                for file_info in archive.list():
                    entries.append({
                        "name": file_info.filename or "",
                        "size": file_info.uncompressed if file_info.uncompressed else 0,
                        "is_dir": file_info.is_directory,
                        "modified_time": str(file_info.creationtime) if file_info.creationtime else None,
                    })
        except Exception as e:
            logger.error(f"[磁带恢复] 读取7z归档失败 {path.name}: {e}")
        return entries

    def _list_zip(self, path: Path) -> List[Dict]:
        """列出 .zip 归档内容"""
        entries = []
        try:
            with zipfile.ZipFile(str(path), 'r') as zf:
                for info in zf.infolist():
                    entries.append({
                        "name": info.filename,
                        "size": info.file_size,
                        "is_dir": info.is_dir(),
                        "modified_time": None,
                    })
        except Exception as e:
            logger.error(f"[磁带恢复] 读取zip归档失败 {path.name}: {e}")
        return entries

    # ================================================================
    # Step 4: 恢复执行
    # ================================================================

    async def create_recovery_task(
        self,
        set_id: str,
        target_path: str,
        overwrite: str = "skip",
        preserve_permissions: bool = True,
        verify_integrity: bool = True,
        selected_archives: Optional[List[str]] = None,
        selected_files: Optional[Dict[str, Optional[List[str]]]] = None,
    ) -> str:
        """创建恢复任务，返回 recovery_id

        Args:
            selected_files: 文件级选择，key=归档文件名, value=文件路径列表
                            value=None/缺少=恢复该归档全部文件
        """
        recovery_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

        target = Path(target_path)
        target.mkdir(parents=True, exist_ok=True)

        set_dir = self._get_mount_point() / set_id
        if not set_dir.exists():
            raise ValueError(f"备份集目录不存在: {set_id}")

        if selected_archives:
            archives = [str(set_dir / a) for a in selected_archives if (set_dir / a).exists()]
        elif selected_files:
            # 文件级选择模式：只恢复指定的归档
            archives = [str(set_dir / a) for a in selected_files.keys() if (set_dir / a).exists()]
        else:
            archives = sorted([str(f) for f in set_dir.iterdir() if f.is_file()],
                              key=lambda s: Path(s).name)

        # 规范化 selected_files：确保每个归档都有对应条目
        # None 或缺失 = 恢复全部文件；空列表 = 不恢复任何文件（跳过）
        normalized_selected_files = {}
        if selected_files:
            for archive_name, file_list in selected_files.items():
                if file_list is None or (set_dir / archive_name).exists() is False:
                    normalized_selected_files[archive_name] = None  # 全部
                else:
                    normalized_selected_files[archive_name] = file_list

        self._recovery_tasks[recovery_id] = {
            "recovery_id": recovery_id,
            "set_id": set_id,
            "target_path": str(target),
            "overwrite": overwrite,
            "preserve_permissions": preserve_permissions,
            "verify_integrity": verify_integrity,
            "archives": archives,
            "total_archives": len(archives),
            "selected_files": normalized_selected_files if normalized_selected_files else None,
            "status": "pending",
            "progress_percent": 0.0,
            "processed_archives": 0,
            "total_files": 0,
            "processed_files": 0,
            "total_bytes": 0,
            "processed_bytes": 0,
            "current_archive": None,
            "started_at": None,
            "completed_at": None,
            "error_message": None,
        }

        logger.info(f"[磁带恢复] 创建恢复任务: {recovery_id}, 备份集: {set_id}, "
                     f"归档数: {len(archives)}, 目标: {target_path}, "
                     f"文件级选择: {bool(normalized_selected_files)}")
        return recovery_id

    async def execute_recovery(self, recovery_id: str) -> bool:
        """执行恢复操作"""
        task = self._recovery_tasks.get(recovery_id)
        if not task:
            logger.error(f"[磁带恢复] 任务不存在: {recovery_id}")
            return False

        task["status"] = "running"
        task["started_at"] = datetime.now().isoformat()

        try:
            target_dir = Path(task["target_path"])
            archives = task["archives"]

            for i, archive_str in enumerate(archives):
                # 检查取消
                if recovery_id in self._cancel_requested:
                    task["status"] = "cancelled"
                    task["error_message"] = "用户取消恢复"
                    logger.info(f"[磁带恢复] 任务已取消: {recovery_id}")
                    return False

                archive_path = Path(archive_str)
                task["current_archive"] = archive_path.name

                # 获取该归档的文件过滤列表
                files_filter = None
                if task.get("selected_files"):
                    files_filter = task["selected_files"].get(archive_path.name)

                logger.info(f"[磁带恢复] 处理归档 {i + 1}/{len(archives)}: {archive_path.name}"
                             + (f" (过滤: {len(files_filter)} 个文件)" if files_filter else ""))

                result = await asyncio.to_thread(
                    self._extract_archive_sync,
                    archive_path,
                    target_dir,
                    task["overwrite"],
                    files_filter,
                )

                task["processed_archives"] = i + 1
                task["processed_files"] += result.get("extracted_count", 0)
                task["processed_bytes"] += result.get("extracted_bytes", 0)
                task["total_files"] += result.get("total_count", 0)
                task["total_bytes"] += result.get("total_bytes", 0)
                task["progress_percent"] = round((i + 1) / len(archives) * 100, 1)

                if result.get("errors"):
                    logger.warning(f"[磁带恢复] 归档 {archive_path.name} 部分文件提取失败: "
                                   f"{result['errors']}")

            task["status"] = "completed"
            task["completed_at"] = datetime.now().isoformat()
            task["progress_percent"] = 100.0
            logger.info(f"[磁带恢复] 恢复完成: {recovery_id}, "
                         f"文件: {task['processed_files']}, "
                         f"大小: {self._format_bytes(task['processed_bytes'])}")

            # 发送通知
            await self._send_notification(recovery_id, "completed")
            return True

        except Exception as e:
            task["status"] = "failed"
            task["error_message"] = str(e)
            task["completed_at"] = datetime.now().isoformat()
            logger.error(f"[磁带恢复] 恢复失败: {recovery_id}, 错误: {e}", exc_info=True)
            await self._send_notification(recovery_id, "failed", str(e))
            return False

    async def _send_notification(self, recovery_id: str, status: str, error: str = None):
        """发送恢复通知"""
        if not self.dingtalk_notifier:
            return
        try:
            task = self._recovery_tasks.get(recovery_id)
            if status == "completed":
                msg = (f"磁带恢复完成\n备份集: {task['set_id']}\n"
                       f"恢复文件: {task['processed_files']} 个\n"
                       f"恢复大小: {self._format_bytes(task['processed_bytes'])}\n"
                       f"目标路径: {task['target_path']}")
            else:
                msg = f"磁带恢复失败\n备份集: {task['set_id']}\n错误: {error}"

            await self.dingtalk_notifier.send_backup_notification(
                f"磁带恢复-{recovery_id[:20]}", status, {"error": msg}
            )
        except Exception:
            pass

    def _extract_archive_sync(self, archive_path: Path, target_dir: Path,
                              overwrite: str, files_filter: Optional[List[str]] = None) -> Dict:
        """同步提取归档文件（在线程池中运行）

        Args:
            files_filter: 仅提取这些文件路径（None=全部提取）
        """
        name = archive_path.name.lower()
        result = {"extracted_count": 0, "extracted_bytes": 0,
                  "total_count": 0, "total_bytes": 0, "errors": []}

        try:
            if name.endswith('.tar.zst'):
                self._extract_tar_zst(archive_path, target_dir, overwrite, result, files_filter)
            elif name.endswith(('.tar.gz', '.tgz')):
                self._extract_tar(archive_path, target_dir, overwrite, 'r:gz', result, files_filter)
            elif name.endswith('.tar'):
                self._extract_tar(archive_path, target_dir, overwrite, 'r:', result, files_filter)
            elif name.endswith('.7z'):
                self._extract_7z(archive_path, target_dir, overwrite, result, files_filter)
            elif name.endswith('.zip'):
                self._extract_zip(archive_path, target_dir, overwrite, result, files_filter)
            else:
                dest = target_dir / archive_path.name
                if not dest.exists() or overwrite == "overwrite":
                    shutil.copy2(str(archive_path), str(dest))
                    size = dest.stat().st_size
                    result["extracted_count"] = 1
                    result["extracted_bytes"] = size
                    result["total_count"] = 1
                    result["total_bytes"] = size
        except Exception as e:
            result["errors"].append(str(e))

        return result

    def _extract_tar(self, path: Path, target_dir: Path, overwrite: str,
                     mode: str, result: Dict, files_filter: Optional[List[str]] = None):
        """提取 tar/tar.gz 归档

        Args:
            files_filter: 仅提取这些文件（None=全部）
        """
        filter_set = self._build_filter_set(files_filter)

        with tarfile.open(str(path), mode) as tar:
            for member in tar:
                if not member.isfile():
                    continue
                self._extract_tar_member(
                    tar, member, target_dir, overwrite,
                    result, filter_set
                )

    def _extract_tar_member(self, tar, member, target_dir: Path,
                            overwrite: str, result: Dict,
                            filter_set: Optional[set] = None):
        """提取单个 tar 成员（流式安全，不会回溯 seek）"""
        # 文件过滤
        if filter_set is not None:
            norm_name = member.name.lstrip('./').lstrip('/')
            if member.name not in filter_set and norm_name not in filter_set:
                return

        result["total_count"] += 1
        result["total_bytes"] += member.size

        dest = target_dir / member.name
        if not self._is_safe_path(target_dir, dest):
            result["errors"].append(f"跳过不安全路径: {member.name}")
            return

        if dest.exists() and overwrite == "skip":
            return

        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            extracted = tar.extractfile(member)
            if extracted:
                data = extracted.read()
                dest.write_bytes(data)
                result["extracted_count"] += 1
                result["extracted_bytes"] += len(data)
        except Exception as e:
            result["errors"].append(f"{member.name}: {e}")

    def _extract_tar_zst(self, path: Path, target_dir: Path, overwrite: str,
                         result: Dict, files_filter: Optional[List[str]] = None):
        """提取 .tar.zst 归档（使用 zstandard 库流式解压）"""
        filter_set = self._build_filter_set(files_filter)

        try:
            if zstd is not None:
                dctx = zstd.ZstdDecompressor()
                with open(str(path), 'rb') as fh:
                    with dctx.stream_reader(fh) as reader:
                        with tarfile.open(fileobj=reader, mode='r|') as tar:
                            for member in tar:
                                if not member.isfile():
                                    continue
                                self._extract_tar_member(
                                    tar, member, target_dir, overwrite,
                                    result, filter_set
                                )
            else:
                # 回退到 CLI
                proc = subprocess.Popen(
                    ['zstd', '-d', str(path), '--stdout'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
                    for member in tar:
                        if not member.isfile():
                            continue
                        self._extract_tar_member(
                            tar, member, target_dir, overwrite,
                            result, filter_set
                        )
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    result["errors"].append("解压 .tar.zst 超时（30秒），进程已终止")
        except FileNotFoundError:
            result["errors"].append("zstd 命令不可用，无法解压 .tar.zst")
        except Exception as e:
            result["errors"].append(f"解压 .tar.zst 失败: {e}")

    def _extract_7z(self, path: Path, target_dir: Path, overwrite: str,
                    result: Dict, files_filter: Optional[List[str]] = None):
        """提取 .7z 归档"""
        if py7zr is None:
            result["errors"].append("py7zr 库未安装")
            return

        with py7zr.SevenZipFile(str(path), mode='r') as archive:
            file_infos = [f for f in archive.list() if not f.is_directory]
            result["total_count"] = len(file_infos)
            result["total_bytes"] = sum(f.uncompressed or 0 for f in file_infos)

            # 文件过滤
            filter_set = None
            if files_filter:
                filter_set = set()
                for f in files_filter:
                    norm = f.lstrip('./').lstrip('/')
                    filter_set.add(norm)
                    filter_set.add(f)

            names_to_extract = []
            for fi in file_infos:
                fname = fi.filename or ""
                # 文件过滤
                if filter_set is not None:
                    norm_name = fname.lstrip('./').lstrip('/')
                    if fname not in filter_set and norm_name not in filter_set:
                        continue

                if overwrite == "skip":
                    dest = target_dir / fname
                    if not dest.exists():
                        names_to_extract.append(fname)
                else:
                    names_to_extract.append(fname)

            if names_to_extract:
                archive.extract(path=str(target_dir), targets=names_to_extract)
                result["extracted_count"] = len(names_to_extract)
            else:
                # 没有文件需要提取（全部跳过或过滤为空）
                pass

            if result["total_count"] > 0:
                result["extracted_bytes"] = result["extracted_count"] * (
                    result["total_bytes"] // result["total_count"]
                )

    def _extract_zip(self, path: Path, target_dir: Path, overwrite: str,
                     result: Dict, files_filter: Optional[List[str]] = None):
        """提取 .zip 归档"""
        # 构建过滤集合
        filter_set = None
        if files_filter:
            filter_set = set()
            for f in files_filter:
                norm = f.lstrip('./').lstrip('/')
                filter_set.add(norm)
                filter_set.add(f)

        with zipfile.ZipFile(str(path), 'r') as zf:
            file_infos = [i for i in zf.infolist() if not i.is_dir()]
            result["total_count"] = len(file_infos)
            result["total_bytes"] = sum(i.file_size for i in file_infos)

            for info in file_infos:
                # 文件过滤
                if filter_set is not None:
                    norm_name = info.filename.lstrip('./').lstrip('/')
                    if info.filename not in filter_set and norm_name not in filter_set:
                        continue

                dest = target_dir / info.filename
                if not self._is_safe_path(target_dir, dest):
                    result["errors"].append(f"跳过不安全路径: {info.filename}")
                    continue

                if dest.exists() and overwrite == "skip":
                    continue

                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    data = zf.read(info.filename)
                    dest.write_bytes(data)
                    result["extracted_count"] += 1
                    result["extracted_bytes"] += len(data)
                except Exception as e:
                    result["errors"].append(f"{info.filename}: {e}")

    def _extract_to_memory(self, archive_path: Path,
                           files_filter: Optional[List[str]] = None) -> List[tuple]:
        """提取归档文件到内存（用于 ZIP 打包下载）

        Returns:
            [(relative_path, bytes_data), ...]
        """
        name = archive_path.name.lower()
        results = []

        try:
            if name.endswith('.tar.zst'):
                results = self._extract_tar_zst_to_memory(archive_path, files_filter)
            elif name.endswith(('.tar.gz', '.tgz')):
                results = self._extract_tar_to_memory(archive_path, 'r:gz', files_filter)
            elif name.endswith('.tar'):
                results = self._extract_tar_to_memory(archive_path, 'r:', files_filter)
            elif name.endswith('.zip'):
                results = self._extract_zip_to_memory(archive_path, files_filter)
            elif name.endswith('.7z'):
                results = self._extract_7z_to_memory(archive_path, files_filter)
            else:
                data = archive_path.read_bytes()
                results = [(archive_path.name, data)]
        except Exception as e:
            logger.error(f"[磁带恢复] 提取到内存失败 {archive_path.name}: {e}")

        return results

    def _build_filter_set(self, files_filter: Optional[List[str]]) -> Optional[set]:
        """构建标准化文件名过滤集合"""
        if not files_filter:
            return None
        filter_set = set()
        for f in files_filter:
            filter_set.add(f)
            filter_set.add(f.lstrip('./').lstrip('/'))
        return filter_set

    def _extract_tar_to_memory(self, path: Path, mode: str,
                                files_filter: Optional[List[str]] = None) -> List[tuple]:
        """提取 tar/tar.gz 到内存"""
        results = []
        filter_set = self._build_filter_set(files_filter)
        try:
            with tarfile.open(str(path), mode) as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    if filter_set is not None:
                        norm = member.name.lstrip('./').lstrip('/')
                        if member.name not in filter_set and norm not in filter_set:
                            continue
                    extracted = tar.extractfile(member)
                    if extracted:
                        results.append((member.name, extracted.read()))
        except Exception as e:
            logger.error(f"[磁带恢复] 提取tar到内存失败: {e}")
        return results

    def _extract_tar_zst_to_memory(self, path: Path,
                                    files_filter: Optional[List[str]] = None) -> List[tuple]:
        """提取 .tar.zst 到内存（使用 zstandard 库流式解压）"""
        results = []
        filter_set = self._build_filter_set(files_filter)
        try:
            if zstd is not None:
                dctx = zstd.ZstdDecompressor()
                with open(str(path), 'rb') as fh:
                    with dctx.stream_reader(fh) as reader:
                        with tarfile.open(fileobj=reader, mode='r|') as tar:
                            for member in tar:
                                if not member.isfile():
                                    continue
                                if filter_set is not None:
                                    norm = member.name.lstrip('./').lstrip('/')
                                    if member.name not in filter_set and norm not in filter_set:
                                        continue
                                extracted = tar.extractfile(member)
                                if extracted:
                                    results.append((member.name, extracted.read()))
            else:
                proc = subprocess.Popen(
                    ['zstd', '-d', str(path), '--stdout'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                with tarfile.open(fileobj=proc.stdout, mode='r|') as tar:
                    for member in tar:
                        if not member.isfile():
                            continue
                        if filter_set is not None:
                            norm = member.name.lstrip('./').lstrip('/')
                            if member.name not in filter_set and norm not in filter_set:
                                continue
                        extracted = tar.extractfile(member)
                        if extracted:
                            results.append((member.name, extracted.read()))
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        except Exception as e:
            logger.error(f"[磁带恢复] 提取tar.zst到内存失败: {e}")
        return results

    def _extract_zip_to_memory(self, path: Path,
                                files_filter: Optional[List[str]] = None) -> List[tuple]:
        """提取 .zip 到内存"""
        results = []
        filter_set = self._build_filter_set(files_filter)
        try:
            with zipfile.ZipFile(str(path), 'r') as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    if filter_set is not None:
                        norm = info.filename.lstrip('./').lstrip('/')
                        if info.filename not in filter_set and norm not in filter_set:
                            continue
                    results.append((info.filename, zf.read(info.filename)))
        except Exception as e:
            logger.error(f"[磁带恢复] 提取zip到内存失败: {e}")
        return results

    def _extract_7z_to_memory(self, path: Path,
                               files_filter: Optional[List[str]] = None) -> List[tuple]:
        """提取 .7z 到内存"""
        if py7zr is None:
            return []
        results = []
        filter_set = self._build_filter_set(files_filter)
        try:
            with py7zr.SevenZipFile(str(path), mode='r') as archive:
                for fi in archive.list():
                    if fi.is_directory:
                        continue
                    fname = fi.filename or ""
                    if filter_set is not None:
                        norm = fname.lstrip('./').lstrip('/')
                        if fname not in filter_set and norm not in filter_set:
                            continue
                    # py7zr: read specific files into memory
                    reads = archive.read([fname])
                    if fname in reads:
                        data = reads[fname].read()
                        results.append((fname, data))
        except Exception as e:
            logger.error(f"[磁带恢复] 提取7z到内存失败: {e}")
        return results

    @staticmethod
    def _is_safe_path(base: Path, target: Path) -> bool:
        """检查目标路径是否在基础目录内（防止路径穿越）"""
        try:
            resolved = target.resolve()
            base_resolved = base.resolve()
            return str(resolved).startswith(str(base_resolved))
        except Exception:
            return False

    # ================================================================
    # 恢复任务管理
    # ================================================================

    async def get_recovery_status(self, recovery_id: str) -> Optional[Dict]:
        """获取恢复任务状态"""
        task = self._recovery_tasks.get(recovery_id)
        if not task:
            return None
        return dict(task)

    async def cancel_recovery(self, recovery_id: str) -> bool:
        """取消恢复任务"""
        if recovery_id not in self._recovery_tasks:
            return False
        self._cancel_requested.add(recovery_id)
        task = self._recovery_tasks[recovery_id]
        if task["status"] == "pending":
            task["status"] = "cancelled"
            task["error_message"] = "用户取消恢复"
        logger.info(f"[磁带恢复] 请求取消任务: {recovery_id}")
        return True

    # ================================================================
    # 工具方法
    # ================================================================

    def _get_mount_point(self) -> Path:
        """获取 LTFS 挂载点"""
        return Path(getattr(self.settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs'))

    @staticmethod
    def _format_bytes(size: int) -> str:
        """格式化字节大小"""
        if not size or size <= 0:
            return "0 B"
        units = ['B', 'KB', 'MB', 'GB', 'TB']
        index = 0
        value = float(size)
        while value >= 1024 and index < len(units) - 1:
            value /= 1024
            index += 1
        return f"{value:.2f} {units[index]}"
