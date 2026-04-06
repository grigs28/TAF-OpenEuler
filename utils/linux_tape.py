#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Linux 磁带操作工具
Linux Tape Operations Utilities

使用 mt/mtx 命令操作 SCSI 磁带设备
"""

import os
import select
import asyncio
import subprocess
import logging
import platform
import threading
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
from datetime import datetime

from config.settings import get_settings

logger = logging.getLogger(__name__)

# ========== 全局磁带操作锁 ==========
# 防止多个备份任务同时操作磁带（擦除、写入等）
_tape_operation_lock = threading.Lock()
_tape_operation_owner = None  # 当前锁持有者

def acquire_tape_operation_lock(owner: str = "unknown", timeout: float = 30.0) -> bool:
    """获取磁带操作锁

    Args:
        owner: 锁持有者标识（如 task_id 或 operation_name）
        timeout: 超时时间（秒）

    Returns:
        True 表示成功获取锁，False 表示获取失败（锁被占用）
    """
    global _tape_operation_owner
    acquired = _tape_operation_lock.acquire(timeout=timeout)
    if acquired:
        _tape_operation_owner = owner
        logger.info(f"[磁带锁] 获取成功 - 持有者: {owner}")
    else:
        logger.warning(f"[磁带锁] 获取失败 - 当前持有者: {_tape_operation_owner}, 请求者: {owner}")
    return acquired

def release_tape_operation_lock():
    """释放磁带操作锁"""
    global _tape_operation_owner
    if _tape_operation_lock.locked():
        logger.info(f"[磁带锁] 释放 - 原持有者: {_tape_operation_owner}")
        _tape_operation_owner = None
        _tape_operation_lock.release()
    else:
        logger.warning("[磁带锁] 尝试释放未锁定的锁")

def is_tape_operation_locked() -> bool:
    """检查磁带操作锁是否被占用"""
    return _tape_operation_lock.locked()

def get_tape_lock_owner() -> Optional[str]:
    """获取当前磁带锁持有者"""
    return _tape_operation_owner


class LinuxTapeOperator:
    """Linux 磁带操作类 - 使用 mt/mtx 命令"""

    def __init__(self, device_path: str = None):
        self.settings = get_settings()
        self.device_path = device_path or getattr(self.settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
        self._initialized = False
        self._is_linux = platform.system() == 'Linux'

    async def initialize(self) -> bool:
        """初始化磁带操作"""
        if not self._is_linux:
            logger.warning("LinuxTapeOperator 仅适用于 Linux 系统")
            return False

        # 检查 mt 命令是否可用
        if not await self._check_command_available('mt'):
            logger.error("mt 命令不可用，请安装 mt-st 包: sudo dnf install mt-st 或 sudo yum install mt-st")
            return False

        # 检查设备是否存在
        if not os.path.exists(self.device_path):
            logger.error(f"磁带设备不存在: {self.device_path}")
            return False

        self._initialized = True
        logger.info(f"Linux 磁带操作初始化完成，设备: {self.device_path}")
        return True

    async def _check_command_available(self, command: str) -> bool:
        """检查命令是否可用"""
        try:
            result = await asyncio.create_subprocess_exec(
                'which', command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await result.communicate()
            return result.returncode == 0
        except Exception:
            return False

    async def _run_mt_command(self, *args, timeout: int = 60) -> Tuple[bool, str, str]:
        """执行 mt 命令"""
        cmd = ['mt', '-f', self.device_path] + list(args)
        logger.info(f"执行命令: {' '.join(cmd)}")

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            stdout_str = stdout.decode('utf-8', errors='ignore') if stdout else ''
            stderr_str = stderr.decode('utf-8', errors='ignore') if stderr else ''

            if process.returncode == 0:
                return True, stdout_str, stderr_str
            else:
                logger.error(f"mt 命令失败: {stderr_str}")
                return False, stdout_str, stderr_str

        except asyncio.TimeoutError:
            logger.error(f"mt 命令超时 ({timeout}s)")
            return False, '', f'命令超时 ({timeout}s)'
        except Exception as e:
            logger.error(f"执行 mt 命令异常: {str(e)}")
            return False, '', str(e)

    async def status(self) -> Dict[str, Any]:
        """获取磁带状态"""
        success, stdout, stderr = await self._run_mt_command('status')

        result = {
            'device': self.device_path,
            'online': False,
            'at_eod': False,
            'at_bot': False,
            'write_protected': False,
            'raw_output': stdout or stderr,
            'error': None if success else stderr
        }

        if success and stdout:
            # 解析 mt status 输出
            output_lower = stdout.lower()
            result['online'] = 'online' in output_lower or 'ready' in output_lower
            result['at_eod'] = 'end of data' in output_lower or 'eod' in output_lower
            result['at_bot'] = 'beginning of tape' in output_lower or 'bot' in output_lower
            result['write_protected'] = 'write protected' in output_lower or 'read only' in output_lower

            # 尝试提取块号和文件号
            import re
            block_match = re.search(r'block\s*[:=]?\s*(\d+)', output_lower)
            file_match = re.search(r'file\s*[:=]?\s*(\d+)', output_lower)

            if block_match:
                result['block_number'] = int(block_match.group(1))
            if file_match:
                result['file_number'] = int(file_match.group(1))

        return result

    async def rewind(self) -> bool:
        """倒带"""
        logger.info(f"正在倒带: {self.device_path}")
        success, stdout, stderr = await self._run_mt_command('rewind')
        if success:
            logger.info("倒带完成")
        return success

    async def eject(self) -> bool:
        """弹出磁带"""
        logger.info(f"正在弹出磁带: {self.device_path}")
        success, stdout, stderr = await self._run_mt_command('eject', timeout=120)
        if success:
            logger.info("磁带已弹出")
        return success

    async def offline(self) -> bool:
        """离线（弹出）磁带"""
        return await self.eject()

    async def seek(self, block: int) -> bool:
        """定位到指定块"""
        logger.info(f"定位到块 {block}")
        success, stdout, stderr = await self._run_mt_command('seek', str(block))
        return success

    async def tell(self) -> Optional[int]:
        """获取当前位置"""
        success, stdout, stderr = await self._run_mt_command('tell')
        if success and stdout:
            import re
            match = re.search(r'block\s*(\d+)', stdout.lower())
            if match:
                return int(match.group(1))
        return None

    async def fsf(self, count: int = 1) -> bool:
        """向前跳过指定数量的文件标记"""
        logger.info(f"向前跳过 {count} 个文件标记")
        success, stdout, stderr = await self._run_mt_command('fsf', str(count))
        return success

    async def bsf(self, count: int = 1) -> bool:
        """向后跳过指定数量的文件标记"""
        logger.info(f"向后跳过 {count} 个文件标记")
        success, stdout, stderr = await self._run_mt_command('bsf', str(count))
        return success

    async def eod(self) -> bool:
        """定位到数据末尾"""
        logger.info("定位到数据末尾")
        success, stdout, stderr = await self._run_mt_command('eod', timeout=300)
        return success

    async def erase(self, long_erase: bool = False) -> bool:
        """擦除磁带"""
        logger.info(f"擦除磁带 (long={long_erase})")
        args = ['erase']
        if long_erase:
            args.append('long')
        # 擦除可能需要很长时间
        success, stdout, stderr = await self._run_mt_command(*args, timeout=3600)
        return success

    def erase_sync(self, device_path: str = None, long_erase: bool = False, timeout: int = 300,
                   ltfs_label: str = None, progress_callback: callable = None) -> Dict[str, Any]:
        """使用 LTFS 格式化磁带（同步版本，用于线程中调用）

        Args:
            device_path: 设备路径，如果为None则使用 LTFS_DEVICE_PATH 或实例的设备路径
            long_erase: 保留参数（LTFS 不需要）
            timeout: 超时时间（秒），默认5分钟
            ltfs_label: LTFS 卷标名称
            progress_callback: 进度回调函数，签名为 callback(message)

        Returns:
            包含 success, stdout, stderr, returncode 的字典
        """
        # 优先使用 LTFS 专用设备路径（SCSI generic 设备）
        dev = getattr(self.settings, 'LTFS_DEVICE_PATH', None) or device_path or self.device_path
        logger.info(f"[同步] LTFS 格式化磁带: {dev}")

        def report_progress(msg):
            """内部进度报告"""
            logger.info(f"[LTFS] {msg}")
            if progress_callback:
                try:
                    # 回调期望单个消息参数
                    progress_callback(msg)
                except Exception as e:
                    logger.warning(f"进度回调异常: {e}")

        # ===== 格式化前清理：卸载 LTFS 并终止进程 =====
        ltfs_mount_point = getattr(self.settings, 'LTFS_MOUNT_POINT', '/mnt/ltfs')
        try:
            if os.path.ismount(ltfs_mount_point):
                report_progress(f"卸载 LTFS 挂载点: {ltfs_mount_point}")
                from utils.ltfs_ops import cleanup_mount
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # 已经在事件循环中，用同步方式
                        subprocess.run(['fusermount', '-u', ltfs_mount_point],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
                        time.sleep(2)
                    else:
                        loop.run_until_complete(cleanup_mount(ltfs_mount_point))
                except RuntimeError:
                    asyncio.run(cleanup_mount(ltfs_mount_point))
                report_progress("LTFS 挂载点已卸载")
        except Exception as e:
            logger.warning(f"[同步] 卸载 LTFS 挂载点失败: {e}")

        try:
            # 终止所有 LTFS 进程
            report_progress("终止 LTFS 进程...")
            subprocess.run(['pkill', '-9', '-f', 'ltfs'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            time.sleep(2)
            report_progress("LTFS 进程已终止")
        except Exception as e:
            logger.warning(f"[同步] 终止 LTFS 进程失败: {e}")

        # 获取 mkltfs 路径
        mkltfs_path = getattr(self.settings, 'MKLTFS_PATH', '/usr/local/bin/mkltfs')

        # 检查 mkltfs 是否存在
        if not os.path.exists(mkltfs_path):
            # 尝试在 PATH 中查找
            try:
                result = subprocess.run(
                    ['which', 'mkltfs'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=5
                )
                if result.returncode == 0:
                    mkltfs_path = result.stdout.decode('utf-8', errors='ignore').strip()
                else:
                    logger.error(f"[同步] mkltfs 命令未找到")
                    return {
                        "success": False,
                        "stdout": "",
                        "stderr": "mkltfs 命令未找到，请安装 LTFS 工具",
                        "returncode": -1
                    }
            except Exception as e:
                logger.error(f"[同步] 查找 mkltfs 失败: {e}")
                return {
                    "success": False,
                    "stdout": "",
                    "stderr": f"查找 mkltfs 失败: {e}",
                    "returncode": -1
                }

        # 构建 mkltfs 命令（不使用 -v，该版本不支持）
        cmd = [mkltfs_path, '-d', dev, '-f']
        if ltfs_label:
            cmd.extend(['-n', ltfs_label])

        report_progress(f"执行命令: {' '.join(cmd)}")

        stdout_lines = []
        stderr_lines = []

        try:
            # 使用 Popen 实时读取输出
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=False
            )

            import select
            import time
            start_time = time.time()

            while True:
                # 检查超时（不 kill，等待进程自行完成）
                if time.time() - start_time > timeout:
                    logger.error(f"[同步] LTFS 格式化超时 ({timeout}秒)，进程仍在后台运行，不强制终止")
                    return {
                        "success": False,
                        "stdout": "\n".join(stdout_lines),
                        "stderr": f"命令执行超时 ({timeout}秒)，进程仍在运行",
                        "returncode": -1,
                        "method": "ltfs"
                    }

                # 使用 select 检查可读的管道
                readable, _, _ = select.select([process.stdout, process.stderr], [], [], 0.5)

                for pipe in readable:
                    if pipe == process.stdout:
                        line = process.stdout.readline()
                        if line:
                            line_str = line.decode('utf-8', errors='ignore').strip()
                            if line_str:
                                stdout_lines.append(line_str)
                                report_progress(line_str)
                    elif pipe == process.stderr:
                        line = process.stderr.readline()
                        if line:
                            line_str = line.decode('utf-8', errors='ignore').strip()
                            if line_str:
                                stderr_lines.append(line_str)
                                report_progress(line_str)

                # 检查进程是否结束
                if process.poll() is not None:
                    # 读取剩余输出
                    remaining_stdout = process.stdout.read()
                    remaining_stderr = process.stderr.read()
                    if remaining_stdout:
                        for line in remaining_stdout.decode('utf-8', errors='ignore').strip().split('\n'):
                            if line:
                                stdout_lines.append(line)
                                report_progress(line)
                    if remaining_stderr:
                        for line in remaining_stderr.decode('utf-8', errors='ignore').strip().split('\n'):
                            if line:
                                stderr_lines.append(line)
                                report_progress(line)
                    break

            success = process.returncode == 0

            if success:
                report_progress(f"LTFS 格式化成功: {dev}")
            else:
                logger.error(f"[同步] LTFS 格式化失败: returncode={process.returncode}")

            return {
                "success": success,
                "stdout": "\n".join(stdout_lines),
                "stderr": "\n".join(stderr_lines),
                "returncode": process.returncode,
                "method": "ltfs"
            }
        except Exception as e:
            logger.error(f"[同步] LTFS 格式化异常: {str(e)}", exc_info=True)
            return {
                "success": False,
                "stdout": "\n".join(stdout_lines),
                "stderr": str(e),
                "returncode": -1,
                "method": "ltfs"
            }

    def setblk_sync(self, device_path: str = None, block_size: int = 0, timeout: int = 30) -> Dict[str, Any]:
        """设置块大小（同步版本）

        Args:
            device_path: 设备路径
            block_size: 块大小，0表示变长块
            timeout: 超时时间

        Returns:
            包含 success, stdout, stderr, returncode 的字典
        """
        dev = device_path or self.device_path
        logger.info(f"[同步] 设置块大小: {dev} -> {block_size}")

        cmd = ['mt', '-f', dev, 'setblk', str(block_size)]

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=timeout,
                text=False
            )

            stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
            stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

            success = result.returncode == 0

            return {
                "success": success,
                "stdout": stdout,
                "stderr": stderr,
                "returncode": result.returncode
            }
        except Exception as e:
            logger.error(f"[同步] 设置块大小异常: {str(e)}")
            return {
                "success": False,
                "stdout": "",
                "stderr": str(e),
                "returncode": -1
            }

    def rewind_sync(self, device_path: str = None, timeout: int = 60) -> Dict[str, Any]:
        """倒带（同步版本）

        Args:
            device_path: 设备路径
            timeout: 超时时间

        Returns:
            包含 success, stdout, stderr, returncode 的字典
        """
        dev = device_path or self.device_path
        logger.info(f"[同步] 倒带: {dev}")

        cmd = ['mt', '-f', dev, 'rewind']

        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                timeout=timeout,
                text=False
            )

            stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
            stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""

            success = result.returncode == 0

            return {
                "success": success,
                "stdout": stdout,
                "stderr": stderr,
                "returncode": result.returncode
            }
        except Exception as e:
            logger.error(f"[同步] 倒带异常: {str(e)}")
            return {
                "success": False,
                "stdout": "",
                "stderr": str(e),
                "returncode": -1
            }

    def status_sync(self, device_path: str = None, timeout: int = 30, retries: int = 3) -> Dict[str, Any]:
        """获取磁带状态（同步版本，带重试）

        Args:
            device_path: 设备路径
            timeout: 超时时间
            retries: 重试次数（设备忙时自动重试）

        Returns:
            包含状态信息的字典：
            - online: 是否在线
            - write_protected: 是否写保护
            - at_bot: 是否在磁带开头
            - at_eod: 是否在数据末尾
            - file_number: 文件号
            - block_number: 块号
            - is_empty: 磁带是否为空
            - can_write: 是否可以写入
        """
        dev = device_path or self.device_path
        logger.info(f"[同步] 获取磁带状态: {dev}")

        cmd = ['mt', '-f', dev, 'status']

        last_error = None
        for attempt in range(retries):
            try:
                result = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    timeout=timeout,
                    text=False
                )

                stdout = result.stdout.decode('utf-8', errors='ignore') if result.stdout else ""
                stderr = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""
                output = stdout + "\n" + stderr
                output_lower = output.lower()

                # 检查设备是否繁忙
                if 'busy' in output_lower or 'device or resource busy' in output_lower:
                    if attempt < retries - 1:
                        logger.warning(f"[同步] 设备繁忙，等待重试 ({attempt + 1}/{retries})...")
                        import time
                        time.sleep(2)  # 等待2秒后重试
                        continue

                status_result = {
                    "success": result.returncode == 0,
                    "device": dev,
                    "online": False,
                    "write_protected": False,
                    "at_bot": False,
                    "at_eod": False,
                    "file_number": -1,
                    "block_number": -1,
                    "is_empty": True,
                    "can_write": False,
                    "raw_output": output,
                    "stderr": stderr if result.returncode != 0 else ""
                }

                # 解析状态 - 检查 General status bits 行
                # 例如: "General status bits on (41010000): BOT ONLINE IM_REP_EN"
                if 'general status bits' in output_lower:
                    status_bits_line = output_lower.split('general status bits')[1] if 'general status bits' in output_lower else ""
                    status_result['online'] = 'online' in status_bits_line
                    status_result['at_bot'] = 'bot' in status_bits_line and 'beginning' not in status_bits_line
                    status_result['at_eod'] = 'eod' in status_bits_line
                else:
                    # 旧版解析方式
                    status_result['online'] = 'online' in output_lower
                    status_result['at_bot'] = 'beginning of tape' in output_lower or ('bot' in output_lower and 'beginning' not in output_lower)

                status_result['write_protected'] = 'write protected' in output_lower or 'read only' in output_lower or 'wr_prot' in output_lower
                status_result['at_eod'] = status_result['at_eod'] or 'end of data' in output_lower or 'eod' in output_lower

                # 解析文件号和块号
                import re
                # 匹配 "File number=0, block number=18865" 格式
                file_match = re.search(r'file\s*number\s*[:=]?\s*(\d+)', output_lower)
                block_match = re.search(r'block\s*number\s*[:=]?\s*(\d+)', output_lower)

                if file_match:
                    status_result['file_number'] = int(file_match.group(1))
                if block_match:
                    status_result['block_number'] = int(block_match.group(1))

                # 判断磁带是否为空 - 需要额外检查
                # file=0, block=0 只表示在开头，不代表为空
                # 使用 mt eod 移动到数据末尾，检查位置
                if status_result['file_number'] == 0 and status_result['block_number'] == 0:
                    # 可能为空，需要进一步验证
                    try:
                        # 移动到数据末尾
                        eod_result = subprocess.run(
                            ['mt', '-f', dev, 'eod'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL,
                            timeout=30,
                            text=False
                        )

                        # 再次获取状态
                        status_result2 = subprocess.run(
                            ['mt', '-f', dev, 'status'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL,
                            timeout=30,
                            text=False
                        )

                        output2 = status_result2.stdout.decode('utf-8', errors='ignore') if status_result2.stdout else ""
                        output2_lower = output2.lower()

                        # 解析移动后的位置
                        file_match2 = re.search(r'file\s*number\s*[:=]?\s*(\d+)', output2_lower)
                        block_match2 = re.search(r'block\s*number\s*[:=]?\s*(\d+)', output2_lower)

                        file_after_eod = int(file_match2.group(1)) if file_match2 else 0
                        block_after_eod = int(block_match2.group(1)) if block_match2 else 0

                        # 如果 eod 后仍在 file=0, block=0，则磁带为空
                        if file_after_eod == 0 and block_after_eod == 0:
                            status_result['is_empty'] = True
                            logger.info(f"[同步] 磁带为空（eod后仍为 file=0, block=0）")
                        else:
                            status_result['is_empty'] = False
                            logger.info(f"[同步] 磁带不为空（eod后 file={file_after_eod}, block={block_after_eod}）")

                        # 倒回开头
                        subprocess.run(
                            ['mt', '-f', dev, 'rewind'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            stdin=subprocess.DEVNULL,
                            timeout=60
                        )

                    except Exception as eod_err:
                        logger.warning(f"[同步] 检查磁带是否为空失败: {eod_err}，假设不为空")
                        status_result['is_empty'] = False
                else:
                    # file > 0 或 block > 0，磁带肯定不为空
                    status_result['is_empty'] = False

                # 判断是否可以写入
                # 需要在线、非写保护
                status_result['can_write'] = (
                    status_result['online'] and
                    not status_result['write_protected']
                )

                logger.info(f"[同步] 磁带状态: online={status_result['online']}, "
                           f"write_protected={status_result['write_protected']}, "
                           f"file={status_result['file_number']}, block={status_result['block_number']}, "
                           f"is_empty={status_result['is_empty']}, can_write={status_result['can_write']}")

                return status_result

            except subprocess.TimeoutExpired:
                last_error = f"命令超时 ({timeout}秒)"
                if attempt < retries - 1:
                    logger.warning(f"[同步] 获取状态超时，等待重试 ({attempt + 1}/{retries})...")
                    import time
                    time.sleep(2)
                    continue
                logger.error(f"[同步] 获取磁带状态超时 ({timeout}秒)")
                return {
                    "success": False,
                    "device": dev,
                    "online": False,
                    "is_empty": None,
                    "can_write": False,
                    "stderr": last_error,
                    "raw_output": ""
                }
            except Exception as e:
                last_error = str(e)
                if attempt < retries - 1:
                    logger.warning(f"[同步] 获取状态异常: {e}，等待重试 ({attempt + 1}/{retries})...")
                    import time
                    time.sleep(2)
                    continue

        # 所有重试都失败
        logger.error(f"[同步] 获取磁带状态失败: {last_error}")
        return {
            "success": False,
            "device": dev,
            "online": False,
            "is_empty": None,
            "can_write": False,
            "stderr": str(last_error),
            "raw_output": ""
        }

    def check_and_prepare_tape_sync(self, device_path: str = None, force_erase: bool = False,
                                     progress_callback: callable = None,
                                     ltfs_label: str = None) -> Dict[str, Any]:
        """检查并准备磁带（同步版本，带进度回调）

        检查磁带状态，如果不为空则使用 LTFS 格式化

        Args:
            device_path: 设备路径
            force_erase: 是否强制格式化（即使磁带为空）
            progress_callback: 进度回调函数，签名为 callback(stage, message, percent)
            ltfs_label: LTFS 卷标名称

        Returns:
            包含 success, message, status 的字典
        """
        dev = device_path or self.device_path
        logger.info(f"[同步] 检查并准备磁带: {dev}")

        def call_progress(stage, message, percent):
            """内部进度回调包装器"""
            if progress_callback:
                try:
                    progress_callback(stage, message, percent)
                except Exception as e:
                    logger.warning(f"进度回调异常: {e}")

        # 步骤1: 获取状态 (0-10%)
        call_progress("checking", "正在检查磁带状态...", 0)
        status = self.status_sync(dev)
        if not status.get("success"):
            call_progress("error", f"无法获取磁带状态", 0)
            return {
                "success": False,
                "message": f"无法获取磁带状态: {status.get('stderr', '未知错误')}",
                "status": status
            }

        # 检查是否在线 (10%)
        call_progress("checking", "验证磁带在线状态...", 10)
        if not status.get("online"):
            call_progress("error", "磁带不在线", 10)
            return {
                "success": False,
                "message": "磁带不在线，请检查磁带是否正确加载",
                "status": status
            }

        # 检查是否写保护 (20%)
        call_progress("checking", "检查写保护状态...", 20)
        if status.get("write_protected"):
            call_progress("error", "磁带写保护", 20)
            return {
                "success": False,
                "message": "磁带处于写保护状态，无法写入",
                "status": status
            }

        # 步骤2: 判断是否需要擦除 (30%)
        # force_erase=False: 只在磁带不为空时擦除
        # force_erase=True: 强制擦除（无论磁带是否为空）
        is_empty = status.get("is_empty", True)
        call_progress("checking", f"磁带状态: {'empty' if is_empty else 'used'}", 30)

        # 判断是否需要擦除
        should_erase = (not is_empty) or force_erase

        if should_erase:
            if is_empty and force_erase:
                logger.info(f"[同步] 强制擦除空磁带（force_erase=True）")
            else:
                logger.info(f"[同步] 磁带不为空（file={status.get('file_number')}, block={status.get('block_number')}），需要擦除")

            # 倒带 (40-50%)
            call_progress("rewinding", "正在倒带...", 40)
            rewind_result = self.rewind_sync(dev)
            if not rewind_result.get("success"):
                call_progress("error", f"倒带失败", 50)
                return {
                    "success": False,
                    "message": f"倒带失败: {rewind_result.get('stderr', '未知错误')}",
                    "status": status
                }
            call_progress("rewinding", "倒带完成", 50)

            # LTFS 格式化 (50-90%)
            call_progress("erasing", "正在使用 LTFS 格式化磁带...", 50)

            # 创建进度回调包装器，将 LTFS 输出转发给外部回调
            def ltfs_progress_wrapper(msg):
                call_progress("erasing", msg, None)

            erase_result = self.erase_sync(dev, long_erase=False, timeout=300,
                                           ltfs_label=ltfs_label,
                                           progress_callback=ltfs_progress_wrapper)
            if not erase_result.get("success"):
                call_progress("error", "LTFS 格式化失败", 90)
                return {
                    "success": False,
                    "message": f"LTFS 格式化失败: {erase_result.get('stderr', '未知错误')}",
                    "status": status
                }

            call_progress("erasing", "LTFS 格式化完成", 90)
            logger.info(f"[同步] LTFS 格式化成功")
        else:
            # 磁带为空且未强制擦除，跳过擦除
            call_progress("skipped", "磁带为空，无需擦除", 90)
            logger.info(f"[同步] 磁带为空，无需擦除")

        # 步骤3: 倒带到开头并设置变长块 (90-100%)
        call_progress("finalizing", "正在倒带并设置块大小...", 90)
        rewind_result = self.rewind_sync(dev)
        if not rewind_result.get("success"):
            logger.warning(f"[同步] 最终倒带失败: {rewind_result.get('stderr')}")

        # 设置变长块 (95-100%)
        call_progress("finalizing", "设置变长块模式...", 95)
        setblk_result = self.setblk_sync(dev, block_size=0)
        if not setblk_result.get("success"):
            logger.warning(f"[同步] 设置块大小失败: {setblk_result.get('stderr')}")

        call_progress("completed", "磁带准备完成", 100)
        return {
            "success": True,
            "message": "磁带准备完成" + ("（已 LTFS 格式化）" if should_erase else "（磁带为空）"),
            "was_erased": should_erase,
            "status": status
        }

    async def setcompression(self, enable: bool = True) -> bool:
        """设置压缩"""
        compression = 'on' if enable else 'off'
        logger.info(f"设置压缩: {compression}")
        success, stdout, stderr = await self._run_mt_command('compression', compression)
        return success

    async def setblk(self, block_size: int) -> bool:
        """设置块大小 (0 = 变长块)"""
        logger.info(f"设置块大小: {block_size}")
        success, stdout, stderr = await self._run_mt_command('setblk', str(block_size))
        return success

    async def wait_for_device_ready(self, timeout: int = 60) -> bool:
        """等待设备就绪"""
        logger.info(f"等待设备就绪 (超时: {timeout}s)")
        start_time = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start_time) < timeout:
            status = await self.status()
            if status.get('online'):
                logger.info("设备已就绪")
                return True
            await asyncio.sleep(1)

        logger.error("等待设备就绪超时")
        return False

    async def write_file_mark(self, count: int = 1) -> bool:
        """写入文件标记"""
        logger.info(f"写入 {count} 个文件标记")
        success, stdout, stderr = await self._run_mt_command('weof', str(count))
        return success

    @staticmethod
    async def scan_devices() -> List[Dict[str, Any]]:
        """扫描系统中的磁带设备（Linux专用，获取详细信息）"""
        import glob
        import re

        devices = []
        device_info_map = {}  # 用于存储设备详细信息

        # 首先使用 lsscsi 获取详细的 SCSI 设备信息
        try:
            process = await asyncio.create_subprocess_exec(
                'lsscsi', '-g',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)

            if process.returncode == 0 and stdout:
                lines = stdout.decode('utf-8', errors='ignore').strip().split('\n')
                for line in lines:
                    # lsscsi -g 输出格式: [H:C:T:L] type vendor model rev /dev/sgN /dev/stN
                    if 'tape' in line.lower() or '/dev/st' in line.lower():
                        # 解析行
                        parts = line.split()

                        # 提取信息
                        vendor = ''
                        model = ''
                        scsi_addr = ''
                        generic_path = ''
                        tape_path = ''

                        for i, part in enumerate(parts):
                            # SCSI地址 [H:C:T:L]
                            if part.startswith('[') and part.endswith(']'):
                                scsi_addr = part
                            # /dev/sgN 通用设备
                            elif part.startswith('/dev/sg'):
                                generic_path = part
                            # /dev/stN 或 /dev/nstN 磁带设备
                            elif part.startswith('/dev/st') or part.startswith('/dev/nst'):
                                tape_path = part
                            # vendor (通常是第一个非地址非路径的字段)
                            elif i > 0 and not part.startswith('/') and not part.startswith('['):
                                if not vendor and part not in ['tape', 'mediumx', 'processor']:
                                    vendor = part
                                elif vendor and not model and part not in ['tape', 'mediumx', 'processor', 'R']:
                                    if not part[0].isdigit() or len(part) > 2:
                                        model = part

                        # 如果找到磁带设备路径，存储信息
                        if tape_path or generic_path:
                            key = tape_path or generic_path
                            device_info_map[key] = {
                                'vendor': vendor or 'Unknown',
                                'model': model or 'Tape Drive',
                                'scsi_addr': scsi_addr,
                                'generic_path': generic_path,
                            }
                            logger.debug(f"lsscsi 解析: {key} -> vendor={vendor}, model={model}")

        except asyncio.TimeoutError:
            logger.warning("lsscsi 命令超时")
        except FileNotFoundError:
            logger.warning("lsscsi 命令未找到，将使用基础扫描")
        except Exception as e:
            logger.debug(f"lsscsi 扫描失败: {str(e)}")

        # 检查 /dev/st* 和 /dev/nst* 设备
        for pattern in ['/dev/st*', '/dev/nst*']:
            for device_path in glob.glob(pattern):
                # 跳过 stderr/stdin/stdout 符号链接
                if 'std' in device_path and device_path not in ['/dev/st0', '/dev/nst0']:
                    continue
                if os.path.exists(device_path):
                    # 获取设备信息
                    info = device_info_map.get(device_path, {})

                    # 尝试从 /sys 获取更多信息
                    sys_info = LinuxTapeOperator._get_sys_info(device_path)

                    device = {
                        'path': device_path,
                        'vendor': info.get('vendor') or sys_info.get('vendor', 'Unknown'),
                        'model': info.get('model') or sys_info.get('model', 'Tape Drive'),
                        'serial': sys_info.get('serial', 'Unknown'),
                        'status': 'online',
                        'type': 'SCSI tape',
                        'rewinding': device_path.startswith('/dev/st'),
                        'scsi_addr': info.get('scsi_addr', ''),
                        'generic_path': info.get('generic_path', ''),
                    }

                    # 从 model 中解析 LTO 代数和容量
                    model_upper = device['model'].upper()
                    lto_gen = None

                    # 匹配 LTO 代数: ULT3580-HH9, ULTRIUM-HH9, LTO-9 等
                    lto_match = re.search(r'ULT3580.*HH(\d)|ULTRIUM.*HH(\d)|LTO[-_]?(\d)', model_upper)
                    if lto_match:
                        lto_gen = int(lto_match.group(1) or lto_match.group(2) or lto_match.group(3))

                    # LTO 代数对应的容量 (原生容量，单位: 字节)
                    lto_capacity_map = {
                        9: 18 * 1024 ** 4,   # 18 TB
                        8: 12 * 1024 ** 4,   # 12 TB
                        7: 6 * 1024 ** 4,    # 6 TB
                        6: 2.5 * 1024 ** 4,  # 2.5 TB
                        5: 1.5 * 1024 ** 4,  # 1.5 TB
                    }

                    if lto_gen and lto_gen in lto_capacity_map:
                        device['is_ibm_lto'] = True
                        device['lto_generation'] = lto_gen
                        device['native_capacity'] = lto_capacity_map[lto_gen]
                        device['supports_worm'] = lto_gen >= 5
                        device['supports_encryption'] = lto_gen >= 4
                    else:
                        device['is_ibm_lto'] = 'ULT3580' in model_upper or 'ULTRIUM' in model_upper

                    devices.append(device)
                    logger.info(f"扫描到磁带设备: {device_path}, vendor={device['vendor']}, model={device['model']}, LTO={lto_gen}")

        return devices

    @staticmethod
    def _get_sys_info(device_path: str) -> Dict[str, str]:
        """从 /sys 文件系统获取设备信息"""
        info = {}

        try:
            # 获取设备号 (st0 -> 0)
            import re
            match = re.search(r'(nst|st)(\d+)', device_path)
            if not match:
                return info

            dev_num = match.group(2)

            # 查找对应的 SCSI 设备目录
            class_path = f'/sys/class/scsi_tape/st{dev_num}'
            if os.path.exists(class_path):
                device_link = os.path.join(class_path, 'device')
                if os.path.islink(device_link):
                    device_dir = os.path.realpath(device_link)

                    # 读取 vendor
                    vendor_file = os.path.join(device_dir, 'vendor')
                    if os.path.exists(vendor_file):
                        with open(vendor_file, 'r') as f:
                            info['vendor'] = f.read().strip()

                    # 读取 model
                    model_file = os.path.join(device_dir, 'model')
                    if os.path.exists(model_file):
                        with open(model_file, 'r') as f:
                            info['model'] = f.read().strip()

                    # 读取 serial (在 vpd_pg80 或其他位置)
                    serial_file = os.path.join(device_dir, 'vpd_pg80')
                    if os.path.exists(serial_file):
                        with open(serial_file, 'rb') as f:
                            data = f.read()
                            if len(data) > 4:
                                info['serial'] = data[4:].decode('ascii', errors='ignore').strip()

        except Exception as e:
            logger.debug(f"从 /sys 获取设备信息失败: {str(e)}")

        return info


# 单例实例
_linux_tape_operator: Optional[LinuxTapeOperator] = None


def get_linux_tape_operator(device_path: str = None) -> LinuxTapeOperator:
    """获取 Linux 磁带操作器实例"""
    global _linux_tape_operator
    if _linux_tape_operator is None:
        _linux_tape_operator = LinuxTapeOperator(device_path)
    return _linux_tape_operator
