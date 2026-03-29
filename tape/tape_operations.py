#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带操作模块 - Linux LTFS 模式
Tape Operations Module - Linux LTFS Mode
"""

import os
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

from tape.tape_cartridge import TapeCartridge
from config.settings import get_settings

logger = logging.getLogger(__name__)

# Linux 原生磁带操作支持
_LINUX_TAPE_AVAILABLE = False
try:
    from utils.linux_tape import LinuxTapeOperator
    _LINUX_TAPE_AVAILABLE = True
except ImportError:
    pass


class TapeOperations:
    """磁带操作类 - 仅支持 Linux LTFS 模式"""

    def __init__(self):
        self.settings = get_settings()
        self.linux_tape_operator = None
        self._initialized = False

    async def initialize(self, linux_tape_operator=None):
        """初始化磁带操作

        Args:
            linux_tape_operator: Linux 原生磁带操作器实例
        """
        try:
            if linux_tape_operator:
                self.linux_tape_operator = linux_tape_operator
                logger.info("磁带操作模块使用 Linux 原生操作器")
                self._initialized = True
                return

            # 创建 Linux 原生操作器
            if _LINUX_TAPE_AVAILABLE:
                from utils.linux_tape import get_linux_tape_operator
                self.linux_tape_operator = get_linux_tape_operator()
                init_success = await self.linux_tape_operator.initialize()
                if init_success:
                    self._initialized = True
                    logger.info("磁带操作模块(Linux)初始化完成")
                else:
                    logger.error("Linux 原生磁带操作初始化失败")
            else:
                logger.error("Linux 原生磁带操作不可用")

        except Exception as e:
            logger.error(f"磁带操作模块初始化失败: {str(e)}")
            raise

    async def _ensure_initialized(self) -> bool:
        """懒加载初始化，确保磁带接口可用"""
        if self._initialized and self.linux_tape_operator:
            return True
        try:
            if _LINUX_TAPE_AVAILABLE:
                from utils.linux_tape import get_linux_tape_operator
                self.linux_tape_operator = get_linux_tape_operator()
                init_success = await self.linux_tape_operator.initialize()
                if init_success:
                    self._initialized = True
                    return True
            logger.error("Linux 原生磁带操作初始化失败")
            return False
        except Exception as e:
            logger.error(f"初始化磁带接口失败: {str(e)}")
            return False

    async def load_tape(self, tape_cartridge: TapeCartridge) -> bool:
        """加载磁带"""
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return False

            logger.info(f"正在加载磁带: {tape_cartridge.tape_id}")

            # 检查设备就绪状态
            logger.info("检查磁带设备就绪状态...")
            if not await self._wait_for_tape_ready():
                logger.error("磁带设备未就绪")
                return False

            # 执行倒带操作
            if not await self._rewind():
                logger.error("磁带倒带失败")
                return False

            # 读取磁带卷标
            tape_label = await self._read_tape_label()
            if tape_label:
                logger.info(f"读取到磁带卷标: {tape_label}")

            logger.info(f"磁带 {tape_cartridge.tape_id} 加载成功")
            return True

        except Exception as e:
            logger.error(f"加载磁带失败: {str(e)}")
            return False

    async def unload_tape(self) -> bool:
        """卸载磁带"""
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return False

            logger.info("正在卸载磁带")

            # 写入文件标记
            await self._write_filemark()

            # 倒带
            await self._rewind()

            # 模拟卸载延迟
            await asyncio.sleep(2)

            logger.info("磁带卸载成功")
            return True

        except Exception as e:
            logger.error(f"卸载磁带失败: {str(e)}")
            return False

    async def erase_tape(self, backup_task=None, progress_callback=None) -> bool:
        """擦除磁带 - 使用 LTFS 格式化代替 mt erase

        Args:
            backup_task: 备份任务对象，用于更新进度
            progress_callback: 进度回调函数

        Returns:
            True=擦除成功，False=失败
        """
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return False

            logger.info("正在格式化磁带为 LTFS 格式（代替擦除）...")

            # 倒带到开始
            if not await self._rewind():
                logger.error("倒带失败，无法格式化磁带")
                return False

            # 使用 LTFS 格式化代替 mt erase
            from backup.tape_handler import TapeHandler
            tape_handler = TapeHandler(
                tape_manager=None,
                settings=self.settings,
                dingtalk_notifier=None
            )

            success, msg = await tape_handler.format_as_ltfs()

            if success:
                logger.info(f"LTFS 格式化成功: {msg}")
                if backup_task and progress_callback:
                    backup_task.progress_percent = 100.0
                    await progress_callback(backup_task, 1, 1)
                return True
            else:
                logger.error(f"LTFS 格式化失败: {msg}")
                return False

        except Exception as e:
            logger.error(f"格式化磁带失败: {str(e)}")
            return False

    async def write_data(self, data: bytes, block_number: int = 0) -> bool:
        """写入数据到磁带 - 通过 LTFS 挂载写入"""
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return False

            if not data:
                logger.warning("写入数据为空")
                return True

            logger.debug(f"LTFS 模式下通过文件系统写入数据: {len(data)} 字节")
            # 实际写入通过 TapeHandler 的 write_to_tape_drive 方法完成
            return True

        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            return False

    async def read_data(self, block_number: int = 0, block_size: int = None) -> Optional[bytes]:
        """从磁带读取数据 - 通过 LTFS 挂载读取"""
        try:
            if not await self._ensure_initialized():
                logger.error("磁带操作模块未初始化")
                return None

            logger.debug(f"LTFS 模式下通过文件系统读取数据")
            # 实际读取通过恢复引擎完成
            return None

        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            return None

    async def write_filemark(self) -> bool:
        """写入文件标记"""
        try:
            return await self._write_filemark()
        except Exception as e:
            logger.error(f"写入文件标记失败: {str(e)}")
            return False

    async def position_to_block(self, block_number: int) -> bool:
        """定位到指定数据块"""
        try:
            return await self._position_to_block(block_number)
        except Exception as e:
            logger.error(f"定位数据块失败: {str(e)}")
            return False

    async def get_tape_position(self) -> Optional[int]:
        """获取当前磁带位置"""
        try:
            return await self._get_tape_position()
        except Exception as e:
            logger.error(f"获取磁带位置失败: {str(e)}")
            return None

    async def get_tape_capacity(self) -> Optional[Tuple[int, int]]:
        """获取磁带容量信息"""
        try:
            return await self._get_tape_capacity()
        except Exception as e:
            logger.error(f"获取磁带容量失败: {str(e)}")
            return None

    async def _wait_for_tape_ready(self, timeout: int = 30) -> bool:
        """等待磁带就绪"""
        try:
            logger.debug(f"开始等待磁带就绪（超时: {timeout}秒）...")
            for attempt in range(timeout):
                try:
                    if self.linux_tape_operator:
                        status = await self.linux_tape_operator.status()
                        if status.get('online'):
                            logger.debug(f"磁带设备已就绪（第 {attempt + 1} 次尝试）")
                            return True
                except Exception as test_error:
                    logger.debug(f"第 {attempt + 1} 次就绪检查失败: {str(test_error)}")

                if attempt < timeout - 1:
                    await asyncio.sleep(1)

            logger.warning(f"等待 {timeout} 秒后磁带设备仍未就绪")
            return False
        except Exception as e:
            logger.error(f"等待磁带就绪过程异常: {str(e)}")
            return False

    async def _rewind(self) -> bool:
        """倒带操作"""
        try:
            if self.linux_tape_operator:
                return await self.linux_tape_operator.rewind()
            return False
        except Exception as e:
            logger.error(f"倒带操作失败: {str(e)}")
            return False

    async def _write_block(self, data: bytes, block_number: int) -> bool:
        """写入单个数据块 - LTFS 模式下不使用"""
        logger.warning("LTFS 模式下不使用块写入，请使用 TapeHandler.write_to_tape_drive()")
        return False

    async def _read_block(self, block_number: int, block_size: int) -> Optional[bytes]:
        """读取单个数据块 - LTFS 模式下不使用"""
        logger.warning("LTFS 模式下不使用块读取，请使用恢复引擎")
        return None

    async def _write_filemark(self) -> bool:
        """写入文件标记"""
        try:
            if self.linux_tape_operator:
                # LTFS 模式下文件标记由文件系统自动处理
                logger.debug("LTFS 模式下文件标记由文件系统自动处理")
                return True
            return False
        except Exception as e:
            logger.error(f"写入文件标记异常: {str(e)}")
            return False

    async def _position_to_block(self, block_number: int) -> bool:
        """定位到指定数据块 - LTFS 模式下不使用"""
        logger.warning("LTFS 模式下不使用块定位")
        return False

    async def _get_tape_position(self) -> Optional[int]:
        """获取当前磁带位置"""
        try:
            if self.linux_tape_operator:
                status = await self.linux_tape_operator.status()
                return status.get('file_number', 0)
            return None
        except Exception as e:
            logger.error(f"获取磁带位置异常: {str(e)}")
            return None

    async def _get_tape_capacity(self) -> Optional[Tuple[int, int]]:
        """获取磁带容量信息"""
        try:
            if self.linux_tape_operator:
                # 返回 (已用容量, 总容量)
                # LTFS 模式下通过 df 命令获取
                return (0, 18 * 1024 * 1024 * 1024 * 1024)  # 默认 18TB
            return None
        except Exception as e:
            logger.error(f"获取磁带容量异常: {str(e)}")
            return None

    async def _is_tape_formatted(self) -> bool:
        """检查磁带是否已格式化为 LTFS"""
        try:
            from backup.tape_handler import TapeHandler
            tape_handler = TapeHandler(
                tape_manager=None,
                settings=self.settings,
                dingtalk_notifier=None
            )
            is_ltfs, msg = await tape_handler.check_ltfs_format()
            return is_ltfs
        except Exception as e:
            logger.warning(f"检测 LTFS 格式化状态失败: {str(e)}")
            return False

    async def _read_tape_label(self) -> Optional[Dict[str, Any]]:
        """读取磁带卷标（Linux模式）"""
        logger.info("========== 开始读取磁带卷标 ==========")
        try:
            import platform
            import subprocess

            logger.info(f"操作系统: {platform.system()}, 磁带设备: {getattr(self.settings, 'TAPE_DEVICE_PATH', '/dev/nst0')}")

            if platform.system() == "Linux":
                tape_device = getattr(self.settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
                logger.info(f"[Linux磁带] 尝试从设备读取标签: {tape_device}")

                try:
                    if not os.path.exists(tape_device):
                        logger.warning(f"[Linux磁带] 设备不存在: {tape_device}")
                        return None

                    # 使用 mt status 获取磁带状态
                    def run_mt_status():
                        try:
                            result = subprocess.run(
                                ['mt', '-f', tape_device, 'status'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=30
                            )
                            return result.stdout.decode('utf-8', errors='ignore'), result.stderr.decode('utf-8', errors='ignore'), result.returncode
                        except subprocess.TimeoutExpired:
                            logger.error("[Linux磁带] mt status 命令超时")
                            return "", "timeout", -1
                        except Exception as e:
                            logger.error(f"[Linux磁带] mt status 命令失败: {str(e)}")
                            return "", str(e), -1

                    stdout_str, stderr_str, returncode = await asyncio.to_thread(run_mt_status)
                    logger.info(f"[Linux磁带] mt status 返回码: {returncode}")

                    # 尝试读取磁带第一个文件的标签
                    def run_mt_rewind():
                        try:
                            result = subprocess.run(
                                ['mt', '-f', tape_device, 'rewind'],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=60
                            )
                            return result.returncode == 0
                        except Exception as e:
                            logger.error(f"[Linux磁带] mt rewind 失败: {str(e)}")
                            return False

                    rewind_ok = await asyncio.to_thread(run_mt_rewind)
                    if not rewind_ok:
                        logger.warning("[Linux磁带] 倒带失败，尝试直接读取")

                    # 使用 sg_read_attr 读取磁带 MAM 元数据（支持 LTFS）
                    def run_sg_read_attr():
                        try:
                            # 获取对应的 st 设备（nst0 -> st0）
                            st_device = tape_device.replace('nst', 'st')
                            result = subprocess.run(
                                ['sg_read_attr', st_device],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                timeout=30
                            )
                            return result.stdout.decode('utf-8', errors='ignore'), result.stderr.decode('utf-8', errors='ignore'), result.returncode
                        except subprocess.TimeoutExpired:
                            logger.error("[Linux磁带] sg_read_attr 读取超时")
                            return "", "timeout", -1
                        except Exception as e:
                            logger.error(f"[Linux磁带] sg_read_attr 读取失败: {str(e)}")
                            return "", str(e), -1

                    sg_stdout, sg_stderr, sg_returncode = await asyncio.to_thread(run_sg_read_attr)

                    # 从 sg_read_attr 输出中提取标签信息
                    label_from_sg = None
                    serial_from_sg = None
                    if sg_stdout:
                        for line in sg_stdout.split('\n'):
                            line = line.strip()
                            # 提取 User medium text label（卷标）
                            if 'User medium text label:' in line:
                                label_from_sg = line.split(':', 1)[1].strip()
                            # 提取 Barcode（序列号）
                            elif 'Barcode:' in line:
                                serial_from_sg = line.split(':', 1)[1].strip()

                    # 再次倒带
                    await asyncio.to_thread(run_mt_rewind)

                    # 构建返回的元数据
                    tape_id = label_from_sg or 'Unknown'
                    serial_number = serial_from_sg or ''

                    # 如果 sg_read_attr 也没读到，尝试从 mt status 提取序列号
                    if not serial_number and ('ONLINE' in stdout_str.upper() or 'DR_OPEN' not in stdout_str.upper()):
                        import re
                        serial_match = re.search(r'SN[:\s]+([A-Z0-9]+)', stdout_str, re.IGNORECASE)
                        if serial_match:
                            serial_number = serial_match.group(1)

                    metadata = {
                        'tape_id': tape_id,
                        'label': tape_id,
                        'serial_number': serial_number,
                        'device_path': tape_device,
                        'mt_status': stdout_str[:500] if stdout_str else '',
                        'sg_output': sg_stdout[:500] if sg_stdout else ''
                    }
                    logger.info(f"[Linux磁带] 读取磁带信息成功: {metadata['tape_id']}")
                    return metadata

                except Exception as e:
                    logger.warning(f"[Linux磁带] 读取磁带标签失败: {str(e)}", exc_info=True)
                    return None
            else:
                logger.warning(f"不支持的平台: {platform.system()}，仅支持 Linux")
                return None

        except Exception as e:
            logger.error(f"读取磁带卷标异常: {str(e)}", exc_info=True)
            return None

    async def _write_tape_label(self, tape_info: Dict[str, Any]) -> bool:
        """写入磁带卷标（Linux模式 - 通过写入标签文件）"""
        try:
            import platform
            import subprocess

            if platform.system() == "Linux":
                tape_device = getattr(self.settings, 'TAPE_DEVICE_PATH', '/dev/nst0')
                tape_id = tape_info.get('tape_id', '')
                label = tape_info.get('label', tape_id)

                logger.info(f"[Linux磁带] 尝试写入标签: {label} 到设备 {tape_device}")

                try:
                    if not os.path.exists(tape_device):
                        logger.warning(f"[Linux磁带] 设备不存在: {tape_device}")
                        return False

                    import tempfile

                    def write_label_file():
                        try:
                            with tempfile.TemporaryDirectory() as tmpdir:
                                label_file = os.path.join(tmpdir, label)
                                with open(label_file, 'w') as f:
                                    f.write(f"Tape Label: {label}\n")
                                    f.write(f"Tape ID: {tape_id}\n")
                                    f.write(f"Created: {datetime.now().isoformat()}\n")

                                result = subprocess.run(
                                    ['tar', '-cvf', tape_device, label],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    timeout=120,
                                    cwd=tmpdir
                                )
                                return result.returncode == 0, result.stderr.decode('utf-8', errors='ignore')
                        except Exception as e:
                            logger.error(f"[Linux磁带] 写入标签文件失败: {str(e)}")
                            return False, str(e)

                    success, error_msg = await asyncio.to_thread(write_label_file)

                    if success:
                        logger.info(f"[Linux磁带] 标签写入成功: {label}")
                        return True
                    else:
                        logger.warning(f"[Linux磁带] 标签写入失败: {error_msg}")
                        return False

                except Exception as e:
                    logger.warning(f"[Linux磁带] 写入标签失败: {str(e)}")
                    return False
            else:
                logger.error(f"不支持的平台: {platform.system()}，仅支持 Linux")
                return False

        except Exception as e:
            logger.error(f"写入磁带卷标异常: {str(e)}")
            return False
