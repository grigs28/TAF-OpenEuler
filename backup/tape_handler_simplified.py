#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带处理模块 - 极简版
Tape Handler Module - Ultra-Simplified Version

极简流程：
1. 检查磁带是否在驱动器（失败→停止任务+发钉钉）
2. 读取现有卷标
3. 直接格式化为 LTFS（失败→停止任务+发钉钉）
4. 挂载（失败→停止任务+发钉钉）

移除：
- 尝试挂载判断格式
- is_tape_empty() 检查
- "有内容则格式化"的判断
"""

import asyncio
import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

from models.backup import BackupSet
from tape.tape_manager import TapeManager
from tape.tape_cartridge import TapeCartridge, TapeStatus

logger = logging.getLogger(__name__)

LTFS_BIN = "/usr/local/bin/ltfs"
MKLTFS_BIN = "/usr/local/bin/mkltfs"
DEFAULT_LTFS_MOUNT = "/mnt/ltfs"


class TapeHandlerSimplified:
    """磁带处理器 - 极简版
    
    流程：
    1. 检查磁带是否在驱动器
    2. 读取现有卷标
    3. 直接格式化为 LTFS
    4. 挂载
    
    失败→停止任务+发钉钉
    """

    def __init__(self, tape_manager: TapeManager = None, settings=None, dingtalk_notifier=None):
        self.tape_manager = tape_manager
        self.settings = settings
        self.dingtalk_notifier = dingtalk_notifier
        self._ltfs_mounted = False
        self._ltfs_mount_point = None
        self._ltfs_process = None

    def _get_tape_device(self) -> str:
        return getattr(self.settings, 'TAPE_DRIVE_LETTER', '/dev/nst0')

    def _get_ltfs_device(self) -> str:
        ltfs_device = getattr(self.settings, 'LTFS_DEVICE_PATH', None)
        if ltfs_device:
            return ltfs_device
        tape_device = self._get_tape_device()
        if tape_device.startswith('/dev/nst') or tape_device.startswith('/dev/st'):
            import os
            for i in range(10):
                sg_path = f'/dev/sg{i}'
                if os.path.exists(sg_path):
                    return sg_path
        return tape_device

    def _get_ltfs_mount_point(self) -> Path:
        mount_point = getattr(self.settings, 'LTFS_MOUNT_POINT', DEFAULT_LTFS_MOUNT)
        return Path(mount_point)

    async def _run_command(self, cmd: list, timeout: int = 300, check: bool = False) -> subprocess.CompletedProcess:
        logger.debug(f"执行命令: {' '.join(cmd)}")
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"命令失败: {' '.join(cmd)}\n{result.stderr}")
        return result

    async def check_tape_in_drive(self) -> Tuple[bool, str]:
        """检查磁带是否在驱动器中"""
        tape_device = self._get_tape_device()
        try:
            result = await self._run_command(['mt', '-f', tape_device, 'status'], timeout=30)
            output = result.stdout + result.stderr
            if 'ONLINE' in output:
                logger.info("[LTFS] 磁带已加载")
                return True, "磁带已加载"
            elif 'DR_OPEN' in output or 'No tape' in output:
                logger.warning("[LTFS] 驱动器中没有磁带")
                return False, "驱动器中没有磁带"
            else:
                logger.warning(f"[LTFS] 磁带状态未知: {output[:100]}")
                return False, f"状态未知: {output[:100]}"
        except Exception as e:
            logger.error(f"[LTFS] 检查磁带状态失败: {e}")
            return False, str(e)

    async def reset_tape_device(self) -> bool:
        """重置磁带设备"""
        tape_device = self._get_tape_device()
        try:
            sg_device = tape_device.replace('/dev/nst', '/dev/sg').replace('/dev/st', '/dev/sg')
            if sg_device == tape_device:
                sg_device = '/dev/sg2'
            await self._run_command(['sg_reset', '-d', '-N', sg_device], timeout=30, check=False)
            await asyncio.sleep(2)
            await self._run_command(['mt', '-f', tape_device, 'rewind'], timeout=60, check=False)
            logger.info("[LTFS] 磁带设备已重置")
            return True
        except Exception as e:
            logger.warning(f"[LTFS] 重置磁带设备失败: {e}")
            return False

    async def format_as_ltfs(self, volume_name: str = "BACKUP", serial: str = None) -> Tuple[bool, str]:
        """格式化磁带为 LTFS 格式"""
        tape_device = self._get_ltfs_device()
        logger.info(f"[LTFS] ========== 开始格式化 ==========")
        logger.info(f"[LTFS] 设备: {tape_device}, 卷标: {volume_name}")
        
        try:
            mount_point = self._get_ltfs_mount_point()
            if mount_point.is_mount():
                await self._run_command(['fusermount', '-u', str(mount_point)], timeout=30, check=False)
                await asyncio.sleep(2)
            await self._run_command(['pkill', '-9', '-f', 'ltfs'], timeout=10, check=False)
            await asyncio.sleep(2)
        except Exception as cleanup_err:
            logger.warning(f"[LTFS] 清理旧进程时出错: {cleanup_err}")
        
        if not serial or len(serial) != 6:
            serial = f"T{int(time.time()) % 100000:05d}"
        
        try:
            cmd = [MKLTFS_BIN, '-d', tape_device, '-n', volume_name, '-s', serial, '-f']
            logger.info(f"[LTFS] 执行命令: {' '.join(cmd)}")
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            async def read_stream(stream, output_list, prefix):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    line_text = line.decode('utf-8', errors='replace').strip()
                    if line_text:
                        output_list.append(line_text)
                        logger.info(f"[LTFS] {prefix}: {line_text}")
            
            stderr_output = []
            stdout_output = []
            
            await asyncio.gather(
                read_stream(process.stdout, stdout_output, "stdout"),
                read_stream(process.stderr, stderr_output, "mkltfs")
            )
            
            await process.wait()
            
            stderr_text = '\n'.join(stderr_output)
            
            if process.returncode == 0 or 'Medium formatted successfully' in stderr_text:
                logger.info(f"[LTFS] ✅ 格式化成功! 卷标: {volume_name}, 序列号: {serial}")
                return True, f"格式化成功: 卷标={volume_name}, 序列号={serial}"
            else:
                error = stderr_text[:500] if stderr_text else "未知错误"
                logger.error(f"[LTFS] ❌ 格式化失败: {error}")
                return False, f"格式化失败: {error}"
        
        except Exception as e:
            logger.error(f"[LTFS] 格式化异常: {e}")
            return False, str(e)

    async def mount_ltfs_only(self) -> Tuple[bool, str]:
        """仅挂载 LTFS（不检查格式，直接挂载）"""
        tape_device = self._get_ltfs_device()
        mount_point = self._get_ltfs_mount_point()
        
        logger.info(f"[LTFS] ========== 开始挂载 ==========")
        logger.info(f"[LTFS] 设备: {tape_device}, 挂载点: {mount_point}")
        
        mount_point.mkdir(parents=True, exist_ok=True)
        
        if mount_point.is_mount():
            try:
                await self._run_command(['fusermount', '-u', str(mount_point)], timeout=30, check=False)
                await asyncio.sleep(2)
            except Exception:
                pass
        
        try:
            for item in mount_point.iterdir():
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
        except Exception as e:
            logger.warning(f"[LTFS] 清空挂载点目录失败: {e}")
        
        try:
            process = await asyncio.create_subprocess_exec(
                LTFS_BIN, '-o', f'devname={tape_device}', str(mount_point),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            await asyncio.sleep(5)
            
            if mount_point.is_mount():
                self._ltfs_mounted = True
                self._ltfs_mount_point = mount_point
                self._ltfs_process = process
                logger.info("[LTFS] ✅ 挂载成功")
                return True, "挂载成功"
            
            await asyncio.sleep(10)
            if mount_point.is_mount():
                self._ltfs_mounted = True
                self._ltfs_mount_point = mount_point
                self._ltfs_process = process
                logger.info("[LTFS] ✅ 挂载成功（延迟检测）")
                return True, "挂载成功"
            
            try:
                process.terminate()
                await asyncio.sleep(1)
                if process.returncode is None:
                    process.kill()
            except Exception:
                pass
            
            _, stderr = await process.communicate()
            error_msg = stderr[:200] if stderr else "挂载超时"
            logger.error(f"[LTFS] ❌ 挂载失败: {error_msg}")
            return False, f"挂载失败: {error_msg}"
        
        except Exception as e:
            logger.error(f"[LTFS] 挂载异常: {e}")
            return False, str(e)

    async def mount_ltfs(self, backup_task=None) -> Tuple[bool, str]:
        """挂载 LTFS（极简版）
        
        极简流程：
        1. 检查磁带是否在驱动器（失败→停止任务+发钉钉）
        2. 读取现有卷标
        3. 直接格式化为 LTFS（失败→停止任务+发钉钉）
        4. 挂载（失败→停止任务+发钉钉）
        """
        async def send_failure_notification(error_msg: str):
            """统一失败通知函数"""
            logger.error(f"[LTFS] ❌ {error_msg}")
            if self.dingtalk_notifier and backup_task:
                try:
                    await self.dingtalk_notifier.send_tape_notification(
                        tape_id="unknown",
                        action="error",
                        details={
                            "error": f"磁带挂载失败: {error_msg}",
                            "task_name": getattr(backup_task, 'task_name', '未知任务'),
                            "task_id": getattr(backup_task, 'id', 'unknown')
                        }
                    )
                    logger.info("[LTFS] 钉钉通知已发送")
                except Exception as notify_error:
                    logger.warning(f"[LTFS] 发送钉钉通知失败: {notify_error}")
        
        logger.info("[LTFS] ========== 极简挂载流程开始 ==========")
        
        if self._ltfs_mounted:
            logger.info("[LTFS] 已经挂载，直接返回")
            return True, "已挂载"
        
        logger.info("[LTFS] 步骤1: 检查磁带是否在驱动器...")
        in_drive, status = await self.check_tape_in_drive()
        if not in_drive:
            error_msg = f"磁带未就绪: {status}"
            await send_failure_notification(error_msg)
            return False, error_msg
        
        existing_label = "Unknown"
        try:
            if self.tape_manager and self.tape_manager.tape_operations:
                tape_ops = self.tape_manager.tape_operations
                if hasattr(tape_ops, '_read_tape_label'):
                    label_info = await tape_ops._read_tape_label()
                    if label_info and label_info.get('tape_id'):
                        existing_label = label_info.get('tape_id')
                        logger.info(f"[LTFS] 读取到现有卷标: {existing_label}")
        except Exception as e:
            logger.warning(f"[LTFS] 读取卷标失败: {e}，将作为新磁带处理")
        
        await self.unmount_ltfs()
        await self.reset_tape_device()
        
        volume_name = "BACKUP"
        serial = None
        
        if existing_label and existing_label != "Unknown":
            volume_name = existing_label
            try:
                import re
                match = re.search(r'TP(\d{2})(\d{2})$', existing_label)
                if match:
                    serial = f"TP{match.group(1)}{match.group(2)}"
                    logger.info(f"[LTFS] 使用现有卷标: {volume_name}, 序列号: {serial}")
            except Exception as e:
                logger.warning(f"[LTFS] 解析现有卷标失败: {e}")
        else:
            logger.info("[LTFS] 磁带无有效卷标，自动生成...")
            try:
                from backup.tape_label_generator import generate_tape_label_and_serial
                label_info = await generate_tape_label_and_serial()
                volume_name = label_info.label
                serial = label_info.serial_number
                logger.info(f"[LTFS] 自动生成标签: {volume_name}, 序列号: {serial}")
            except Exception as e:
                logger.warning(f"[LTFS] 自动生成标签失败: {e}，使用默认值")
        
        logger.info("[LTFS] 步骤2: 直接格式化为 LTFS...")
        success, msg = await self.format_as_ltfs(volume_name=volume_name, serial=serial)
        if not success:
            error_msg = f"格式化失败: {msg}"
            await send_failure_notification(error_msg)
            return False, error_msg
        
        await asyncio.sleep(3)
        
        logger.info("[LTFS] 步骤3: 挂载 LTFS...")
        success, msg = await self.mount_ltfs_only()
        if not success:
            error_msg = f"挂载失败: {msg}"
            await send_failure_notification(error_msg)
            return False, error_msg
        
        if serial and volume_name != "BACKUP" and (existing_label == "Unknown" or not existing_label):
            # 使用 psycopg 同步连接注册磁带到数据库（与添加磁带相同的方式）
            try:
                from utils.db_connection_helper import get_psycopg_connection_from_url, set_autocommit
                from config.settings import get_settings as _get_settings

                _settings = _get_settings()
                _db_url = _settings.DATABASE_URL
                _conn, _is_psycopg3 = get_psycopg_connection_from_url(_db_url, prefer_psycopg3=True)
                try:
                    set_autocommit(_conn, _is_psycopg3, autocommit=True)
                    with _conn.cursor() as cur:
                        cur.execute("SELECT 1 FROM tape_cartridges WHERE tape_id = %s", (volume_name,))
                        _tape_exists = cur.fetchone() is not None

                        if not _tape_exists:
                            logger.info(f"[LTFS] 数据库中不存在磁带 {volume_name}，开始录入...")
                            from datetime import datetime
                            now = datetime.now()
                            _expiry_month = now.month + 6
                            _expiry_year = now.year
                            while _expiry_month > 12:
                                _expiry_year += 1
                                _expiry_month -= 12
                            _expiry_date = datetime(_expiry_year, _expiry_month, 1)

                            cur.execute(
                                """
                                INSERT INTO tape_cartridges
                                (tape_id, label, status, media_type, generation, serial_number, location,
                                 capacity_bytes, used_bytes, retention_months, notes, manufactured_date, expiry_date, auto_erase, health_score)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    volume_name,
                                    volume_name,
                                    'available',
                                    'LTO',
                                    9,
                                    serial,
                                    '',
                                    18 * 1024 ** 4,
                                    0,
                                    6,
                                    '备份任务格式化后自动注册',
                                    now,
                                    _expiry_date,
                                    True,
                                    100
                                )
                            )
                            logger.info(f"[LTFS] ✅ 磁带已注册到数据库: {volume_name}")
                        else:
                            logger.info(f"[LTFS] 数据库中已存在磁带 {volume_name}，跳过录入")

                    if self.tape_manager:
                        from datetime import datetime
                        new_tape = TapeCartridge(
                            tape_id=volume_name,
                            label=volume_name,
                            status=TapeStatus.AVAILABLE,
                            generation=9,
                            serial_number=serial,
                            capacity_bytes=18 * 1024**4,
                            manufactured_date=datetime.now()
                        )
                        self.tape_manager.tape_cartridges[volume_name] = new_tape
                        self.tape_manager.current_tape = new_tape
                finally:
                    _conn.close()
            except Exception as e:
                logger.warning(f"[LTFS] 数据库注册异常: {e}")
        
        logger.info(f"[LTFS] ========== ✅ 极简挂载流程完成 ==========")
        return True, f"格式化并挂载成功: {volume_name}"

    async def unmount_ltfs(self) -> bool:
        """卸载 LTFS"""
        if not self._ltfs_mounted:
            return True
        
        mount_point = self._ltfs_mount_point
        if not mount_point:
            return True
        
        try:
            await self._run_command(['fusermount', '-u', str(mount_point)], timeout=60, check=False)
            self._ltfs_mounted = False
            self._ltfs_process = None
            logger.info("[LTFS] 已卸载")
            return True
        except Exception as e:
            logger.warning(f"[LTFS] 卸载失败: {e}")
            try:
                await self._run_command(['umount', '-l', str(mount_point)], timeout=30, check=False)
                self._ltfs_mounted = False
                return True
            except:
                return False

    async def write_to_tape_drive(self, source_path: str, backup_set: BackupSet, group_idx: int) -> Optional[str]:
        """将文件写入磁带"""
        try:
            source_file = Path(source_path)
            if not source_file.exists():
                logger.error(f"[LTFS] 源文件不存在: {source_path}")
                return None
            
            source_size = source_file.stat().st_size
            source_size_mb = source_size / (1024 * 1024)
            logger.info(f"[LTFS] 准备写入文件: {source_file.name} ({source_size_mb:.2f} MB)")
            
            success, msg = await self.mount_ltfs()
            if not success:
                logger.error(f"[LTFS] 挂载失败，停止写入: {msg}")
                return None
            
            mount_point = self._ltfs_mount_point
            if not mount_point:
                logger.error("[LTFS] 挂载点为空")
                return None
            
            target_dir = mount_point / backup_set.set_id
            target_dir.mkdir(parents=True, exist_ok=True)
            target_file = target_dir / source_file.name
            
            logger.info(f"[LTFS] 开始复制: {source_file} -> {target_file}")
            start_time = time.time()
            
            try:
                await asyncio.to_thread(shutil.copy2, str(source_file), str(target_file))
            except asyncio.CancelledError:
                logger.warning("[LTFS] 复制任务被取消")
                raise
            except Exception as e:
                logger.error(f"[LTFS] 复制失败: {e}")
                return None
            
            elapsed = time.time() - start_time
            speed_mb = source_size_mb / elapsed if elapsed > 0 else 0
            
            if not target_file.exists():
                logger.error(f"[LTFS] 目标文件不存在: {target_file}")
                return None
            
            target_size = target_file.stat().st_size
            if target_size != source_size:
                logger.error(f"[LTFS] 文件大小不匹配: 源={source_size}, 目标={target_size}")
                try:
                    target_file.unlink()
                except:
                    pass
                return None
            
            logger.info(f"[LTFS] ✅ 写入成功: {source_size_mb:.2f} MB, 耗时 {elapsed:.1f}s, 速度 {speed_mb:.2f} MB/s")
            
            try:
                await asyncio.to_thread(source_file.unlink)
                logger.info(f"[LTFS] 源文件已删除: {source_file}")
            except Exception as e:
                logger.warning(f"[LTFS] 删除源文件失败: {e}")
            
            relative_path = str(target_file.relative_to(mount_point))
            return relative_path
        
        except asyncio.CancelledError:
            logger.warning("[LTFS] 写入任务被取消")
            raise
        except Exception as e:
            logger.error(f"[LTFS] 写入磁带失败: {e}")
            return None
