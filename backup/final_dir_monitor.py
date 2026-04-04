#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Final目录监控器
独立线程监控final目录，发现文件后顺序移动到磁带
"""

import asyncio
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Set
from datetime import datetime

from config.settings import get_settings
from backup.tape_handler import TapeHandler
from models.backup import BackupSet
from backup.utils import format_bytes

logger = logging.getLogger(__name__)


class FinalDirMonitor:
    """Final目录监控器
    
    功能：
    1. 独立线程监控final目录（不阻塞其他程序，也不被其他程序阻塞）
    2. 每10秒轮询扫描final目录
    3. 发现文件后顺序移动到磁带（移动完一个再移动下一个）
    4. 支持任务完成判断
    """
    
    def __init__(self, tape_handler: TapeHandler, settings=None, dingtalk_notifier=None):
        """
        初始化Final目录监控器

        Args:
            tape_handler: TapeHandler实例，用于实际移动文件
            settings: 系统设置
            dingtalk_notifier: 钉钉通知器（可选）
        """
        self.tape_handler = tape_handler
        self.settings = settings or get_settings()
        self.dingtalk_notifier = dingtalk_notifier
        self._worker_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        self._scan_interval = 10  # 扫描间隔（秒）
        self._processed_files: Set[str] = set()  # 已处理文件的集合（完整路径）
        self._current_tape_file: Optional[str] = None  # 当前正在写入磁带的文件路径
        
    def start(self):
        """启动监控线程"""
        with self._lock:
            if self._running:
                logger.warning("[Final监控] 监控线程已经在运行")
                return
            
            self._running = True
            self._worker_thread = threading.Thread(
                target=self._monitor_loop,
                name="FinalDirMonitor",
                daemon=True
            )
            self._worker_thread.start()
            logger.info("[Final监控] Final目录监控线程已启动（10秒轮询扫描）")
    
    def stop(self):
        """停止监控线程（等待当前文件写入磁带完成）"""
        with self._lock:
            if not self._running:
                return

            self._running = False
            if self._worker_thread and self._worker_thread.is_alive():
                logger.info("[Final监控] 等待当前磁带写入完成...")
                self._worker_thread.join(timeout=300)  # 5分钟，等待大文件写入完成
                if self._worker_thread.is_alive():
                    logger.warning("[Final监控] 监控线程未能在5分钟内停止")
                else:
                    logger.info("[Final监控] Final目录监控线程已停止")
    
    def _get_final_dir(self) -> Path:
        """获取final目录路径"""
        compress_dir = Path(self.settings.BACKUP_COMPRESS_DIR)
        final_dir = compress_dir / "final"
        return final_dir

    def _send_failure_notification(self, error_msg: str):
        """发送失败通知到钉钉"""
        logger.error(f"[Final监控] 准备发送失败通知: {error_msg}")
        if self.dingtalk_notifier:
            try:
                # 在新的事件循环中运行异步通知
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        self.dingtalk_notifier.send_backup_notification(
                            "磁带写入",
                            "failed",
                            {'error': error_msg}
                        )
                    )
                    logger.info("[Final监控] 失败通知已发送到钉钉")
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)
            except Exception as notify_error:
                logger.warning(f"[Final监控] 发送钉钉通知失败: {str(notify_error)}")
        else:
            logger.warning("[Final监控] 未配置钉钉通知器，无法发送失败通知")

    def _extract_backup_set_id_from_path(self, file_path: Path) -> Optional[str]:
        """从文件路径提取backup_set.set_id
        
        路径格式: temp/compress/final/{set_id}/backup_xxx.tar.zst
        """
        try:
            # 获取相对于final目录的路径
            final_dir = self._get_final_dir()
            try:
                relative_path = file_path.relative_to(final_dir)
                # 第一级目录就是set_id
                parts = relative_path.parts
                if len(parts) >= 1:
                    return parts[0]  # set_id
            except ValueError:
                # 如果无法计算相对路径，尝试从文件名提取
                filename = file_path.name
                if filename.startswith("backup_"):
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        return parts[1]  # set_id
            return None
        except Exception as e:
            logger.debug(f"[Final监控] 提取backup_set_id失败: {file_path}, 错误: {str(e)}")
            return None

    def _move_file_to_tape(self, file_path: Path) -> bool:
        """
        移动单个文件到磁带

        Args:
            file_path: 源文件路径

        Returns:
            bool: 是否成功
        """
        try:
            source_file = file_path
            if not source_file.exists():
                logger.warning(f"[Final监控] 文件不存在: {source_file}")
                return False

            # 获取源文件大小（用于验证）
            source_size = source_file.stat().st_size
            logger.info(f"[Final监控] 开始移动文件到磁带: {source_file.name} (大小: {format_bytes(source_size)})")

            # 从路径提取backup_set_id
            backup_set_id = self._extract_backup_set_id_from_path(source_file)
            if not backup_set_id:
                logger.warning(f"[Final监控] 无法从路径提取backup_set_id: {source_file}")
                # 创建一个临时的BackupSet对象
                backup_set = BackupSet()
                backup_set.set_id = "unknown"
            else:
                backup_set = BackupSet()
                backup_set.set_id = backup_set_id

            # 目标路径由 tape_handler 处理（Linux 用 tar 写磁带设备，Windows 用 LTFS 盘符）
            # 这里不需要预先创建目录

            # 在工作线程中运行异步操作
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # 调用tape_handler的write_to_tape_drive方法
                tape_file_path = loop.run_until_complete(
                    self.tape_handler.write_to_tape_drive(
                        str(source_file),
                        backup_set,
                        0  # group_idx，这里不需要，传0
                    )
                )

                if tape_file_path:
                    logger.info(f"[Final监控] ✅ 文件已成功移动到磁带: {source_file.name} -> {tape_file_path}")

                    # 记录文件写入磁带到数据库
                    try:
                        from utils.log_utils import log_operation
                        from models.system_log import OperationType
                        loop.run_until_complete(
                            log_operation(
                                operation_type=OperationType.TAPE_LOAD,
                                resource_type="tape",
                                resource_name=str(file_path.name),
                                operation_name="文件写入磁带",
                                operation_description=f"文件 {file_path.name} 已写入磁带",
                                category="tape",
                                success=True,
                            )
                        )
                    except Exception:
                        pass

                    return True
                else:
                    error_msg = f"文件移动到磁带失败: {source_file.name}"
                    logger.error(f"[Final监控] ❌ {error_msg}")
                    # 发送钉钉通知
                    self._send_failure_notification(error_msg)
                    return False

            finally:
                # 确保关闭事件循环，释放资源
                try:
                    pending = asyncio.all_tasks(loop)
                    for task_obj in pending:
                        task_obj.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    pass
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)

        except Exception as e:
            logger.error(f"[Final监控] 移动文件到磁带失败: {file_path}, 错误: {str(e)}", exc_info=True)
            return False
    
    def _monitor_loop(self):
        """监控循环：扫描final目录，发现文件后顺序移动到磁带"""
        logger.info("[Final监控] ========== Final目录监控线程已启动 ==========")
        logger.info(f"[Final监控] 扫描间隔: {self._scan_interval}秒")
        
        try:
            while self._running:
                try:
                    # 扫描final目录
                    final_dir = self._get_final_dir()
                    
                    if not final_dir.exists():
                        logger.debug(f"[Final监控] final目录不存在: {final_dir}，等待 {self._scan_interval} 秒后重试")
                        time.sleep(self._scan_interval)
                        continue
                    
                    # 递归扫描所有子目录，查找压缩文件
                    found_files = []
                    for root, dirs, files in os.walk(final_dir):
                        root_path = Path(root)
                        for file_name in files:
                            file_path = root_path / file_name
                            
                            if not file_path.is_file():
                                continue
                            
                            # 检查是否是压缩文件
                            if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                                # 检查是否已处理过
                                file_key = str(file_path)
                                if file_key not in self._processed_files:
                                    found_files.append(file_path)
                    
                    # 顺序处理找到的文件（移动完一个再移动下一个）
                    if found_files:
                        logger.info(f"[Final监控] 扫描到 {len(found_files)} 个新文件待移动到磁带")
                        
                        for file_path in found_files:
                            if not self._running:
                                break
                            
                            file_key = str(file_path)
                            
                            # 检查文件是否仍然存在
                            if not file_path.exists():
                                logger.debug(f"[Final监控] 文件已不存在，跳过: {file_path.name}")
                                self._processed_files.add(file_key)
                                continue
                            
                            # 移动文件到磁带
                            self._current_tape_file = str(file_path)
                            success = self._move_file_to_tape(file_path)
                            self._current_tape_file = None
                            
                            # 标记为已处理（无论成功与否，避免重复处理）
                            self._processed_files.add(file_key)
                            
                            if success:
                                logger.debug(f"[Final监控] 文件处理完成: {file_path.name}")

                            else:
                                logger.error(f"[Final监控] 文件处理失败: {file_path.name}")
                    else:
                        # 没有找到新文件，等待后继续扫描
                        time.sleep(self._scan_interval)
                        
                except Exception as scan_error:
                    logger.error(f"[Final监控] 扫描final目录时发生错误: {str(scan_error)}", exc_info=True)
                    time.sleep(self._scan_interval)
                    
        except Exception as e:
            logger.error(f"[Final监控] 监控循环异常: {str(e)}", exc_info=True)
        finally:
            logger.info("[Final监控] Final目录监控线程已退出")
    
    def is_final_dir_empty(self, set_id: str = None) -> bool:
        """
        检查final目录是否为空（用于任务完成判断）

        Args:
            set_id: 备份集ID（如 "2026-03_000001"）。如果传入，只检查 final/{set_id}/ 子目录；
                    如果为 None，检查整个 final/ 目录。

        Returns:
            bool: final目录（指定set_id目录）是否为空
        """
        try:
            final_dir = self._get_final_dir()
            if set_id:
                final_dir = final_dir / set_id

            if not final_dir.exists():
                return True

            # 检查是否有压缩文件
            for root, dirs, files in os.walk(final_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    if file_path.is_file():
                        # 检查是否是压缩文件
                        if file_path.suffix in ['.7z', '.gz', '.tar', '.zst'] or file_path.name.endswith('.tar.gz'):
                            return False

            return True
        except Exception as e:
            logger.error(f"[Final监控] 检查final目录是否为空时发生错误: {str(e)}")
            return True  # 出错时假设为空，避免阻塞
    
    def get_processed_count(self) -> int:
        """获取已处理文件数量"""
        return len(self._processed_files)

    def cleanup_final_dir(self) -> int:
        """清理final目录中所有文件（保留当前正在写入磁带的文件）

        用于系统关闭时，删除未写入磁带的压缩文件，避免下次启动时重复处理。
        正在写入磁带的文件会被保留（由 FinalDirMonitor.stop() 等待其完成）。

        Returns:
            int: 删除的文件数量
        """
        deleted_count = 0
        try:
            final_dir = self._get_final_dir()
            if not final_dir.exists():
                return 0

            current_file = self._current_tape_file
            if current_file:
                logger.info(f"[Final监控] 清理final目录，保留当前写入文件: {current_file}")
            else:
                logger.info("[Final监控] 清理final目录（无正在写入的文件）")

            for root, dirs, files in os.walk(final_dir):
                for file_name in files:
                    file_path = Path(root) / file_name
                    if not file_path.is_file():
                        continue

                    # 保留正在写入磁带的文件
                    if current_file and str(file_path) == current_file:
                        logger.info(f"[Final监控] 保留正在写入的文件: {file_path}")
                        continue

                    try:
                        file_path.unlink(missing_ok=True)
                        deleted_count += 1
                        logger.info(f"[Final监控] 已删除: {file_path}")
                    except Exception as e:
                        logger.warning(f"[Final监控] 删除文件失败 {file_path}: {e}")

            # 清理空的 set_id 子目录
            if deleted_count > 0:
                for item in final_dir.iterdir():
                    if item.is_dir():
                        try:
                            # 检查目录是否为空
                            has_files = any(
                                Path(root) / f
                                for root, _, files in os.walk(item)
                                for f in files
                                if (Path(root) / f).is_file()
                            )
                            if not has_files:
                                shutil.rmtree(item, ignore_errors=True)
                                logger.info(f"[Final监控] 已清理空目录: {item}")
                        except Exception:
                            pass

            logger.info(f"[Final监控] final目录清理完成，共删除 {deleted_count} 个文件")
        except Exception as e:
            logger.error(f"[Final监控] 清理final目录失败: {e}")

        return deleted_count

    def cleanup_set_id_dir(self, set_id: str) -> bool:
        """清理指定set_id的空目录（任务完成后调用）

        在确认final目录中所有压缩文件已移动到磁带后，安全删除空的set_id子目录。
        会再次检查目录内是否有文件，防止与监控线程的竞态条件。

        Args:
            set_id: 备份集ID (e.g., "2026-03_000001")

        Returns:
            bool: 是否成功清理
        """
        try:
            final_dir = self._get_final_dir()
            set_id_dir = final_dir / set_id

            if not set_id_dir.exists():
                return True  # 已不存在

            if not set_id_dir.is_dir():
                logger.warning(f"[Final监控] set_id路径不是目录: {set_id_dir}")
                return False

            # 再次确认目录内没有文件（防止与监控线程的竞态）
            has_files = False
            for root, dirs, files in os.walk(set_id_dir):
                for f in files:
                    fp = Path(root) / f
                    if fp.is_file():
                        logger.warning(f"[Final监控] set_id目录仍有文件，跳过清理: {fp}")
                        has_files = True
                        break
                if has_files:
                    break

            if has_files:
                return False

            # 目录为空，安全删除
            shutil.rmtree(set_id_dir, ignore_errors=True)
            logger.info(f"[Final监控] 已清理空的set_id目录: {set_id_dir}")
            return True
        except Exception as e:
            logger.error(f"[Final监控] 清理set_id目录失败: {e}")
            return False



