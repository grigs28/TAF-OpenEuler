#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理器
Tape Manager Module
"""

import os
import platform
import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path

from config.settings import get_settings
from tape.tape_cartridge import TapeCartridge, TapeStatus
from tape.tape_operations import TapeOperations

logger = logging.getLogger(__name__)

# Linux 原生磁带操作支持
_LINUX_TAPE_AVAILABLE = False
try:
    from utils.linux_tape import LinuxTapeOperator, get_linux_tape_operator
    _LINUX_TAPE_AVAILABLE = True
except ImportError:
    pass


def _parse_tape_status(status_value: str) -> TapeStatus:
    """
    解析磁带状态值（处理大小写不匹配问题）
    
    Args:
        status_value: 状态值（可能是大写、小写或混合大小写）
    
    Returns:
        TapeStatus: 磁带状态枚举值
    
    Raises:
        ValueError: 如果状态值无效
    """
    if not status_value:
        return TapeStatus.AVAILABLE
    
    # 转换为小写并去除空白
    status_lower = status_value.lower().strip()
    
    # 尝试直接匹配
    try:
        return TapeStatus(status_lower)
    except ValueError:
        # 如果直接匹配失败，尝试匹配枚举值
        for status in TapeStatus:
            if status.value.lower() == status_lower:
                return status
        
        # 如果仍然无法匹配，记录警告并返回默认值
        logger.warning(f"无法解析磁带状态值 '{status_value}'，使用默认值 AVAILABLE")
        return TapeStatus.AVAILABLE


class TapeManager:
    """磁带管理器"""

    def __init__(self):
        self.settings = get_settings()
        self.linux_tape_operator = None  # Linux 原生磁带操作器
        self.tape_operations = TapeOperations()
        self.tape_cartridges: Dict[str, TapeCartridge] = {}
        self.current_tape: Optional[TapeCartridge] = None
        self._initialized = False
        self._monitoring_task = None
        self.cached_devices: List[Dict[str, Any]] = []  # 缓存的设备列表
        self._scanning_task = None  # 后台扫描任务
        self._scan_in_progress = False  # 扫描进行中标志

    async def initialize(self):
        """初始化磁带管理器"""
        try:
            # 使用 Linux 原生磁带操作
            if _LINUX_TAPE_AVAILABLE:
                logger.info("使用 Linux 原生磁带操作")
                try:
                    self.linux_tape_operator = get_linux_tape_operator()
                    init_success = await self.linux_tape_operator.initialize()
                    if init_success:
                        logger.info("Linux 原生磁带操作初始化完成")
                        await self.tape_operations.initialize(linux_tape_operator=self.linux_tape_operator)
                        logger.info("磁带操作模块初始化完成（Linux 原生模式）")
                    else:
                        logger.warning("Linux 原生磁带操作初始化失败，磁带设备管理功能将不可用")
                except Exception as linux_err:
                    logger.warning(f"Linux 原生磁带操作初始化失败: {str(linux_err)}")
                    self.linux_tape_operator = None
            else:
                logger.warning("Linux 原生磁带操作不可用，磁带设备管理功能将不可用")

            # 尝试从配置快速加载设备（不阻塞）
            cached_devices = await self._load_cached_devices()
            if cached_devices:
                logger.info(f"从配置读取到 {len(cached_devices)} 个磁带设备（缓存）")
                self.cached_devices = cached_devices
            else:
                logger.info("配置中无设备缓存，将在后台扫描设备")

            # 在后台异步检测磁带设备（不阻塞启动）
            self._scanning_task = asyncio.create_task(self._detect_tape_devices())

            # 加载磁带信息
            await self._load_tape_inventory()

            # 启动磁带监控任务
            if self.settings.TAPE_CHECK_INTERVAL > 0:
                self._monitoring_task = asyncio.create_task(self._monitoring_loop())

            self._initialized = True
            logger.info("磁带管理器初始化完成（设备扫描在后台进行）")

        except Exception as e:
            logger.error(f"磁带管理器初始化失败: {str(e)}")
            raise

    async def _detect_tape_devices(self):
        """检测磁带设备（后台异步执行，不阻塞启动）"""
        if self._scan_in_progress:
            logger.debug("设备扫描已在进行中，跳过")
            return

        # 使用 LinuxTapeOperator 扫描设备
        if self.linux_tape_operator:
            logger.info("使用 Linux 原生方式扫描磁带设备...")
            self._scan_in_progress = True
            try:
                devices = await LinuxTapeOperator.scan_devices()
                logger.info(f"检测到 {len(devices)} 个磁带设备")
                for device in devices:
                    logger.info(f"设备: {device.get('path', 'unknown')}")
                if devices:
                    await self._save_cached_devices(devices)
                    self.cached_devices = devices
            except Exception as e:
                logger.error(f"Linux 设备扫描失败: {str(e)}")
            finally:
                self._scan_in_progress = False
            return

        # 如果没有 LinuxTapeOperator，使用缓存
        if hasattr(self, 'cached_devices') and self.cached_devices:
            logger.info(f"使用缓存的设备信息（{len(self.cached_devices)} 个）")
            return

        logger.warning("LinuxTapeOperator 不可用且无设备缓存，跳过设备扫描")
        self.cached_devices = []

    async def _load_cached_devices(self) -> List[Dict[str, Any]]:
        """从配置加载缓存的设备信息（openGauss模式从数据库读取，其他模式从.env文件读取）"""
        try:
            import json
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # openGauss模式：从数据库读取
                try:
                    async with get_opengauss_connection() as conn:
                        row = await conn.fetchrow(
                            "SELECT config_value FROM system_config WHERE config_key = $1",
                            "TAPE_DEVICES_CACHE"
                        )
                        if row and row.get('config_value'):
                            return json.loads(row['config_value'])
                    return []
                except Exception as db_err:
                    logger.debug(f"从数据库加载缓存设备失败: {str(db_err)}")
                    return []
            else:
                # 非openGauss模式：从.env文件读取
                from pathlib import Path
                env_file = Path(".env")
                if not env_file.exists():
                    return []
                
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("TAPE_DEVICES_CACHE="):
                            devices_json = line.split("=", 1)[1].strip()
                            if devices_json:
                                return json.loads(devices_json)
                return []
        except Exception as e:
            logger.debug(f"加载缓存设备失败: {str(e)}")
            return []

    async def _save_cached_devices(self, devices: List[Dict[str, Any]]):
        """保存设备信息到配置（openGauss模式保存到数据库，其他模式保存到.env文件）"""
        try:
            import json
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            from models.system_config import ConfigType
            
            if is_opengauss():
                # openGauss模式：保存到数据库
                try:
                    devices_json = json.dumps(devices, ensure_ascii=False)
                    
                    async with get_opengauss_connection() as conn:
                        # 检查配置是否存在
                        row = await conn.fetchrow(
                            "SELECT config_key FROM system_config WHERE config_key = $1",
                            "TAPE_DEVICES_CACHE"
                        )
                        
                        if row:
                            # 更新现有配置
                            await conn.execute(
                                "UPDATE system_config SET config_value = $1, updated_at = CURRENT_TIMESTAMP WHERE config_key = $2",
                                devices_json, "TAPE_DEVICES_CACHE"
                            )
                        else:
                            # 插入新配置
                            await conn.execute(
                                "INSERT INTO system_config (config_key, config_value, config_type, category, description, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                                "TAPE_DEVICES_CACHE", devices_json, ConfigType.JSON.value, "tape", "磁带设备缓存（自动生成，请勿手动修改）"
                            )
                        
                        # psycopg3 binary protocol 需要显式提交事务
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        try:
                            await actual_conn.commit()
                            logger.debug(f"设备缓存已提交到数据库")
                        except Exception as commit_err:
                            logger.warning(f"提交设备缓存事务失败（可能已自动提交）: {commit_err}")
                            try:
                                await actual_conn.rollback()
                            except:
                                pass
                    
                    logger.info(f"已保存 {len(devices)} 个设备到数据库")
                except Exception as db_err:
                    logger.warning(f"保存设备缓存到数据库失败: {str(db_err)}")
            else:
                # 非openGauss模式：保存到.env文件
                from pathlib import Path
                env_file = Path(".env")
                devices_json = json.dumps(devices, ensure_ascii=False)
                
                # 读取现有.env内容
                lines = []
                if env_file.exists():
                    with open(env_file, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                
                # 更新或添加 TAPE_DEVICES_CACHE
                updated = False
                for i, line in enumerate(lines):
                    if line.strip().startswith("TAPE_DEVICES_CACHE="):
                        lines[i] = f"TAPE_DEVICES_CACHE={devices_json}\n"
                        updated = True
                        break
                
                if not updated:
                    lines.append(f"\n# 磁带设备缓存（自动生成，请勿手动修改）\n")
                    lines.append(f"TAPE_DEVICES_CACHE={devices_json}\n")
                
                # 写入文件
                with open(env_file, "w", encoding="utf-8") as f:
                    f.writelines(lines)
                
                logger.info(f"已保存 {len(devices)} 个设备到配置")
        except Exception as e:
            logger.warning(f"保存设备缓存失败: {str(e)}")

    async def _verify_devices(self, devices: List[Dict[str, Any]]) -> bool:
        """验证设备是否可用（至少测试一个设备）"""
        if not devices:
            return False
        if self.linux_tape_operator is None:
            logger.warning("LinuxTapeOperator 不可用，无法验证设备")
            return False
        try:
            # 测试第一个设备是否可用
            first_device = devices[0]
            device_path = first_device.get('path')
            if not device_path:
                return False
            # 检查设备状态
            status = await self.linux_tape_operator.status()
            return status.get('online', False)
        except Exception:
            return False

    async def get_cached_devices(self) -> List[Dict[str, Any]]:
        """获取缓存的设备列表（优先使用缓存，如果缓存为空且扫描未进行中才触发扫描）"""
        if hasattr(self, 'cached_devices') and self.cached_devices:
            return self.cached_devices

        # 如果后台扫描正在进行，等待一下或返回空（避免重复扫描）
        if self._scan_in_progress:
            logger.debug("设备扫描正在进行中，等待完成...")
            # 等待最多3秒
            for _ in range(30):
                await asyncio.sleep(0.1)
                if hasattr(self, 'cached_devices') and self.cached_devices:
                    return self.cached_devices
            logger.warning("等待扫描超时，返回空列表")
            return []

        # 如果没有缓存且扫描未进行，触发扫描
        if self.linux_tape_operator is None:
            logger.warning("LinuxTapeOperator 不可用，无法扫描设备")
            return []

        logger.info("缓存为空，触发设备扫描...")
        try:
            devices = await LinuxTapeOperator.scan_devices()
            if devices:
                await self._save_cached_devices(devices)
                self.cached_devices = devices
                return devices
        except Exception as e:
            logger.error(f"获取设备列表失败: {str(e)}")
        return []

    async def _load_tape_inventory(self):
        """加载磁带库存信息"""
        try:
            # 从数据库加载磁带信息
            await self._load_tape_inventory_from_db()
        except Exception as e:
            logger.error(f"加载磁带库存信息失败: {str(e)}")
    
    async def _load_tape_inventory_from_db(self):
        """从数据库加载磁带库存信息"""
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用 openGauss 原生 SQL 查询
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    rows = await conn.fetch(
                        """
                        SELECT tape_id, label, status, first_use_date, manufactured_date, expiry_date,
                               capacity_bytes, used_bytes, serial_number, location,
                               media_type, generation, manufacturer, purchase_date, created_at
                        FROM tape_cartridges
                        ORDER BY COALESCE(first_use_date, manufactured_date, created_at) DESC
                        """
                    )
                    
                    for row in rows:
                        # 使用first_use_date、manufactured_date或created_at作为created_date（dataclass需要）
                        created_date = (row.get('first_use_date') or 
                                       row.get('manufactured_date') or 
                                       row.get('created_at') or 
                                       None)
                        
                        # 解析状态值（处理大小写不匹配问题）
                        status_value = row.get('status')
                        tape_status = _parse_tape_status(status_value) if status_value else TapeStatus.AVAILABLE
                        
                        tape = TapeCartridge(
                            tape_id=row['tape_id'],
                            label=row['label'],
                            status=tape_status,
                            created_date=created_date,
                            expiry_date=row.get('expiry_date'),
                            capacity_bytes=row['capacity_bytes'] or 0,
                            used_bytes=row['used_bytes'] or 0,
                            serial_number=row.get('serial_number') or '',
                            location=row.get('location') or '',
                            media_type=row.get('media_type') or 'LTO',
                            generation=row.get('generation') or 8,
                            manufacturer=row.get('manufacturer') or ''
                        )
                        self.tape_cartridges[tape.tape_id] = tape
                    
                    logger.info(f"从数据库加载了 {len(rows)} 个磁带信息")
            else:
                # 非 openGauss 数据库，暂时使用示例数据
                sample_tapes = [
                    TapeCartridge(
                        tape_id="TAPE001",
                        label="备份磁带001",
                        status=TapeStatus.AVAILABLE,
                        capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                        used_bytes=0,
                        created_date=datetime.now() - timedelta(days=30),
                        expiry_date=datetime.now() + timedelta(days=150),
                        location="磁带柜-1-A"
                    ),
                ]
                for tape in sample_tapes:
                    self.tape_cartridges[tape.tape_id] = tape
                logger.info(f"加载了 {len(self.tape_cartridges)} 个磁带信息（示例数据）")
        except Exception as e:
            logger.error(f"从数据库加载磁带库存信息失败: {str(e)}")
            # 如果加载失败，至少确保内存中有示例数据
            if not self.tape_cartridges:
                logger.warning("使用示例数据作为后备")
                sample_tapes = [
                    TapeCartridge(
                        tape_id="TAPE001",
                        label="备份磁带001",
                        status=TapeStatus.AVAILABLE,
                        capacity_bytes=self.settings.MAX_VOLUME_SIZE,
                        used_bytes=0,
                        created_date=datetime.now() - timedelta(days=30),
                        expiry_date=datetime.now() + timedelta(days=150),
                        location="磁带柜-1-A"
                    ),
                ]
                for tape in sample_tapes:
                    self.tape_cartridges[tape.tape_id] = tape

    async def _get_tape_from_db(self, tape_id: str) -> Optional[TapeCartridge]:
        """从数据库获取单个磁带信息"""
        try:
            from utils.scheduler.db_utils import is_opengauss, get_opengauss_connection
            
            if is_opengauss():
                # 使用 openGauss 原生 SQL 查询
                # 使用连接池
                async with get_opengauss_connection() as conn:
                    # 使用 fetch 而不是 fetchrow（openGauss 使用 asyncpg）
                    rows = await conn.fetch(
                        """
                        SELECT tape_id, label, status, first_use_date, manufactured_date, expiry_date,
                               capacity_bytes, used_bytes, serial_number, location,
                               media_type, generation, manufacturer, purchase_date, created_at
                        FROM tape_cartridges
                        WHERE tape_id = $1
                        """,
                        tape_id
                    )
                    
                    if rows and len(rows) > 0:
                        row = rows[0]
                        # 使用first_use_date、manufactured_date或created_at作为created_date（dataclass需要）
                        created_date = (row.get('first_use_date') or 
                                         row.get('manufactured_date') or 
                                         row.get('created_at') or 
                                         None)
                        
                        # 解析状态值（处理大小写不匹配问题）
                        status_value = row.get('status')
                        tape_status = _parse_tape_status(status_value) if status_value else TapeStatus.AVAILABLE
                        
                        tape = TapeCartridge(
                            tape_id=row['tape_id'],
                            label=row['label'],
                            status=tape_status,
                            created_date=created_date,
                            expiry_date=row.get('expiry_date'),
                            capacity_bytes=row['capacity_bytes'] or 0,
                            used_bytes=row['used_bytes'] or 0,
                            serial_number=row.get('serial_number') or '',
                            location=row.get('location') or '',
                            media_type=row.get('media_type') or 'LTO',
                            generation=row.get('generation') or 8,
                            manufacturer=row.get('manufacturer') or ''
                        )
                        logger.info(f"从数据库成功获取磁带 {tape_id}")
                        return tape
                    else:
                        logger.warning(f"数据库中未找到磁带 {tape_id}")
                        return None
            else:
                # 非 openGauss 数据库，返回None
                logger.warning("数据库不是 openGauss，无法从数据库获取磁带")
                return None
        except Exception as e:
            logger.error(f"从数据库获取磁带 {tape_id} 失败: {str(e)}", exc_info=True)
            import traceback
            logger.error(f"异常堆栈:\n{traceback.format_exc()}")
            return None

    async def get_available_tape(self) -> Optional[TapeCartridge]:
        """获取可用磁带"""
        try:
            # 如果内存中没有磁带，先从数据库加载
            if not self.tape_cartridges:
                await self._load_tape_inventory_from_db()
            
            # 查找可用磁带
            for tape in self.tape_cartridges.values():
                if tape.status == TapeStatus.AVAILABLE and not tape.is_expired:
                    return tape

            # 如果没有可用磁带，尝试从数据库重新加载
            await self._load_tape_inventory_from_db()
            
            # 再次查找可用磁带
            for tape in self.tape_cartridges.values():
                if tape.status == TapeStatus.AVAILABLE and not tape.is_expired:
                    return tape

            # 如果没有可用磁带，尝试清理过期磁带
            # 已禁用：不再自动清理过期磁带
            # if self.settings.AUTO_ERASE_EXPIRED:
            #     await self._cleanup_expired_tapes()
            #
            #     # 再次查找可用磁带
            #     for tape in self.tape_cartridges.values():
            #         if tape.status == TapeStatus.AVAILABLE and not tape.is_expired:
            #             return tape

            logger.warning("没有可用的磁带")
            return None

        except Exception as e:
            logger.error(f"获取可用磁带失败: {str(e)}")
            return None

    async def load_tape(self, tape_id: str) -> bool:
        """加载磁带"""
        try:
            logger.info(f"开始加载磁带: {tape_id}")
            
            # 如果磁带不在内存中，先从数据库加载
            if tape_id not in self.tape_cartridges:
                logger.info(f"磁带 {tape_id} 不在内存中，尝试从数据库加载")
                # 尝试从数据库加载该磁带
                try:
                    tape = await self._get_tape_from_db(tape_id)
                    if tape:
                        self.tape_cartridges[tape_id] = tape
                        logger.info(f"从数据库成功加载磁带 {tape_id}")
                    else:
                        logger.error(f"磁带 {tape_id} 在数据库中不存在")
                        return False
                except Exception as db_error:
                    logger.error(f"从数据库加载磁带 {tape_id} 时出错: {str(db_error)}", exc_info=True)
                    import traceback
                    logger.error(f"异常堆栈:\n{traceback.format_exc()}")
                    return False

            tape = self.tape_cartridges[tape_id]
            logger.info(f"找到磁带对象: {tape.tape_id}, 状态: {tape.status.value}")

            # 检查磁带状态
            if tape.is_expired:
                logger.warning(f"磁带 {tape_id} 已过期，将进行擦除")
                try:
                    await self.erase_tape(tape_id)
                except Exception as erase_error:
                    logger.error(f"擦除过期磁带 {tape_id} 失败: {str(erase_error)}", exc_info=True)
                    return False

            # 加载磁带
            logger.info(f"调用 tape_operations.load_tape 加载磁带: {tape_id}")
            try:
                success = await self.tape_operations.load_tape(tape)
                logger.info(f"tape_operations.load_tape 返回: {success}")
            except Exception as op_error:
                logger.error(f"tape_operations.load_tape 抛出异常: {str(op_error)}", exc_info=True)
                import traceback
                logger.error(f"异常堆栈:\n{traceback.format_exc()}")
                return False
            
            if success:
                self.current_tape = tape
                tape.status = TapeStatus.IN_USE
                tape.last_used_date = datetime.now()
                
                # 更新数据库
                try:
                    await self._update_tape_status_in_database(tape_id, 'IN_USE')
                    logger.info(f"数据库状态更新成功: {tape_id}")
                except Exception as db_error:
                    logger.warning(f"更新数据库磁带状态失败: {db_error}", exc_info=True)
                
                logger.info(f"磁带 {tape_id} 加载成功")
                return True
            else:
                logger.error(f"磁带 {tape_id} 加载失败（tape_operations.load_tape 返回 False）")
                return False

        except Exception as e:
            logger.error(f"加载磁带 {tape_id} 失败: {str(e)}", exc_info=True)
            import traceback
            logger.error(f"异常堆栈:\n{traceback.format_exc()}")
            return False

    async def unload_tape(self) -> bool:
        """卸载当前磁带"""
        try:
            if not self.current_tape:
                logger.warning("没有加载的磁带")
                return True

            tape_id = self.current_tape.tape_id
            success = await self.tape_operations.unload_tape()

            if success:
                self.current_tape.status = TapeStatus.AVAILABLE
                tape_to_unload = self.current_tape
                self.current_tape = None
                
                # 更新数据库
                try:
                    await self._update_tape_status_in_database(tape_id, 'AVAILABLE')
                except Exception as db_error:
                    logger.warning(f"更新数据库磁带状态失败: {db_error}")
                
                logger.info(f"磁带 {tape_id} 卸载成功")
                return True
            else:
                logger.error(f"磁带 {tape_id} 卸载失败")
                return False

        except Exception as e:
            logger.error(f"卸载磁带失败: {str(e)}")
            return False

    async def erase_tape(self, tape_id: str) -> bool:
        """擦除磁带"""
        try:
            # 如果磁带不在内存中，先从数据库加载
            if tape_id not in self.tape_cartridges:
                logger.info(f"磁带 {tape_id} 不在内存中，尝试从数据库加载")
                # 尝试从数据库加载该磁带
                tape = await self._get_tape_from_db(tape_id)
                if tape:
                    self.tape_cartridges[tape_id] = tape
                    logger.info(f"从数据库成功加载磁带 {tape_id}")
                else:
                    logger.error(f"磁带 {tape_id} 在数据库中不存在")
                    return False

            tape = self.tape_cartridges[tape_id]

            # 加载磁带（如果未加载）
            was_loaded = self.current_tape and self.current_tape.tape_id == tape_id
            if not was_loaded:
                if not await self.load_tape(tape_id):
                    return False

            # 先保存磁带信息用于擦除后重新写入标签
            tape_info = {
                "tape_id": tape.tape_id,
                "label": tape.label,
                "serial_number": tape.serial_number,
                "created_date": tape.created_date,
                "expiry_date": tape.expiry_date
            }
            
            # 执行擦除操作
            success = await self.tape_operations.erase_tape()
            if success:
                # 重置磁带信息（内存）- 保留磁带标签和创建日期，只更新时间字段
                tape.used_bytes = 0
                # tape.created_date 不变，保留原始创建日期
                # tape.expiry_date 不变，保留原始过期日期
                tape.status = TapeStatus.AVAILABLE
                tape.last_erase_date = datetime.now()
                
                # 更新数据库
                try:
                    await self._update_tape_in_database(tape)
                except Exception as db_error:
                    logger.warning(f"更新数据库磁带信息失败: {db_error}")
                
                # 擦除后重新写入磁带标签以保持标签不变
                logger.info("擦除完成，如需更新卷标请通过 LtfsCmdFormat.exe 重新格式化磁带")

                logger.info(f"磁带 {tape_id} 擦除成功")

            # 如果原本未加载，则卸载
            if not was_loaded:
                await self.unload_tape()

            return success

        except Exception as e:
            logger.error(f"擦除磁带 {tape_id} 失败: {str(e)}")
            return False

    async def write_data(self, data: bytes, block_number: int = 0) -> bool:
        """写入数据到磁带"""
        try:
            if not self.current_tape:
                logger.error("没有加载的磁带")
                return False

            success = await self.tape_operations.write_data(data, block_number)
            if success:
                # 更新磁带使用信息（内存）
                self.current_tape.used_bytes += len(data)
                self.current_tape.last_write_date = datetime.now()

                # 更新数据库中的磁带使用信息
                try:
                    await self._update_tape_in_database(self.current_tape)
                except Exception as db_error:
                    logger.warning(f"更新数据库磁带使用信息失败: {db_error}")

                # 检查容量
                if self.current_tape.used_bytes >= self.current_tape.capacity_bytes:
                    logger.warning(f"磁带 {self.current_tape.tape_id} 已满")
                    await self.unload_tape()

            return success

        except Exception as e:
            logger.error(f"写入数据失败: {str(e)}")
            return False

    async def read_data(self, block_number: int = 0, block_size: int = None) -> Optional[bytes]:
        """从磁带读取数据"""
        try:
            if not self.current_tape:
                logger.error("没有加载的磁带")
                return None

            if block_size is None:
                block_size = self.settings.DEFAULT_BLOCK_SIZE

            return await self.tape_operations.read_data(block_number, block_size)

        except Exception as e:
            logger.error(f"读取数据失败: {str(e)}")
            return None

    async def get_tape_info(self) -> Optional[Dict[str, Any]]:
        """获取当前磁带信息"""
        if not self.current_tape:
            return None

        try:
            scsi_info = None
            tape_info = {
                'tape_id': self.current_tape.tape_id,
                'label': self.current_tape.label,
                'status': self.current_tape.status.value,
                'capacity_bytes': self.current_tape.capacity_bytes,
                'used_bytes': self.current_tape.used_bytes,
                'free_bytes': self.current_tape.capacity_bytes - self.current_tape.used_bytes,
                'usage_percent': (self.current_tape.used_bytes / self.current_tape.capacity_bytes) * 100,
                'created_date': self.current_tape.created_date.isoformat(),
                'expiry_date': self.current_tape.expiry_date.isoformat(),
                'location': self.current_tape.location,
                'scsi_info': scsi_info
            }
            return tape_info

        except Exception as e:
            logger.error(f"获取磁带信息失败: {str(e)}")
            return None

    async def check_retention_periods(self):
        """检查磁带保留期"""
        try:
            expired_tapes = []
            for tape in self.tape_cartridges.values():
                if tape.is_expired and tape.status != TapeStatus.EXPIRED:
                    expired_tapes.append(tape)

            if expired_tapes:
                logger.info(f"发现 {len(expired_tapes)} 个过期磁带")

                if self.settings.AUTO_ERASE_EXPIRED:
                    for tape in expired_tapes:
                        logger.info(f"自动擦除过期磁带: {tape.tape_id}")
                        await self.erase_tape(tape.tape_id)

                        # 发送通知
                        from utils.dingtalk_notifier import DingTalkNotifier
                        notifier = DingTalkNotifier()
                        await notifier.send_tape_notification(
                            tape.tape_id,
                            "expired",
                            {'expiry_date': tape.expiry_date.isoformat()}
                        )

        except Exception as e:
            logger.error(f"检查磁带保留期失败: {str(e)}")

    async def _cleanup_expired_tapes(self):
        """清理过期磁带
        
        注意：此方法已禁用，不再自动调用过期检查
        """
        # 已禁用：不再自动调用过期检查
        # await self.check_retention_periods()
        pass

    async def _monitoring_loop(self):
        """磁带监控循环"""
        while self._initialized:
            try:
                # 检查磁带状态
                if self.current_tape:
                    tape_info = await self.get_tape_info()
                    if tape_info:
                        # 检查容量预警
                        usage_percent = tape_info['usage_percent']
                        if usage_percent > 90:
                            from utils.dingtalk_notifier import DingTalkNotifier
                            notifier = DingTalkNotifier()
                            await notifier.send_capacity_warning(usage_percent, tape_info)

                # 等待下次检查
                await asyncio.sleep(self.settings.TAPE_CHECK_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"磁带监控循环异常: {str(e)}")
                await asyncio.sleep(60)  # 出错后等待1分钟

    async def health_check(self) -> bool:
        """健康检查"""
        try:
            # 检查 LinuxTapeOperator
            if self.linux_tape_operator is None:
                logger.warning("LinuxTapeOperator 不可用，健康检查返回 False")
                return False
            try:
                status = await self.linux_tape_operator.status()
                if not status.get('online', False):
                    return False
            except Exception:
                return False

            # 检查当前磁带状态
            if self.current_tape:
                tape_info = await self.get_tape_info()
                if not tape_info:
                    return False

            return True

        except Exception as e:
            logger.error(f"磁带管理器健康检查失败: {str(e)}")
            return False

    async def get_inventory_status(self) -> Dict[str, Any]:
        """获取库存状态"""
        try:
            # 优先从数据库获取统计信息
            try:
                from config.settings import get_settings
                from datetime import datetime
                from utils.db_connection_helper import get_psycopg_connection_from_url
                
                settings = get_settings()
                database_url = settings.DATABASE_URL
                
                # 连接数据库（使用统一的连接辅助函数，支持 psycopg2 和 psycopg3）
                conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
                
                if conn:
                    
                    try:
                        with conn.cursor() as cur:
                            # 统计总数和各状态数量
                            cur.execute("SELECT COUNT(*) FROM tape_cartridges")
                            total_tapes = cur.fetchone()[0] or 0
                            
                            cur.execute("SELECT COUNT(*) FROM tape_cartridges WHERE status = 'AVAILABLE'")
                            available_tapes = cur.fetchone()[0] or 0
                            
                            cur.execute("SELECT COUNT(*) FROM tape_cartridges WHERE status = 'IN_USE'")
                            in_use_tapes = cur.fetchone()[0] or 0
                            
                            # 统计过期磁带
                            cur.execute("SELECT COUNT(*) FROM tape_cartridges WHERE expiry_date < %s", (datetime.now(),))
                            expired_tapes = cur.fetchone()[0] or 0
                            
                            # 统计容量
                            cur.execute("SELECT SUM(capacity_bytes), SUM(used_bytes) FROM tape_cartridges")
                            row = cur.fetchone()
                            total_capacity = row[0] or 0
                            used_capacity = row[1] or 0
                        
                        return {
                            'total_tapes': total_tapes,
                            'available_tapes': available_tapes,
                            'in_use_tapes': in_use_tapes,
                            'expired_tapes': expired_tapes,
                            'total_capacity_bytes': total_capacity,
                            'used_capacity_bytes': used_capacity,
                            'free_capacity_bytes': total_capacity - used_capacity,
                            'usage_percent': (used_capacity / total_capacity * 100) if total_capacity > 0 else 0,
                            'current_tape': self.current_tape.tape_id if self.current_tape else None
                        }
                    finally:
                        conn.close()
                
            except Exception as db_err:
                logger.warning(f"从数据库获取库存状态失败，回退到内存数据: {db_err}")
                pass
            
            # 回退到内存数据
            total_tapes = len(self.tape_cartridges)
            available_tapes = len([t for t in self.tape_cartridges.values() if t.status == TapeStatus.AVAILABLE])
            in_use_tapes = len([t for t in self.tape_cartridges.values() if t.status == TapeStatus.IN_USE])
            expired_tapes = len([t for t in self.tape_cartridges.values() if t.is_expired])

            total_capacity = sum(t.capacity_bytes for t in self.tape_cartridges.values())
            used_capacity = sum(t.used_bytes for t in self.tape_cartridges.values())

            return {
                'total_tapes': total_tapes,
                'available_tapes': available_tapes,
                'in_use_tapes': in_use_tapes,
                'expired_tapes': expired_tapes,
                'total_capacity_bytes': total_capacity,
                'used_capacity_bytes': used_capacity,
                'free_capacity_bytes': total_capacity - used_capacity,
                'usage_percent': (used_capacity / total_capacity * 100) if total_capacity > 0 else 0,
                'current_tape': self.current_tape.tape_id if self.current_tape else None
            }

        except Exception as e:
            logger.error(f"获取库存状态失败: {str(e)}")
            return {}

    async def _update_tape_in_database(self, tape: TapeCartridge):
        """更新数据库中的磁带信息"""
        try:
            from config.settings import get_settings
            from utils.db_connection_helper import get_psycopg_connection_from_url
            
            settings = get_settings()
            database_url = settings.DATABASE_URL
            
            # 连接数据库（使用统一的连接辅助函数，支持 psycopg2 和 psycopg3）
            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            
            if not conn:
                return
            
            try:
                with conn.cursor() as cur:
                    # 更新磁带使用信息
                    cur.execute("""
                        UPDATE tape_cartridges
                        SET 
                            used_bytes = %s,
                            write_count = write_count + 1
                        WHERE tape_id = %s
                    """, (tape.used_bytes, tape.tape_id))
                    
                    conn.commit()
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"更新数据库磁带信息失败: {str(e)}")
            raise

    async def _update_tape_status_in_database(self, tape_id: str, status: str):
        """更新数据库中的磁带状态"""
        try:
            from config.settings import get_settings
            from utils.db_connection_helper import get_psycopg_connection_from_url
            
            settings = get_settings()
            database_url = settings.DATABASE_URL
            
            # 连接数据库（使用统一的连接辅助函数，支持 psycopg2 和 psycopg3）
            conn, is_psycopg3 = get_psycopg_connection_from_url(database_url, prefer_psycopg3=True)
            
            if not conn:
                return
            
            try:
                with conn.cursor() as cur:
                    # 更新磁带状态
                    cur.execute("""
                        UPDATE tape_cartridges
                        SET status = %s
                        WHERE tape_id = %s
                    """, (status, tape_id))
                    
                    conn.commit()
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"更新数据库磁带状态失败: {str(e)}")
            raise

    async def shutdown(self):
        """关闭磁带管理器"""
        try:
            self._initialized = False

            # 停止监控任务
            if self._monitoring_task:
                self._monitoring_task.cancel()
                try:
                    await self._monitoring_task
                except asyncio.CancelledError:
                    pass

            # 卸载当前磁带
            if self.current_tape:
                await self.unload_tape()

            # 关闭SCSI接口
            # 无需关闭 ITDT 接口

            logger.info("磁带管理器已关闭")

        except Exception as e:
            logger.error(f"关闭磁带管理器时发生错误: {str(e)}")