#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
内存数据库写入器 - 完全按照openGauss BackupFile模型重写
Memory Database Writer - Rewritten to match openGauss BackupFile model exactly
"""

import asyncio
import logging
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import aiosqlite

from utils.scheduler.db_utils import get_opengauss_connection
from utils.datetime_utils import now, format_datetime

logger = logging.getLogger(__name__)


class MemoryDBWriter:
    """内存数据库写入器 - 与openGauss BackupFile模型完全一致"""
    
    # 类级别缓存：backup_files 表是否存在（None=未检查, True=存在, False=不存在）
    _backup_files_table_exists = None

    def __init__(self, backup_set_db_id: int,
                 sync_batch_size: int = 3000,           # 同步批次大小
                 sync_interval: int = 30,                # 同步间隔(秒)
                 max_memory_files: int = 5000000,        # 内存中最大文件数（500万）
                 checkpoint_interval: int = 300,         # 检查点间隔(秒)
                 checkpoint_retention_hours: int = 24,   # 检查点保留时间(小时)
                 enable_checkpoint: bool = False):        # 是否启用检查点，默认不启用

        self.backup_set_db_id = backup_set_db_id
        self.sync_batch_size = sync_batch_size
        self.sync_interval = sync_interval
        self.max_memory_files = max_memory_files
        self.checkpoint_interval = checkpoint_interval
        self.checkpoint_retention_hours = checkpoint_retention_hours
        self.enable_checkpoint = enable_checkpoint  # 是否启用检查点

        # 检查点目录：使用项目根目录下的 temp/checkpoints 目录（仅在启用检查点时创建）
        if self.enable_checkpoint:
            project_root = Path(__file__).parent.parent  # backup -> 项目根目录
            self.checkpoint_dir = project_root / "temp" / "checkpoints"
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.checkpoint_dir = None

        # 内存数据库
        self.memory_db = None
        self.db_connection = None

        # openGauss 统一调度器（仅 openGauss 模式使用）
        self.db_scheduler = None
        
        # 同步相关
        self._is_syncing = False
        self._sync_task = None
        self._checkpoint_task = None
        self._last_sync_time = 0
        self._sync_start_time = 0  # 当前同步开始时间
        self._last_checkpoint_time = 0
        self._last_trigger_time = 0  # 防止频繁触发同步
        self._last_file_added_time = time.time()  # 记录最后添加文件的时间
        self._checkpoint_files = []  # 记录创建的检查点文件列表 [(文件路径, 创建时间, 最大未同步文件ID), ...]
        # 用于检测同步是否卡住：记录上一次的待同步文件数量和检查时间
        self._last_pending_count = -1  # 上一次的待同步文件数量（-1表示未初始化）
        self._last_pending_check_time = 0  # 上一次检查待同步文件数量的时间

        # 统计信息
        self._stats = {
            'total_files': 0,
            'synced_files': 0,
            'sync_batches': 0,
            'total_time': 0,
            'sync_time': 0,
            'memory_usage': 0
        }

    async def initialize(self):
        """初始化内存数据库和同步任务"""
        # 启动时清理过期的检查点文件（仅在启用检查点时）
        if self.enable_checkpoint:
            await self._cleanup_old_checkpoints_on_startup()
        await self._setup_memory_database()
        
        # openGauss 模式：使用统一调度器
        from utils.scheduler.db_utils import is_opengauss
        if is_opengauss():
            from backup.compression_db_updater import OpenGaussDBScheduler
            self.db_scheduler = OpenGaussDBScheduler(
                backup_set_db_id=self.backup_set_db_id,
                batch_size=self.sync_batch_size
            )
            self.db_scheduler.start()
            logger.info(f"[内存数据库] openGauss 统一调度器已启动")
        
        await self._start_sync_tasks()
        logger.info(f"内存数据库写入器已初始化 (backup_set_id={self.backup_set_db_id}, checkpoint={self.enable_checkpoint})")

    async def _setup_memory_database(self):
        """设置内存数据库 - 完全按照openGauss BackupFile模型"""
        # 创建内存数据库连接
        self.db_connection = await aiosqlite.connect(":memory:")
        self.memory_db = self.db_connection

        # 创建表结构 - 与openGauss BackupFile模型完全一致
        await self._create_tables()

        # 性能优化：配置SQLite PRAGMA参数以最大化写入速度
        # 1. WAL模式：提升并发写入性能
        await self.memory_db.execute("PRAGMA journal_mode=WAL")
        
        # 2. 同步模式：OFF最快（内存数据库，数据最终会同步到openGauss，风险可控）
        # 注意：内存数据库数据最终会同步到openGauss，即使崩溃也不会丢失已同步的数据
        await self.memory_db.execute("PRAGMA synchronous=OFF")
        
        # 3. 增大缓存大小：从10000页增加到50000页（约200MB，可根据内存调整）
        # 每页默认4KB，50000页 = 200MB
        # 注意：负值表示以KB为单位
        await self.memory_db.execute("PRAGMA cache_size=-50000")
        
        # 4. 临时存储使用内存
        await self.memory_db.execute("PRAGMA temp_store=memory")
        
        # 5. 启用内存映射：提升大数据库性能（内存数据库本身在内存中，但可优化内部操作）
        # 设置mmap_size为1GB（内存数据库通常不会超过此大小）
        await self.memory_db.execute("PRAGMA mmap_size=1073741824")
        
        # 6. 锁定模式：EXCLUSIVE模式提升写入性能（内存数据库单连接，无需共享）
        await self.memory_db.execute("PRAGMA locking_mode=EXCLUSIVE")
        
        # 7. 优化器设置：优化查询计划器（写入场景也有一定优化效果）
        await self.memory_db.execute("PRAGMA optimize")
        
        # 注意：page_size必须在创建数据库之前设置，对已创建的数据库无效
        # 内存数据库使用默认4KB页面大小，对性能影响较小（数据都在内存中）
        
        logger.debug("内存数据库性能优化配置已应用（WAL模式、同步关闭、大缓存、内存映射、独占锁）")

    async def _create_tables(self):
        """创建内存表结构 - 与openGauss BackupFile模型字段完全一致"""
        # 文件表 - 与models.backup.BackupFile完全一致的字段顺序和类型
        await self.memory_db.execute("""
            CREATE TABLE IF NOT EXISTS backup_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                backup_set_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                directory_path TEXT,
                display_name TEXT,
                file_type TEXT NOT NULL DEFAULT 'file',
                file_size BIGINT NOT NULL,
                compressed_size BIGINT,
                file_permissions TEXT,
                file_owner TEXT,
                file_group TEXT,
                created_time TIMESTAMP,
                modified_time TIMESTAMP,
                accessed_time TIMESTAMP,
                tape_block_start BIGINT,
                tape_block_count INTEGER,
                compressed BOOLEAN DEFAULT FALSE,
                encrypted BOOLEAN DEFAULT FALSE,
                checksum TEXT,
                is_copy_success BOOLEAN DEFAULT FALSE,
                copy_status_at TIMESTAMP,
                backup_time TIMESTAMP NOT NULL,
                chunk_number INTEGER,
                version INTEGER DEFAULT 1,
                file_metadata TEXT,
                tags TEXT,
                synced_to_opengauss BOOLEAN DEFAULT FALSE,
                sync_error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 创建索引提升查询性能
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_path ON backup_files(file_path)")
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_synced ON backup_files(synced_to_opengauss)")
        await self.memory_db.execute("CREATE INDEX IF NOT EXISTS idx_backup_files_backup_set ON backup_files(backup_set_id)")

        await self.memory_db.commit()

    async def _start_sync_tasks(self):
        """启动同步任务"""
        # 启动定期同步任务
        self._sync_task = asyncio.create_task(self._sync_loop())
        logger.info(f"内存数据库同步任务已启动 (同步间隔: {self.sync_interval}秒, 批次大小: {self.sync_batch_size})")

        # 仅在启用检查点时启动检查点任务
        if self.enable_checkpoint:
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            logger.debug(f"检查点任务已启动 (间隔: {self.checkpoint_interval}秒)")
        else:
            self._checkpoint_task = None
            logger.debug("检查点功能已禁用")

    async def add_file(self, file_info: Dict):
        """添加文件到内存数据库 - 根据文件扫描器输出正确映射（单个文件）"""
        if not self.memory_db:
            await self.initialize()

        try:
            # 准备插入数据 - 根据文件扫描器输出格式映射到BackupFile模型
            insert_data = self._prepare_insert_data_from_scanner(file_info)

            # 插入到内存数据库 - 字段顺序与BackupFile模型一致
            # 注意：显式包含 synced_to_opengauss 和 sync_error 字段，确保数据一致性
            # 验证 backup_set_id 是否正确
            backup_set_id_in_data = insert_data[0] if insert_data else None
            if backup_set_id_in_data != self.backup_set_db_id:
                logger.error(
                    f"[内存数据库] ⚠️⚠️ 错误：文件数据的 backup_set_id={backup_set_id_in_data} "
                    f"与 MemoryDBWriter 的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                )
            
            await self.memory_db.execute("""
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                    synced_to_opengauss, sync_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data + (False, None))  # 显式设置 synced_to_opengauss = FALSE, sync_error = NULL

            await self.memory_db.commit()

            self._stats['total_files'] += 1
            self._last_file_added_time = time.time()  # 更新最后添加文件时间

            # 检查是否需要立即同步（不阻塞，使用 create_task）
            # 注意：_check_sync_need 内部会调用 _trigger_sync，而 _trigger_sync 使用 create_task
            # 所以这里直接 await 也不会阻塞，但为了更清晰，可以改为不 await
            # 实际上 _check_sync_need 很快返回，await 不会阻塞
            await self._check_sync_need()

        except Exception as e:
            file_path = file_info.get('path', 'unknown')
            logger.error(
                f"添加文件到内存数据库失败: {e}, "
                f"文件路径: {file_path[:200]}, "
                f"file_info键: {list(file_info.keys())}, "
                f"file_info值: {dict((k, type(v).__name__ if not isinstance(v, (str, int, bool, type(None))) else v) for k, v in file_info.items())}"
            )
            raise

    async def add_files_batch(self, file_info_list: List[Dict]):
        """批量添加文件到内存数据库 - 使用批量插入优化性能
        
        性能优化策略：
        1. 使用显式事务控制
        2. 批量准备数据，减少循环开销
        3. 使用executemany一次性插入所有记录
        4. 单次提交事务
        
        Args:
            file_info_list: 文件信息列表
        """
        if not file_info_list:
            return
        
        if not self.memory_db:
            await self.initialize()

        try:
            # 性能优化：批量准备插入数据，使用列表推导式减少开销
            # 预先定义prepare函数引用，避免循环中重复查找
            prepare_func = self._prepare_insert_data_from_scanner
            insert_data_list = []
            failed_files = []
            
            # 批量准备数据（优化：减少异常处理开销）
            for file_info in file_info_list:
                try:
                    insert_data = prepare_func(file_info)
                    # 添加 synced_to_opengauss 和 sync_error 字段
                    insert_data_list.append(insert_data + (False, None))
                except Exception as e:
                    file_path = file_info.get('path', 'unknown')
                    failed_files.append((file_path, str(e)))
                    continue
            
            if not insert_data_list:
                if failed_files:
                    logger.warning(f"批量插入：所有 {len(file_info_list)} 个文件的数据准备都失败")
                else:
                    logger.warning("批量插入：没有有效的数据可以插入")
                return
            
            # 记录失败的文件（如果有）
            if failed_files:
                logger.warning(f"批量插入：{len(failed_files)} 个文件数据准备失败，已跳过")
            
            # 性能优化：使用显式事务控制，确保批量操作的原子性
            # 注意：SQLite默认自动提交，但显式BEGIN可以确保批量操作的性能
            batch_size = len(insert_data_list)
            
            # 使用 executemany 批量插入（性能优化：一次插入多个文件，只提交一次）
            # executemany内部会优化批量插入，比循环执行INSERT快得多
            await self.memory_db.executemany("""
                INSERT INTO backup_files (
                    backup_set_id, file_path, file_name, directory_path, display_name,
                    file_type, file_size, compressed_size, file_permissions, file_owner,
                    file_group, created_time, modified_time, accessed_time, tape_block_start,
                    tape_block_count, compressed, encrypted, checksum, is_copy_success,
                    copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                    synced_to_opengauss, sync_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, insert_data_list)

            # 只提交一次（批量插入的关键优化）
            await self.memory_db.commit()

            # 更新统计信息
            self._stats['total_files'] += batch_size
            self._last_file_added_time = time.time()  # 更新最后添加文件时间

            # 数据写入保证机制：
            # 1. executemany 执行成功（如果失败会抛出异常）
            # 2. commit() 成功提交事务（如果失败会抛出异常）
            # 3. 如果任何步骤失败，异常会被捕获并向上抛出，扫描器会处理（回退到逐个添加）
            # 因此，如果方法正常返回（没有抛出异常），数据已经成功写入并持久化
            logger.debug(f"批量插入完成：成功插入 {batch_size} 个文件到内存数据库（已提交事务）")

            # 检查是否需要立即同步（批量添加后只检查一次）
            await self._check_sync_need()

        except Exception as e:
            logger.error(
                f"批量添加文件到内存数据库失败: {e}, "
                f"文件数量: {len(file_info_list)}",
                exc_info=True
            )
            # 回滚事务（如果失败）
            try:
                await self.memory_db.rollback()
            except:
                pass
            raise

    async def add_files_batch_direct_to_opengauss(self, file_info_list: List[Dict]) -> int:
        """直接批量写入openGauss - 使用原生SQL批量插入，严禁SQLAlchemy解析openGauss
        
        在openGauss模式下，跳过内存数据库，直接批量写入openGauss数据库
        按SCAN_UPDATE_INTERVAL累积后一次性写入，顺序执行
        
        Args:
            file_info_list: 文件信息列表
            
        Returns:
            int: 成功写入的文件数
        """
        if not file_info_list:
            return 0
        
        try:
            # 准备批量插入数据
            insert_data = []
            for file_info in file_info_list:
                try:
                    # 准备数据（与_prepare_insert_data_from_scanner相同逻辑）
                    data_tuple = self._prepare_insert_data_for_opengauss(file_info)
                    insert_data.append(data_tuple)
                except Exception as e:
                    file_path = file_info.get('path', 'unknown')
                    logger.warning(f"准备批量插入数据失败: {file_path[:200]}, 错误: {str(e)}")
                    continue
            
            if not insert_data:
                logger.warning("批量插入：没有有效的数据可以插入")
                return 0
            
            # 使用原生SQL批量插入到openGauss（支持多表方案：按任务写入对应的 backup_files_* 表）
            async with get_opengauss_connection() as conn:
                # 确定目标表名：通过 backup_set_id 关联到 backup_tasks.backup_files_table
                table_name = "backup_files"
                try:
                    # 从 backup_sets -> backup_tasks 获取表名
                    row = await conn.fetchrow(
                        """
                        SELECT bt.backup_files_table
                        FROM backup_sets bs
                        JOIN backup_tasks bt ON bs.backup_task_id = bt.id
                        WHERE bs.id = $1
                        """,
                        self.backup_set_db_id,
                    )
                    if row and row.get("backup_files_table"):
                        candidate = row["backup_files_table"]
                        if isinstance(candidate, str) and candidate.startswith("backup_files_"):
                            table_name = candidate
                except Exception as table_err:
                    logger.warning(f"[openGauss直接写入] 获取 backup_files 目标表名失败，将回退到主表 backup_files: {table_err}")

                await conn.executemany(
                    f"""
                    INSERT INTO {table_name} (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, compressed, encrypted, checksum, is_copy_success,
                        copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23, $24, $25::jsonb, $26::jsonb, NOW(), NOW()
                    )
                    """,
                    insert_data,
                )

                # psycopg3 binary protocol 需要显式提交事务
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    await actual_conn.commit()
                    logger.debug(f"[openGauss直接插入] 批量插入事务已提交: {len(insert_data)} 个文件，到表 {table_name}")
                except Exception as commit_err:
                    logger.warning(f"提交批量插入事务失败（可能已自动提交）: {commit_err}")
                    # 如果不在事务中，commit() 可能会失败，尝试回滚
                    try:
                        await actual_conn.rollback()
                    except Exception:
                        pass
            
            # 更新统计信息
            inserted_count = len(insert_data)
            self._stats['total_files'] += inserted_count
            self._stats['synced_files'] += inserted_count  # 直接写入，已同步
            self._last_file_added_time = time.time()
            
            logger.debug(f"[openGauss直接写入] 批量插入完成：成功插入 {inserted_count} 个文件到openGauss数据库（已提交事务）")
            
            return inserted_count
            
        except Exception as e:
            logger.error(
                f"[openGauss直接写入] 批量添加文件到openGauss数据库失败: {e}, "
                f"文件数量: {len(file_info_list)}",
                exc_info=True
            )
            raise

    def _prepare_insert_data_for_opengauss(self, file_info: Dict) -> tuple:
        """为openGauss准备插入数据（与_prepare_insert_data_from_scanner逻辑相同，但返回格式适配openGauss）"""
        # 基本路径信息 - 来自文件扫描器
        file_path = file_info.get('path', '')
        file_name = file_info.get('name') or Path(file_path).name

        # 目录路径
        directory_path = str(Path(file_path).parent) if file_path and Path(file_path).parent != Path(file_path).anchor else None

        # 显示名称（暂时与文件名相同）
        display_name = file_name

        # 文件类型 - 根据扫描器输出判断
        if file_info.get('is_file', True):
            file_type = 'file'
        elif file_info.get('is_dir', False):
            file_type = 'directory'
        elif file_info.get('is_symlink', False):
            file_type = 'symlink'
        else:
            file_type = 'file'

        # 文件大小 - 关键字段！直接从扫描器的size字段获取
        file_size = file_info.get('size', 0) or 0

        # 压缩大小（初始为None，压缩时更新）
        compressed_size = None

        # 文件权限 - 来自扫描器
        file_permissions = file_info.get('permissions')

        # 文件所有者和组（初始为None，Linux环境下可扩展）
        file_owner = None
        file_group = None

        # 时间戳处理 - 优先使用扫描器提供的modified_time
        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)

        # 创建时间和访问时间（暂时使用修改时间作为默认值）
        created_time = modified_time
        accessed_time = modified_time

        # 磁带相关信息（初始为None，压缩时更新）
        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None

        # 备份时间
        backup_time = datetime.now(timezone.utc)

        # 其他字段
        chunk_number = None
        version = 1

        # 元数据（记录扫描时信息）
        file_metadata = json.dumps({
            'scanned_at': datetime.now(timezone.utc).isoformat(),
            'scanner_source': 'file_scanner',
            'original_permissions': file_permissions,
            'file_type_detected': file_info.get('is_file', True)
        })

        # 标签
        tags = json.dumps({'status': 'scanned'})

        # 返回格式：按照openGauss INSERT语句的字段顺序（不包含created_at和updated_at，它们在SQL中使用NOW()）
        return (
            self.backup_set_db_id,     # backup_set_id
            file_path,                 # file_path
            file_name,                 # file_name
            directory_path,            # directory_path
            display_name,              # display_name
            file_type,                 # file_type
            file_size,                 # file_size
            compressed_size,           # compressed_size
            file_permissions,          # file_permissions
            file_owner,                # file_owner
            file_group,                # file_group
            created_time,              # created_time
            modified_time,             # modified_time
            accessed_time,             # accessed_time
            tape_block_start,          # tape_block_start
            tape_block_count,          # tape_block_count
            compressed,                # compressed
            encrypted,                 # encrypted
            checksum,                  # checksum
            is_copy_success,           # is_copy_success
            copy_status_at,            # copy_status_at
            backup_time,               # backup_time
            chunk_number,              # chunk_number
            version,                   # version
            file_metadata,             # file_metadata
            tags                       # tags
        )

    def _prepare_insert_data_from_scanner(self, file_info: Dict) -> tuple:
        """
        根据文件扫描器输出格式准备插入数据
        文件扫描器输出格式:
        {
            'path': str(file_path),
            'name': file_path.name,
            'size': stat.st_size,
            'modified_time': datetime.fromtimestamp(stat.st_mtime),
            'permissions': oct(stat.st_mode)[-3:],
            'is_file': file_path.is_file(),
            'is_dir': file_path.is_dir(),
            'is_symlink': file_path.is_symlink()
        }
        """
        # 基本路径信息 - 来自文件扫描器
        file_path = file_info.get('path', '')
        file_name = file_info.get('name') or Path(file_path).name

        # 目录路径
        directory_path = str(Path(file_path).parent) if file_path and Path(file_path).parent != Path(file_path).anchor else None

        # 显示名称（暂时与文件名相同）
        display_name = file_name

        # 文件类型 - 根据扫描器输出判断
        if file_info.get('is_file', True):
            file_type = 'file'
        elif file_info.get('is_dir', False):
            file_type = 'directory'
        elif file_info.get('is_symlink', False):
            file_type = 'symlink'
        else:
            file_type = 'file'

        # 文件大小 - 关键字段！直接从扫描器的size字段获取
        file_size = file_info.get('size', 0) or 0

        # 压缩大小（初始为None，压缩时更新）
        compressed_size = None

        # 文件权限 - 来自扫描器
        file_permissions = file_info.get('permissions')

        # 文件所有者和组（初始为None，Linux环境下可扩展）
        file_owner = None
        file_group = None

        # 时间戳处理 - 优先使用扫描器提供的modified_time
        modified_time = file_info.get('modified_time')
        if isinstance(modified_time, datetime):
            modified_time = modified_time.replace(tzinfo=timezone.utc)
        else:
            modified_time = datetime.now(timezone.utc)

        # 创建时间和访问时间（暂时使用修改时间作为默认值）
        created_time = modified_time
        accessed_time = modified_time

        # 磁带相关信息（初始为None，压缩时更新）
        tape_block_start = None
        tape_block_count = None
        compressed = False
        encrypted = False
        checksum = None
        is_copy_success = False
        copy_status_at = None

        # 备份时间
        backup_time = datetime.now(timezone.utc)

        # 其他字段
        chunk_number = None
        version = 1

        # 元数据（记录扫描时信息）
        file_metadata = json.dumps({
            'scanned_at': datetime.now(timezone.utc).isoformat(),
            'scanner_source': 'file_scanner',
            'original_permissions': file_permissions,
            'file_type_detected': file_info.get('is_file', True)
        })

        # 标签
        tags = json.dumps({'status': 'scanned'})

        return (
            self.backup_set_db_id,     # backup_set_id
            file_path,                 # file_path
            file_name,                 # file_name
            directory_path,            # directory_path
            display_name,              # display_name
            file_type,                 # file_type
            file_size,                 # file_size - 关键字段！
            compressed_size,           # compressed_size
            file_permissions,          # file_permissions
            file_owner,                # file_owner
            file_group,                # file_group
            created_time,              # created_time
            modified_time,             # modified_time
            accessed_time,             # accessed_time
            tape_block_start,          # tape_block_start
            tape_block_count,          # tape_block_count
            compressed,                # compressed
            encrypted,                 # encrypted
            checksum,                  # checksum
            is_copy_success,           # is_copy_success
            copy_status_at,            # copy_status_at
            backup_time,               # backup_time
            chunk_number,              # chunk_number
            version,                   # version
            file_metadata,             # file_metadata
            tags                       # tags
        )

    async def _check_sync_need(self):
        """检查是否需要同步 - 优化openGauss模式下的同步触发，尽快复制到openGauss
        
        重要：此方法不会阻塞扫描线程。
        - 检查条件很快完成（只是查询计数和时间比较）
        - 调用 _trigger_sync 时，_trigger_sync 使用 create_task 创建异步任务，立即返回
        - 即使使用 await _check_sync_need()，也会很快返回，不会等待同步完成
        """
        # 如果同步正在进行中，直接返回，避免重复触发和产生大量日志
        if self._is_syncing:
            return
        
        # 检查数据库类型，openGauss模式下使用更激进的同步策略
        from utils.scheduler.db_utils import is_opengauss
        is_opengauss_mode = is_opengauss()
        
        current_time = time.time()
        pending_files = await self._get_pending_sync_count()

        # openGauss模式优化：尽可能早地同步扫描结果到openGauss
        # 由于文件扫描速度远大于同步速度，采用最激进的同步策略
        if is_opengauss_mode:
            # 条件1（最高优先级）：只要有待同步文件，且距离上次同步超过1秒，立即触发
            # 这是最激进的策略，确保尽快同步，避免积压
            if pending_files > 0:
                time_since_last_sync = current_time - self._last_sync_time
                # 如果距离上次同步超过1秒，立即触发（避免过于频繁导致性能问题）
                if time_since_last_sync >= 1:
                    await self._trigger_sync("pending_files_available")
                    return
                # 如果距离上次同步不到1秒，但待同步文件很多（超过批次大小），也立即触发
                if pending_files >= self.sync_batch_size:
                    await self._trigger_sync("large_pending_batch")
                    return

            # 条件2：文件数量达到批次大小的10%（更早触发，避免积压）
            # 默认 sync_batch_size=3000，所以 300 个文件就触发同步
            if pending_files >= self.sync_batch_size // 10:
                await self._trigger_sync("batch_size_reached")
                return

            # 条件3：内存中文件过多，且有足够待同步文件（降低阈值到5%）
            # 只要有少量待同步文件就触发，避免积压
            memory_threshold = min(self.max_memory_files, self.sync_batch_size)
            if (self._stats['total_files'] >= memory_threshold and
                pending_files >= self.sync_batch_size // 20):  # 降低到5%，更早触发
                await self._trigger_sync("memory_limit_reached")
                return

            # 条件4：超时机制 - 使用更短的超时（3秒）
            # 如果3秒内没有新文件添加，但有待同步文件，立即同步
            time_since_last_file = current_time - self._last_file_added_time
            if (time_since_last_file >= 3 and pending_files > 0):
                await self._trigger_sync("scan_completed_timeout")
                return

            # 条件5：检查扫描是否可能完成 - 使用更短的间隔（2秒）和更低的阈值（80%）
            # 更早地处理接近完成的同步
            if pending_files > 0:
                sync_ratio = (self._stats['synced_files'] / max(1, self._stats['total_files']))
                if (sync_ratio >= 0.80 and  # 降低到80%
                    current_time - self._last_sync_time >= 2):  # 缩短到2秒
                    await self._trigger_sync("almost_complete")
                    return
        else:
            # SQLite模式：保持原有逻辑
            # 条件1：文件数量达到批次大小
            if pending_files >= self.sync_batch_size:
                await self._trigger_sync("batch_size_reached")
                return

            # 条件2：达到同步间隔时间
            if current_time - self._last_sync_time >= self.sync_interval:
                await self._trigger_sync("interval_reached")
                return

            # 条件3：内存中文件过多，且有足够待同步文件
            memory_threshold = min(self.max_memory_files, self.sync_batch_size * 2)
            if (self._stats['total_files'] >= memory_threshold and
                pending_files >= self.sync_batch_size // 2):
                await self._trigger_sync("memory_limit_reached")
                return

            # 条件4：超时机制 - 扫描完成但没有达到批量大小的剩余文件
            time_since_last_file = current_time - self._last_file_added_time
            if (time_since_last_file >= 60 and pending_files > 0):
                await self._trigger_sync("scan_completed_timeout")
                return

            # 条件5：检查扫描是否可能完成
            if pending_files > 0:
                sync_ratio = (self._stats['synced_files'] / max(1, self._stats['total_files']))
                if (sync_ratio >= 0.98 and
                    current_time - self._last_sync_time >= 30):
                    await self._trigger_sync("almost_complete")
                    return

    async def _get_pending_sync_count(self) -> int:
        """获取待同步文件数量（仅当前备份集）"""
        # 检查数据库连接是否已关闭
        if not self.memory_db:
            return 0
        try:
            # 检查连接是否有效
            if hasattr(self.memory_db, '_conn') and self.memory_db._conn is None:
                return 0
        except (ValueError, AttributeError):
            # 连接已关闭
            return 0
        
        try:
            async with self.memory_db.execute(
                "SELECT COUNT(*) FROM backup_files WHERE backup_set_id = ? AND synced_to_opengauss = FALSE",
                (self.backup_set_db_id,)
            ) as cursor:
                result = await cursor.fetchone()
                return result[0] if result else 0
        except (ValueError, sqlite3.ProgrammingError) as e:
            # 连接已关闭，返回 0
            logger.debug(f"获取待同步文件数量时数据库连接已关闭: {e}")
            return 0

    async def _trigger_sync(self, reason: str):
        """触发同步 - 增加防抖动机制，异步执行不阻塞扫描线程
        
        重要：此方法使用 asyncio.create_task 创建异步任务，不会阻塞调用者。
        即使使用 await _trigger_sync()，也会立即返回，不会等待同步完成。
        同步在后台任务中执行，与扫描线程并行运行。
        """
        current_time = time.time()

        # 防抖动：openGauss模式下使用更短的防抖时间（0.3秒），SQLite模式保持1秒
        from utils.scheduler.db_utils import is_opengauss
        debounce_time = 0.3 if is_opengauss() else 1.0
        if current_time - self._last_trigger_time < debounce_time:
            logger.debug(f"同步触发过于频繁，跳过 (原因: {reason})")
            return

        if self._is_syncing:
            # 降低日志级别，避免同步进行时产生大量重复日志
            logger.debug(f"同步已在进行中，跳过触发 (原因: {reason})")
            return

        self._last_trigger_time = current_time
        
        # 检查数据库类型以显示正确的日志
        from utils.scheduler.db_utils import is_opengauss
        db_type = "openGauss" if is_opengauss() else "SQLite"
        logger.debug(f"触发同步到{db_type} (原因: {reason})")
        
        # 创建异步任务执行同步，不阻塞当前线程（扫描线程）
        # 关键：asyncio.create_task 会立即返回，不会等待任务完成
        # 这样扫描和同步可以并行执行，互不阻塞
        # 即使调用者使用 await _trigger_sync()，也会立即返回，不会阻塞
        asyncio.create_task(self._sync_to_opengauss(reason))

    async def _sync_loop(self):
        """定期同步循环 - 每30秒检查一次同步状态并报告"""
        from utils.scheduler.db_utils import is_opengauss
        is_opengauss_mode = is_opengauss()
        
        # openGauss模式下使用更短的同步间隔（5秒），确保及时同步
        # 因为文件扫描速度远大于同步速度，需要更频繁地触发同步
        # 只要有待同步文件，_check_sync_need 会立即触发，这里只是作为兜底机制
        sync_loop_interval = 5 if is_opengauss_mode else self.sync_interval
        
        # 状态报告间隔（30秒）
        status_report_interval = 30.0
        last_status_report_time = time.time()
        
        logger.info(f"内存数据库同步循环已启动，等待同步间隔... (openGauss模式: {sync_loop_interval}秒, 其他模式: {self.sync_interval}秒)")
        while True:
            try:
                await asyncio.sleep(sync_loop_interval)
                
                logger.debug(f"定期同步触发（间隔: {sync_loop_interval}秒）")

                if not self._is_syncing:
                    await self._sync_to_opengauss("scheduled")
                else:
                    # 同步正在进行中，每30秒报告一次状态
                    current_time = time.time()
                    pending_count = await self._get_pending_sync_count()
                    total_scanned = self._stats['total_files']
                    total_synced = self._stats['synced_files']
                    
                    # 计算当前同步已持续的时间
                    if self._sync_start_time > 0:
                        sync_duration = current_time - self._sync_start_time
                    else:
                        # 如果没有记录开始时间，使用上次完成时间作为参考
                        sync_duration = current_time - self._last_sync_time if self._last_sync_time > 0 else 0
                    
                    # 计算同步速度
                    sync_speed = total_synced / sync_duration if sync_duration > 0 else 0
                    
                    # 检测待同步文件数量是否有变化，如果有变化则重置计时器
                    if self._last_pending_count != pending_count:
                        # 待同步文件数量有变化，说明没有卡住，重置计时器
                        if self._last_pending_count >= 0:  # 不是第一次检查
                            logger.debug(
                                f"待同步文件数量有变化 ({self._last_pending_count} -> {pending_count})，"
                                f"判定未卡住，重置计时器"
                            )
                        self._last_pending_count = pending_count
                        self._last_pending_check_time = current_time
                    else:
                        # 待同步文件数量没有变化，检查是否卡住
                        if self._last_pending_check_time > 0:
                            stuck_duration = current_time - self._last_pending_check_time
                            # 如果待同步文件数量持续不变超过5分钟，记录警告（可能是卡住了）
                            if stuck_duration > 300:
                                logger.warning(
                                    f"⚠️⚠️ 警告：待同步文件数量已持续 {stuck_duration:.1f} 秒（超过5分钟）未变化，"
                                    f"可能已卡住！待同步: {pending_count} 个文件。"
                                    f"建议检查数据库连接是否正常。"
                                )
                    
                    # 每30秒报告一次状态
                    if current_time - last_status_report_time >= status_report_interval:
                        logger.info(
                            f"[同步状态报告] 同步正在进行中 - "
                            f"待同步: {pending_count} 个文件，"
                            f"累计总扫描: {total_scanned} 个，"
                            f"累计总同步: {total_synced} 个，"
                            f"同步速度: {sync_speed:.1f} 个文件/秒，"
                            f"已持续: {sync_duration:.1f}秒"
                        )
                        last_status_report_time = current_time
                    else:
                        logger.debug(
                            f"同步正在进行中，跳过本次定期同步 - "
                            f"待同步: {pending_count} 个，"
                            f"累计总扫描: {total_scanned} 个，累计总同步: {total_synced} 个，"
                            f"当前同步已持续: {sync_duration:.1f}秒"
                        )

            except asyncio.CancelledError:
                logger.debug("内存数据库同步循环被取消")
                break
            except Exception as e:
                logger.error(f"同步循环异常: {e}", exc_info=True)
                await asyncio.sleep(5)  # 错误后短暂等待

    async def _checkpoint_loop(self):
        """检查点循环 - 持久化保护"""
        while True:
            try:
                await asyncio.sleep(self.checkpoint_interval)
                await self._create_checkpoint()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"检查点循环异常: {e}")
                await asyncio.sleep(30)  # 错误后等待更长时间

    async def _sync_to_opengauss(self, reason: str = "manual"):
        """同步文件到主数据库（openGauss 或 SQLite）
        
        每次同步时，循环处理所有未同步的文件，直到全部同步完成（分批处理）
        重要：持续同步直到没有待同步文件，不会中途停止
        """
        if self._is_syncing:
            logger.warning(
                f"[openGauss同步] 同步已在进行中，跳过本次同步请求 (原因: {reason})。"
                f"如果此状态持续，可能是之前的同步未正确完成。"
            )
            return

        logger.info(f"[openGauss同步] 开始同步 (原因: {reason})，设置 _is_syncing = True")
        self._is_syncing = True
        self._sync_start_time = time.time()  # 记录同步开始时间
        sync_start_time = self._sync_start_time
        total_synced_count = 0
        batch_number = 0
        
        # 状态通报相关变量
        import asyncio
        status_report_interval = 30.0  # 每30秒通报一次
        last_status_report_time = time.time()

        try:
            # 记录同步开始时的待同步文件数
            initial_pending_count = await self._get_pending_sync_count()
            if initial_pending_count > 0:
                logger.info(f"[openGauss同步开始] 待同步文件数: {initial_pending_count} 个 (原因: {reason})")
            else:
                logger.info(f"[openGauss同步开始] 没有待同步文件 (原因: {reason})")
            
            # 循环同步，直到所有未同步的文件都处理完成
            max_batches = 1000  # 防止无限循环
            while batch_number < max_batches:
                # 每30秒通报一次同步状态（不中断正在同步的任务）
                current_time = time.time()
                if current_time - last_status_report_time >= status_report_interval:
                    pending_count = await self._get_pending_sync_count()
                    sync_duration = current_time - sync_start_time
                    total_scanned = self._stats['total_files']
                    total_synced_accumulated = self._stats['synced_files']
                    
                    logger.info(
                        f"【openGauss同步状态】同步进行中 - "
                        f"已处理批次: {batch_number}，"
                        f"剩余待同步:▶️{pending_count} 个文件，"
                        f"本次已同步: {total_synced_count} 个，"
                        f"累计总扫描: {total_scanned} 个，"
                        f"累计总同步: {total_synced_accumulated} 个，"
                        f"已持续: {sync_duration:.1f}秒"
                    )
                    last_status_report_time = current_time
                # 获取待同步的文件批次（每次获取一批）
                get_files_start = time.time()
                files_to_sync = await self._get_files_to_sync()
                get_files_time = time.time() - get_files_start
                if get_files_time > 1.0:
                    logger.info(f"[openGauss同步] 获取待同步文件耗时较长: {get_files_time:.2f}秒")

                if not files_to_sync:
                    # 没有更多文件需要同步
                    if batch_number == 0:
                        logger.info("内存数据库中没有文件需要同步到openGauss")
                    break

                batch_number += 1
                logger.info(
                    f"[openGauss批次 {batch_number}] 开始同步 {len(files_to_sync)} 个文件 "
                    f"(原因: {reason}, backup_set_db_id={self.backup_set_db_id})"
                )

                # 准备批量插入数据
                prepare_start = time.time()
                file_data_map = []
                for file_record in files_to_sync:
                    file_data_map.append((file_record, None))  # 第二个参数在openGauss模式下不需要
                prepare_time = time.time() - prepare_start
                if prepare_time > 1.0:
                    logger.warning(f"[openGauss批次 {batch_number}] 准备数据耗时较长: {prepare_time:.2f}秒")

                # 同步到openGauss（使用统一调度器）
                batch_sync_start = time.time()
                try:
                    logger.debug(f"[openGauss批次 {batch_number}] 提交到统一调度器，文件数: {len(file_data_map)}")
                    
                    # 使用统一调度器（如果已初始化）
                    if self.db_scheduler:
                        await self.db_scheduler.submit_sync_files(file_data_map)
                        # 注意：统一调度器是异步的，这里只是提交，不等待完成
                        # 实际插入会在调度器的后台循环中完成
                        # file_data_map 格式: [(file_record, None), ...]
                        # file_record 格式: (id, backup_set_id, file_path, ...)
                        # 需要提取 file_record[0] 作为 file_id，并确保是整数类型
                        synced_file_ids = [int(file_record[0]) for file_record, _ in file_data_map if file_record]
                    else:
                        # 回退到直接插入（兼容旧逻辑）
                        synced_file_ids = await self._insert_files_to_opengauss(file_data_map)
                    
                    batch_sync_time = time.time() - batch_sync_start
                    logger.info(f"[openGauss批次 {batch_number}] 已提交到统一调度器，耗时: {batch_sync_time:.2f}秒")
                    
                    # 批次同步完成后，检查是否需要报告状态（如果批次耗时很长，可能已经超过30秒）
                    current_time_after_sync = time.time()
                    if current_time_after_sync - last_status_report_time >= status_report_interval:
                        pending_count = await self._get_pending_sync_count()
                        sync_duration = current_time_after_sync - sync_start_time
                        total_scanned = self._stats['total_files']
                        total_synced_accumulated = self._stats['synced_files']
                        
                        logger.info(
                            f"【openGauss同步状态】批次 {batch_number} 完成后状态 - "
                            f"剩余待同步:▶️{pending_count} 个文件，"
                            f"本次已同步: {total_synced_count} 个，"
                            f"累计总扫描: {total_scanned} 个，"
                            f"累计总同步: {total_synced_accumulated} 个，"
                            f"已持续: {sync_duration:.1f}秒"
                        )
                        last_status_report_time = current_time_after_sync
                except Exception as batch_error:
                    batch_sync_time = time.time() - batch_sync_start
                    logger.error(
                        f"[openGauss批次 {batch_number}] ❌ 同步失败: {str(batch_error)}，"
                        f"耗时: {batch_sync_time:.2f}秒，文件数: {len(file_data_map)}",
                        exc_info=True
                    )
                    # 记录详细的错误信息
                    import traceback
                    logger.error(f"[openGauss批次 {batch_number}] 错误堆栈:\n{traceback.format_exc()}")
                    # 标记失败的文件，但不丢弃数据 - 这些文件会在下次同步时重试
                    failed_file_ids = []
                    for file_record, _ in file_data_map:
                        if file_record and len(file_record) > 0:
                            file_id = file_record[0]
                            failed_file_ids.append(file_id)
                    
                    if failed_file_ids:
                        error_msg = f"同步失败: {str(batch_error)[:200]}"  # 限制错误消息长度
                        await self._mark_sync_error_for_files(failed_file_ids, error_msg)
                        logger.warning(
                            f"[openGauss批次 {batch_number}] 已标记 {len(failed_file_ids)} 个文件同步失败，"
                            f"这些文件将在下次同步时重试（数据不会丢失）"
                        )
                    # 继续处理下一批，不中断整个同步流程
                    synced_file_ids = []  # 确保 synced_file_ids 是空列表
                    continue

                # 更新同步状态（只标记成功同步的文件）
                if synced_file_ids:
                    await self._mark_files_synced(synced_file_ids)

                # 更新统计
                synced_count = len(synced_file_ids)
                total_synced_count += synced_count
                self._stats['synced_files'] += synced_count
                self._stats['sync_batches'] += 1

                logger.info(
                    f"[openGauss批次 {batch_number}] ✅ 同步完成: {synced_count}/{len(files_to_sync)} 个文件已成功同步，"
                    f"耗时: {batch_sync_time:.2f}秒"
                )

            if batch_number >= max_batches:
                logger.warning(
                    f"[openGauss同步] 达到最大批次限制 ({max_batches})，停止同步。"
                    f"可能还有文件未同步，将在下次同步时继续。"
                )

            # 所有批次同步完成
            if batch_number > 0:
                sync_time = time.time() - sync_start_time
                self._stats['sync_time'] += sync_time
                self._last_sync_time = time.time()
                
                # 检查是否还有未同步的文件
                final_pending_count = await self._get_pending_sync_count()
                
                # 获取累计统计信息
                total_scanned = self._stats['total_files']
                total_synced_accumulated = self._stats['synced_files']
                
                logger.info(
                    f"✅ openGauss同步完成: 共 {batch_number} 个批次，总耗时 {sync_time:.2f}秒，"
                    f"同步开始时待同步: {initial_pending_count} 个，"
                    f"同步完成后剩余: {final_pending_count} 个，"
                    f"本次同步: {total_synced_count} 个，"
                    f"累计总扫描: {total_scanned} 个，"
                    f"累计总同步: {total_synced_accumulated} 个"
                )

        except Exception as e:
            logger.error(f"[openGauss同步] 同步过程异常: {e}", exc_info=True)
        finally:
            logger.info(f"[openGauss同步] 同步结束，设置 _is_syncing = False")
            self._is_syncing = False
            self._sync_start_time = 0  # 重置同步开始时间

    async def _get_files_to_sync(self) -> List[Tuple]:
        """获取待同步的文件 - 按照BackupFile模型字段顺序（仅当前备份集）"""
        # 先检查内存数据库中有多少文件
        async with self.memory_db.execute("""
            SELECT COUNT(*) FROM backup_files
            WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
        """, (self.backup_set_db_id,)) as cursor:
            pending_count = (await cursor.fetchone())[0]
        
        # 检查是否有其他 backup_set_id 的文件
        async with self.memory_db.execute("""
            SELECT DISTINCT backup_set_id, COUNT(*) as cnt
            FROM backup_files
            WHERE synced_to_opengauss = FALSE
            GROUP BY backup_set_id
            LIMIT 5
        """) as cursor:
            all_pending = await cursor.fetchall()
        
        if pending_count > 0:
            logger.debug(
                f"内存数据库中待同步文件: backup_set_id={self.backup_set_db_id}, "
                f"数量={pending_count}"
            )
        
        async with self.memory_db.execute("""
            SELECT id, backup_set_id, file_path, file_name, directory_path, display_name,
                   file_type, file_size, compressed_size, file_permissions, file_owner,
                   file_group, created_time, modified_time, accessed_time, tape_block_start,
                   tape_block_count, compressed, encrypted, checksum, is_copy_success,
                   copy_status_at, backup_time, chunk_number, version, file_metadata, tags
            FROM backup_files
            WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
            ORDER BY id
            LIMIT ?
        """, (self.backup_set_db_id, self.sync_batch_size)) as cursor:
            files = await cursor.fetchall()
            if files:
                # 验证第一个文件的 backup_set_id
                first_file_backup_set_id = files[0][1] if len(files[0]) > 1 else None
                if first_file_backup_set_id != self.backup_set_db_id:
                    logger.error(
                        f"[同步] ⚠️⚠️ 错误：待同步文件的 backup_set_id={first_file_backup_set_id} "
                        f"与 MemoryDBWriter 的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                    )
            return files

    def _parse_datetime_from_sqlite(self, dt_value) -> datetime:
        """将SQLite的datetime值转换为Python datetime对象"""
        if dt_value is None:
            return None

        if isinstance(dt_value, datetime):
            return dt_value

        if isinstance(dt_value, str):
            try:
                # SQLite返回的字符串格式："2025-04-27 06:04:31.136616+00:00"
                # 或 "2025-04-27 06:04:31"
                if '+' in dt_value:
                    # 处理带时区的格式
                    return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
                else:
                    # 处理不带时区的格式
                    naive_dt = datetime.fromisoformat(dt_value)
                    return naive_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                # 如果解析失败，返回当前时间
                return datetime.now(timezone.utc)

        # 其他情况，返回当前时间
        return datetime.now(timezone.utc)

    async def _insert_files_to_opengauss(self, file_data_map: List[Tuple]) -> List[int]:
        """将扫描文件同步到 openGauss 主库（调用方负责确保串行执行）
        
        借鉴SQLite的_insert_files_to_sqlite实现，只修改写数据库部分
        """
        logger.info(f"[openGauss同步] _insert_files_to_opengauss 开始，文件数: {len(file_data_map)}")
        if not file_data_map:
            logger.warning("[openGauss同步] _insert_files_to_opengauss: file_data_map 为空")
            return []
        
        synced_file_ids: List[int] = []
        
        # 准备批量插入数据
        insert_data = []
        logger.debug(f"[openGauss同步] 开始准备批量插入数据，文件数: {len(file_data_map)}")
        
        for file_record, _ in file_data_map:
            if not file_record:
                continue

            file_id = file_record[0]
            backup_set_id = file_record[1]  # 确保backup_set_id正确传递
            
            # 验证backup_set_id是否有效
            if backup_set_id is None or backup_set_id != self.backup_set_db_id:
                logger.error(
                    f"[openGauss同步] ⚠️ 文件 backup_set_id={backup_set_id} "
                    f"与 MemoryDBWriter 的 backup_set_db_id={self.backup_set_db_id} 不匹配！"
                    f"文件ID: {file_id}"
                )
                continue
            
            file_path = file_record[2]
            file_name = file_record[3]
            directory_path = file_record[4]
            display_name = file_record[5]
            file_type = file_record[6] or "file"
            file_size = file_record[7] or 0
            compressed_size = file_record[8]
            file_permissions = file_record[9]
            file_owner = file_record[10]
            file_group = file_record[11]
            created_time = self._parse_datetime_from_sqlite(file_record[12])
            modified_time = self._parse_datetime_from_sqlite(file_record[13])
            accessed_time = self._parse_datetime_from_sqlite(file_record[14])
            tape_block_start = file_record[15]
            tape_block_count = file_record[16]
            compressed = bool(file_record[17])
            encrypted = bool(file_record[18])
            checksum = file_record[19]
            is_copy_success = bool(file_record[20])
            copy_status_at = self._parse_datetime_from_sqlite(file_record[21])
            backup_time = self._parse_datetime_from_sqlite(file_record[22])
            chunk_number = file_record[23]
            version = file_record[24]
            file_metadata = file_record[25]
            tags = file_record[26]

            # 准备批量插入的数据元组（按照 VALUES 子句的顺序）
            data_tuple = (
                backup_set_id, file_path, file_name,
                directory_path, display_name, file_type,
                file_size, compressed_size, file_permissions,
                file_owner, file_group, created_time,
                modified_time, accessed_time, tape_block_start,
                tape_block_count, compressed, encrypted,
                checksum, is_copy_success, copy_status_at,
                backup_time, chunk_number, version,
                file_metadata, tags
            )
            insert_data.append(data_tuple)
            synced_file_ids.append(file_id)  # 先添加到列表，如果插入失败会在后面处理

        if not insert_data:
            logger.warning(f"[openGauss同步] 没有有效的数据可以插入（准备的数据为空，原始文件数: {len(file_data_map)}）")
            return []

        logger.info(f"[openGauss同步] 准备插入 {len(insert_data)} 个文件到 openGauss backup_files 表")

        # 执行批量插入
        try:
            logger.debug("[openGauss同步] 获取 openGauss 连接...")
            async with get_opengauss_connection() as conn:
                logger.debug("[openGauss同步] 已获取 openGauss 连接，开始检查表是否存在...")
                # 首先检查表是否存在（只在第一次检查，使用类级别缓存）
                if not hasattr(self.__class__, '_backup_files_table_exists'):
                    self.__class__._backup_files_table_exists = None
                
                if self.__class__._backup_files_table_exists is None:
                    try:
                        # 尝试查询表是否存在
                        logger.debug("[openGauss同步] 检查 backup_files 表是否存在...")
                        await conn.fetchrow("SELECT 1 FROM backup_files LIMIT 1")
                        self.__class__._backup_files_table_exists = True
                        logger.info("[openGauss同步] ✅ backup_files 表存在，可以执行插入操作")
                    except Exception as table_check_err:
                        # 异常时显式回滚，避免长事务锁表
                        error_msg = str(table_check_err)
                        try:
                            actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                            if hasattr(actual_conn, 'info'):
                                transaction_status = actual_conn.info.transaction_status
                                if transaction_status in (1, 3):  # INTRANS or INERROR
                                    await actual_conn.rollback()
                                    logger.debug("[openGauss同步] 检查表时异常，事务已回滚")
                        except Exception as rollback_err:
                            logger.warning(f"[openGauss同步] 回滚事务失败: {str(rollback_err)}")
                        
                        if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(table_check_err).__name__):
                            self.__class__._backup_files_table_exists = False
                            logger.warning(
                                f"[openGauss同步] backup_files 表不存在，跳过文件插入（可能是数据库未初始化）。"
                                f"请运行数据库初始化脚本创建表。错误: {error_msg}"
                            )
                            return []
                        else:
                            # 其他错误，可能是连接问题，暂时标记为未知，下次再检查
                            logger.warning(f"[openGauss同步] 检查 backup_files 表时出错: {error_msg}")
                            # 不设置缓存，下次再检查
                
                # 如果表不存在（已确认），直接返回
                if self.__class__._backup_files_table_exists is False:
                    logger.error("[openGauss同步] ❌ backup_files 表不存在，无法插入数据")
                    return []
                
                # 使用 executemany 实现真正的批量插入
                # 注意：id 字段使用 DEFAULT，让数据库自动生成（适用于 SERIAL 或序列）
                logger.info(f"[openGauss同步] 开始执行批量插入，数据量: {len(insert_data)} 条")
                insert_start_time = time.time()
                try:
                    # 记录执行前的状态
                    logger.debug(f"[openGauss同步] 调用 executemany 前，准备插入 {len(insert_data)} 条数据")
                    # 不使用 asyncio.wait_for，允许批量插入自然完成，不中断
                    rowcount = await conn.executemany("""
                    INSERT INTO backup_files (
                        backup_set_id, file_path, file_name, directory_path, display_name,
                        file_type, file_size, compressed_size, file_permissions, file_owner,
                        file_group, created_time, modified_time, accessed_time, tape_block_start,
                        tape_block_count, compressed, encrypted, checksum, is_copy_success,
                        copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                        $21, $22, $23, $24, $25::jsonb, $26::jsonb, NOW(), NOW()
                    )
                """, insert_data)
                    insert_time = time.time() - insert_start_time
                    logger.info(f"[openGauss同步] ✅ executemany 执行完成: 影响行数={rowcount}, 耗时={insert_time:.2f}秒")
                except Exception as executemany_err:
                    insert_time = time.time() - insert_start_time
                    logger.error(f"[openGauss同步] ❌ executemany 执行失败: {str(executemany_err)}, 耗时={insert_time:.2f}秒", exc_info=True)
                    # 注意：executemany 在 psycopg3_compat 中已经处理了回滚，但为了确保，这里也检查一下
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug("[openGauss同步] executemany 异常后，事务已回滚")
                    except Exception as rollback_err:
                        logger.warning(f"[openGauss同步] 回滚事务失败: {str(rollback_err)}")
                    raise  # 重新抛出异常，让外层处理
                
                # 注意：executemany 在 psycopg3_compat 中已经显式提交了事务
                # 验证事务状态，确保已提交
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    transaction_status = actual_conn.info.transaction_status
                    if transaction_status == 0:  # IDLE: 已提交
                        logger.info(f"[openGauss同步] ✅ 批量插入事务已提交: {rowcount} 个文件已持久化到数据库（事务状态=IDLE）")
                    elif transaction_status == 1:  # INTRANS: 仍在事务中
                        logger.error(f"[openGauss同步] ❌ executemany 后事务状态仍为 INTRANS！尝试再次提交...")
                        try:
                            await actual_conn.commit()
                            new_status = actual_conn.info.transaction_status
                            if new_status == 0:
                                logger.info(f"[openGauss同步] ✅ 重试提交成功，事务状态=IDLE")
                            else:
                                logger.error(f"[openGauss同步] ❌ 重试提交后事务状态仍为 {new_status}")
                        except Exception as retry_commit_err:
                            logger.error(f"[openGauss同步] ❌ 重试提交失败: {str(retry_commit_err)}")
                    else:
                        logger.warning(f"[openGauss同步] ⚠️ 事务状态: {transaction_status} (0=IDLE, 1=INTRANS, 3=INERROR)")
                except Exception as status_check_err:
                    logger.warning(f"[openGauss同步] ⚠️ 检查事务状态时出错: {str(status_check_err)}")
                
                # 检查实际插入的行数
                expected_count = len(insert_data)
                if rowcount != expected_count:
                    logger.error(
                        f"[openGauss同步] ⚠️ 批量插入部分失败: 期望插入 {expected_count} 个文件，"
                        f"实际插入 {rowcount} 个文件，缺失 {expected_count - rowcount} 个。"
                    )
                    # 如果部分失败，返回成功插入的文件ID（这里简化处理，实际应该更精确）
                    if rowcount > 0:
                        synced_file_ids = synced_file_ids[:rowcount]
                    else:
                        synced_file_ids = []
                else:
                    logger.info(f"[openGauss同步] ✅ 批量插入成功: {rowcount} 个文件已插入到openGauss")
                
                # 立即验证数据是否真的写入数据库
                try:
                    verify_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM backup_files WHERE backup_set_id = $1",
                        self.backup_set_db_id
                    )
                    logger.info(f"[openGauss同步] ✅ 数据验证: backup_files 表中 backup_set_id={self.backup_set_db_id} 的记录数: {verify_count} (期望至少 {rowcount} 条)")
                    if verify_count < rowcount:
                        logger.error(
                            f"[openGauss同步] ❌ 数据验证失败: 表中只有 {verify_count} 条记录，但应该至少有 {rowcount} 条！"
                            f"可能原因：1) 事务未提交 2) 数据被回滚 3) 其他连接看不到未提交的数据"
                        )
                except Exception as verify_err:
                    logger.warning(f"[openGauss同步] ⚠️ 验证数据时出错: {str(verify_err)}")
                    # 验证失败时，如果连接还在事务中，需要回滚
                    try:
                        actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                        if hasattr(actual_conn, 'info'):
                            transaction_status = actual_conn.info.transaction_status
                            if transaction_status in (1, 3):  # INTRANS or INERROR
                                await actual_conn.rollback()
                                logger.debug("[openGauss同步] 验证数据异常后，事务已回滚")
                    except Exception as rollback_err:
                        logger.warning(f"[openGauss同步] 回滚事务失败: {str(rollback_err)}")
                    
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[openGauss同步] ❌ _insert_files_to_opengauss 异常: {error_msg}", exc_info=True)
            # 异常时显式回滚，避免长事务锁表（如果连接还在上下文中）
            # 注意：如果异常发生在 async with 块外，连接可能已经释放
            try:
                # 尝试获取连接并回滚（如果还在上下文中）
                if 'conn' in locals():
                    actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                    if hasattr(actual_conn, 'info'):
                        transaction_status = actual_conn.info.transaction_status
                        if transaction_status in (1, 3):  # INTRANS or INERROR
                            await actual_conn.rollback()
                            logger.debug("[openGauss同步] 外层异常处理，事务已回滚")
            except Exception as rollback_err:
                # 连接可能已经释放，忽略回滚错误
                logger.debug(f"[openGauss同步] 外层异常处理时回滚失败（可能连接已释放）: {str(rollback_err)}")
            # 如果是表不存在的错误，记录警告并缓存结果
            if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(e).__name__):
                self.__class__._backup_files_table_exists = False
                logger.error(
                    f"[openGauss同步] ❌ backup_files 表不存在，跳过文件插入（可能是数据库未初始化）。"
                    f"请运行数据库初始化脚本创建表。错误: {error_msg}"
                )
                logger.warning(
                    f"[openGauss同步] 注意：文件数据目前仅保存在内存数据库（SQLite）中，"
                    f"未同步到openGauss。创建 backup_files 表后，文件将在下次同步时自动同步。"
                )
            else:
                logger.error(f"[openGauss同步] ❌ 批量插入失败: {error_msg}", exc_info=True)
            # 插入失败，返回空列表
            synced_file_ids = []

        logger.info(f"[openGauss同步] _insert_files_to_opengauss 完成，返回 {len(synced_file_ids)} 个已同步的文件ID")
        return synced_file_ids

    async def _batch_sync_to_opengauss(self, files: List[Tuple]) -> Tuple[int, List[int]]:
        """批量同步到openGauss - 借鉴SQLite的同步逻辑
        
        只修改写数据库部分，使用_insert_files_to_opengauss
        """
        if not files:
            return 0, []

        # 准备文件数据映射
        file_data_map = []
        for file_record in files:
            file_data_map.append((file_record, None))
        
        # 使用_insert_files_to_opengauss插入数据
        synced_file_ids = await self._insert_files_to_opengauss(file_data_map)
        
        synced_count = len(synced_file_ids)
        return synced_count, synced_file_ids

    async def _fallback_individual_insert(self, file_data_map: List[Tuple], failed_files: List[Tuple]) -> Tuple[int, List[int]]:
        """批量插入失败时的回退方案：逐个插入以确定失败的文件
        
        Args:
            file_data_map: 文件记录和数据的对应关系列表 [(file_record, data_tuple), ...]
            failed_files: 失败文件列表，用于追加新的失败记录
            
        Returns:
            (成功同步的文件数, 成功同步的文件ID列表)
        """
        # 检查数据库类型
        from utils.scheduler.db_utils import is_opengauss
        synced_count = 0
        synced_file_ids = []
        
        # 关键修复：为每个 INSERT 使用独立的事务，避免一个失败影响其他
        # 在 PostgreSQL/openGauss 中，一旦事务中出现错误，整个事务就会被标记为中止状态
        # 后续的所有命令都会被忽略，直到事务被回滚
        
        # 先检查备份集是否存在（避免外键约束错误）
        backup_set_id = self.backup_set_db_id
        backup_set_exists = False
        try:
            async with get_opengauss_connection() as conn:
                check_row = await conn.fetchrow(
                    "SELECT id FROM backup_sets WHERE id = $1",
                    backup_set_id
                )
                backup_set_exists = check_row is not None
        except Exception as check_err:
            logger.warning(f"检查备份集是否存在失败: {check_err}，将继续尝试插入")
            backup_set_exists = False
        
        if not backup_set_exists:
            logger.error(
                f"⚠️⚠️ 备份集不存在: backup_set_id={backup_set_id}，"
                f"无法插入文件。这可能是备份集创建失败或事务未提交导致的。"
                f"将跳过所有 {len(file_data_map)} 个文件的插入。"
            )
            # 将所有文件标记为失败
            for idx, (file_record, data_tuple) in enumerate(file_data_map):
                file_path_str = file_record[2] if len(file_record) > 2 else 'unknown'
                failed_files.append((idx, file_path_str, f"备份集不存在: backup_set_id={backup_set_id}"))
            return 0, []
        
        # 先检查表是否存在（只检查一次，避免重复检查）
        table_exists = True
        table_check_done = False
        
        for idx, (file_record, data_tuple) in enumerate(file_data_map):
            # 为每个文件使用独立的连接和事务
            async with get_opengauss_connection() as conn:
                try:
                    # 第一次插入时检查表是否存在
                    if not table_check_done:
                        try:
                            # 尝试查询表是否存在
                            await conn.fetchrow("SELECT 1 FROM backup_files LIMIT 1")
                            table_exists = True
                        except Exception as table_check_err:
                            error_msg = str(table_check_err)
                            if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(table_check_err).__name__):
                                table_exists = False
                                logger.warning(
                                    f"backup_files 表不存在，跳过所有文件插入（可能是数据库未初始化）: {error_msg}"
                                )
                                # 将所有剩余文件标记为失败
                                for remaining_idx in range(idx, len(file_data_map)):
                                    remaining_file_record = file_data_map[remaining_idx][0]
                                    remaining_file_path = remaining_file_record[2] if len(remaining_file_record) > 2 else 'unknown'
                                    failed_files.append((remaining_idx, remaining_file_path, f"表不存在: {error_msg}"))
                                break
                        table_check_done = True
                    
                    # 如果表不存在，跳过插入
                    if not table_exists:
                        continue
                    
                    # psycopg3 的 execute 在兼容层中已经使用事务上下文管理器，会自动提交
                    # 使用 RETURNING id 获取生成的 ID（如果表有自增序列）
                    # 注意：id 字段不包含在插入列表中，数据库会自动生成（SERIAL 或序列）
                    # 如果表定义中 id 不是 SERIAL，需要在插入时使用 DEFAULT 或 nextval()
                    result = await conn.fetchrow("""
                        INSERT INTO backup_files (
                            backup_set_id, file_path, file_name, directory_path, display_name,
                            file_type, file_size, compressed_size, file_permissions, file_owner,
                            file_group, created_time, modified_time, accessed_time, tape_block_start,
                            tape_block_count, compressed, encrypted, checksum, is_copy_success,
                            copy_status_at, backup_time, chunk_number, version, file_metadata, tags,
                            created_at, updated_at
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                            $21, $22, $23, $24, $25::jsonb, $26::jsonb, NOW(), NOW()
                        ) RETURNING id
                    """, *data_tuple)
                    
                    # 获取生成的 ID
                    if result:
                        generated_id = result.get('id')
                        if generated_id:
                            # 更新 file_record 中的 ID（用于后续查询）
                            file_record = file_data_map[idx][0]
                            # file_record 是元组，不可变，需要重新创建
                            # 但这里我们只需要记录 ID，不需要修改 file_record
                            pass
                    
                    synced_count += 1
                    # 记录成功同步的文件ID（file_record[0] 是文件ID）
                    synced_file_ids.append(file_record[0])
                except Exception as e:
                    # 错误发生时，连接释放时会自动回滚，不需要手动回滚
                    file_path_str = file_record[2] if len(file_record) > 2 else 'unknown'
                    error_msg = str(e)
                    
                    # 如果表不存在，记录警告并跳过后续文件
                    if "does not exist" in error_msg.lower() or "relation" in error_msg.lower() or "UndefinedTable" in str(type(e).__name__):
                        if not table_check_done:
                            logger.warning(
                                f"backup_files 表不存在，跳过所有文件插入（可能是数据库未初始化）: {error_msg}"
                            )
                            table_exists = False
                            table_check_done = True
                            # 将所有剩余文件标记为失败
                            for remaining_idx in range(idx, len(file_data_map)):
                                remaining_file_record = file_data_map[remaining_idx][0]
                                remaining_file_path = remaining_file_record[2] if len(remaining_file_record) > 2 else 'unknown'
                                failed_files.append((remaining_idx, remaining_file_path, f"表不存在: {error_msg}"))
                            break
                        continue
                    
                    # 检查是否是重复键冲突（虽然模型中没有唯一约束，但可能有其他约束）
                    if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower() or "already exists" in error_msg.lower():
                        # 重复键冲突：文件可能已经存在，跳过但不标记为失败
                        logger.debug(f"文件已存在（跳过）: {file_path_str}")
                        # 不添加到 failed_files，也不增加 synced_count
                        # 但需要标记为已同步（因为文件已经在数据库中）
                        synced_count += 1
                        synced_file_ids.append(file_record[0])
                        continue
                    
                    # 检查是否是重复键冲突（虽然模型中没有唯一约束，但可能有其他约束）
                    if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower() or "already exists" in error_msg.lower():
                        # 重复键冲突：文件可能已经存在，跳过但不标记为失败
                        logger.debug(f"文件已存在（跳过）: {file_path_str}")
                        # 不添加到 failed_files，也不增加 synced_count
                        # 但需要标记为已同步（因为文件已经在数据库中）
                        synced_count += 1
                        synced_file_ids.append(file_record[0])
                        continue
                    
                    # 如果是外键约束错误，提供更详细的错误信息
                    if "foreign key constraint" in error_msg.lower() and "backup_sets" in error_msg.lower():
                        logger.error(
                            f"回退逐个插入失败（索引 {idx}）: 外键约束错误 - 备份集不存在\n"
                            f"  备份集ID: {backup_set_id}\n"
                            f"  文件: {file_path_str}\n"
                            f"  错误详情: {error_msg}"
                        )
                    else:
                        logger.error(f"回退逐个插入失败（索引 {idx}）: {error_msg}, 文件: {file_path_str}")
                    # 注意：这里不记录原始索引，因为 file_data_map 中只包含成功准备数据的文件
                    failed_files.append((idx, file_path_str, error_msg))
        
        return synced_count, synced_file_ids

    async def _mark_files_synced(self, file_ids: List[int]):
        """标记文件已同步（仅当前备份集）"""
        if not file_ids:
            return

        placeholders = ','.join(['?' for _ in file_ids])
        await self.memory_db.execute(
            f"UPDATE backup_files SET synced_to_opengauss = TRUE, sync_error = NULL WHERE backup_set_id = ? AND id IN ({placeholders})",
            [self.backup_set_db_id] + file_ids
        )
        await self.memory_db.commit()
        
        # 同步成功后，清理已完全同步的检查点文件
        await self._cleanup_synced_checkpoints()

    async def _mark_sync_error(self, files: List[Tuple], error_message: str):
        """标记同步错误（仅当前备份集）"""
        # 检查数据库连接是否已关闭
        if not self.memory_db:
            logger.warning("内存数据库连接已关闭，无法标记同步错误")
            return
        
        try:
            # 检查连接是否有效
            if hasattr(self.memory_db, '_conn') and self.memory_db._conn is None:
                logger.warning("内存数据库连接已关闭，无法标记同步错误")
                return
        except (ValueError, AttributeError):
            logger.warning("内存数据库连接已关闭，无法标记同步错误")
            return
        
        try:
            file_ids = [f[0] for f in files]
            placeholders = ','.join(['?' for _ in file_ids])
            await self.memory_db.execute(
                f"UPDATE backup_files SET sync_error = ? WHERE backup_set_id = ? AND id IN ({placeholders})",
                [error_message, self.backup_set_db_id] + file_ids
            )
            await self.memory_db.commit()
        except (ValueError, sqlite3.ProgrammingError) as e:
            # 连接已关闭，记录警告但不抛出异常
            logger.warning(f"标记同步错误时数据库连接已关闭: {e}")

    async def _mark_sync_error_for_files(self, file_ids: List[int], error_message: str):
        """标记文件同步错误（使用文件ID列表，仅当前备份集）
        
        Args:
            file_ids: 文件ID列表
            error_message: 错误消息
        """
        if not file_ids:
            return
        
        # 检查数据库连接是否已关闭
        if not self.memory_db:
            logger.warning("内存数据库连接已关闭，无法标记同步错误")
            return
        
        try:
            # 检查连接是否有效
            if hasattr(self.memory_db, '_conn') and self.memory_db._conn is None:
                logger.warning("内存数据库连接已关闭，无法标记同步错误")
                return
        except (ValueError, AttributeError):
            logger.warning("内存数据库连接已关闭，无法标记同步错误")
            return
        
        try:
            placeholders = ','.join(['?' for _ in file_ids])
            await self.memory_db.execute(
                f"UPDATE backup_files SET sync_error = ? WHERE backup_set_id = ? AND id IN ({placeholders})",
                [error_message, self.backup_set_db_id] + file_ids
            )
            await self.memory_db.commit()
            logger.debug(f"已标记 {len(file_ids)} 个文件同步错误: {error_message}")
        except (ValueError, sqlite3.ProgrammingError) as e:
            # 连接已关闭，记录警告但不抛出异常
            logger.warning(f"标记同步错误时数据库连接已关闭: {e}")

    async def _create_checkpoint(self):
        """创建检查点 - 持久化保护"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        
        try:
            # 确保检查点目录存在
            if self.checkpoint_dir:
                self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # 获取创建检查点时的最大未同步文件ID
            max_unsynced_id = await self._get_max_unsynced_file_id()
            
            # 在项目目录下的 temp/checkpoints 目录中创建检查点文件
            checkpoint_filename = f"tmp{int(time.time() * 1000)}.sql"
            checkpoint_file = str(self.checkpoint_dir / checkpoint_filename)

            # 备份内存数据库
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                async for line in self.memory_db.iterdump():
                    f.write(f"{line}\n")

            self._last_checkpoint_time = time.time()
            # 记录检查点文件：(文件路径, 创建时间, 最大未同步文件ID)
            self._checkpoint_files.append((checkpoint_file, self._last_checkpoint_time, max_unsynced_id))
            logger.debug(f"检查点已创建: {checkpoint_file} (最大未同步文件ID: {max_unsynced_id})")
            
            # 清理过期的检查点文件
            await self._cleanup_old_checkpoints()

        except Exception as e:
            logger.error(f"创建检查点失败: {e}")
    
    async def _get_max_unsynced_file_id(self) -> int:
        """获取当前最大未同步文件ID（仅当前备份集）"""
        try:
            async with self.memory_db.execute("""
                SELECT MAX(id) FROM backup_files WHERE backup_set_id = ? AND synced_to_opengauss = FALSE
            """, (self.backup_set_db_id,)) as cursor:
                result = await cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except Exception as e:
            logger.debug(f"获取最大未同步文件ID失败: {e}")
            return 0
    
    async def _cleanup_old_checkpoints_on_startup(self):
        """启动时清理所有过期的检查点文件"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        
        try:
            import os
            current_time = time.time()
            retention_seconds = self.checkpoint_retention_hours * 3600
            
            # 清理检查点目录中的所有过期文件
            if self.checkpoint_dir and self.checkpoint_dir.exists():
                import glob
                pattern = str(self.checkpoint_dir / 'tmp*.sql')
                cleaned_count = 0
                for old_file in glob.glob(pattern):
                    try:
                        file_stat = os.stat(old_file)
                        file_age = current_time - file_stat.st_mtime
                        if file_age > retention_seconds:
                            os.remove(old_file)
                            cleaned_count += 1
                            logger.debug(f"启动时已删除过期检查点文件: {old_file}")
                    except Exception as e:
                        logger.debug(f"清理检查点文件时出错（忽略）: {old_file}, {e}")
                
                if cleaned_count > 0:
                    logger.debug(f"启动时已清理 {cleaned_count} 个过期检查点文件")
            else:
                # 如果目录不存在，创建它
                self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
                
        except Exception as e:
            logger.warning(f"启动时清理检查点文件失败: {e}")
    
    async def _cleanup_synced_checkpoints(self):
        """清理已完全同步到openGauss的检查点文件"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            # 获取当前已同步的最大文件ID
            async with self.memory_db.execute("""
                SELECT MAX(id) FROM backup_files WHERE synced_to_opengauss = TRUE
            """) as cursor:
                result = await cursor.fetchone()
                max_synced_id = result[0] if result and result[0] is not None else 0
            
            if max_synced_id <= 0:
                return  # 还没有同步任何文件
            
            # 清理所有已完全同步的检查点文件
            # 如果检查点创建时的最大未同步文件ID <= 当前已同步的最大文件ID，说明该检查点的所有数据都已同步
            files_to_remove = []
            for checkpoint_info in self._checkpoint_files[:]:
                if len(checkpoint_info) >= 3:
                    checkpoint_file, create_time, max_unsynced_id = checkpoint_info
                    # 如果检查点创建时的最大未同步文件ID <= 当前已同步的最大文件ID，说明该检查点的所有数据都已同步
                    if max_unsynced_id <= max_synced_id:
                        try:
                            if os.path.exists(checkpoint_file):
                                os.remove(checkpoint_file)
                                logger.debug(f"检查点文件已完全同步到openGauss，已删除: {checkpoint_file} (检查点最大未同步ID: {max_unsynced_id}, 当前已同步最大ID: {max_synced_id})")
                            files_to_remove.append(checkpoint_info)
                        except Exception as e:
                            logger.warning(f"删除已同步的检查点文件失败: {checkpoint_file}, 错误: {e}")
                else:
                    # 兼容旧格式：(文件路径, 创建时间)
                    checkpoint_file, create_time = checkpoint_info
                    # 旧格式的检查点文件无法判断是否已完全同步，跳过
                    pass
            
            # 从列表中移除已删除的文件
            for item in files_to_remove:
                if item in self._checkpoint_files:
                    self._checkpoint_files.remove(item)
                    
        except Exception as e:
            logger.warning(f"清理已同步的检查点文件失败: {e}")
    
    async def _cleanup_old_checkpoints(self):
        """清理过期的检查点文件（定期调用）"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            current_time = time.time()
            retention_seconds = self.checkpoint_retention_hours * 3600
            
            # 清理记录列表中的过期文件（仅清理未同步的过期文件）
            files_to_remove = []
            for checkpoint_info in self._checkpoint_files[:]:
                if len(checkpoint_info) >= 3:
                    checkpoint_file, create_time, max_unsynced_id = checkpoint_info
                else:
                    # 兼容旧格式：(文件路径, 创建时间)
                    checkpoint_file, create_time = checkpoint_info
                    max_unsynced_id = None
                
                if current_time - create_time > retention_seconds:
                    try:
                        if os.path.exists(checkpoint_file):
                            os.remove(checkpoint_file)
                            logger.debug(f"已删除过期检查点文件: {checkpoint_file}")
                        files_to_remove.append(checkpoint_info)
                    except Exception as e:
                        logger.warning(f"删除检查点文件失败: {checkpoint_file}, 错误: {e}")
            
            # 从列表中移除已删除的文件
            for item in files_to_remove:
                if item in self._checkpoint_files:
                    self._checkpoint_files.remove(item)
            
            # 同时清理检查点目录中可能遗留的过期文件（通过文件名模式匹配）
            try:
                import glob
                if self.checkpoint_dir.exists():
                    pattern = str(self.checkpoint_dir / 'tmp*.sql')
                    for old_file in glob.glob(pattern):
                        try:
                            # 如果文件不在记录列表中，检查是否过期
                            file_in_list = any(old_file == (cf[0] if isinstance(cf, tuple) else cf) for cf in self._checkpoint_files)
                            if not file_in_list:
                                file_stat = os.stat(old_file)
                                file_age = current_time - file_stat.st_mtime
                                if file_age > retention_seconds:
                                    os.remove(old_file)
                                    logger.debug(f"已删除检查点目录中的过期文件: {old_file}")
                        except Exception as e:
                            logger.debug(f"清理检查点文件时出错（忽略）: {old_file}, {e}")
            except Exception as e:
                logger.debug(f"清理检查点目录文件失败（忽略）: {e}")
                
        except Exception as e:
            logger.warning(f"清理过期检查点文件失败: {e}")
    
    async def _cleanup_all_checkpoints(self):
        """清理所有检查点文件（停止时调用）"""
        if not self.enable_checkpoint:
            return  # 检查点功能已禁用，直接返回
        try:
            import os
            for checkpoint_info in self._checkpoint_files[:]:
                # 兼容新旧格式
                if len(checkpoint_info) >= 3:
                    checkpoint_file = checkpoint_info[0]
                else:
                    checkpoint_file = checkpoint_info[0]
                
                try:
                    if os.path.exists(checkpoint_file):
                        os.remove(checkpoint_file)
                        logger.debug(f"已删除检查点文件: {checkpoint_file}")
                except Exception as e:
                    logger.warning(f"删除检查点文件失败: {checkpoint_file}, 错误: {e}")
            self._checkpoint_files.clear()
        except Exception as e:
            logger.warning(f"清理所有检查点文件失败: {e}")

    async def force_sync(self):
        """强制同步所有待同步文件"""
        logger.info("强制同步所有待同步文件")
        await self._sync_to_opengauss("force_sync")

    async def stop(self):
        """停止内存数据库写入器"""
        logger.info("停止内存数据库写入器")

        # 停止 openGauss 统一调度器
        if self.db_scheduler:
            logger.info("[内存数据库] 停止 openGauss 统一调度器...")
            await self.db_scheduler.stop()
            self.db_scheduler = None

        # 停止同步任务
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        # 如果检查点任务还在运行，等待它完成（仅在启用检查点时）
        if self.enable_checkpoint and self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # 最后一次同步
        if self.memory_db:
            try:
                await self.force_sync()
                # 创建最终检查点（仅在启用检查点时）
                if self.enable_checkpoint:
                    await self._create_checkpoint()
            except Exception as e:
                logger.error(f"最终同步失败: {e}")
        
        # 清理所有检查点文件（停止时，仅在启用检查点时）
        if self.enable_checkpoint:
            await self._cleanup_all_checkpoints()

        # 关闭数据库连接
        if self.memory_db:
            await self.memory_db.close()

    async def clear_database(self):
        """清空内存数据库中的所有数据（仅当前备份集）"""
        if not self.memory_db:
            logger.warning("内存数据库未初始化，无法清空")
            return
        
        try:
            # 删除当前备份集的所有文件记录
            async with self.memory_db.execute(
                "DELETE FROM backup_files WHERE backup_set_id = ?",
                (self.backup_set_db_id,)
            ) as cursor:
                deleted_count = cursor.rowcount
            
            await self.memory_db.commit()
            
            # 重置统计信息
            self._stats = {
                'total_files': 0,
                'synced_files': 0,
                'sync_batches': 0,
                'total_time': 0,
                'sync_time': 0,
                'memory_usage': 0
            }
            
            logger.info(f"已清空内存数据库（备份集ID: {self.backup_set_db_id}），删除了 {deleted_count} 条记录")
            
        except Exception as e:
            logger.error(f"清空内存数据库失败: {e}", exc_info=True)
            raise

    async def check_database_schema(self):
        """检查内存数据库的字段设置"""
        if not self.memory_db:
            logger.warning("内存数据库未初始化，无法检查")
            return
        
        try:
            # 获取表结构
            async with self.memory_db.execute("PRAGMA table_info(backup_files)") as cursor:
                columns = await cursor.fetchall()
            
            logger.info("========== 内存数据库字段检查 ==========")
            logger.info(f"表名: backup_files")
            logger.info(f"字段数量: {len(columns)}")
            logger.info("字段列表:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, pk = col
                logger.info(f"  [{col_id}] {col_name}: {col_type} (NOT NULL: {not_null}, DEFAULT: {default_val}, PK: {pk})")
            
            # 检查关键字段的默认值
            synced_col = next((c for c in columns if c[1] == 'synced_to_opengauss'), None)
            if synced_col:
                logger.info(f"synced_to_opengauss 字段: 类型={synced_col[2]}, 默认值={synced_col[4]}")
            else:
                logger.warning("未找到 synced_to_opengauss 字段！")
            
            # 检查当前数据状态
            async with self.memory_db.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN synced_to_opengauss = TRUE THEN 1 END) as synced,
                    COUNT(CASE WHEN synced_to_opengauss = FALSE THEN 1 END) as pending,
                    COUNT(CASE WHEN synced_to_opengauss IS NULL THEN 1 END) as null_synced
                FROM backup_files
                WHERE backup_set_id = ?
            """, (self.backup_set_db_id,)) as cursor:
                result = await cursor.fetchone()
                if result:
                    total, synced, pending, null_synced = result
                    logger.info(f"当前数据状态（备份集ID: {self.backup_set_db_id}）:")
                    logger.info(f"  总文件数: {total}")
                    logger.info(f"  已同步: {synced}")
                    logger.info(f"  待同步: {pending}")
                    logger.info(f"  synced_to_opengauss 为 NULL: {null_synced}")
                    if null_synced > 0:
                        logger.warning(f"⚠️ 发现 {null_synced} 个文件的 synced_to_opengauss 字段为 NULL，这可能导致同步问题！")
            
            logger.info("=========================================")
            
        except Exception as e:
            logger.error(f"检查内存数据库字段设置失败: {e}", exc_info=True)
            raise

    def get_stats(self) -> Dict:
        """获取统计信息"""
        stats = self._stats.copy()
        stats['memory_usage'] = self._stats['total_files'] * 2  # 估算内存使用(KB)
        stats['pending_sync'] = self._stats['total_files'] - self._stats['synced_files']
        stats['sync_progress'] = (self._stats['synced_files'] / max(1, self._stats['total_files'])) * 100

        return stats

    async def get_sync_status(self) -> Dict:
        """获取同步状态详情"""
        if not self.memory_db:
            return {'status': 'not_initialized'}

        async with self.memory_db.execute("""
            SELECT
                COUNT(*) as total_files,
                COUNT(CASE WHEN synced_to_opengauss = TRUE THEN 1 END) as synced_files,
                COUNT(CASE WHEN synced_to_opengauss = FALSE AND sync_error IS NOT NULL THEN 1 END) as error_files,
                COUNT(CASE WHEN synced_to_opengauss = FALSE THEN 1 END) as pending_files,
                SUM(file_size) as total_size
            FROM backup_files
        """) as cursor:
            result = await cursor.fetchone()

            return {
                'total_files': result[0],
                'synced_files': result[1],
                'error_files': result[2],
                'pending_files': result[3],
                'total_size': result[4] or 0,
                'sync_progress': (result[1] / max(1, result[0])) * 100,
                'is_syncing': self._is_syncing,
                'last_sync_time': self._last_sync_time,
                'last_checkpoint_time': self._last_checkpoint_time
            }