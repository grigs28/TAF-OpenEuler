#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理模块
Database Management Module
"""

import asyncio
import logging
from typing import Optional

from .settings import get_settings

logger = logging.getLogger(__name__)


class DatabaseManager:
    """数据库管理器"""

    def __init__(self):
        self.settings = get_settings()
        self._initialized = False
        self._is_opengauss_cache: Optional[bool] = None

    def is_opengauss_database(self) -> bool:
        """对外提供是否为 openGauss 的统一判断"""
        if self._is_opengauss_cache is None:
            self._is_opengauss_cache = self._detect_opengauss()
        return bool(self._is_opengauss_cache)

    def _detect_opengauss(self) -> bool:
        """根据URL/显式配置/服务器版本自动识别openGauss"""
        raw_url = (self.settings.DATABASE_URL or "").lower()
        if "opengauss" in raw_url:
            return True

        flavor = getattr(self.settings, "DB_FLAVOR", None)
        if flavor and "opengauss" in flavor.lower():
            logger.info("通过 DB_FLAVOR 显式配置识别为 openGauss 数据库")
            return True

        # 尝试使用 psycopg2 或 psycopg3 检测
        conn = None
        try:
            from utils.db_connection_helper import get_psycopg_connection_from_url
            conn, is_psycopg3 = get_psycopg_connection_from_url(self._build_database_url(), prefer_psycopg3=True)
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                row = cur.fetchone()
                version_str = row[0] if row else ""
                if version_str and "opengauss" in version_str.lower():
                    logger.info(f"自动检测到 openGauss 数据库: {version_str}")
                    return True
        except Exception as detect_error:
            logger.debug(f"自动检测 openGauss 数据库失败: {detect_error}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        return False

    async def initialize(self):
        """初始化数据库连接"""
        try:
            # 构建数据库URL
            raw_database_url = self.settings.DATABASE_URL
            database_url = self._build_database_url()

            # openGauss使用原生SQL，不创建SQLAlchemy引擎
            logger.info("openGauss数据库，跳过SQLAlchemy引擎创建，将使用原生SQL查询")

            # 创建表
            await self.create_tables()

            self._initialized = True
            logger.info("数据库连接初始化成功")

        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            raise

    def _build_database_url(self) -> str:
        """构建同步数据库URL"""
        url = self.settings.DATABASE_URL
        # 将opengauss URL转换为postgresql URL（兼容）
        if url.startswith("opengauss://"):
            return url.replace("opengauss://", "postgresql://")
        return url

    def _build_async_database_url(self) -> str:
        """构建异步数据库URL"""
        url = self.settings.DATABASE_URL
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        elif url.startswith("opengauss://"):
            return url.replace("opengauss://", "postgresql+asyncpg://")
        else:
            return url

    async def create_tables(self):
        """创建数据库表"""
        try:
            # 导入所有模型以确保它们被注册
            from models import backup, tape, user, system_log, system_config, scheduled_task

            # 使用psycopg3（优先）或psycopg2（回退）直接创建表
            try:
                import psycopg
                driver_name = "psycopg3"
            except ImportError:
                try:
                    import psycopg2
                    driver_name = "psycopg2"
                except ImportError:
                    driver_name = "psycopg2/psycopg3（未安装）"
            logger.info(f"将使用{driver_name}创建数据库表...")
            await self._create_tables_with_psycopg2()

        except Exception as e:
            logger.error(f"创建数据库表失败: {str(e)}")
            raise
    
    async def _create_tables_with_psycopg2(self):
        """使用psycopg2或psycopg3直接连接创建表（解决openGauss版本解析问题）"""
        import re
        
        # 优先尝试使用 psycopg3（同步版本），如果失败则使用 psycopg2
        use_psycopg3 = False
        try:
            import psycopg
            use_psycopg3 = True
            logger.info("使用 psycopg3（同步）创建数据库表...")
        except ImportError:
            try:
                import psycopg2
                logger.info("使用 psycopg2 创建数据库表...")
            except ImportError:
                logger.error("psycopg2 和 psycopg3 都未安装，无法创建数据库表")
                raise ImportError("需要安装 psycopg2 或 psycopg3 来创建数据库表")
        
        # 解析数据库URL获取连接信息
        database_url = self.settings.DATABASE_URL
        if database_url.startswith("opengauss://"):
            database_url = database_url.replace("opengauss://", "postgresql://", 1)
        
        pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
        match = re.match(pattern, database_url)
        if not match:
            raise ValueError("无法解析数据库连接URL")
        
        username, password, host, port, database = match.groups()
        
        # 使用 psycopg3 或 psycopg2 直接连接
        if use_psycopg3:
            import psycopg
            conn = psycopg.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                dbname=database
            )
        else:
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=database
            )
        
        try:
            # 使用原生 SQL 创建表，不依赖 SQLAlchemy（符合规则：严禁 SQLAlchemy 解析 openGauss）
            # 从模型定义中提取表结构信息，生成原生 SQL
            from utils.sql_generator import get_table_definition_from_model, generate_create_table_sql
            from models.base import Base  # noqa: F401 - Base.metadata 用于获取表结构元数据

            with conn.cursor() as cur:
                # 先创建枚举类型（明确定义所有枚举类型）
                from models.scheduled_task import ScheduleType, ScheduledTaskStatus, TaskActionType
                from models.system_log import LogLevel, LogCategory, OperationType
                from models.backup import BackupTaskType, BackupTaskStatus, BackupFileType
                
                # 定义所有需要的枚举类型（确保名称和值正确）
                from models.system_config import ConfigType, ConfigCategory
                from models.tape import TapeStatus, TapeOperationType, TapeLogLevel
                from models.user import UserStatus, PermissionCategory
                from models.backup import BackupSetStatus
                
                enum_definitions = {
                    'scheduletype': [e.value for e in ScheduleType],  # ['once', 'interval', 'daily', 'weekly', 'monthly', 'yearly', 'cron']
                    'scheduledtaskstatus': [e.value for e in ScheduledTaskStatus],  # ['active', 'inactive', 'running', 'paused', 'error']
                    'taskactiontype': [e.value for e in TaskActionType],  # ['backup', 'recovery', 'cleanup', 'health_check', 'retention_check', 'custom']
                    'loglevel': [e.value for e in LogLevel],  # ['debug', 'info', 'warning', 'error', 'critical']
                    'logcategory': [e.value for e in LogCategory],  # ['system', 'backup', 'recovery', 'tape', 'user', 'security', 'performance', 'api', 'web', 'database']
                    'operationtype': [e.value for e in OperationType],  # 所有操作类型
                    'backuptasktype': [e.value for e in BackupTaskType],  # ['full', 'incremental', 'differential', 'monthly_full']
                    'backuptaskstatus': [e.value for e in BackupTaskStatus],  # ['pending', 'running', 'completed', 'failed', 'cancelled', 'paused']
                    'backupsetstatus': [e.value for e in BackupSetStatus],  # ['active', 'archived', 'corrupted', 'deleted']
                    'backupfiletype': [e.value for e in BackupFileType],  # ['file', 'directory', 'symlink']
                    'configtype': [e.value for e in ConfigType],  # ['string', 'integer', 'float', 'boolean', 'json', 'encrypted']
                    'configcategory': [e.value for e in ConfigCategory],  # ['application', 'database', 'web', 'security', 'tape', 'backup', 'scheduler', 'notification', 'performance', 'monitoring', 'storage']
                    'tapestatus': [e.value for e in TapeStatus],  # ['new', 'available', 'in_use', 'full', 'expired', 'error', 'maintenance', 'retired']
                    'tapeoperationtype': [e.value for e in TapeOperationType],  # ['load', 'unload', 'write', 'read', 'erase', 'rewind', 'verify', 'clean', 'error', 'maintenance']
                    'tapeloglevel': [e.value for e in TapeLogLevel],  # ['info', 'warning', 'error', 'debug']
                    'userstatus': [e.value for e in UserStatus],  # ['active', 'inactive', 'locked', 'suspended']
                    'permissioncategory': [e.value for e in PermissionCategory],  # ['backup', 'recovery', 'tape', 'system', 'user', 'log', 'config', 'monitor']
                }
                
                # 初始化统计列表
                created_enums = []
                existing_enums = []
                
                for enum_name, enum_values in enum_definitions.items():
                    # 检查枚举类型是否已存在
                    cur.execute("""
                        SELECT 1 FROM pg_type WHERE typname = %s
                    """, (enum_name,))
                    exists = cur.fetchone()
                    
                    if not exists:
                        # 创建枚举类型
                        quoted_values = ', '.join([f"'{v}'" for v in enum_values])
                        enum_sql = f'CREATE TYPE {enum_name} AS ENUM ({quoted_values})'
                        cur.execute(enum_sql)
                        created_enums.append(enum_name)
                    else:
                        existing_enums.append(enum_name)
                
                # 汇总输出，减少日志刷屏
                if created_enums:
                    logger.info(f"创建了 {len(created_enums)} 个新枚举类型")
                if existing_enums:
                    logger.debug(f"跳过 {len(existing_enums)} 个已存在的枚举类型")
                
                # 也处理其他表中的枚举类型（从SQLAlchemy元数据获取）
                for table in Base.metadata.tables.values():
                    for column in table.columns:
                        if hasattr(column.type, 'enums'):
                            enum_name = column.type.name
                            # 如果已经在上面定义过，跳过
                            if enum_name.lower() in enum_definitions:
                                continue
                            
                            # 获取枚举值
                            try:
                                enum_values = [e.value for e in column.type.enums]
                            except AttributeError:
                                enum_values = list(column.type.enums)
                            
                            # 检查枚举类型是否已存在
                            cur.execute("""
                                SELECT 1 FROM pg_type WHERE typname = %s
                            """, (enum_name,))
                            if not cur.fetchone():
                                quoted_values = ', '.join([f"'{v}'" for v in enum_values])
                                enum_sql = f"CREATE TYPE {enum_name} AS ENUM ({quoted_values})"
                                cur.execute(enum_sql)
                                created_enums.append(enum_name)
                            else:
                                existing_enums.append(enum_name)
                
                # 同步枚举值：将 Python 枚举中新增的值添加到数据库已有枚举中
                updated_enums = []
                for enum_name, enum_values in enum_definitions.items():
                    for val in enum_values:
                        try:
                            cur.execute(
                                f"SELECT 1 FROM pg_enum e JOIN pg_type t ON e.enumtypid = t.oid "
                                f"WHERE t.typname = %s AND e.enumlabel = %s",
                                (enum_name, val)
                            )
                            if not cur.fetchone():
                                cur.execute(f"ALTER TYPE {enum_name} ADD VALUE %s", (val,))
                                updated_enums.append(f"{enum_name}.{val}")
                        except Exception as e:
                            logger.debug(f"同步枚举值 {enum_name}.{val} 失败（可能已存在）: {e}")
                if updated_enums:
                    conn.commit()
                    logger.info(f"同步了 {len(updated_enums)} 个新枚举值: {', '.join(updated_enums)}")

                # 关键修复：在创建表之前提交枚举类型的创建（openGauss模式下需要显式提交）
                conn.commit()
                logger.debug("枚举类型创建已提交")
                
                # 创建表
                created_tables = []
                existing_tables = []
                
                # 遍历所有表，使用原生 SQL 创建
                # Base.metadata.sorted_tables 已经按照依赖关系排序（被引用的表在前）
                logger.info(f"开始创建表，Base.metadata 中共有 {len(Base.metadata.sorted_tables)} 个表")
                table_names = [t.name for t in Base.metadata.sorted_tables]
                logger.info(f"表创建顺序: {', '.join(table_names)}")
                for table in Base.metadata.sorted_tables:
                    logger.debug(f"处理表: {table.name}")
                    # 多表方案：跳过基础表 backup_files，仅使用 backup_files_template 和分表
                    if table.name == "backup_files":
                        logger.info("跳过基础表 backup_files（多表方案仅使用 backup_files_template 和分表）")
                        existing_tables.append(table.name)
                        continue
                    # 检查表是否已存在
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = %s
                    """, (table.name,))
                    table_exists = cur.fetchone()
                    if not table_exists:
                        logger.info(f"表 {table.name} 不存在，开始创建...")
                        # 从模型定义中提取表结构，生成原生 SQL（不依赖 SQLAlchemy 编译）
                        columns = get_table_definition_from_model(table.name)
                        if columns:
                            logger.debug(f"表 {table.name} 成功提取到 {len(columns)} 个列定义")
                            # 检查是否有唯一约束字段（用于调试）
                            unique_cols = [col[0] for col in columns if col[4] and 'UNIQUE' in str(col[4])]
                            if unique_cols:
                                logger.debug(f"表 {table.name} 的唯一约束字段: {', '.join(unique_cols)}")
                            try:
                                create_sql = generate_create_table_sql(table.name, columns)
                                # 记录生成的 SQL（用于调试，特别是对于有枚举类型的表）
                                has_enum = False
                                if columns:
                                    try:
                                        has_enum = any(
                                            col is not None and len(col) > 1 and col[1] is not None and 
                                            str(col[1]).lower() in ['tapestatus', 'tapeoperationtype', 'tapeloglevel', 
                                                                      'backuptasktype', 'backuptaskstatus', 'backupsetstatus', 'backupfiletype',
                                                                      'scheduletype', 'scheduledtaskstatus', 'taskactiontype',
                                                                      'loglevel', 'logcategory', 'operationtype', 'errorlevel',
                                                                      'configtype', 'configcategory', 'userstatus', 'permissioncategory']
                                            for col in columns
                                        )
                                    except (TypeError, IndexError) as e:
                                        logger.warning(f"检查表 {table.name} 的枚举类型时出错: {e}，跳过枚举类型检查")
                                        has_enum = False
                                if has_enum:
                                    logger.info(f"为表 {table.name} 生成的 SQL（包含枚举类型）:\n{create_sql}")
                                else:
                                    logger.debug(f"为表 {table.name} 生成的 SQL:\n{create_sql}")
                                cur.execute(create_sql)
                                # 关键修复：在创建每个表后立即提交（openGauss模式下需要显式提交）
                                conn.commit()
                                created_tables.append(table.name)
                            except Exception as sql_err:
                                error_msg = str(sql_err)
                                logger.error(f"创建表 {table.name} 失败: {error_msg}")
                                
                                # 如果是外键约束错误，可能是表创建顺序问题
                                if "no unique constraint" in error_msg.lower() or "foreign key" in error_msg.lower():
                                    logger.error(f"外键约束错误，可能是被引用的表 {table.name} 还未创建")
                                    logger.error(f"请检查表创建顺序，确保被引用的表先创建")
                                    # 记录所有已创建的表
                                    logger.error(f"已创建的表: {', '.join(created_tables)}")
                                
                                # 如果错误信息包含 "missing FROM-clause"，记录完整的 SQL 以便调试
                                if "missing FROM-clause" in error_msg.lower() or "FROM-clause entry" in error_msg.lower():
                                    logger.error(f"生成的 SQL（可能有语法错误）:\n{create_sql if 'create_sql' in locals() else 'N/A'}")
                                    logger.error(f"列定义详情:")
                                    for i, col in enumerate(columns):
                                        logger.error(f"  列 {i+1}: {col}")
                                
                                # 记录生成的 SQL 以便调试
                                if 'create_sql' in locals():
                                    logger.error(f"生成的 SQL:\n{create_sql}")
                                
                                raise
                        else:
                            logger.error(f"无法为表 {table.name} 生成 SQL 定义，跳过（这可能导致功能异常）")
                            logger.error(f"表 {table.name} 的元数据: {table}")
                            existing_tables.append(table.name)
                    else:
                        # 表已存在，检查是否需要添加唯一约束（特别是对于外键引用的字段）
                        if table.name == 'tape_cartridges':
                            logger.info(f"表 {table.name} 已存在，检查 tape_id 字段的唯一约束...")
                            try:
                                # 检查 tape_id 字段是否有唯一约束
                                cur.execute("""
                                    SELECT COUNT(*) 
                                    FROM information_schema.table_constraints tc
                                    JOIN information_schema.constraint_column_usage ccu 
                                        ON tc.constraint_name = ccu.constraint_name
                                    WHERE tc.table_name = 'tape_cartridges' 
                                        AND ccu.column_name = 'tape_id'
                                        AND tc.constraint_type = 'UNIQUE'
                                """)
                                unique_count = cur.fetchone()[0]
                                if unique_count == 0:
                                    logger.warning(f"表 {table.name} 的 tape_id 字段缺少唯一约束，正在添加...")
                                    try:
                                        # 添加唯一约束
                                        cur.execute("""
                                            ALTER TABLE tape_cartridges 
                                            ADD CONSTRAINT tape_cartridges_tape_id_unique UNIQUE (tape_id)
                                        """)
                                        conn.commit()
                                        logger.info(f"✅ 成功为表 {table.name} 的 tape_id 字段添加唯一约束")
                                    except Exception as add_unique_err:
                                        error_msg = str(add_unique_err)
                                        if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
                                            logger.info(f"表 {table.name} 的 tape_id 字段唯一约束已存在（可能名称不同）")
                                        else:
                                            logger.warning(f"为表 {table.name} 添加唯一约束失败: {error_msg}")
                                else:
                                    logger.debug(f"表 {table.name} 的 tape_id 字段已有唯一约束")
                            except Exception as check_err:
                                logger.warning(f"检查表 {table.name} 的唯一约束时出错: {check_err}")
                        existing_tables.append(table.name)
                
                # 汇总输出，减少日志刷屏
                if created_tables:
                    logger.info(f"创建了 {len(created_tables)} 个新表: {', '.join(created_tables[:5])}{'...' if len(created_tables) > 5 else ''}")
                if existing_tables:
                    logger.debug(f"跳过 {len(existing_tables)} 个已存在的表")

                # ========= 多表方案：为 backup_files 创建模板表和分组元数据表 =========
                try:
                    # 1. 创建 backup_files_template（如果不存在），结构与模型定义的 backup_files 相同
                    logger.info("检查并创建 backup_files_template（多表方案模板表）...")
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = 'backup_files_template'
                        """
                    )
                    exists_template = cur.fetchone()
                    if not exists_template:
                        # 直接从模型定义提取 backup_files 的列定义，生成 backup_files_template 表
                        columns = get_table_definition_from_model("backup_files")
                        if columns:
                            logger.info("根据模型定义创建表 backup_files_template（不再依赖基础表 backup_files）")
                            create_template_sql = generate_create_table_sql("backup_files_template", columns)
                            cur.execute(create_template_sql)
                            conn.commit()
                        else:
                            logger.warning("无法从模型定义提取 backup_files 结构，跳过创建 backup_files_template")

                    # 2. 创建 backup_files_groups 元数据表（记录每个任务对应的物理文件表）
                    logger.info("检查并创建 backup_files_groups（多表方案元数据表）...")
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = 'backup_files_groups'
                        """
                    )
                    exists_groups = cur.fetchone()
                    if not exists_groups:
                        cur.execute(
                            """
                            CREATE TABLE backup_files_groups (
                                id BIGSERIAL PRIMARY KEY,
                                table_name TEXT NOT NULL,
                                task_id BIGINT NOT NULL,
                                created_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                        conn.commit()

                    # 3. 为 backup_tasks 增加 backup_files_group_id / backup_files_table 字段（如果不存在）
                    logger.info("检查并为 backup_tasks 添加多表方案相关字段...")
                    # backup_files_group_id
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'backup_tasks' AND column_name = 'backup_files_group_id'
                        """
                    )
                    has_group_id = cur.fetchone()
                    if not has_group_id:
                        cur.execute(
                            """
                            ALTER TABLE backup_tasks
                            ADD COLUMN backup_files_group_id BIGINT
                            """
                        )
                        conn.commit()
                    # backup_files_table
                    cur.execute(
                        """
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'backup_tasks' AND column_name = 'backup_files_table'
                        """
                    )
                    has_table_name = cur.fetchone()
                    if not has_table_name:
                        cur.execute(
                            """
                            ALTER TABLE backup_tasks
                            ADD COLUMN backup_files_table TEXT
                            """
                        )
                        conn.commit()

                except Exception as multi_err:
                    # 多表方案相关结构创建失败时，仅记录警告，不阻止主流程
                    logger.warning(f"创建多表方案相关结构时出错（backup_files_template / backup_files_groups 等）: {multi_err}", exc_info=True)

                # ========= 磁带归档内容缓存表 =========
                try:
                    logger.info("检查并创建 tape_archive_contents（磁带归档内容缓存表）...")
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'tape_archive_contents'
                    """)
                    if not cur.fetchone():
                        cur.execute("""
                            CREATE TABLE tape_archive_contents (
                                id BIGSERIAL PRIMARY KEY,
                                tape_label VARCHAR(64),
                                set_id VARCHAR(128) NOT NULL,
                                archive_filename VARCHAR(256) NOT NULL,
                                archive_size BIGINT DEFAULT 0,
                                file_path TEXT NOT NULL,
                                file_size BIGINT DEFAULT 0,
                                is_dir BOOLEAN DEFAULT FALSE,
                                modified_time TIMESTAMPTZ,
                                created_at TIMESTAMPTZ DEFAULT NOW()
                            )
                        """)
                        conn.commit()
                        logger.info("✅ 创建表 tape_archive_contents 成功")
                    else:
                        logger.debug("表 tape_archive_contents 已存在")

                    # 索引（幂等）
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tape_arc_cache
                        ON tape_archive_contents(tape_label, set_id, archive_filename)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_tape_arc_cache_search
                        ON tape_archive_contents(tape_label, set_id, file_path)
                    """)
                    conn.commit()
                except Exception as cache_err:
                    logger.warning(f"创建磁带归档内容缓存表时出错: {cache_err}", exc_info=True)

                # 检查并添加缺失的字段（字段迁移）
                self._migrate_missing_columns(cur)

                # 关键修复：无论表是新创建还是已存在，都强制检查并修改字段类型为 TEXT
                # 在提交前执行，确保在同一事务中完成
                logger.info("========== 开始检查 backup_files 表字段类型 ==========")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_files'
                    """)
                    if cur.fetchone():
                        # 表已存在，强制检查并修改字段类型为 TEXT
                        logger.info("backup_files 表已存在，强制检查并修改字段类型为 TEXT...")
                        logger.info("确保路径、文件名、展示名称字段为 TEXT 类型（无长度限制）...")
                        text_fields = ['file_name', 'file_path', 'directory_path', 'display_name']
                        modified_fields = []
                        skipped_fields = []
                        error_fields = []
                        
                        for field_name in text_fields:
                            try:
                                # 检查字段当前类型
                                cur.execute("""
                                    SELECT data_type, character_maximum_length
                                    FROM information_schema.columns 
                                    WHERE table_name = 'backup_files' AND column_name = %s
                                """, (field_name,))
                                result = cur.fetchone()
                                if not result:
                                    logger.warning(f"字段 backup_files.{field_name} 不存在，跳过")
                                    continue
                                
                                current_type, max_length = result
                                if current_type == 'character varying':
                                    # 字段是 VARCHAR，强制改为 TEXT
                                    logger.info(f"将 backup_files.{field_name} 从 VARCHAR({max_length}) 改为 TEXT...")
                                    try:
                                        cur.execute(f"ALTER TABLE backup_files ALTER COLUMN {field_name} TYPE TEXT USING {field_name}::TEXT")
                                        # 再次验证修改是否成功
                                        cur.execute("""
                                            SELECT data_type FROM information_schema.columns 
                                            WHERE table_name = 'backup_files' AND column_name = %s
                                        """, (field_name,))
                                        verify_result = cur.fetchone()
                                        if verify_result and verify_result[0] == 'text':
                                            modified_fields.append(field_name)
                                            logger.info(f"✅ 成功将 backup_files.{field_name} 改为 TEXT 类型（已验证）")
                                        else:
                                            error_fields.append(f"{field_name} (修改后验证失败)")
                                            logger.error(f"❌ backup_files.{field_name} 修改后验证失败，当前类型: {verify_result[0] if verify_result else 'unknown'}")
                                    except Exception as alter_err:
                                        error_fields.append(f"{field_name} ({str(alter_err)})")
                                        logger.error(f"❌ 修改 backup_files.{field_name} 失败: {str(alter_err)}", exc_info=True)
                                elif current_type == 'text':
                                    skipped_fields.append(field_name)
                                    logger.debug(f"backup_files.{field_name} 已是 TEXT 类型，无需修改")
                                else:
                                    logger.warning(f"backup_files.{field_name} 类型为 {current_type}，不是 VARCHAR 或 TEXT")
                            except Exception as check_err:
                                error_fields.append(f"{field_name} (检查失败: {str(check_err)})")
                                logger.error(f"❌ 检查 backup_files.{field_name} 失败: {str(check_err)}", exc_info=True)
                    
                        # 汇总输出
                        if modified_fields:
                            logger.info(f"========== ✅ 成功修改了 {len(modified_fields)} 个字段为 TEXT: {', '.join(modified_fields)} ==========")
                        if skipped_fields:
                            logger.info(f"跳过 {len(skipped_fields)} 个已是 TEXT 类型的字段: {', '.join(skipped_fields)}")
                        if error_fields:
                            logger.error(f"========== ❌ {len(error_fields)} 个字段修改失败: ==========")
                            for err_field in error_fields:
                                logger.error(f"   - {err_field}")
                            logger.error(f"这会导致长文件名/路径无法同步到数据库！")
                        else:
                            logger.info("========== 字段类型检查完成，所有字段都是 TEXT 类型 ==========")
                    else:
                        logger.info("backup_files 表不存在，将在创建时自动设置为 TEXT 类型")
                except Exception as text_check_err:
                    logger.error(f"========== 字段类型检查和修改过程发生异常 ==========")
                    logger.error(f"错误信息: {str(text_check_err)}", exc_info=True)
                    logger.error(f"这可能导致长文件名/路径无法同步到数据库！")
                    # 不抛出异常，避免影响表创建流程，但记录详细错误信息
                
                # 检查并修复 backup_files 表的 id 字段（确保是 SERIAL 或使用序列）
                logger.info("========== 开始检查 backup_files 表 id 字段 ==========")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_files'
                    """)
                    if cur.fetchone():
                        # 检查 id 字段类型和默认值
                        cur.execute("""
                            SELECT data_type, column_default, is_nullable
                            FROM information_schema.columns 
                            WHERE table_name = 'backup_files' AND column_name = 'id'
                        """)
                        result = cur.fetchone()
                        if result:
                            data_type, column_default, is_nullable = result
                            # 检查是否是 SERIAL 类型（SERIAL 类型在 information_schema 中显示为 integer，但默认值是序列）
                            if column_default and 'nextval' in str(column_default).lower():
                                logger.debug("backup_files.id 字段已使用序列，无需修复")
                            elif data_type == 'integer' and is_nullable == 'NO':
                                # id 字段是 INTEGER NOT NULL 但没有序列，需要创建序列并设置默认值
                                logger.info("backup_files.id 字段是 INTEGER NOT NULL 但没有序列，正在修复...")
                                try:
                                    # 创建序列
                                    cur.execute("""
                                        CREATE SEQUENCE IF NOT EXISTS backup_files_id_seq
                                    """)
                                    # 设置 id 字段的默认值为序列的 nextval
                                    cur.execute("""
                                        ALTER TABLE backup_files 
                                        ALTER COLUMN id SET DEFAULT nextval('backup_files_id_seq')
                                    """)
                                    # 设置序列的所有者为表
                                    cur.execute("""
                                        ALTER SEQUENCE backup_files_id_seq OWNED BY backup_files.id
                                    """)
                                    # 设置序列的当前值为表中最大 id + 1
                                    cur.execute("""
                                        SELECT setval('backup_files_id_seq', COALESCE((SELECT MAX(id) FROM backup_files), 0) + 1, false)
                                    """)
                                    logger.info("✅ 成功为 backup_files.id 字段创建序列并设置默认值")
                                except Exception as seq_err:
                                    logger.error(f"❌ 修复 backup_files.id 字段失败: {str(seq_err)}", exc_info=True)
                            else:
                                logger.debug(f"backup_files.id 字段类型: {data_type}, 默认值: {column_default}, 可为空: {is_nullable}")
                        else:
                            logger.warning("backup_files.id 字段不存在，这不应该发生")
                except Exception as id_check_err:
                    logger.error(f"检查 backup_files.id 字段失败: {str(id_check_err)}", exc_info=True)
                
                # 检查并修改 backup_tasks 表的 total_bytes 字段类型为 NUMERIC（避免 int64 溢出）
                logger.info("========== 开始检查 backup_tasks 表 total_bytes 字段类型 ==========")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_tasks'
                    """)
                    if cur.fetchone():
                        # 检查 total_bytes 字段类型
                        cur.execute("""
                            SELECT data_type FROM information_schema.columns 
                            WHERE table_name = 'backup_tasks' AND column_name = 'total_bytes'
                        """)
                        result = cur.fetchone()
                        if result:
                            current_type = result[0]
                            if current_type in ('bigint', 'integer'):
                                # 字段是 BIGINT 或 INTEGER，需要改为 NUMERIC
                                logger.info(f"将 backup_tasks.total_bytes 从 {current_type.upper()} 改为 NUMERIC...")
                                try:
                                    cur.execute("""
                                        ALTER TABLE backup_tasks 
                                        ALTER COLUMN total_bytes TYPE NUMERIC USING total_bytes::NUMERIC
                                    """)
                                    # 验证修改是否成功
                                    cur.execute("""
                                        SELECT data_type FROM information_schema.columns 
                                        WHERE table_name = 'backup_tasks' AND column_name = 'total_bytes'
                                    """)
                                    verify_result = cur.fetchone()
                                    if verify_result and verify_result[0] == 'numeric':
                                        logger.info("✅ 成功将 backup_tasks.total_bytes 改为 NUMERIC 类型（已验证）")
                                    else:
                                        logger.error(f"❌ backup_tasks.total_bytes 修改后验证失败，当前类型: {verify_result[0] if verify_result else 'unknown'}")
                                except Exception as alter_err:
                                    logger.error(f"❌ 修改 backup_tasks.total_bytes 失败: {str(alter_err)}", exc_info=True)
                            elif current_type == 'numeric':
                                logger.debug("backup_tasks.total_bytes 已是 NUMERIC 类型，无需修改")
                            else:
                                logger.warning(f"backup_tasks.total_bytes 类型为 {current_type}，不是 BIGINT 或 NUMERIC")
                        else:
                            logger.warning("backup_tasks.total_bytes 字段不存在，跳过类型检查")
                    else:
                        logger.info("backup_tasks 表不存在，将在创建时自动设置 total_bytes 为 NUMERIC 类型")
                except Exception as total_bytes_check_err:
                    logger.error(f"========== total_bytes 字段类型检查过程发生异常 ==========")
                    logger.error(f"错误信息: {str(total_bytes_check_err)}", exc_info=True)
                    # 不抛出异常，避免影响表创建流程
                
                # 创建索引（优化查询性能）- 必须在 with 块内，在 cursor 关闭之前
                self._create_indexes_for_backup_files(cur)
                # 关键修复：在创建索引后立即提交（openGauss模式下需要显式提交）
                conn.commit()
                logger.debug("索引创建已提交")
            
            # 最终提交（确保所有修改都已提交）
            # 注意：由于上面已经在每个关键步骤后提交，这里主要是确保一致性
            logger.info(f"使用{'psycopg3' if use_psycopg3 else 'psycopg2'}成功创建数据库表、字段迁移、字段类型检查和索引创建完成")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"数据库初始化失败: {str(e)}", exc_info=True)
            raise
        finally:
            # 关闭连接（表创建和字段类型检查已完成）
            conn.close()
    
    def _migrate_missing_columns(self, cur):
        """检查并添加缺失的字段（字段迁移）
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            from models.backup import BackupTask, BackupSet

            # 定义需要迁移的字段（表名 -> [(字段名, SQL类型, 默认值), ...]）
            migrations = {
                'backup_tasks': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('scan_status', 'VARCHAR(50)', "'pending'", '扫描状态'),
                    ('scan_completed_at', 'TIMESTAMPTZ', 'NULL', '扫描完成时间'),
                    ('operation_stage', 'VARCHAR(50)', 'NULL', '操作阶段（scan/compress/copy/finalize）'),
                ],
                'backup_sets': [
                    ('compressed_bytes', 'BIGINT', '0', '压缩后字节数'),
                    ('compression_ratio', 'REAL', 'NULL', '压缩比'),
                ],
                'backup_files': [
                    ('directory_path', 'VARCHAR(1000)', 'NULL', '目录路径'),
                    ('display_name', 'VARCHAR(255)', 'NULL', '展示名称'),
                    ('is_copy_success', 'BOOLEAN', 'FALSE', '是否复制成功'),
                    ('copy_status_at', 'TIMESTAMPTZ', 'NULL', '复制状态更新时间'),
                ],
            }
            
            added_columns = []
            existing_columns = []
            
            for table_name, columns in migrations.items():
                # 检查表是否存在
                cur.execute("""
                    SELECT 1 FROM information_schema.tables WHERE table_name = %s
                """, (table_name,))
                if not cur.fetchone():
                    # 表不存在，跳过
                    continue
                
                # 获取表中现有的所有列名
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s
                """, (table_name,))
                existing_cols = {row[0] for row in cur.fetchall()}
                
                # 检查每个需要迁移的字段
                for col_name, col_type, default_value, comment in columns:
                    if col_name not in existing_cols:
                        # 字段不存在，需要添加
                        default_clause = f"DEFAULT {default_value}" if default_value != 'NULL' else ""
                        comment_clause = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'" if comment else ""
                        
                        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {default_clause}"
                        cur.execute(alter_sql)
                        added_columns.append(f"{table_name}.{col_name}")
                        
                        # 添加注释（如果提供）
                        if comment_clause:
                            try:
                                cur.execute(comment_clause)
                            except Exception as comment_err:
                                # 注释添加失败不影响主流程
                                logger.debug(f"添加字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                    else:
                        existing_columns.append(f"{table_name}.{col_name}")
            
            # 汇总输出
            if added_columns:
                logger.info(f"添加了 {len(added_columns)} 个缺失字段: {', '.join(added_columns)}")
            if existing_columns:
                logger.debug(f"跳过 {len(existing_columns)} 个已存在的字段")
                
        except Exception as e:
            logger.warning(f"字段迁移检查失败: {str(e)}，但不影响表创建流程")
            # 不抛出异常，避免影响主流程
    
    def _create_indexes_for_backup_files(self, cur):
        """为 backup_files 表创建索引（优化查询性能）
        
        索引说明：
        1. idx_backup_files_set_path: 优化 WHERE backup_set_id = ? AND file_path = ANY(?)
           用于验证查询和 mark_files_as_copied 查询
        
        2. idx_backup_files_set_copy_status: 优化 WHERE backup_set_id = ? AND is_copy_success = FALSE
           用于 fetch_pending_files_grouped_by_size 查询
        
        3. idx_backup_files_set_copy_type_id: 复合索引，优化待压缩文件查询
           用于 fetch_pending_files_grouped_by_size 的分批查询和排序
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            # 检查 backup_files 表是否存在
            cur.execute("""
                SELECT 1 FROM information_schema.tables WHERE table_name = 'backup_files'
            """)
            if not cur.fetchone():
                logger.debug("backup_files 表不存在，跳过索引创建")
                return
            
            # 定义需要创建的索引
            indexes = [
                {
                    'name': 'idx_backup_files_set_path',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_path 
                        ON backup_files(backup_set_id, file_path)
                    """,
                    'description': '优化验证查询和 mark_files_as_copied 查询（backup_set_id + file_path）'
                },
                {
                    'name': 'idx_backup_files_set_copy_status',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_copy_status 
                        ON backup_files(backup_set_id, is_copy_success)
                        WHERE is_copy_success = FALSE OR is_copy_success IS NULL
                    """,
                    'description': '部分索引：优化待压缩文件查询（只索引未压缩文件，节省空间）'
                },
                {
                    'name': 'idx_backup_files_set_copy_type_id',
                    'sql': """
                        CREATE INDEX IF NOT EXISTS idx_backup_files_set_copy_type_id 
                        ON backup_files(backup_set_id, is_copy_success, file_type, id)
                        WHERE (is_copy_success = FALSE OR is_copy_success IS NULL) AND file_type = 'file'::backupfiletype
                    """,
                    'description': '部分复合索引：优化 fetch_pending_files_grouped_by_size 的分批查询和排序'
                }
            ]
            
            created_indexes = []
            existing_indexes = []
            error_indexes = []
            
            for index_def in indexes:
                try:
                    # 检查索引是否已存在
                    cur.execute("""
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'backup_files' AND indexname = %s
                    """, (index_def['name'],))
                    if cur.fetchone():
                        existing_indexes.append(index_def['name'])
                        logger.debug(f"索引 {index_def['name']} 已存在，跳过")
                        continue
                    
                    # 创建索引
                    cur.execute(index_def['sql'])
                    created_indexes.append(index_def['name'])
                    logger.info(f"✅ 创建索引 {index_def['name']}: {index_def['description']}")
                    
                except Exception as index_err:
                    error_indexes.append(f"{index_def['name']} ({str(index_err)})")
                    logger.warning(f"创建索引 {index_def['name']} 失败: {str(index_err)}")
                    # 继续创建其他索引，不中断流程
            
            # 汇总输出
            if created_indexes:
                logger.info(f"========== 成功创建 {len(created_indexes)} 个索引: {', '.join(created_indexes)} ==========")
            if existing_indexes:
                logger.debug(f"跳过 {len(existing_indexes)} 个已存在的索引: {', '.join(existing_indexes)}")
            if error_indexes:
                logger.warning(f"========== {len(error_indexes)} 个索引创建失败: ==========")
                for err_index in error_indexes:
                    logger.warning(f"   - {err_index}")
                logger.warning("这可能会影响查询性能，但不会影响功能")
            else:
                logger.info("========== 索引创建完成，查询性能已优化 ==========")
                
        except Exception as e:
            logger.warning(f"索引创建过程发生异常: {str(e)}，但不影响表创建流程", exc_info=True)
            # 不抛出异常，避免影响主流程
    
    def _migrate_column_lengths(self, cur):
        """修改现有字段的类型（字段类型迁移）- openGauss
        将路径、文件名相关字段从 VARCHAR 改为 TEXT 类型（无长度限制）
        
        Args:
            cur: psycopg2 cursor对象
        """
        try:
            # 定义需要修改类型的字段（表名 -> [(字段名, 注释), ...]）
            text_migrations = {
                'backup_files': [
                    ('file_name', '文件名'),
                    ('file_path', '文件路径'),
                    ('directory_path', '目录路径'),
                ],
            }
            
            modified_columns = []
            skipped_columns = []
            error_columns = []
            
            logger.info("========== 开始检查字段类型迁移（VARCHAR -> TEXT）==========")
            logger.info("注意：如果数据库字段仍然是 VARCHAR(255)，需要执行此迁移将字段改为 TEXT 类型")
            
            for table_name, columns in text_migrations.items():
                # 检查表是否存在
                cur.execute("""
                    SELECT 1 FROM information_schema.tables WHERE table_name = %s
                """, (table_name,))
                if not cur.fetchone():
                    # 表不存在，跳过
                    logger.debug(f"表 {table_name} 不存在，跳过字段类型迁移")
                    continue
                
                # 获取表中现有字段的类型信息
                cur.execute("""
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns 
                    WHERE table_name = %s AND (data_type = 'character varying' OR data_type = 'text')
                """, (table_name,))
                existing_cols = {}
                for row in cur.fetchall():
                    col_name, data_type, max_length = row
                    existing_cols[col_name] = {'type': data_type, 'length': max_length}
                
                # 检查每个需要修改类型的字段
                for col_name, comment in columns:
                    if col_name not in existing_cols:
                        # 字段不存在，跳过
                        logger.debug(f"字段 {table_name}.{col_name} 不存在，跳过")
                        continue
                    
                    current_type = existing_cols[col_name]['type']
                    current_length = existing_cols[col_name]['length']
                    
                    # 如果已经是 TEXT 类型，跳过
                    if current_type == 'text':
                        skipped_columns.append(f"{table_name}.{col_name} (已是 TEXT 类型)")
                        logger.debug(f"字段 {table_name}.{col_name} 已是 TEXT 类型，跳过")
                        continue
                    
                    # 如果是 VARCHAR 类型，改为 TEXT
                    if current_type == 'character varying':
                        try:
                            logger.info(f"正在修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            # 使用 USING 子句确保数据转换成功
                            alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE TEXT USING {col_name}::TEXT"
                            cur.execute(alter_sql)
                            modified_columns.append(f"{table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            logger.info(f"✅ 成功修改字段类型: {table_name}.{col_name} (VARCHAR({current_length}) -> TEXT)")
                            
                            # 更新注释（如果提供）
                            if comment:
                                try:
                                    comment_sql = f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment}'"
                                    cur.execute(comment_sql)
                                except Exception as comment_err:
                                    logger.debug(f"更新字段注释失败 {table_name}.{col_name}: {str(comment_err)}")
                        except Exception as alter_err:
                            error_msg = f"{table_name}.{col_name}: {str(alter_err)}"
                            error_columns.append(error_msg)
                            logger.error(f"❌ 修改字段类型失败: {error_msg}")
            
            # 汇总输出
            if modified_columns:
                logger.info(f"✅ 成功修改了 {len(modified_columns)} 个字段类型: {', '.join(modified_columns)}")
            if skipped_columns:
                logger.info(f"跳过 {len(skipped_columns)} 个已是 TEXT 类型的字段（无需迁移）")
            if error_columns:
                logger.error(f"❌ {len(error_columns)} 个字段类型修改失败:")
                for error_col in error_columns:
                    logger.error(f"   - {error_col}")
                logger.error(f"这些字段可能仍然是 VARCHAR(255)，长文件名/路径可能无法同步")
                
            # 如果没有任何字段被修改，且表存在，记录信息
            if not modified_columns and not skipped_columns and not error_columns:
                logger.info("未找到需要迁移的字段（表可能不存在或字段已迁移）")
                
        except Exception as e:
            logger.error(f"========== 字段类型迁移检查失败 ==========")
            logger.error(f"错误信息: {str(e)}", exc_info=True)
            logger.error(f"这可能导致长文件名/路径无法同步到数据库")
            # 不抛出异常，避免影响主流程，但记录详细错误信息
    
    async def close(self):
        """关闭数据库连接"""
        logger.info("数据库连接已关闭")

    async def health_check(self) -> bool:
        """数据库健康检查"""
        try:
            import asyncpg
            import re

            database_url = self.settings.DATABASE_URL
            if database_url.startswith("opengauss://"):
                database_url = database_url.replace("opengauss://", "postgresql://", 1)
            pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
            match = re.match(pattern, database_url)
            if not match:
                return False
            username, password, host, port, database = match.groups()
            conn = await asyncpg.connect(
                host=host,
                port=int(port),
                user=username,
                password=password,
                database=database,
                timeout=5
            )
            try:
                await conn.execute("SELECT 1")
            finally:
                await conn.close()
            return True
        except Exception as e:
            logger.error(f"数据库健康检查失败: {str(e)}")
            return False


# 全局数据库管理器实例
db_manager = DatabaseManager()