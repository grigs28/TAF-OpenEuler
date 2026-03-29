#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库工具函数
Database Utility Functions
"""

import asyncio
import logging
from typing import Optional, Tuple
from contextlib import asynccontextmanager
from config.database import db_manager
from utils.opengauss.guard import get_opengauss_monitor

logger = logging.getLogger(__name__)

# 全局连接池
_opengauss_pool: Optional[object] = None
_pool_lock = asyncio.Lock()


def is_opengauss() -> bool:
    """检查当前数据库是否为openGauss"""
    if hasattr(db_manager, "is_opengauss_database"):
        try:
            return db_manager.is_opengauss_database()
        except Exception:
            pass
    database_url = db_manager.settings.DATABASE_URL
    return "opengauss" in str(database_url).lower()


def is_redis() -> bool:
    """检查当前数据库是否为Redis"""
    database_url = db_manager.settings.DATABASE_URL
    db_flavor = getattr(db_manager.settings, 'DB_FLAVOR', None)
    # 优先使用DB_FLAVOR配置
    if db_flavor and db_flavor.lower() == "redis":
        return True
    # 从DATABASE_URL判断
    url_lower = str(database_url).lower()
    return url_lower.startswith("redis://") or url_lower.startswith("rediss://")


def is_sqlite() -> bool:
    """检查当前数据库是否为SQLite"""
    database_url = db_manager.settings.DATABASE_URL
    db_flavor = getattr(db_manager.settings, 'DB_FLAVOR', None)
    # 优先使用DB_FLAVOR配置
    if db_flavor and db_flavor.lower() == "sqlite":
        return True
    # 从DATABASE_URL判断
    url_lower = str(database_url).lower()
    return url_lower.startswith("sqlite://") or url_lower.startswith("sqlite+aiosqlite://")


@asynccontextmanager
async def get_sqlite_connection():
    """获取 SQLite 数据库连接（上下文管理器）

    用于 SQLite 模式下的数据库操作
    """
    import aiosqlite
    database_url = db_manager.settings.DATABASE_URL
    # 提取 SQLite 文件路径
    if database_url.startswith("sqlite:///"):
        db_path = database_url[len("sqlite:///"):]
    elif database_url.startswith("sqlite+aiosqlite:///"):
        db_path = database_url[len("sqlite+aiosqlite:///"):]
    else:
        raise ValueError(f"无效的 SQLite 数据库 URL: {database_url}")

    conn = await aiosqlite.connect(db_path)
    # 启用外键约束
    await conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        await conn.close()


async def get_backup_files_table_by_set_id(conn, backup_set_id: int) -> str:
    """根据 backup_set_id 获取对应的 backup_files 物理表名（多表方案）

    - 优先从 backup_tasks.backup_files_table 读取
    - 表名必须以 'backup_files_' 开头，否则回退为主表 'backup_files'
    - 仅在 openGauss 模式下使用
    """
    table_name = "backup_files"
    try:
        row = await conn.fetchrow(
            """
            SELECT bt.backup_files_table
            FROM backup_sets bs
            JOIN backup_tasks bt ON bs.backup_task_id = bt.id
            WHERE bs.id = $1
            """,
            backup_set_id,
        )
        if row and row.get("backup_files_table"):
            candidate = row["backup_files_table"]
            if isinstance(candidate, str) and candidate.startswith("backup_files_"):
                table_name = candidate
    except Exception as e:
        logger.warning(f"[多表方案] 根据 backup_set_id={backup_set_id} 获取 backup_files 表名失败，回退到主表 backup_files: {e}")
    return table_name


async def _create_opengauss_pool():
    """创建openGauss连接池（优先使用 psycopg3，修复 BufferError）"""
    import re
    
    # 尝试使用 psycopg3，如果失败则回退到 asyncpg
    # 先导入 asyncpg 作为备用（即使可能不使用）
    import asyncpg
    
    use_psycopg3 = True
    try:
        from psycopg_pool import AsyncConnectionPool
        from psycopg import AsyncConnection
        logger.info("使用 psycopg3 创建连接池（修复 BufferError）")
    except ImportError:
        use_psycopg3 = False
        logger.warning("psycopg3 未安装，使用 asyncpg。建议安装: pip install 'psycopg[binary,pool]>=3.1.8'")
    
    database_url = db_manager.settings.DATABASE_URL
    url = database_url.replace("opengauss://", "postgresql://")
    pattern = r'postgresql://([^:]+):([^@]+)@([^:]+):(\d+)/(.+)'
    match = re.match(pattern, url)
    if not match:
        raise ValueError("无法解析openGauss数据库URL")
    
    username, password, host, port, database = match.groups()
    
    # 从配置获取连接池参数
    pool_size = getattr(db_manager.settings, 'DB_POOL_SIZE', 10)
    max_overflow = getattr(db_manager.settings, 'DB_MAX_OVERFLOW', 20)
    pool_timeout = getattr(db_manager.settings, 'DB_POOL_TIMEOUT', 30.0)
    command_timeout = getattr(db_manager.settings, 'DB_COMMAND_TIMEOUT', 60.0)
    max_inactive_lifetime = getattr(db_manager.settings, 'DB_MAX_INACTIVE_CONNECTION_LIFETIME', 600.0)
    query_dop = getattr(db_manager.settings, 'DB_QUERY_DOP', 16)  # openGauss 查询并行度
    min_size = max(1, pool_size // 2)  # 最小连接数
    max_size = pool_size + max_overflow  # 最大连接数
    
    monitor = get_opengauss_monitor()

    if use_psycopg3:
        # 使用 psycopg3（修复 BufferError，使用 binary protocol）
        # 连接配置函数：设置 query_dop
        async def configure_connection(conn: AsyncConnection):
            """连接配置函数：设置 openGauss 查询并行度
            
            注意：psycopg3 的连接池要求配置函数执行后连接处于空闲状态（不在事务中）
            因此执行 SET 语句后需要确保连接不在事务中
            """
            try:
                # 执行 SET 语句（会话级别，不会开启事务）
                async with conn.cursor() as cur:
                    await cur.execute(f"SET query_dop = {query_dop};")
                
                # 确保连接不在事务中（psycopg3 连接池要求）
                # 即使 SET 语句不会开启事务，也显式提交以确保连接状态正确
                try:
                    await conn.commit()
                except Exception:
                    # 如果不在事务中，commit() 可能会失败，尝试回滚
                    try:
                        await conn.rollback()
                    except:
                        pass
                
                logger.debug(f"已设置 openGauss 查询并行度: query_dop = {query_dop}")
            except Exception as e:
                # 如果出错，确保连接不在事务中
                try:
                    await conn.rollback()
                except:
                    pass
                logger.warning(f"设置 query_dop 失败（可能不是 openGauss 数据库）: {str(e)}")
        
        # 连接重置函数：在连接返回池时清理事务状态
        async def reset_connection(conn: AsyncConnection):
            """连接重置函数：在连接返回池时清理事务状态"""
            try:
                # 检查事务状态：0=IDLE（不在事务中），1=INTRANS（在事务中），3=INERROR（错误状态）
                transaction_status = conn.info.transaction_status
                
                # 记录重置开始（用于调试）- 使用 DEBUG 级别，避免在 INFO 级别时显示
                logger.debug(f"[连接池重置] 开始重置连接，事务状态: {transaction_status} (0=IDLE, 1=INTRANS, 3=INERROR)")
                
                if transaction_status == 1:  # INTRANS: 在事务中但未提交
                    # 如果还在事务中，尝试提交而不是回滚（避免数据丢失）
                    logger.warning(f"[连接池重置] ⚠️ 检测到未提交的事务（INTRANS），尝试提交以避免数据丢失")
                    try:
                        # 尝试提交事务
                        await conn.commit()
                        
                        # 等待一小段时间让状态更新
                        import asyncio
                        await asyncio.sleep(0.01)  # 等待10ms让状态更新
                        
                        # 再次检查事务状态
                        new_status = conn.info.transaction_status
                        if new_status == 0:
                            logger.info("[连接池重置] ✅ 事务已成功提交，连接状态=IDLE")
                        else:
                            logger.error(f"[连接池重置] ❌ 提交后事务状态仍为 {new_status}，执行回滚")
                            try:
                                await conn.rollback()
                                logger.debug("[连接池重置] 事务已回滚")
                            except Exception as rollback_err:
                                logger.warning(f"[连接池重置] 回滚失败: {str(rollback_err)}")
                    except Exception as commit_err:
                        logger.error(f"[连接池重置] ❌ 提交失败: {str(commit_err)}，执行回滚", exc_info=True)
                        try:
                            await conn.rollback()
                            logger.debug("[连接池重置] 事务已回滚")
                        except Exception as rollback_err:
                            logger.warning(f"[连接池重置] 回滚失败: {str(rollback_err)}")
                elif transaction_status == 3:  # INERROR: 错误状态
                    # 如果处于错误状态，回滚以清理状态
                    logger.debug("[连接池重置] 检测到错误状态，执行回滚")
                    try:
                        await conn.rollback()
                    except Exception:
                        pass
                elif transaction_status == 0:  # IDLE: 不在事务中
                    # 连接状态正常，但为了确保 psycopg3 内部的 _reset_connection 不会检测到 INTRANS
                    # 我们显式回滚以确保连接处于完全干净的状态（即使状态显示为 IDLE）
                    # 这是因为 psycopg3 内部的 _reset_connection 可能会检测到不同的状态
                    try:
                        # 显式回滚以确保连接处于完全干净的状态
                        await conn.rollback()
                        logger.debug("[连接池重置] 连接状态正常（IDLE），已执行回滚确保干净状态")
                    except Exception as rollback_err:
                        # 如果回滚失败（可能已经不在事务中），这是正常的
                        logger.debug(f"[连接池重置] 回滚失败（可忽略，可能已不在事务中）: {str(rollback_err)}")
                else:
                    logger.warning(f"[连接池重置] 未知的事务状态: {transaction_status}")
            except Exception as e:
                # 重置失败，记录但不中断流程
                logger.warning(f"[连接池重置] 重置连接时出错: {str(e)}", exc_info=True)
        
        try:
            # psycopg3 使用 binary protocol，修复 BufferError
            # 构建连接字符串
            conninfo = f"host={host} port={port} user={username} password={password} dbname={database}"
            pool = AsyncConnectionPool(
                conninfo=conninfo,
                min_size=min_size,
                max_size=max_size,
                open=False,  # 延迟打开
                configure=configure_connection,  # 连接配置回调（连接创建时）
                reset=reset_connection,  # 连接重置回调（连接返回池时）
                max_idle=max_inactive_lifetime,  # 非活跃连接的最大生命周期（秒）
                max_lifetime=max_inactive_lifetime * 2,  # 连接最大生命周期
                reconnect_timeout=pool_timeout,  # 重连超时
            )
            await pool.open()
            logger.info(
                f"openGauss连接池创建成功（psycopg3，binary protocol）: min_size={min_size}, max_size={max_size}, "
                f"timeout={pool_timeout}s, command_timeout={command_timeout}s, "
                f"max_inactive_lifetime={max_inactive_lifetime}s, query_dop={query_dop}"
            )
            if monitor.enabled:
                logger.debug(
                    "openGauss 连接池参数: host=%s port=%s db=%s min=%s max=%s",
                    host,
                    port,
                    database,
                    min_size,
                    max_size,
                )
                monitor.ensure_running()
            return pool
        except Exception as e:
            logger.error(f"创建psycopg3连接池失败: {str(e)}，回退到 asyncpg", exc_info=True)
            use_psycopg3 = False
    
    if not use_psycopg3:
        # 回退到 asyncpg
        # 连接初始化函数：设置 query_dop
        async def init_connection(conn):
            """连接初始化函数：设置 openGauss 查询并行度"""
            try:
                await conn.execute(f"SET query_dop = {query_dop};")
                logger.debug(f"已设置 openGauss 查询并行度: query_dop = {query_dop}")
            except Exception as e:
                logger.warning(f"设置 query_dop 失败（可能不是 openGauss 数据库）: {str(e)}")
        
        try:
            pool = await asyncpg.create_pool(
                host=host,
                port=int(port),
                user=username,
                password=password,
                database=database,
                min_size=min_size,
                max_size=max_size,
                timeout=pool_timeout,
                command_timeout=command_timeout,
                max_queries=50000,
                max_inactive_connection_lifetime=max_inactive_lifetime,
                init=init_connection,
            )
            logger.info(
                f"openGauss连接池创建成功（asyncpg）: min_size={min_size}, max_size={max_size}, "
                f"timeout={pool_timeout}s, command_timeout={command_timeout}s, "
                f"max_inactive_lifetime={max_inactive_lifetime}s, query_dop={query_dop}"
            )
            if monitor.enabled:
                logger.debug(
                    "openGauss 连接池参数: host=%s port=%s db=%s min=%s max=%s",
                    host,
                    port,
                    database,
                    min_size,
                    max_size,
                )
                monitor.ensure_running()
            return pool
        except Exception as e:
            logger.error(f"创建openGauss连接池失败: {str(e)}", exc_info=True)
            raise


async def get_opengauss_pool():
    """获取openGauss连接池（单例模式）"""
    global _opengauss_pool
    
    if _opengauss_pool is None:
        async with _pool_lock:
            # 双重检查
            if _opengauss_pool is None:
                _opengauss_pool = await _create_opengauss_pool()
    
    # 检查连接池是否已关闭（兼容 asyncpg 和 psycopg3）
    # 检查连接池是否已关闭（兼容 asyncpg 和 psycopg3）
    is_closing = False
    if _opengauss_pool:
        if hasattr(_opengauss_pool, 'is_closing'):
            is_closing = _opengauss_pool.is_closing()
        elif hasattr(_opengauss_pool, 'closed'):
            is_closing = _opengauss_pool.closed
        # psycopg3 的 AsyncConnectionPool 没有 is_closing 属性，使用其他方式检查
    
    if is_closing:
        async with _pool_lock:
            if hasattr(_opengauss_pool, 'is_closing') and _opengauss_pool.is_closing():
                _opengauss_pool = await _create_opengauss_pool()
            elif hasattr(_opengauss_pool, 'closed') and _opengauss_pool.closed:
                _opengauss_pool = await _create_opengauss_pool()
    
    return _opengauss_pool


class ConnectionWrapper:
    """连接包装器，用于自动管理连接池连接"""
    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn
        self._released = False
    
    def __getattr__(self, name):
        """代理所有属性访问到实际连接"""
        return getattr(self.conn, name)
    
    async def release(self):
        """释放连接回连接池"""
        if not self._released and self.conn:
            try:
                await self.pool.release(self.conn)
                self._released = True
            except Exception as e:
                logger.warning(f"释放连接失败: {str(e)}")
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        return self.conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口，自动释放连接"""
        await self.release()


async def _acquire_connection():
    """从连接池获取连接（内部函数，兼容 asyncpg 和 psycopg3）"""
    global _opengauss_pool
    
    pool = await get_opengauss_pool()
    monitor = get_opengauss_monitor()
    conn = None
    retry_count = 0
    max_retries = 3
    
    # 从配置获取获取连接的超时时间
    acquire_timeout = getattr(db_manager.settings, 'DB_ACQUIRE_TIMEOUT', 10.0)
    
    # 检测是 psycopg3 还是 asyncpg
    is_psycopg3 = hasattr(pool, 'getconn') or hasattr(pool, 'connection')
    
    while retry_count < max_retries:
        try:
            # 从连接池获取连接
            if is_psycopg3:
                # psycopg3: 使用 getconn() 获取连接（异步）
                # 关键修复：psycopg3 的 getconn() 没有内置超时，需要显式添加超时保护
                acquire_coro = pool.getconn()
                # 使用 asyncio.wait_for 显式添加超时，避免无限等待
                acquire_coro = asyncio.wait_for(acquire_coro, timeout=acquire_timeout)
            else:
                # asyncpg: 使用 acquire()（已有超时参数）
                import asyncpg
                acquire_coro = pool.acquire(timeout=acquire_timeout)
            
            if monitor.enabled:
                monitor.ensure_running()
                # monitor.watch 的超时时间应该大于 acquire_timeout，确保能捕获到超时
                conn = await monitor.watch(
                    acquire_coro,
                    operation="pool.acquire" if not is_psycopg3 else "pool.getconn",
                    timeout=acquire_timeout + 5,  # 增加缓冲时间，确保能捕获超时
                    metadata={"retry": retry_count},
                    critical=True,
                )
            else:
                conn = await acquire_coro
            
            # psycopg3 需要包装成兼容接口
            if is_psycopg3:
                from utils.scheduler.psycopg3_compat import AsyncPGCompatConnection
                conn = AsyncPGCompatConnection(conn, pool=pool)
            
            return pool, conn
        except asyncio.TimeoutError:
            retry_count += 1
            # 记录连接池状态，帮助诊断问题
            pool_status = "未知"
            try:
                if is_psycopg3:
                    # psycopg3 连接池状态
                    if hasattr(pool, '_pool'):
                        pool_obj = pool._pool
                        if hasattr(pool_obj, 'stats'):
                            stats = pool_obj.stats()
                            pool_status = f"psycopg3连接池: 已用={stats.get('used', 'N/A')}, 空闲={stats.get('idle', 'N/A')}, 等待={stats.get('waiting', 'N/A')}"
                        elif hasattr(pool_obj, '_pool'):
                            # 尝试获取内部状态
                            pool_status = f"psycopg3连接池: 状态检查失败（可能连接池已满）"
                    else:
                        pool_status = f"psycopg3连接池: 无法获取状态信息"
                else:
                    # asyncpg 连接池状态
                    if hasattr(pool, 'get_size'):
                        size = pool.get_size()
                        idle = pool.get_idle_size()
                        pool_status = f"asyncpg连接池: 总大小={size}, 空闲={idle}, 已用={size - idle}"
            except Exception as status_err:
                pool_status = f"连接池状态检查失败: {str(status_err)}"
            
            if retry_count >= max_retries:
                logger.error(
                    f"获取数据库连接超时，已重试{retry_count}次。连接池状态: {pool_status}。"
                    f"可能原因：1) 连接池已满（所有连接被占用） 2) 数据库响应慢 3) 网络问题。"
                    f"建议：检查是否有连接泄漏，或增加 DB_POOL_SIZE / DB_MAX_OVERFLOW 配置。"
                )
                raise
            logger.warning(
                f"获取数据库连接超时，重试 {retry_count}/{max_retries}。连接池状态: {pool_status}"
            )
            await asyncio.sleep(0.5 * retry_count)  # 指数退避
        except (ConnectionError, OSError) as e:
            # 连接丢失或网络错误，重置连接池并重试
            retry_count += 1
            error_msg = str(e)
            if retry_count >= max_retries:
                logger.error(f"数据库连接丢失，已重试{retry_count}次: {error_msg}")
                raise
            logger.warning(f"数据库连接丢失，重置连接池并重试 {retry_count}/{max_retries}: {error_msg}")
            async with _pool_lock:
                try:
                    if _opengauss_pool:
                        if hasattr(_opengauss_pool, 'is_closing') and not _opengauss_pool.is_closing():
                            await _opengauss_pool.close()
                        elif hasattr(_opengauss_pool, 'close'):
                            await _opengauss_pool.close()
                except Exception:
                    pass
                _opengauss_pool = None
            await asyncio.sleep(1.0 * retry_count)  # 等待后重试
        except Exception as e:
            # 检查是否是 asyncpg 特定异常
            error_type = type(e).__name__
            error_msg = str(e)
            
            # asyncpg 特定异常
            if "ConnectionDoesNotExistError" in error_type or "connection_lost" in error_msg.lower() or "unexpected connection" in error_msg.lower():
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"数据库连接异常，已重试{retry_count}次: {error_msg}")
                    raise
                logger.warning(f"数据库连接异常，重置连接池并重试 {retry_count}/{max_retries}: {error_msg}")
                async with _pool_lock:
                    try:
                        if _opengauss_pool:
                            if hasattr(_opengauss_pool, 'is_closing') and not _opengauss_pool.is_closing():
                                await _opengauss_pool.close()
                            elif hasattr(_opengauss_pool, 'close'):
                                await _opengauss_pool.close()
                    except Exception:
                        pass
                    _opengauss_pool = None
                await asyncio.sleep(1.0 * retry_count)
            else:
                logger.error(f"获取数据库连接失败: {error_msg}", exc_info=True)
                raise


@asynccontextmanager
async def get_opengauss_connection():
    """
    获取openGauss数据库连接（使用连接池，自动管理，兼容 asyncpg 和 psycopg3）
    
    用法：
    async with get_opengauss_connection() as conn:
        # 使用 conn
        rows = await conn.fetch("SELECT * FROM backup_tasks")
        # 连接自动释放回连接池
    """
    pool, conn = await _acquire_connection()
    
    # 检测是 psycopg3 还是 asyncpg
    is_psycopg3 = hasattr(pool, 'putconn') or hasattr(pool, 'connection')
    
    try:
        yield conn
    finally:
        try:
            monitor = get_opengauss_monitor()
            
            if is_psycopg3:
                # psycopg3: 释放连接
                # 注意：连接池的 reset 函数会在连接返回池时自动清理事务状态
                # 但为了减少警告日志，我们在释放前也尝试清理
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                
                # 在释放连接前，强制确保连接处于干净状态
                # 关键：psycopg3 连接池内部的 _reset_connection 会在我们的 reset_connection **之前**执行
                # 所以我们必须在这里就确保连接处于干净状态，避免内部的 _reset_connection 检测到 INTRANS
                try:
                    # 检查事务状态：0=IDLE（不在事务中），1=INTRANS（在事务中），3=INERROR（错误状态）
                    transaction_status = actual_conn.info.transaction_status
                    
                    if transaction_status == 1:  # INTRANS: 在事务中但未提交
                        # 如果还在事务中，尝试提交而不是回滚（避免数据丢失）
                        logger.warning("[连接释放] ⚠️ 检测到未提交的事务（INTRANS），尝试提交以避免数据丢失")
                        try:
                            await actual_conn.commit()
                            # 等待一小段时间让状态更新
                            import asyncio
                            await asyncio.sleep(0.01)  # 等待10ms让状态更新
                            
                            # 再次检查事务状态，确保已变为 IDLE
                            new_status = actual_conn.info.transaction_status
                            if new_status == 0:
                                logger.info("[连接释放] ✅ 事务已成功提交，连接状态=IDLE")
                            else:
                                logger.error(f"[连接释放] ❌ 提交后事务状态仍为 {new_status}，执行回滚")
                                try:
                                    await actual_conn.rollback()
                                    logger.debug("[连接释放] 事务已回滚")
                                except Exception as rollback_err:
                                    logger.warning(f"[连接释放] 回滚失败: {str(rollback_err)}")
                        except Exception as commit_err:
                            logger.error(f"[连接释放] ❌ 提交失败: {str(commit_err)}，执行回滚", exc_info=True)
                            try:
                                await actual_conn.rollback()
                                logger.debug("[连接释放] 事务已回滚")
                            except Exception:
                                pass
                    elif transaction_status == 3:  # INERROR: 错误状态
                        # 如果处于错误状态，回滚以清理状态
                        logger.debug("[连接释放] 检测到错误状态，执行回滚")
                        try:
                            await actual_conn.rollback()
                        except Exception:
                            pass
                    # 注意：如果状态是 IDLE（已提交），不需要执行 rollback()
                    # rollback() 在 IDLE 状态下虽然不会报错，但可能会影响已提交的数据
                    # 只在 INTRANS 或 INERROR 状态下才需要清理
                    # 如果状态是 IDLE，说明事务已经提交，直接释放连接即可
                    if transaction_status == 0:  # IDLE: 不在事务中，已提交
                        logger.debug("[连接释放] 连接状态正常（IDLE），事务已提交，无需回滚")
                except Exception as status_check_err:
                    # 如果检查失败（可能连接已关闭），这是正常的
                    logger.debug(f"[连接释放] 检查事务状态时出错（可忽略）: {str(status_check_err)}")
                    pass
                
                # 释放连接（连接池的 reset 函数会最终清理事务状态）
                if hasattr(conn, 'release'):
                    # 如果 conn 是 AsyncPGCompatConnection，使用其 release 方法
                    await conn.release()
                elif hasattr(pool, 'putconn'):
                    release_coro = pool.putconn(actual_conn)
                    if monitor.enabled:
                        try:
                            await monitor.watch(
                                release_coro,
                                operation="pool.putconn",
                                timeout=5.0,
                            )
                        except Exception as watch_error:
                            error_msg = str(watch_error)
                            logger.debug(f"释放连接时遇到错误（可忽略）: {error_msg}")
                    else:
                        await release_coro
            else:
                # asyncpg: 使用 release 释放连接
                release_coro = pool.release(conn._conn if hasattr(conn, '_conn') else conn)
                if monitor.enabled:
                    try:
                        await monitor.watch(
                            release_coro,
                            operation="pool.release",
                            timeout=5.0,
                        )
                    except Exception as watch_error:
                        # 监控可能会捕获异常，但我们需要检查是否是 UNLISTEN 错误
                        error_msg = str(watch_error)
                        if "UNLISTEN" not in error_msg and "not yet supported" not in error_msg:
                            raise
                        # UNLISTEN 错误可以忽略
                        logger.debug(f"释放连接时遇到 openGauss 限制（可忽略）: {error_msg}")
                else:
                    await release_coro
        except Exception as e:
            # 错误处理（兼容 asyncpg 和 psycopg3）
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else repr(e)
            
            # 检查连接状态
            conn_info = ""
            try:
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                if hasattr(actual_conn, 'is_closed'):
                    conn_info = f", 连接已关闭: {actual_conn.is_closed()}"
                elif hasattr(actual_conn, 'closed'):
                    conn_info = f", 连接已关闭: {actual_conn.closed}"
            except Exception:
                conn_info = ", 无法检查连接状态"
            
            # asyncpg 特定错误
            if not is_psycopg3:
                import asyncpg
                if isinstance(e, asyncpg.exceptions.FeatureNotSupportedError) or "UNLISTEN" in error_msg or "not yet supported" in error_msg:
                    logger.debug(f"释放连接时遇到 openGauss 限制（可忽略）: {error_type}: {error_msg}")
                elif isinstance(e, (asyncpg.exceptions.InterfaceError, asyncpg.exceptions.InternalClientError)):
                    logger.debug(f"释放连接时连接已无效（可忽略）: {error_type}: {error_msg}{conn_info}")
                else:
                    logger.warning(f"释放连接失败: {error_type}: {error_msg}{conn_info}", exc_info=False)
            else:
                # psycopg3 错误处理
                if "UNLISTEN" in error_msg or "not yet supported" in error_msg:
                    logger.debug(f"释放连接时遇到 openGauss 限制（可忽略）: {error_type}: {error_msg}")
                else:
                    logger.warning(f"释放连接失败: {error_type}: {error_msg}{conn_info}", exc_info=False)


async def close_opengauss_pool():
    """关闭openGauss连接池"""
    global _opengauss_pool
    # 检查连接池是否存在且未关闭
    pool_closed = False
    if _opengauss_pool:
        if hasattr(_opengauss_pool, 'is_closing'):
            pool_closed = _opengauss_pool.is_closing()
        elif hasattr(_opengauss_pool, 'closed'):
            pool_closed = _opengauss_pool.closed
        else:
            # psycopg3 的 AsyncConnectionPool 没有 is_closing，检查其他属性
            pool_closed = False
    
    if _opengauss_pool and not pool_closed:
        try:
            await _opengauss_pool.close()
            logger.info("openGauss连接池已关闭")
        except Exception as e:
            logger.error(f"关闭openGauss连接池失败: {str(e)}", exc_info=True)
        finally:
            _opengauss_pool = None

