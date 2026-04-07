#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
计划任务调度器
Task Scheduler Module
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Callable, Any, Optional, List
from croniter import croniter

from config.settings import get_settings
from models.backup import BackupTask
from models.tape import TapeCartridge
from models.scheduled_task import ScheduledTask, ScheduledTaskStatus
from config.database import db_manager

from .db_utils import is_opengauss, get_opengauss_connection
from .schedule_calculator import calculate_next_run_time
from .task_executor import create_task_executor
from .task_storage import (
    load_tasks_from_db, get_task_by_id, get_all_tasks,
    add_task as storage_add_task, delete_task as storage_delete_task,
    update_task as storage_update_task
)

logger = logging.getLogger(__name__)


class BackupScheduler:
    """备份任务调度器"""

    def __init__(self):
        self.settings = get_settings()
        self.running = False
        self.tasks: Dict[str, Dict] = {}
        self._scheduler_task = None
        self.system_instance = None

    async def initialize(self, system_instance):
        """初始化调度器"""
        self.system_instance = system_instance

        # 注册默认任务
        await self._register_default_tasks()

        logger.info("计划任务调度器初始化完成")

    async def _register_default_tasks(self):
        """注册默认任务

        注意：计划任务的执行时间由用户在Web界面设置时确定，不再使用默认配置。
        保留期检查任务已取消，改为在打开磁带管理页面时检查。
        """
        if self.settings.SCHEDULER_ENABLED:
            # 月度备份任务 - 已废弃，计划任务通过Web界面创建，使用用户设置的Cron表达式
            # await self.register_task(
            #     "monthly_backup",
            #     self.settings.MONTHLY_BACKUP_CRON,
            #     self._execute_monthly_backup,
            #     "月度完整备份任务"
            # )

            # 保留期检查任务 - 已取消，改为在打开磁带管理页面时检查
            # await self.register_task(
            #     "retention_check",
            #     self.settings.RETENTION_CHECK_CRON,
            #     self._execute_retention_check,
            #     "磁带保留期检查任务"
            # )

            # 系统健康检查任务
            await self.register_task(
                "health_check",
                "0 */6 * * *",  # 每6小时执行一次
                self._execute_health_check,
                "系统健康检查任务"
            )

    async def register_task(self, task_id: str, cron_expression: str,
                          func: Callable, description: str = ""):
        """注册任务"""
        try:
            self.tasks[task_id] = {
                'cron': cron_expression,
                'func': func,
                'description': description,
                'last_run': None,
                'next_run': self._get_next_run_time(cron_expression),
                'enabled': True
            }
            logger.info(f"注册任务: {task_id} - {description}")
        except Exception as e:
            logger.error(f"注册任务失败 {task_id}: {str(e)}")

    async def unregister_task(self, task_id: str):
        """注销任务"""
        if task_id in self.tasks:
            del self.tasks[task_id]
            logger.info(f"注销任务: {task_id}")

    async def enable_task(self, task_id: str):
        """启用任务"""
        if task_id in self.tasks:
            self.tasks[task_id]['enabled'] = True
            logger.info(f"启用任务: {task_id}")

    async def disable_task(self, task_id: str):
        """禁用任务"""
        if task_id in self.tasks:
            self.tasks[task_id]['enabled'] = False
            logger.info(f"禁用任务: {task_id}")

    async def start(self):
        """启动调度器"""
        if self.running:
            return

        self.running = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("计划任务调度器已启动")

    async def stop(self):
        """停止调度器"""
        self.running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        logger.info("计划任务调度器已停止")

    async def _scheduler_loop(self):
        """调度器主循环"""
        while self.running:
            try:
                current_time = datetime.now()

                for task_id, task_info in self.tasks.items():
                    if not task_info['enabled']:
                        continue

                    if current_time >= task_info['next_run']:
                        await self._execute_task(task_id, task_info)
                        # 更新下次执行时间
                        task_info['last_run'] = current_time
                        task_info['next_run'] = self._get_next_run_time(task_info['cron'])

                # 每分钟检查一次
                await asyncio.sleep(360)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"调度器循环出错: {str(e)}")
                await asyncio.sleep(360)

    async def _execute_task(self, task_id: str, task_info: Dict):
        """执行任务"""
        try:
            logger.info(f"开始执行任务: {task_id} - {task_info['description']}")

            start_time = datetime.now()
            await task_info['func']()
            end_time = datetime.now()

            duration = (end_time - start_time).total_seconds()
            logger.info(f"任务执行完成: {task_id}, 耗时: {duration:.2f}秒")

        except Exception as e:
            logger.error(f"任务执行失败 {task_id}: {str(e)}")

            # 发送错误通知
            if self.system_instance and self.system_instance.dingtalk_notifier:
                await self.system_instance.dingtalk_notifier.send_system_notification(
                    "任务执行失败",
                    f"任务 {task_id} 执行失败: {str(e)}"
                )

    def _get_next_run_time(self, cron_expression: str) -> datetime:
        """获取下次执行时间"""
        cron = croniter(cron_expression, datetime.now())
        return cron.get_next(datetime)

    async def _execute_monthly_backup(self):
        """执行月度备份"""
        logger.info("开始执行月度备份任务")

        if self.system_instance and self.system_instance.backup_engine:
            # 创建月度备份任务
            backup_task = BackupTask(
                task_name=f"月度备份-{datetime.now().strftime('%Y-%m')}",
                task_type="monthly_full",
                source_paths=["/"],  # 这里需要根据实际配置调整
                status="pending"
            )

            # 执行备份
            await self.system_instance.backup_engine.execute_backup_task(backup_task)

    async def _execute_retention_check(self):
        """执行保留期检查"""
        logger.info("开始执行磁带保留期检查")

        if self.system_instance and self.system_instance.tape_manager:
            await self.system_instance.tape_manager.check_retention_periods()

    async def _execute_health_check(self):
        """执行系统健康检查"""
        logger.info("开始执行系统健康检查")

        health_status = {
            'database': await self._check_database_health(),
            'tape_drive': await self._check_tape_drive_health(),
            'disk_space': await self._check_disk_space(),
            'services': await self._check_services_health()
        }

        # 检查是否有异常
        unhealthy_services = [k for k, v in health_status.items() if not v]
        if unhealthy_services:
            if self.system_instance and self.system_instance.dingtalk_notifier:
                await self.system_instance.dingtalk_notifier.send_system_notification(
                    "系统健康检查异常",
                    f"以下服务状态异常: {', '.join(unhealthy_services)}"
                )

    async def _check_database_health(self) -> bool:
        """检查数据库健康状态"""
        try:
            from config.database import db_manager
            return await db_manager.health_check()
        except Exception:
            return False

    async def _check_tape_drive_health(self) -> bool:
        """检查磁带驱动器健康状态"""
        try:
            if self.system_instance and self.system_instance.tape_manager:
                return await self.system_instance.tape_manager.health_check()
            return False
        except Exception:
            return False

    async def _check_disk_space(self) -> bool:
        """检查磁盘空间"""
        try:
            import shutil
            total, used, free = shutil.disk_usage("/")
            free_percent = (free / total) * 100
            return free_percent > 10  # 剩余空间大于10%
        except Exception:
            return False

    async def _check_services_health(self) -> bool:
        """检查各项服务健康状态"""
        # 这里可以添加更多服务检查
        return True

    def get_task_status(self) -> Dict[str, Any]:
        """获取任务状态"""
        return {
            'scheduler_running': self.running,
            'total_tasks': len(self.tasks),
            'enabled_tasks': len([t for t in self.tasks.values() if t['enabled']]),
            'tasks': {
                task_id: {
                    'description': task['description'],
                    'enabled': task['enabled'],
                    'last_run': task['last_run'].isoformat() if task['last_run'] else None,
                    'next_run': task['next_run'].isoformat() if task['next_run'] else None,
                    'cron': task['cron']
                }
                for task_id, task in self.tasks.items()
            }
        }


# 任务类型优先级：backup(1) > tape verify(2) > directory verify(3) > 其他(9)
_ACTION_PRIORITY = {
    'backup': 1,
    'verify': 2,
    'health_check': 10,
}


def _get_action_priority(task_info: Dict) -> int:
    """获取任务优先级（数值越小优先级越高）"""
    task = task_info.get('task')
    if not task:
        return 9
    action_type = task.action_type
    action_value = action_type.value if hasattr(action_type, 'value') else str(action_type)
    # 磁带验证优先于目录验证
    if action_value == 'verify':
        config = task.action_config or {}
        if config.get('verify_type') == 'tape':
            return 2
        return 3
    return _ACTION_PRIORITY.get(action_value, 9)


class TaskScheduler:
    """增强的计划任务调度器 - 支持数据库持久化和多种调度方式"""

    def __init__(self):
        self.settings = get_settings()
        self.running = False
        self.tasks: Dict[int, Dict] = {}  # key: scheduled_task.id
        self._scheduler_task = None
        self.system_instance = None
        self._running_executions: Dict[int, asyncio.Task] = {}  # 正在运行的任务
        self._execution_lock = asyncio.Lock()  # 全局互斥锁：同时只允许一个任务执行

    async def initialize(self, system_instance):
        """初始化调度器"""
        self.system_instance = system_instance

        # 从数据库加载计划任务
        await self._load_tasks_from_db()

        logger.info(f"计划任务调度器初始化完成，加载了 {len(self.tasks)} 个任务")

    async def _load_tasks_from_db(self):
        """从数据库加载计划任务"""
        try:
            scheduled_tasks = await load_tasks_from_db(enabled_only=True)
            logger.info(f"[任务加载] 从数据库加载启用的计划任务，共 {len(scheduled_tasks)} 个")

            loaded_count = 0
            failed_count = 0
            skipped_count = 0

            for task in scheduled_tasks:
                result = await self._load_task(task)
                if result == 'loaded':
                    loaded_count += 1
                elif result == 'failed':
                    failed_count += 1
                elif result == 'skipped':
                    skipped_count += 1

            logger.info(
                f"[任务加载] 任务加载完成 - "
                f"成功: {loaded_count}, "
                f"失败: {failed_count}, "
                f"跳过: {skipped_count}, "
                f"总计: {len(scheduled_tasks)}"
            )

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"[任务加载] 从数据库加载任务失败: {str(e)}")
            logger.error(f"[任务加载] 错误详情:\n{error_detail}")

    async def _load_task(self, scheduled_task: ScheduledTask):
        """加载单个任务到内存"""
        try:
            task_id = scheduled_task.id
            task_name = scheduled_task.task_name
            schedule_type = scheduled_task.schedule_type.value if hasattr(scheduled_task.schedule_type, 'value') else str(scheduled_task.schedule_type)
            enabled = scheduled_task.enabled
            status = scheduled_task.status.value if hasattr(scheduled_task.status, 'value') else str(scheduled_task.status)

            logger.debug(
                f"[任务加载] 开始加载任务 - "
                f"ID: {task_id}, "
                f"名称: {task_name}, "
                f"调度类型: {schedule_type}, "
                f"启用状态: {enabled}, "
                f"任务状态: {status}"
            )

            # 检查任务是否启用
            if not enabled:
                logger.warning(
                    f"[任务加载] 任务未启用，跳过加载 - "
                    f"ID: {task_id}, "
                    f"名称: {task_name}"
                )
                return 'skipped'

            # 计算下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            if not next_run:
                logger.warning(
                    f"[任务加载] 无法计算下次执行时间，跳过加载 - "
                    f"ID: {task_id}, "
                    f"名称: {task_name}, "
                    f"调度类型: {schedule_type}, "
                    f"调度配置: {scheduled_task.schedule_config}"
                )
                return 'skipped'

            # 创建任务执行函数
            execute_func = create_task_executor(scheduled_task, self.system_instance)

            self.tasks[scheduled_task.id] = {
                'task': scheduled_task,
                'execute_func': execute_func,
                'next_run': next_run,
                'last_run': scheduled_task.last_run_time
            }

            # 更新数据库中的下次执行时间
            async with get_opengauss_connection() as conn:
                await conn.execute(
                    """
                    UPDATE scheduled_tasks
                    SET next_run_time = $1
                    WHERE id = $2
                    """,
                    next_run,
                    scheduled_task.id
                )
                # psycopg3 binary protocol 需要显式提交事务
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    await actual_conn.commit()
                    logger.debug(f"[任务加载] 任务 {scheduled_task.id} next_run_time 更新已提交到数据库")
                except Exception as commit_err:
                    logger.warning(f"[任务加载] 提交任务 next_run_time 更新事务失败（可能已自动提交）: {commit_err}")
                    try:
                        await actual_conn.rollback()
                    except:
                        pass

            logger.info(
                f"[任务加载] 任务加载成功 - "
                f"ID: {task_id}, "
                f"名称: {task_name}, "
                f"调度类型: {schedule_type}, "
                f"下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else 'N/A'}"
            )
            return 'loaded'

        except Exception as e:
            logger.error(
                f"[任务加载] 加载任务失败 - "
                f"ID: {scheduled_task.id if scheduled_task else 'N/A'}, "
                f"名称: {scheduled_task.task_name if scheduled_task else 'N/A'}, "
                f"错误: {str(e)}"
            )
            import traceback
            logger.debug(f"[任务加载] 错误详情:\n{traceback.format_exc()}")
            return 'failed'

    async def add_task(self, scheduled_task: ScheduledTask) -> bool:
        """添加计划任务"""
        try:
            # 计算下次执行时间
            next_run = calculate_next_run_time(scheduled_task)
            scheduled_task.next_run_time = next_run

            # 保存到数据库
            success = await storage_add_task(scheduled_task)

            if not success:
                return False

            # 如果任务已启用，加载到内存
            if scheduled_task.enabled:
                await self._load_task(scheduled_task)

            logger.info(f"添加计划任务成功: {scheduled_task.task_name} (ID: {scheduled_task.id})")
            return True

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"添加计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return False

    async def delete_task(self, task_id: int) -> bool:
        """删除计划任务"""
        try:
            # 停止正在运行的任务
            if task_id in self._running_executions:
                self._running_executions[task_id].cancel()
                del self._running_executions[task_id]

            # 从内存中移除
            if task_id in self.tasks:
                del self.tasks[task_id]

            # 从数据库删除
            success = await storage_delete_task(task_id)

            if success:
                logger.info(f"删除计划任务成功: task_id={task_id}")
                return True
            else:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"删除计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return False

    async def update_task(self, task_id: int, updates: Dict[str, Any]) -> bool:
        """更新计划任务"""
        try:
            # 获取任务
            task = await get_task_by_id(task_id)
            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False

            # 更新字段
            for key, value in updates.items():
                if hasattr(task, key):
                    setattr(task, key, value)

            # 重新计算下次执行时间
            next_run = calculate_next_run_time(task)

            # 更新到数据库
            updated_task = await storage_update_task(task_id, updates, next_run_time=next_run)

            if not updated_task:
                return False

            # 重新加载任务
            if updated_task.enabled:
                await self._load_task(updated_task)
                logger.info(f"更新计划任务成功: {updated_task.task_name} (已启用)")
            else:
                # 如果任务被禁用，从内存中移除
                if task_id in self.tasks:
                    del self.tasks[task_id]
                    logger.info(f"更新计划任务成功: {updated_task.task_name} (已禁用，已从内存中移除)")
                # 如果任务正在运行，停止它
                if task_id in self._running_executions:
                    logger.warning(
                        f"任务被禁用时仍在运行，正在停止 - "
                        f"任务ID: {task_id}, "
                        f"任务名称: {updated_task.task_name}"
                    )
                    await self.stop_task(task_id)
                else:
                    logger.info(f"更新计划任务成功: {updated_task.task_name} (已禁用)")

            return True

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"更新计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return False

    async def run_task(self, task_id: int, run_options: Optional[Dict[str, Any]] = None) -> bool:
        """立即运行计划任务（手动运行，跳过启用状态检查）"""
        try:
            task = await get_task_by_id(task_id)

            if not task:
                logger.warning(f"未找到任务 ID: {task_id}")
                return False

            # 手动运行时，即使任务未启用也可以运行
            # 如果任务不在内存中，强制加载（忽略 enabled 状态）
            if task_id not in self.tasks:
                # 手动运行时，即使任务未启用也允许运行
                # 先尝试正常加载，如果因为未启用或时间已过而跳过，则强制加载
                load_result = await self._load_task(task)

                if load_result == 'skipped':
                    # 任务被跳过（可能是因为未启用或执行时间已过），但手动运行允许执行，强制加载到内存
                    logger.info(f"[手动运行] 任务被跳过，但允许手动运行，强制加载到内存 - ID: {task_id}, 名称: {task.task_name}, 跳过原因: {'未启用' if not task.enabled else '执行时间已过'}")
                    # 计算下次执行时间
                    next_run = calculate_next_run_time(task)
                    if not next_run:
                        # 如果无法计算下次执行时间（比如一次性任务时间已过），使用当前时间作为下次执行时间（仅用于手动运行）
                        from datetime import datetime
                        next_run = datetime.now()
                        logger.debug(f"[手动运行] 无法计算下次执行时间，使用当前时间: {next_run}")

                    # 创建任务执行函数（手动运行标记为True）
                    execute_func = create_task_executor(
                        task,
                        self.system_instance,
                        manual_run=True,
                        run_options=run_options
                    )
                    # 临时加载到内存（不检查 enabled 状态）
                    self.tasks[task_id] = {
                        'task': task,
                        'next_run': next_run,
                        'execute_func': execute_func,
                        'last_run': task.last_run_time
                    }
                elif load_result != 'loaded':
                    # 加载失败（非跳过原因）
                    logger.error(f"[手动运行] 任务加载失败: {load_result} - ID: {task_id}")
                    return False

            # 确保任务在内存中
            if task_id not in self.tasks:
                logger.error(f"[手动运行] 任务不在内存中，无法运行 - ID: {task_id}")
                return False

            # 获取执行函数（如果内存中没有，创建新的）
            task_info = self.tasks.get(task_id)
            if task_info and 'execute_func' in task_info:
                # 如果执行函数已存在，但 run_options 可能不同，需要重新创建
                execute_func = task_info['execute_func']
            else:
                # 创建执行函数并执行（手动运行标记为True，因为是从Web界面点击运行的）
                execute_func = create_task_executor(
                    task,
                    self.system_instance,
                    manual_run=True,
                    run_options=run_options
                )

            # 在后台执行（不阻塞）
            logger.info(f"[手动运行] 创建后台执行任务 - 任务ID: {task_id}, 任务名称: {task.task_name}")
            execution_task = asyncio.create_task(execute_func())
            self._running_executions[task_id] = execution_task

            # 等待一小段时间，检查任务是否因为获取锁失败而立即退出
            await asyncio.sleep(0.1)

            # 如果任务已完成且是因为锁失败，捕获异常
            if execution_task.done():
                try:
                    await execution_task
                except RuntimeError as e:
                    if "任务已在执行中" in str(e):
                        logger.warning(f"[手动运行] 任务获取锁失败: {str(e)}")
                        # 从运行列表中移除
                        if task_id in self._running_executions:
                            del self._running_executions[task_id]
                        raise RuntimeError(str(e))
                    raise
                except Exception as e:
                    # 其他异常也抛出
                    if task_id in self._running_executions:
                        del self._running_executions[task_id]
                    raise

            logger.info(f"[手动运行] 后台执行任务已创建并启动 - 任务ID: {task_id}, 任务名称: {task.task_name}")

            logger.info(f"立即运行计划任务: {task.task_name} (ID: {task_id}, 手动运行)")
            return True

        except Exception as e:
            import traceback
            logger.error(f"立即运行计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{traceback.format_exc()}")
            return False

    async def stop_task(self, task_id: int) -> bool:
        """停止正在运行的任务"""
        try:
            if task_id in self._running_executions:
                execution_task = self._running_executions[task_id]
                execution_task.cancel()

                try:
                    await execution_task
                except asyncio.CancelledError:
                    pass

                del self._running_executions[task_id]

                # 更新任务状态
                await storage_update_task(task_id, {'status': ScheduledTaskStatus.PAUSED})

                logger.info(f"停止计划任务成功: task_id={task_id}")
                return True
            else:
                logger.warning(f"任务未在运行: task_id={task_id}")
                return False

        except Exception as e:
            logger.error(f"停止计划任务失败: {str(e)}")
            return False

    async def enable_task(self, task_id: int) -> bool:
        """启用计划任务"""
        return await self.update_task(task_id, {'enabled': True, 'status': ScheduledTaskStatus.ACTIVE})

    async def disable_task(self, task_id: int) -> bool:
        """禁用计划任务"""
        return await self.update_task(task_id, {'enabled': False, 'status': ScheduledTaskStatus.INACTIVE})

    async def start(self):
        """启动调度器"""
        if self.running:
            logger.warning("[调度器启动] 调度器已在运行中")
            return

        self.running = True
        total_tasks = len(self.tasks)
        logger.info(
            f"[调度器启动] 启动计划任务调度器 - "
            f"内存中任务数: {total_tasks}"
        )

        # 输出所有已加载任务的详细信息
        if total_tasks > 0:
            logger.info("[调度器启动] 已加载的任务列表:")
            for task_id, task_info in self.tasks.items():
                task = task_info.get('task')
                task_name = task.task_name if task else f"任务ID:{task_id}"
                next_run = task_info.get('next_run')
                schedule_type = task.schedule_type.value if task and hasattr(task.schedule_type, 'value') else 'N/A'
                last_success = task.last_success_time if task else None
                last_run = task.last_run_time if task else None
                last_error = task.last_error if task else None

                # 构建执行状态信息
                execution_status = []
                if last_success:
                    execution_status.append(f"上次成功: {last_success.strftime('%Y-%m-%d %H:%M:%S')}")
                elif last_run:
                    # 有运行记录但没有成功记录，说明执行过但失败了
                    execution_status.append(f"上次运行: {last_run.strftime('%Y-%m-%d %H:%M:%S')} (失败)")
                else:
                    execution_status.append("从未执行")

                execution_info = ", ".join(execution_status) if execution_status else "无执行记录"

                # 如果有错误信息，显示错误摘要（最多100个字符）
                error_info = ""
                if last_error:
                    error_preview = last_error[:100] + "..." if len(last_error) > 100 else last_error
                    error_info = f"\n      失败原因: {error_preview}"

                logger.info(
                    f"  - 任务ID: {task_id}, "
                    f"名称: {task_name}, "
                    f"调度类型: {schedule_type}, "
                    f"下次执行: {next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else 'N/A'}, "
                    f"{execution_info}{error_info}"
                )
        else:
            logger.warning("[调度器启动] 内存中没有任何任务，请检查:")
            logger.warning("  1. 数据库中是否有启用的计划任务（enabled=True）")
            logger.warning("  2. 任务的下次执行时间是否计算成功")
            logger.warning("  3. 任务加载过程中是否有错误")

        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info("[调度器启动] 计划任务调度器主循环已启动")

    async def stop(self):
        """停止调度器"""
        self.running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass

        # 停止所有正在运行的任务
        for task_id in list(self._running_executions.keys()):
            await self.stop_task(task_id)

        logger.info("计划任务调度器已停止")

    async def _scheduler_loop(self):
        """调度器主循环"""
        loop_count = 0
        while self.running:
            try:
                current_time = datetime.now()
                loop_count += 1

                # 每10次循环输出一次统计信息（约10分钟）
                if loop_count % 10 == 0:
                    total_tasks = len(self.tasks)
                    running_tasks = len(self._running_executions)
                    logger.info(
                        f"[调度器主循环] 检查任务状态 - "
                        f"总任务数: {total_tasks}, "
                        f"运行中: {running_tasks}, "
                        f"当前时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                # 锁守卫：有任务正在执行时跳过本轮，不产生排队协程
                if self._execution_lock.locked():
                    await asyncio.sleep(360)
                    continue

                # 检查每个任务
                triggered_tasks = []
                for task_id, task_info in list(self.tasks.items()):
                    task = task_info.get('task')
                    task_name = task.task_name if task else f"任务ID:{task_id}"
                    next_run = task_info.get('next_run')

                    # 检查任务是否启用（双重检查，确保安全）
                    if not task or not task.enabled:
                        # 如果任务被禁用，从内存中移除（防止内存泄漏）
                        if task_id in self.tasks:
                            logger.debug(
                                f"[调度器主循环] 任务已禁用，从内存中移除 - "
                                f"任务ID: {task_id}, "
                                f"任务名称: {task_name}"
                            )
                            del self.tasks[task_id]
                        # 如果任务正在运行，停止它
                        if task_id in self._running_executions:
                            logger.warning(
                                f"[调度器主循环] 检测到已禁用的任务仍在运行，停止执行 - "
                                f"任务ID: {task_id}, "
                                f"任务名称: {task_name}"
                            )
                            await self.stop_task(task_id)
                        continue

                    # 检查 next_run 是否为 None
                    if next_run is None:
                        logger.warning(
                            f"[调度器主循环] 任务的下次执行时间为空 - "
                            f"任务ID: {task_id}, "
                            f"任务名称: {task_name}, "
                            f"建议重新加载任务"
                        )
                        continue

                    # 时间未到，跳过
                    if current_time < next_run:
                        continue

                    # 月度去重：本月已成功执行过的月度任务不再触发
                    schedule_type = task.schedule_type.value if task and hasattr(task.schedule_type, 'value') else 'N/A'
                    last_success = task.last_success_time if task else None
                    if schedule_type.lower() == 'monthly' and last_success:
                        if (last_success.year == current_time.year and
                                last_success.month == current_time.month):
                            # 重算 next_run 到下个月，更新内存和数据库
                            new_next_run = calculate_next_run_time(task)
                            if new_next_run:
                                task_info['next_run'] = new_next_run
                                await self._persist_next_run(task_id, new_next_run)
                                logger.info(
                                    f"[调度器主循环] 月度任务本月已执行，跳过 - "
                                    f"任务ID: {task_id}, 任务名称: {task_name}, "
                                    f"下次执行: {new_next_run.strftime('%Y-%m-%d %H:%M:%S')}"
                                )
                            continue

                    # 收集待执行任务
                    triggered_tasks.append((task_id, task_info, task_name))

                    # 立即更新 next_run 到下一个周期，防止重复触发
                    new_next_run = calculate_next_run_time(task)
                    if new_next_run:
                        task_info['next_run'] = new_next_run
                        await self._persist_next_run(task_id, new_next_run)
                    else:
                        # 无法计算下次时间（如一次性任务已过期），设为远未来防止重复
                        task_info['next_run'] = current_time.replace(year=current_time.year + 10)

                # 按优先级排序：backup > tape verify > directory verify > 其他
                if triggered_tasks:
                    triggered_tasks.sort(key=lambda t: _get_action_priority(t[1]))

                    task_names = [t[2] for t in triggered_tasks]
                    logger.info(
                        f"[调度器主循环] 本次检查触发 {len(triggered_tasks)} 个任务"
                        f"（按优先级排序）: {', '.join(task_names)}"
                    )

                    # 后台顺序执行：高优先级先执行，完成后执行下一个
                    asyncio.create_task(
                        self._execute_triggered_tasks(triggered_tasks)
                    )

                # 每6分钟检查一次
                await asyncio.sleep(360)

            except asyncio.CancelledError:
                logger.info("[调度器主循环] 收到取消信号，退出主循环")
                break
            except Exception as e:
                logger.error(f"[调度器主循环] 调度器循环出错: {str(e)}", exc_info=True)
                await asyncio.sleep(360)

    async def _execute_triggered_tasks(self, triggered_tasks: list):
        """按优先级顺序执行触发的任务（一次只执行一个）"""
        for task_id, task_info, task_name in triggered_tasks:
            async with self._execution_lock:
                logger.info(
                    f"[调度器顺序执行] 开始执行 - "
                    f"任务ID: {task_id}, 任务名称: {task_name}"
                )
                try:
                    await task_info['execute_func']()
                except Exception as e:
                    logger.error(
                        f"[调度器顺序执行] 任务执行失败 - "
                        f"任务ID: {task_id}, 任务名称: {task_name}: {e}"
                    )
                finally:
                    if task_id in self._running_executions:
                        del self._running_executions[task_id]
                logger.info(
                    f"[调度器顺序执行] 任务完成 - "
                    f"任务ID: {task_id}, 任务名称: {task_name}"
                )

    async def _persist_next_run(self, task_id: int, next_run: datetime):
        """将 next_run 持久化到数据库"""
        try:
            async with get_opengauss_connection() as conn:
                await conn.execute(
                    "UPDATE scheduled_tasks SET next_run_time = $1 WHERE id = $2",
                    next_run, task_id
                )
                actual_conn = conn._conn if hasattr(conn, '_conn') else conn
                try:
                    await actual_conn.commit()
                except Exception:
                    try:
                        await actual_conn.rollback()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"[调度器] 更新任务 {task_id} next_run 到数据库失败: {e}")

    async def get_tasks(self, enabled_only: bool = False) -> List[ScheduledTask]:
        """获取所有计划任务"""
        try:
            return await get_all_tasks(enabled_only=enabled_only)
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"获取计划任务列表失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return []

    async def get_task(self, task_id: int) -> Optional[ScheduledTask]:
        """获取单个计划任务"""
        try:
            return await get_task_by_id(task_id)
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            logger.error(f"获取计划任务失败: {str(e)}")
            logger.error(f"错误详情:\n{error_detail}")
            return None
