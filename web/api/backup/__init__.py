#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
备份管理API模块
Backup Management API Module
"""

from fastapi import APIRouter

# 导入所有子模块
from . import models, utils
from . import tasks_create, tasks_query, tasks_update, tasks_delete
from . import operations, statistics, sets
from . import smb_to_tape  # 一次性 SMB 到磁带备份
from . import one_time  # 一次性备份任务 API# 创建主路由器
router = APIRouter()

# 注册所有子模块的路由
router.include_router(tasks_create.router, tags=["备份任务"])
router.include_router(tasks_query.router, tags=["备份任务"])
router.include_router(tasks_update.router, tags=["备份任务"])
router.include_router(tasks_delete.router, tags=["备份任务"])
router.include_router(operations.router, tags=["备份操作"])
router.include_router(statistics.router, tags=["备份统计"])
router.include_router(sets.router, tags=["备份集"])
router.include_router(smb_to_tape.router, tags=["一次性备份"])
router.include_router(one_time.router, prefix="/one-time", tags=["一次性备份"])

__all__ = ['router', 'models', 'utils']

