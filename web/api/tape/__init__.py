#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
磁带管理API模块
Tape Management API Module
"""

from fastapi import APIRouter

# 导入所有子模块
from . import models, crud, label, operations, device, utils
# 导入拆分后的模块
from . import tape_create, tape_query, tape_update, tape_statistics, tape_history, tape_delete

# 创建主路由器
router = APIRouter()

# 注册所有子模块的路由
# 注意：crud.router 现在为空，保留以保持向后兼容
router.include_router(crud.router, tags=["磁带CRUD"])
# 注册拆分后的模块路由
router.include_router(tape_create.router, tags=["磁带管理"])
router.include_router(tape_query.router, tags=["磁带管理"])
router.include_router(tape_update.router, tags=["磁带管理"])
router.include_router(tape_statistics.router, tags=["磁带管理"])
router.include_router(tape_history.router, tags=["磁带管理"])
router.include_router(tape_delete.router, tags=["磁带管理"])
# 其他模块
router.include_router(label.router, tags=["磁带标签"])
router.include_router(operations.router, prefix="/operations", tags=["磁带操作"])
router.include_router(device.router, tags=["磁带设备"])
router.include_router(utils.router, tags=["磁带工具"])

__all__ = ['router', 'models']

