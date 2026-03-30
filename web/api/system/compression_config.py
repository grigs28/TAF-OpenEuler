#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统管理API - 压缩配置
System Management API - Compression Configuration
"""

import logging
import re
from typing import Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from config.env_file_manager import EnvFileManager
from config.settings import get_settings

logger = logging.getLogger(__name__)
router = APIRouter()


class CompressionConfigRequest(BaseModel):
    """压缩配置请求"""
    compression_method: Optional[str] = Field(None, description="压缩方法: pgzip、zstd 或 tar")
    compression_threads: Optional[int] = Field(None, description="压缩线程数")
    compression_level: Optional[int] = Field(None, description="压缩级别")
    compress_directly_to_tape: Optional[bool] = Field(None, description="是否直接压缩到磁带机")
    pgzip_block_size: Optional[str] = Field(None, description="PGZip块大小（如 512M、1G）")
    pgzip_threads: Optional[int] = Field(None, description="PGZip线程数")
    zstd_threads: Optional[int] = Field(None, description="zstd线程数")


@router.get("/compression")
async def get_compression_config():
    """获取压缩配置"""
    try:
        settings = get_settings()

        env_manager = EnvFileManager()
        env_values = env_manager.read_env_file()

        compression_level = int(env_values.get("COMPRESSION_LEVEL", settings.COMPRESSION_LEVEL))

        pgzip_block_size = env_values.get("PGZIP_BLOCK_SIZE", settings.PGZIP_BLOCK_SIZE)
        pgzip_threads = int(env_values.get("PGZIP_THREADS", env_values.get("COMPRESSION_THREADS", settings.PGZIP_THREADS)))

        compress_directly_to_tape_str = env_values.get("COMPRESS_DIRECTLY_TO_TAPE")
        if compress_directly_to_tape_str is not None:
            compress_directly_to_tape = compress_directly_to_tape_str.lower() in ("true", "1", "yes", "on")
        else:
            compress_directly_to_tape = getattr(settings, 'COMPRESS_DIRECTLY_TO_TAPE', True)

        zstd_threads = int(env_values.get("ZSTD_THREADS", env_values.get("COMPRESSION_THREADS", settings.ZSTD_THREADS)))

        return {
            "compression_method": env_values.get("COMPRESSION_METHOD", settings.COMPRESSION_METHOD),
            "compression_threads": int(env_values.get("COMPRESSION_THREADS", settings.COMPRESSION_THREADS)),
            "compression_level": compression_level,
            "compress_directly_to_tape": compress_directly_to_tape,
            "pgzip_block_size": pgzip_block_size,
            "pgzip_threads": pgzip_threads,
            "zstd_threads": zstd_threads,
            "available_methods": ["pgzip", "zstd", "tar"],
        }
    except Exception as e:
        logger.error(f"获取压缩配置失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/compression")
async def update_compression_config(config: CompressionConfigRequest, request: Request):
    """更新压缩配置"""
    try:
        env_manager = EnvFileManager()

        updates = {}

        if config.compression_method is not None:
            if config.compression_method not in ["pgzip", "zstd", "tar"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"无效的压缩方法: {config.compression_method}，必须是 'pgzip'、'zstd' 或 'tar'"
                )
            updates["COMPRESSION_METHOD"] = config.compression_method

        if config.compression_threads is not None:
            if config.compression_threads < 1 or config.compression_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="compression_threads 必须在 1-64 之间"
                )
            updates["COMPRESSION_THREADS"] = str(config.compression_threads)

        if config.compression_level is not None:
            if config.compression_level < 0 or config.compression_level > 19:
                raise HTTPException(
                    status_code=400,
                    detail="compression_level 必须在 0-19 之间"
                )
            updates["COMPRESSION_LEVEL"] = str(config.compression_level)

        if config.compress_directly_to_tape is not None:
            updates["COMPRESS_DIRECTLY_TO_TAPE"] = "true" if config.compress_directly_to_tape else "false"

        if config.pgzip_threads is not None:
            if config.pgzip_threads < 1 or config.pgzip_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="pgzip_threads 必须在 1-64 之间"
                )
            updates["PGZIP_THREADS"] = str(config.pgzip_threads)

        if config.pgzip_block_size is not None:
            block_value = config.pgzip_block_size.strip()
            if not block_value:
                raise HTTPException(status_code=400, detail="pgzip_block_size 不能为空")
            if not re.match(r'^\d+(\.\d+)?[kKmMgG]?$', block_value):
                raise HTTPException(
                    status_code=400,
                    detail="pgzip_block_size 格式不正确，应为数字加可选单位（K/M/G）"
                )
            updates["PGZIP_BLOCK_SIZE"] = block_value.upper()

        if config.zstd_threads is not None:
            if config.zstd_threads < 1 or config.zstd_threads > 64:
                raise HTTPException(
                    status_code=400,
                    detail="zstd_threads 必须在 1-64 之间"
                )
            updates["ZSTD_THREADS"] = str(config.zstd_threads)

        # 写入.env文件
        if updates:
            env_manager.write_env_file(updates)
            logger.info(f"更新压缩配置: {updates}")

            from config.settings import reload_settings
            reload_settings()
            logger.info("压缩配置已重新加载，新配置将立即生效")

        return {
            "success": True,
            "message": "压缩配置已更新并重新加载，新配置将立即生效",
            "updated": updates
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新压缩配置失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
