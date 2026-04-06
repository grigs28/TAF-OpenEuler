#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LTFS 安全操作工具集

遵循 IBM Storage Archive / Quantum 官方文档的标准流程：
https://www.ibm.com/docs/en/storage-archive-sde/2.4.7?topic=systems-unmounting-tape-media

标准卸载流程（5步）：
1. sync — 强制同步缓存到磁带
2. fusermount -u — 卸载 LTFS 挂载点
3. 等待 LTFS 进程完全退出（写入索引和 CM 数据）
4. 验证挂载点已卸载
5. mt offline — 磁带离线（弹出）

提供三个级别的操作：
- safe_unmount_ltfs(): 完整5步安全卸载（用于备份完成、系统关闭）
- cleanup_mount(): 快速清理挂载点（用于格式化前、挂载前清理）
- eject_tape(): 磁带弹出（mt offline/eject）
"""

import asyncio
import logging
import subprocess
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


async def safe_unmount_ltfs(
    mount_point,
    tape_device=None,
    wait_ltfs=True,
    max_wait=3600,
):
    """完整5步安全卸载 LTFS（遵循 IBM/Quantum 官方文档）

    Args:
        mount_point: LTFS 挂载点路径（str 或 Path）
        tape_device: 磁带设备路径（如 /dev/nst0），为 None 则跳过 mt offline
        wait_ltfs: 是否等待 LTFS 进程退出（索引+CM数据写入）
        max_wait: 等待 LTFS 进程退出的最大秒数

    Returns:
        (success, message)
    """
    mount_point = Path(mount_point)

    if not mount_point.is_mount():
        return True, "挂载点未挂载，无需卸载"

    try:
        # 步骤 1: sync — 强制同步缓存
        logger.info("[LTFS卸载] 1/5 同步缓存...")
        try:
            subprocess.run(["sync"], timeout=120, check=False)
            logger.info("[LTFS卸载] sync 完成")
        except Exception as e:
            logger.warning(f"[LTFS卸载] sync 失败: {e}")

        # 步骤 2: fusermount -u — 卸载挂载点
        logger.info("[LTFS卸载] 2/5 卸载挂载点...")
        unmount_ok = await _do_unmount(mount_point)
        if unmount_ok:
            logger.info("[LTFS卸载] fusermount 卸载成功")
        else:
            logger.warning("[LTFS卸载] fusermount 失败，尝试 umount -l...")
            try:
                subprocess.run(
                    ["umount", "-l", str(mount_point)],
                    timeout=30, check=False, capture_output=True,
                )
            except Exception:
                pass

        # 步骤 3: 等待 LTFS 进程完全退出（写索引+CM数据）
        if wait_ltfs:
            # fusermount 返回后先检查 LTFS 进程是否已退出
            try:
                result = subprocess.run(
                    ["pgrep", "-x", "ltfs"],
                    capture_output=True, text=True, timeout=5,
                )
                already_exited = result.returncode != 0
            except Exception:
                already_exited = True

            if already_exited:
                logger.info("[LTFS卸载] 3/5 LTFS 进程已退出（fusermount 返回时已清理完毕）")
            else:
                logger.info("[LTFS卸载] 3/5 等待 LTFS 进程退出（写入索引和CM数据）...")
                exited, waited = await _wait_ltfs_exit(max_wait)
                if exited:
                    logger.info(f"[LTFS卸载] LTFS 进程已退出（等待 {waited} 秒）")
                else:
                    logger.warning(f"[LTFS卸载] LTFS 进程在 {max_wait} 秒后仍未退出")
        else:
            logger.info("[LTFS卸载] 3/5 跳过等待 LTFS 进程退出")

        # 步骤 4: 验证挂载点已卸载（双重检查：is_mount + mount | grep）
        logger.info("[LTFS卸载] 4/5 验证挂载点...")
        still_mounted = mount_point.is_mount()
        if not still_mounted:
            # 用 mount 命令二次确认（FUSE 挂载点有时 is_mount 不准确）
            try:
                mount_check = subprocess.run(
                    ["mount"], capture_output=True, text=True, timeout=5,
                )
                for line in mount_check.stdout.splitlines():
                    if str(mount_point) in line:
                        still_mounted = True
                        break
            except Exception:
                pass
        if still_mounted:
            logger.warning("[LTFS卸载] 挂载点仍然存在，尝试强制卸载...")
            try:
                subprocess.run(
                    ["umount", "-l", str(mount_point)],
                    timeout=30, check=False, capture_output=True,
                )
                await asyncio.sleep(2)
            except Exception:
                pass
            # 二次验证
            _still = mount_point.is_mount()
            if not _still:
                try:
                    mc = subprocess.run(["mount"], capture_output=True, text=True, timeout=5)
                    for line in mc.stdout.splitlines():
                        if str(mount_point) in line:
                            _still = True
                            break
                except Exception:
                    pass
            if _still:
                return False, f"挂载点 {mount_point} 卸载失败"
        else:
            logger.info("[LTFS卸载] 挂载点已卸载")

        # 步骤 5: mt offline — 磁带离线
        if tape_device:
            logger.info(f"[LTFS卸载] 5/5 磁带离线 ({tape_device})...")
            eject_ok, eject_msg = await eject_tape(tape_device)
            if eject_ok:
                logger.info("[LTFS卸载] 磁带已离线")
            else:
                logger.warning(f"[LTFS卸载] mt offline 失败（非致命）: {eject_msg}")
        else:
            logger.info("[LTFS卸载] 5/5 未指定设备，跳过 mt offline")

        logger.info("[LTFS卸载] 安全卸载完成")
        return True, "安全卸载完成"

    except Exception as e:
        msg = f"安全卸载失败: {e}"
        logger.error(f"[LTFS卸载] {msg}")
        return False, msg


async def cleanup_mount(mount_point):
    """快速清理挂载点（用于格式化前、挂载前清理损坏挂载点等场景）

    仅执行 fusermount -u，不等待 LTFS 进程退出，不执行 mt offline。
    适用于：格式化前清理、挂载前清理损坏挂载点、测试前的快速卸载。

    Args:
        mount_point: LTFS 挂载点路径（str 或 Path）

    Returns:
        True=清理成功或无需清理，False=清理失败
    """
    mount_point = Path(mount_point)

    if not mount_point.is_mount():
        return True

    logger.info(f"[LTFS清理] 清理挂载点: {mount_point}")

    # 尝试 fusermount
    ok = await _do_unmount(mount_point)
    if ok:
        logger.info("[LTFS清理] fusermount 清理成功")
        return True

    # fallback: umount -l
    logger.warning("[LTFS清理] fusermount 失败，尝试 umount -l...")
    try:
        subprocess.run(
            ["umount", "-l", str(mount_point)],
            timeout=30, check=False, capture_output=True,
        )
        await asyncio.sleep(2)
    except Exception as e:
        logger.warning(f"[LTFS清理] umount -l 失败: {e}")

    still_mounted = mount_point.is_mount()
    if still_mounted:
        logger.warning(f"[LTFS清理] 挂载点 {mount_point} 仍存在")
    else:
        logger.info("[LTFS清理] 挂载点已清理")

    return not still_mounted


async def eject_tape(tape_device, timeout=600):
    """磁带弹出（mt offline）

    mt offline 会将磁带卷回并弹出（与 mt eject 等价）。
    超时后不杀进程（mt 仍在后台运行），返回超时状态由调用方处理。

    Args:
        tape_device: 磁带设备路径（如 /dev/nst0）
        timeout: 超时秒数（默认600s），超时不杀进程

    Returns:
        (success, message)
    """
    if not tape_device:
        return False, "未指定磁带设备"

    try:
        logger.info(f"[磁带弹出] mt -f {tape_device} offline（超时 {timeout}s，超时不杀进程）...")
        proc = await asyncio.create_subprocess_exec(
            "mt", "-f", tape_device, "offline",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # 轮询等待，每60秒输出日志，超时不杀进程
        waited = 0
        while proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=60)
                break
            except asyncio.TimeoutError:
                waited += 60
                logger.info(f"[磁带弹出] mt offline 仍在运行... ({waited}s)")
                if waited >= timeout:
                    logger.warning(f"[磁带弹出] 等待 {timeout}s 超时，mt 进程仍在后台运行，不强制终止")
                    return False, f"mt offline 超时 ({timeout}s)，进程仍在后台运行，请人工确认"

        # 进程已退出
        if proc.returncode == 0:
            logger.info("[磁带弹出] 磁带已弹出")
            return True, "磁带已弹出"
        else:
            # 读取 stderr
            try:
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=5)
            except Exception:
                stderr = None
            err = stderr.decode("utf-8", errors="ignore").strip() if stderr else f"返回码 {proc.returncode}"
            logger.error(f"[磁带弹出] mt offline 失败: {err}")
            return False, err
    except FileNotFoundError:
        return False, "mt 命令未找到"
    except Exception as e:
        return False, str(e)


# ========== 内部辅助函数 ==========


async def _do_unmount(mount_point):
    """执行 fusermount -u 卸载"""
    if not mount_point.is_mount():
        return True

    try:
        proc = await asyncio.create_subprocess_exec(
            "fusermount", "-u", str(mount_point),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
        if proc.returncode == 0:
            return True
        err = stderr.decode("utf-8", errors="ignore") if stderr else ""
        logger.debug(f"[LTFS] fusermount 返回码 {proc.returncode}: {err}")
        return False
    except FileNotFoundError:
        return False
    except asyncio.TimeoutError:
        logger.warning("[LTFS] fusermount 超时")
        return False
    except Exception as e:
        logger.warning(f"[LTFS] fusermount 失败: {e}")
        return False


async def _wait_ltfs_exit(max_wait=3600, poll_interval=5):
    """等待 LTFS 进程完全退出

    fusermount -u 返回后，LTFS 进程仍在写入索引和 CM 数据到磁带。
    必须等 LTFS 进程退出才能安全拔带。

    Returns:
        (exited, waited_seconds)
    """
    waited = 0
    while waited < max_wait:
        try:
            result = subprocess.run(
                ["pgrep", "-x", "ltfs"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                return True, waited
        except Exception:
            return True, waited
        await asyncio.sleep(poll_interval)
        waited += poll_interval

    return False, waited
