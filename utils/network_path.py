#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
网络路径工具 (Linux 版本)
支持 SMB/CIFS 网络路径挂载
"""

import os
import logging
import subprocess
import tempfile
from typing import Dict, List, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

# 已挂载的 SMB 共享缓存
_mounted_shares: Dict[str, str] = {}


def _get_smb_credentials() -> tuple:
    """从配置获取 SMB 凭据

    优先级：Settings > 环境变量

    Returns:
        tuple: (username, password, domain)
    """
    # 首先尝试从 Settings 获取
    try:
        from config.settings import get_settings
        settings = get_settings()
        username = settings.SMB_USERNAME
        password = settings.SMB_PASSWORD
        domain = getattr(settings, 'SMB_DOMAIN', '') or ''
        if username and password:
            return username, password, domain
    except Exception as e:
        logger.debug(f"从 Settings 获取 SMB 凭据失败: {e}")

    # 回退到环境变量
    username = os.environ.get('SMB_USERNAME', '')
    password = os.environ.get('SMB_PASSWORD', '')
    domain = os.environ.get('SMB_DOMAIN', '')

    return username, password, domain


def _get_smb_mount_base() -> str:
    """获取 SMB 挂载基础目录"""
    try:
        from config.settings import get_settings
        settings = get_settings()
        return settings.SMB_MOUNT_BASE or "/mnt/smb"
    except Exception:
        return os.environ.get('SMB_MOUNT_BASE', '/mnt/smb')


def is_unc_path(path: str) -> bool:
    """检查路径是否为 UNC 网络路径"""
    if not path:
        return False
    path_normalized = path.replace('/', '\\')
    return path_normalized.startswith('\\\\') and not path_normalized.startswith('\\\\?\\')


def normalize_unc_path(path: str) -> str:
    """规范化 UNC 路径"""
    if not path:
        return path
    return path.replace('/', '\\')


def get_unc_server_and_share(path: str) -> Optional[Dict[str, str]]:
    """解析 UNC 路径，提取服务器和共享名"""
    if not is_unc_path(path):
        return None
    
    normalized = normalize_unc_path(path)
    parts = normalized.split('\\')
    parts = [p for p in parts if p]  # 移除空部分
    
    if len(parts) < 2:
        return None
    
    server = parts[0]
    share = parts[1]
    subpath = '\\'.join(parts[2:]) if len(parts) > 2 else ''
    
    return {
        'server': server,
        'share': share,
        'subpath': subpath,
        'full_path': normalized
    }


async def validate_network_path(path: str, username: str = None, password: str = None) -> dict:
    """验证网络路径并挂载 SMB 共享

    Args:
        path: UNC 路径 (如 \\server\share\path)
        username: SMB 用户名（可选，默认从配置读取）
        password: SMB 密码（可选，默认从配置读取）

    Returns:
        dict: {
            'valid': bool,
            'path': str,  # 本地挂载路径
            'original_path': str,  # 原始 UNC 路径
            'expanded_paths': List[str],  # 展开后的路径列表
            'is_unc': bool,
            'mount_point': str,  # 挂载点
            'message': str
        }
    """
    result = {
        'valid': False,
        'path': path,
        'original_path': path,
        'expanded_paths': [],
        'is_unc': is_unc_path(path),
        'mount_point': None,
        'message': ''
    }

    if not result['is_unc']:
        # 本地路径，直接检查
        if os.path.exists(path):
            result['valid'] = True
            result['message'] = '本地路径验证通过'
        else:
            result['message'] = f'本地路径不存在: {path}'
        return result

    # UNC 路径处理
    parsed = get_unc_server_and_share(path)
    if not parsed:
        result['message'] = f'无效的 UNC 路径格式: {path}'
        return result

    server = parsed['server']
    share = parsed['share']
    subpath = parsed['subpath']

    # 从配置获取凭据（如果未提供）
    domain = None
    if not username or not password:
        config_username, config_password, config_domain = _get_smb_credentials()
        if not username:
            username = config_username
        if not password:
            password = config_password
        if not domain:
            domain = config_domain

    # 检查凭据是否配置
    if not username or not password:
        result['message'] = 'SMB 凭据未配置，请在 .env 文件中设置 SMB_USERNAME 和 SMB_PASSWORD'
        logger.error(result['message'])
        return result

    # 构建挂载点（使用配置中的挂载基础目录）
    mount_base = _get_smb_mount_base()
    mount_key = f"//{server}/{share}"
    mount_point = f"{mount_base}/{server}_{share}"

    # 检查是否已挂载
    if mount_key in _mounted_shares:
        mount_point = _mounted_shares[mount_key]
        logger.info(f"SMB 共享已挂载: {mount_key} -> {mount_point}")
    else:
        # 创建挂载点
        Path(mount_point).mkdir(parents=True, exist_ok=True)

        # 获取当前用户用于挂载权限
        current_user = os.environ.get('USER', 'root')

        # 构建挂载选项
        mount_options = f'username={username},password={password},vers=3.0,uid={current_user},gid={current_user}'
        if domain:
            mount_options += f',domain={domain}'

        # 挂载 SMB 共享 (使用 sudo)
        mount_cmd = [
            'sudo', 'mount', '-t', 'cifs',
            f'//{server}/{share}',
            mount_point,
            '-o', mount_options
        ]
        
        try:
            result_mount = subprocess.run(
                mount_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result_mount.returncode == 0:
                _mounted_shares[mount_key] = mount_point
                logger.info(f"SMB 共享挂载成功: {mount_key} -> {mount_point}")
            else:
                # 检查是否已经挂载
                if os.path.ismount(mount_point):
                    _mounted_shares[mount_key] = mount_point
                    logger.info(f"SMB 共享已挂载: {mount_key} -> {mount_point}")
                else:
                    result['message'] = f'挂载 SMB 共享失败: {result_mount.stderr}'
                    logger.error(result['message'])
                    return result
        except subprocess.TimeoutExpired:
            result['message'] = '挂载 SMB 共享超时'
            logger.error(result['message'])
            return result
        except Exception as e:
            result['message'] = f'挂载 SMB 共享异常: {str(e)}'
            logger.error(result['message'])
            return result
    
    # 构建本地路径
    local_path = os.path.join(mount_point, subpath.replace('\\', '/'))
    
    if os.path.exists(local_path):
        result['valid'] = True
        result['path'] = local_path
        result['mount_point'] = mount_point
        result['expanded_paths'] = [local_path]
        result['message'] = f'SMB 路径验证通过，已挂载到: {local_path}'
    else:
        result['valid'] = True  # 路径有效但目录可能不存在
        result['path'] = local_path
        result['mount_point'] = mount_point
        result['expanded_paths'] = [local_path]
        result['message'] = f'SMB 路径已挂载，但子路径不存在: {local_path}'
    
    return result


def cleanup_mounts():
    """清理所有挂载的 SMB 共享"""
    global _mounted_shares
    for mount_key, mount_point in _mounted_shares.items():
        try:
            subprocess.run(['umount', mount_point], capture_output=True, timeout=10)
            logger.info(f"已卸载 SMB 共享: {mount_key}")
        except Exception as e:
            logger.warning(f"卸载 SMB 共享失败: {mount_key}, {str(e)}")
    _mounted_shares = {}


def list_smb_shares(server: str, username: str = None, password: str = None) -> dict:
    """列出 SMB 服务器的所有共享

    Args:
        server: SMB 服务器地址（如 192.168.0.79 或 //192.168.0.79）
        username: SMB 用户名（可选，默认从配置读取）
        password: SMB 密码（可选，默认从配置读取）

    Returns:
        dict: {
            'success': bool,
            'shares': List[dict],  # [{'name': 'share1', 'type': 'Disk'}, ...]
            'message': str
        }
    """
    result = {
        'success': False,
        'shares': [],
        'message': ''
    }

    # 规范化服务器地址
    server = server.strip().strip('/').replace('\\', '/')
    if '/' in server:
        server = server.split('/')[0]

    # 从配置获取凭据
    domain = None
    if not username or not password:
        config_username, config_password, config_domain = _get_smb_credentials()
        if not username:
            username = config_username
        if not password:
            password = config_password
        if not domain:
            domain = config_domain

    # 检查凭据
    if not username or not password:
        result['message'] = 'SMB 凭据未配置，请在 .env 文件中设置 SMB_USERNAME 和 SMB_PASSWORD'
        return result

    # 使用 smbclient 列出共享
    try:
        cmd = ['smbclient', '-L', f'//{server}', '-U', f'{username}%{password}']
        if domain:
            cmd.extend(['-W', domain])

        logger.info(f"执行命令: smbclient -L //{server} -U {username}***")
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )

        if process.returncode != 0:
            result['message'] = f'连接服务器失败: {process.stderr}'
            return result

        # 解析输出
        lines = process.stdout.split('\n')
        shares = []
        in_share_list = False

        for line in lines:
            line = line.strip()
            # 查找共享列表的开始标记
            if 'Sharename' in line and 'Type' in line:
                in_share_list = True
                continue
            if in_share_list:
                # 跳过分隔线
                if line.startswith('---') or not line:
                    continue
                # 结束标记
                if line.startswith('Server') or line.startswith('Workgroup'):
                    break

                # 解析共享行: "Sharename  Type  Comment"
                parts = line.split()
                if len(parts) >= 2:
                    share_name = parts[0]
                    share_type = parts[1]

                    # 跳过空名称
                    if not share_name:
                        continue

                    # 只保留 Disk 类型（文件共享），过滤 IPC/Printer/disabled 等
                    if share_type != 'Disk':
                        continue

                    shares.append({
                        'name': share_name,
                        'type': share_type,
                        'full_path': f'//{server}/{share_name}'
                    })

        result['success'] = True
        result['shares'] = shares
        result['message'] = f'找到 {len(shares)} 个共享'

    except subprocess.TimeoutExpired:
        result['message'] = '连接服务器超时'
    except FileNotFoundError:
        result['message'] = 'smbclient 命令未找到，请安装 smbclient (apt install smbclient)'
    except Exception as e:
        result['message'] = f'列出共享失败: {str(e)}'

    return result
