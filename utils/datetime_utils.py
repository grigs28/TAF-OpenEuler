#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
时间日期工具模块
DateTime Utility Module
统一处理所有时间日期相关的操作
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Union
import re

logger = logging.getLogger(__name__)


class DateTimeUtils:
    """时间日期工具类"""
    
    # 支持的日期格式列表（按优先级排序）
    DATE_FORMATS = [
        '%Y-%m-%d %H:%M:%S',      # 2025-11-05 01:45:00
        '%Y/%m/%d %H:%M:%S',      # 2025/11/05 01:45:00
        '%Y-%m-%d %H:%M',         # 2025-11-05 01:45
        '%Y/%m/%d %H:%M',         # 2025/11/05 01:45
        '%Y-%m-%dT%H:%M:%S',      # ISO格式: 2025-11-05T01:45:00
        '%Y-%m-%dT%H:%M:%S.%f',  # ISO格式（带微秒）: 2025-11-05T01:45:00.123456
        '%Y-%m-%dT%H:%M:%SZ',     # ISO格式（UTC）: 2025-11-05T01:45:00Z
        '%Y-%m-%dT%H:%M',         # ISO格式（不带秒）: 2025-11-05T01:45
        '%Y-%m-%d',               # 日期: 2025-11-05
        '%Y/%m/%d',               # 日期: 2025/11/05
    ]
    
    @staticmethod
    def parse_datetime(datetime_str: Union[str, None], default: Optional[datetime] = None) -> Optional[datetime]:
        """
        解析日期时间字符串，支持多种格式
        
        Args:
            datetime_str: 日期时间字符串
            default: 解析失败时的默认值
            
        Returns:
            datetime对象，如果解析失败返回default或None
        """
        if not datetime_str:
            return default
        
        # 转换为字符串
        datetime_str = str(datetime_str).strip()
        
        # 检查无效日期字符串
        datetime_lower = datetime_str.lower()
        invalid_patterns = ['invalid date', 'invalid', 'none', 'null', '', 'nan', 'undefined']
        
        # 检查是否完全匹配无效模式
        if datetime_lower in invalid_patterns:
            logger.warning(f"检测到无效日期字符串: {datetime_str}")
            return default
        
        # 检查是否包含 "invalid"（前端可能返回 "Invalid Date" 或 "Invalid Date:00"）
        if 'invalid' in datetime_lower:
            logger.warning(f"检测到无效日期字符串: {datetime_str}")
            return default
        
        # 检查是否以 "invalid" 开头（处理 "Invalid Date:00" 这种情况）
        if datetime_lower.startswith('invalid'):
            logger.warning(f"检测到无效日期字符串: {datetime_str}")
            return default
        
        # 尝试使用 fromisoformat（Python 3.7+）
        try:
            # 处理 ISO 格式（可能包含 Z 或 +00:00）
            iso_str = datetime_str.replace('Z', '+00:00')
            if 'T' in iso_str and '+' not in iso_str and '-' not in iso_str[10:]:
                # 简单 ISO 格式，尝试添加时区
                iso_str = iso_str + '+00:00'
            return datetime.fromisoformat(iso_str)
        except (ValueError, AttributeError):
            pass
        
        # 尝试各种格式
        for fmt in DateTimeUtils.DATE_FORMATS:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
        
        # 如果所有格式都失败，记录警告并返回默认值
        logger.warning(f"无法解析日期时间字符串: {datetime_str}，使用默认值")
        return default
    
    @staticmethod
    def format_datetime(dt: Optional[datetime], format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        """
        格式化日期时间为字符串
        
        Args:
            dt: datetime对象
            format_str: 格式字符串
            
        Returns:
            格式化后的字符串，如果dt为None返回空字符串
        """
        if dt is None:
            return ''
        
        try:
            return dt.strftime(format_str)
        except (ValueError, AttributeError) as e:
            logger.warning(f"格式化日期时间失败: {dt}, 错误: {str(e)}")
            return ''
    
    @staticmethod
    def format_for_display(dt: Optional[datetime], locale: str = 'zh-CN') -> str:
        """
        格式化日期时间用于显示（中文格式）
        
        Args:
            dt: datetime对象
            locale: 语言环境
            
        Returns:
            格式化后的字符串
        """
        if dt is None:
            return '未设置'
        
        if locale == 'zh-CN':
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def format_for_api(dt: Optional[datetime]) -> str:
        """
        格式化日期时间用于API（ISO格式）
        
        Args:
            dt: datetime对象
            
        Returns:
            ISO格式字符串
        """
        if dt is None:
            return ''
        
        return dt.isoformat()
    
    @staticmethod
    def format_for_database(dt: Optional[datetime]) -> str:
        """
        格式化日期时间用于数据库（标准格式）
        
        Args:
            dt: datetime对象
            
        Returns:
            数据库格式字符串: YYYY-MM-DD HH:MM:SS
        """
        if dt is None:
            return ''
        
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def format_for_frontend(dt: Optional[datetime], format_type: str = 'datetime-local') -> str:
        """
        格式化日期时间用于前端输入框
        
        Args:
            dt: datetime对象
            format_type: 格式类型
                - 'datetime-local': HTML5 datetime-local格式 (YYYY-MM-DDTHH:MM)
                - 'date': 日期格式 (YYYY-MM-DD)
                - 'time': 时间格式 (HH:MM)
                
        Returns:
            前端格式字符串
        """
        if dt is None:
            return ''
        
        if format_type == 'datetime-local':
            return dt.strftime('%Y-%m-%dT%H:%M')
        elif format_type == 'date':
            return dt.strftime('%Y-%m-%d')
        elif format_type == 'time':
            return dt.strftime('%H:%M')
        else:
            return dt.strftime('%Y-%m-%dT%H:%M')
    
    @staticmethod
    def parse_from_frontend(datetime_str: str, format_type: str = 'datetime-local') -> Optional[datetime]:
        """
        从前端格式解析日期时间
        
        Args:
            datetime_str: 前端日期时间字符串
            format_type: 格式类型
            
        Returns:
            datetime对象
        """
        if not datetime_str:
            return None
        
        datetime_str = str(datetime_str).strip()
        
        # 检查无效日期
        if 'invalid' in datetime_str.lower():
            logger.warning(f"从前端接收到无效日期: {datetime_str}")
            return None
        
        try:
            if format_type == 'datetime-local':
                # HTML5 datetime-local格式: YYYY-MM-DDTHH:MM
                if 'T' in datetime_str:
                    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M')
                else:
                    # 尝试空格分隔
                    return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
            elif format_type == 'date':
                return datetime.strptime(datetime_str, '%Y-%m-%d')
            elif format_type == 'time':
                # 时间格式，需要结合当前日期
                time_obj = datetime.strptime(datetime_str, '%H:%M').time()
                return datetime.combine(datetime.now().date(), time_obj)
            else:
                return DateTimeUtils.parse_datetime(datetime_str)
        except (ValueError, AttributeError) as e:
            logger.warning(f"解析前端日期时间失败: {datetime_str}, 格式: {format_type}, 错误: {str(e)}")
            return None
    
    @staticmethod
    def is_valid_datetime(dt: Optional[datetime]) -> bool:
        """
        检查日期时间是否有效
        
        Args:
            dt: datetime对象
            
        Returns:
            如果有效返回True，否则返回False
        """
        if dt is None:
            return False
        
        try:
            # 检查日期时间是否在合理范围内
            min_date = datetime(1900, 1, 1)
            max_date = datetime(2100, 12, 31)
            return min_date <= dt <= max_date
        except (ValueError, AttributeError):
            return False
    
    @staticmethod
    def now() -> datetime:
        """
        获取当前时间
        
        Returns:
            当前datetime对象
        """
        return datetime.now()
    
    @staticmethod
    def today() -> datetime:
        """
        获取今天的日期（时间设为00:00:00）
        
        Returns:
            今天的datetime对象
        """
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    @staticmethod
    def add_days(dt: datetime, days: int) -> datetime:
        """
        添加天数
        
        Args:
            dt: datetime对象
            days: 天数（可以为负数）
            
        Returns:
            新的datetime对象
        """
        return dt + timedelta(days=days)
    
    @staticmethod
    def add_hours(dt: datetime, hours: int) -> datetime:
        """
        添加小时数
        
        Args:
            dt: datetime对象
            hours: 小时数（可以为负数）
            
        Returns:
            新的datetime对象
        """
        return dt + timedelta(hours=hours)
    
    @staticmethod
    def add_minutes(dt: datetime, minutes: int) -> datetime:
        """
        添加分钟数
        
        Args:
            dt: datetime对象
            minutes: 分钟数（可以为负数）
            
        Returns:
            新的datetime对象
        """
        return dt + timedelta(minutes=minutes)
    
    @staticmethod
    def days_between(dt1: datetime, dt2: datetime) -> int:
        """
        计算两个日期之间的天数差
        
        Args:
            dt1: 第一个日期
            dt2: 第二个日期
            
        Returns:
            天数差（dt2 - dt1）
        """
        return (dt2 - dt1).days
    
    @staticmethod
    def hours_between(dt1: datetime, dt2: datetime) -> float:
        """
        计算两个日期之间的小时差
        
        Args:
            dt1: 第一个日期
            dt2: 第二个日期
            
        Returns:
            小时差（dt2 - dt1）
        """
        delta = dt2 - dt1
        return delta.total_seconds() / 3600
    
    @staticmethod
    def normalize_datetime_str(datetime_str: str) -> Optional[str]:
        """
        规范化日期时间字符串（转换为标准格式）
        
        Args:
            datetime_str: 日期时间字符串
            
        Returns:
            规范化后的字符串（YYYY-MM-DD HH:MM:SS），如果解析失败返回None
        """
        dt = DateTimeUtils.parse_datetime(datetime_str)
        if dt:
            return DateTimeUtils.format_for_database(dt)
        return None


# 便捷函数（全局可访问）
def parse_datetime(datetime_str: Union[str, None], default: Optional[datetime] = None) -> Optional[datetime]:
    """解析日期时间字符串"""
    return DateTimeUtils.parse_datetime(datetime_str, default)


def format_datetime(dt: Optional[datetime], format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
    """格式化日期时间"""
    return DateTimeUtils.format_datetime(dt, format_str)


def format_for_display(dt: Optional[datetime], locale: str = 'zh-CN') -> str:
    """格式化日期时间用于显示"""
    return DateTimeUtils.format_for_display(dt, locale)


def format_for_api(dt: Optional[datetime]) -> str:
    """格式化日期时间用于API"""
    return DateTimeUtils.format_for_api(dt)


def format_for_database(dt: Optional[datetime]) -> str:
    """格式化日期时间用于数据库"""
    return DateTimeUtils.format_for_database(dt)


def format_for_frontend(dt: Optional[datetime], format_type: str = 'datetime-local') -> str:
    """格式化日期时间用于前端"""
    return DateTimeUtils.format_for_frontend(dt, format_type)


def parse_from_frontend(datetime_str: str, format_type: str = 'datetime-local') -> Optional[datetime]:
    """从前端格式解析日期时间"""
    return DateTimeUtils.parse_from_frontend(datetime_str, format_type)


def is_valid_datetime(dt: Optional[datetime]) -> bool:
    """检查日期时间是否有效"""
    return DateTimeUtils.is_valid_datetime(dt)


def now() -> datetime:
    """获取当前时间"""
    return DateTimeUtils.now()


def today() -> datetime:
    """获取今天的日期"""
    return DateTimeUtils.today()


def normalize_datetime_str(datetime_str: str) -> Optional[str]:
    """规范化日期时间字符串"""
    return DateTimeUtils.normalize_datetime_str(datetime_str)


def month_offset(dt: datetime) -> int:
    """将日期转换为绝对月数（year*12 + month），用于月份差值计算"""
    return dt.year * 12 + dt.month


def month_diff(dt1: datetime, dt2: datetime) -> int:
    """计算两个日期之间的月份差（dt2 - dt1），自动处理跨年"""
    return month_offset(dt2) - month_offset(dt1)


def is_within_month_range(dt: datetime, center: datetime, offset: int = 1) -> bool:
    """
    判断 dt 是否在 center 的 ±offset 个月范围内

    Args:
        dt: 要判断的日期
        center: 中心日期
        offset: 允许的月份偏移量（默认1）

    Returns:
        True 如果在范围内

    示例:
        center=2026-03, offset=1 → 有效范围: 2026-02, 2026-03, 2026-04
        center=2026-01, offset=1 → 有效范围: 2025-12, 2026-01, 2026-02
    """
    return abs(month_offset(dt) - month_offset(center)) <= offset


def recent_months(count: int = 6, base_date: Optional[datetime] = None) -> list[str]:
    """
    生成最近 N 个月的 'YYYY-MM' 列表（从当月往前）

    Args:
        count: 月份数量
        base_date: 基准日期（默认当前日期）

    Returns:
        'YYYY-MM' 字符串列表，如 ['2026-03', '2026-02', '2026-01', '2025-12', ...]
    """
    if base_date is None:
        base_date = datetime.now()
    base = month_offset(base_date)
    result = []
    for i in range(count):
        total = base - i
        y, m = divmod(total - 1, 12)
        result.append(f"{y:04d}-{m + 1:02d}")
    return result

