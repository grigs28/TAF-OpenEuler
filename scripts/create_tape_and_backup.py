#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建磁带和一次性备份任务脚本
使用 psycopg3 原生连接
"""

import json
import sys
import os
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg


def create_tape_and_backup():
    """创建磁带和备份任务"""
    now = datetime.now()
    year = now.year
    month = now.month
    tape_label = f"TP{year}{month:02d}01"
    source_path = r"\\192.168.0.79\bak\sysbak\192.168.0.50\D"

    print(f"当前时间: {now}")
    print(f"磁带标签: {tape_label}")
    print(f"备份源: {source_path}")

    # 使用 psycopg3 原生连接
    conn = psycopg.connect(
        host='192.168.0.33',
        port=5560,
        dbname='backup_db',
        user='grigs',
        password='Slnwg123$'
    )
    conn.autocommit = True
    print("数据库连接成功!")

    try:
        cur = conn.cursor()

        # 检查磁带是否存在
        cur.execute(
            "SELECT tape_id, serial_number FROM tape_cartridges WHERE label = %s",
            (tape_label,)
        )
        existing_tape = cur.fetchone()

        serial_number = None
        if existing_tape:
            serial_number = existing_tape[1]
            print(f"磁带 {tape_label} 已存在, serial_number={serial_number}")
        else:
            # 计算序列号
            cur.execute(
                "SELECT COUNT(*) FROM tape_cartridges WHERE serial_number LIKE %s",
                (f"TP{month:02d}%",)
            )
            count_row = cur.fetchone()
            sequence = (count_row[0] if count_row else 0) + 1
            serial_number = f"TP{month:02d}{sequence:02d}"

            capacity_bytes = 18 * 1024 * (1024 ** 3)  # 18TB
            created_date = datetime(year, month, 1)

            # 计算过期日期（6个月后）
            expiry_year = created_date.year
            expiry_month = created_date.month + 6
            while expiry_month > 12:
                expiry_year += 1
                expiry_month -= 12
            expiry_date = datetime(expiry_year, expiry_month, 1)

            # 插入磁带
            cur.execute(
                """
                INSERT INTO tape_cartridges (
                    tape_id, label, status, media_type, generation,
                    serial_number, location, capacity_bytes, used_bytes,
                    retention_months, notes, manufactured_date, expiry_date,
                    auto_erase, health_score, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (tape_label, tape_label, "available", "LTO", 8,
                serial_number, "机房", capacity_bytes, 0,
                6, "通过脚本创建", created_date, expiry_date,
                True, 100, now, now)
            )
            print(f"磁带创建成功: {tape_label}, serial_number={serial_number}")

        # 创建备份任务模板
        task_name = f"备份-192.168.0.50-D"
        cur.execute(
            """
            INSERT INTO backup_tasks (
                task_name, task_type, status, is_template, source_paths,
                compression_enabled, encryption_enabled, retention_days,
                description, enable_simple_scan,
                created_at, updated_at, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (task_name, "full", "pending", True,
            json.dumps([source_path]),
            True, False, 180,
            f"一次性备份任务 - {source_path}",
            True, now, now, "script")
        )
        row = cur.fetchone()
        task_id = row[0]
        print(f"备份任务模板创建成功: ID={task_id}")

        # 创建 backup_files 分组
        table_name = f"backup_files_{task_id:06d}"
        cur.execute(
            "INSERT INTO backup_files_groups (table_name, task_id) VALUES (%s, %s) RETURNING id",
            (table_name, task_id)
        )
        row = cur.fetchone()
        group_id = row[0]

        cur.execute(
            "UPDATE backup_tasks SET backup_files_group_id = %s, backup_files_table = %s WHERE id = %s",
            (group_id, table_name, task_id)
        )

        # 创建物理表
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {table_name} (LIKE backup_files_template INCLUDING ALL)"
        )
        print(f"备份文件表 {table_name} 创建成功")

        # 创建一次性计划任务
        schedule_time = datetime.now() + timedelta(minutes=1)
        schedule_task_name = f"一次性-{task_name}"
        schedule_config = {"datetime": schedule_time.strftime("%Y-%m-%d %H:%M:%S")}
        task_metadata = {"backup_task_id": task_id}

        cur.execute(
            """
            INSERT INTO scheduled_tasks (
                task_name, description, schedule_type, schedule_config,
                action_type, action_config, status, enabled,
                tags, task_metadata, backup_task_id, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (schedule_task_name,
            f"一次性备份: {source_path}",
            "once",
            json.dumps(schedule_config),
            "backup",
            json.dumps({}),
            "active",
            True,
            json.dumps([]),
            json.dumps(task_metadata),
            task_id,
            now, now)
        )
        row = cur.fetchone()
        scheduled_task_id = row[0]
        print(f"计划任务创建成功: ID={scheduled_task_id}, 计划执行时间: {schedule_time}")

        cur.close()

        print("\n" + "=" * 60)
        print("创建完成!")
        print(f"磁带卷标: {tape_label}")
        print(f"磁带序列号: {serial_number}")
        print(f"备份任务模板ID: {task_id}")
        print(f"计划任务ID: {scheduled_task_id}")
        print(f"计划执行时间: {schedule_time}")

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    create_tape_and_backup()
