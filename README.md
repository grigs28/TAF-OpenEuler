# TAF - 企业级磁带备份系统
# Enterprise Tape Backup System

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20openEuler-orange.svg)](https://www.openeuler.org/)
[![Database](https://img.shields.io/badge/database-openGauss-blue.svg)](https://opengauss.org/zh/)
[![Version](https://img.shields.io/badge/version-v0.2.3-brightgreen.svg)](CHANGELOG.md)

---

## 中文文档 | Chinese Documentation

<details>
<summary><b>项目简介</b></summary>

**TAF (Tape Archive File)** 是一个基于 Python 开发的企业级磁带备份系统，专为 **Linux** 平台设计，在 **openEuler** 上开发。系统提供智能备份策略、磁带生命周期管理、LTFS 文件系统支持和完整的 RESTful API。

### 核心特性

- **智能备份策略** - 支持完整备份、增量备份、差异备份、镜像备份、归档备份
- **多压缩算法** - Zstandard、PGZip、7-Zip、Tar 等多种压缩方法
- **磁带生命周期管理** - 自动管理磁带库存、格式化、擦除、过期检测
- **计划任务调度** - 支持 Cron 风格的定时备份任务
- **现代化 Web 界面** - 深色科技主题，响应式设计
- **openGauss 数据库** - 生产级数据库支持，原生 SQL 查询
- **高性能架构** - 异步处理、批量操作、连接池、内存数据库
- **钉钉/微信通知** - 实时推送备份状态、错误告警
- **LTFS 支持** - Linux LTFS 磁带文件系统
- **SMB 网络路径** - 支持 SMB/CIFS 网络共享备份

</details>

<details>
<summary><b>系统要求</b></summary>

| 组件 | 要求 | 说明 |
|------|------|------|
| **操作系统** | Linux (openEuler 22+ / Ubuntu 20.04+ / CentOS 7+) | 仅支持 Linux 平台 |
| **Python** | 3.8+ | 推荐使用 Conda 管理 |
| **数据库** | openGauss 5.x+ | 生产环境必需 |
| **内存** | 4GB+ | 推荐 8GB |
| **磁盘空间** | 50GB+ | 用于临时文件和日志 |
| **磁带设备** | LTO 驱动器 (SCSI 接口) | 支持 LTO-4 及以上 |

**可选组件**：
- Redis - 高性能任务存储和缓存
- LTFS - 磁带文件系统支持
- 7-Zip - 7z 压缩格式支持

</details>

<details>
<summary><b>快速开始</b></summary>

```bash
# 1. 克隆项目
git clone https://github.com/yourusername/TAF.git
cd TAF

# 2. 创建 Python 环境
conda create -n taf python=3.9
conda activate taf

# 3. 安装依赖
pip install -r requirements.txt
pip install aiosqlite zstandard

# 4. 配置 openGauss 数据库
sudo su - omm
gsql -d postgres
CREATE DATABASE taf_backup;
CREATE USER taf_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE taf_backup TO taf_user;
ALTER USER taf_user WITH SYSID;

# 5. 配置环境变量
cp .env.sample .env
# 编辑 .env 文件设置数据库连接等配置

# 6. 启动系统
python main.py

# 7. 访问 Web 界面
# http://localhost:8080
```

</details>

<details>
<summary><b>环境变量配置</b></summary>

```ini
# openGauss 数据库配置
DATABASE_URL=opengauss://taf_user:password@localhost:5432/taf_backup
DB_HOST=localhost
DB_PORT=5432
DB_USER=taf_user
DB_PASSWORD=password
DB_DATABASE=taf_backup

# 连接池配置
DB_POOL_SIZE=40
DB_MAX_OVERFLOW=80
DB_POOL_TIMEOUT=30.0

# Web 服务配置
WEB_PORT=8080
WEB_HOST=0.0.0.0

# 压缩配置
COMPRESSION_METHOD=zstd
COMPRESSION_THREADS=4

# 磁带设备配置 (Linux)
TAPE_DEVICE_PATH=/dev/nst0
SG_DEVICE_PATH=/dev/sg2

# LTFS 配置
LTFS_BINARY_PATH=/usr/local/bin/ltfs
LTFS_MOUNT_POINT=/mnt/ltfs

# 钉钉通知配置
DINGTALK_API_URL=http://localhost:5555
DINGTALK_API_KEY=your-api-key
DINGTALK_DEFAULT_PHONE=13800000000

# 微信通知配置 (可选)
WECHAT_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/web/send
WECHAT_ENABLED=false
WECHAT_REPORT_INTERVAL=30

# SMB/CIFS 网络路径配置
SMB_USERNAME=administrator
SMB_PASSWORD=your-password
SMB_DOMAIN=DOMAIN
SMB_MOUNT_BASE=/mnt/smb
```

</details>

<details>
<summary><b>项目结构</b></summary>

```
TAF/
├── main.py                      # 主程序入口
├── requirements.txt             # Python 依赖包
├── .env.sample                  # 环境配置模板
├── CHANGELOG.md                 # 版本更新日志
├── CLAUDE.md                    # Claude AI 开发指导
│
├── config/                      # 配置管理
│   ├── settings.py              # 系统配置类
│   ├── database.py              # 数据库连接管理
│   ├── database_init.py         # 数据库初始化
│   ├── redis_db.py              # Redis 连接管理
│   └── config_manager.py        # 配置管理器
│
├── models/                      # 数据模型
│   ├── backup.py                # 备份任务、备份集模型
│   ├── tape.py                  # 磁带模型
│   ├── scheduled_task.py        # 计划任务模型
│   ├── user.py                  # 用户模型
│   ├── system_log.py            # 系统日志模型
│   ├── system_config.py         # 系统配置模型
│   ├── data_classes.py          # 数据类定义
│   ├── notification_user.py     # 通知用户模型
│   └── base.py                  # 基础模型类
│
├── backup/                      # 备份处理模块
│   ├── backup_engine.py         # 备份引擎（主控制器）
│   ├── compressor.py            # 压缩处理器
│   ├── compression_worker.py    # 并行压缩工作线程
│   ├── backup_db.py             # 备份数据库操作
│   ├── file_scanner.py          # 文件扫描器
│   ├── tape_handler.py          # 磁带处理器
│   ├── memory_db_writer.py      # 内存数据库写入器
│   ├── file_group_prefetcher.py # 文件分组预取器
│   ├── final_dir_monitor.py     # 最终目录监控器
│   ├── backup_scanner.py        # 备份扫描协调
│   ├── backup_task_manager.py   # 备份任务管理
│   ├── backup_notifier.py       # 备份通知器
│   ├── concurrent_dir_scanner.py # 并行目录扫描
│   ├── sequential_dir_scanner.py # 顺序目录扫描
│   ├── file_move_worker.py      # 文件移动工作线程
│   └── utils.py                 # 备份工具函数
│
├── tape/                        # 磁带管理模块
│   ├── tape_manager.py          # 磁带管理器
│   ├── tape_operations.py       # 磁带操作
│   └── tape_cartridge.py        # 磁带盒类
│
├── utils/                       # 工具模块
│   ├── scheduler/               # 计划任务调度器
│   │   ├── scheduler.py         # 任务调度器
│   │   ├── task_storage.py      # 任务存储（openGauss）
│   │   ├── task_executor.py     # 任务执行器
│   │   ├── task_status_checker.py # 任务状态检查
│   │   ├── task_unlocker.py     # 任务锁释放
│   │   ├── schedule_calculator.py # 调度计算器
│   │   ├── action_handlers.py   # 动作处理器
│   │   ├── db_utils.py          # 数据库工具函数
│   │   └── redis_task_storage.py # Redis 任务存储
│   ├── opengauss/               # openGauss 相关
│   │   └── guard.py            # openGauss 连接守护
│   ├── linux_tape.py            # Linux 原生磁带操作
│   ├── libltfs_wrapper.py       # LTFS 包装器
│   ├── tape_tools.py            # 磁带工具集
│   ├── dingtalk_notifier.py     # 钉钉通知器
│   ├── wechat_notifier.py       # 微信通知器
│   ├── network_path.py          # 网络路径处理
│   ├── log_utils.py             # 日志工具
│   ├── datetime_utils.py        # 日期时间工具
│   └── production_guard.py      # 生产环境保护
│
├── recovery/                    # 恢复模块
│   └── recovery_engine.py       # 恢复引擎
│
├── web/                         # Web 应用
│   ├── app.py                   # FastAPI 应用入口
│   ├── api/                     # RESTful API
│   │   ├── backup/              # 备份管理 API
│   │   │   ├── operations.py    # 备份操作
│   │   │   ├── sets.py          # 备份集
│   │   │   ├── tasks_*.py       # 任务 CRUD
│   │   │   └── backup_statistics.py
│   │   ├── tape/                # 磁带管理 API
│   │   │   ├── device.py        # 设备管理
│   │   │   ├── operations.py    # 磁带操作
│   │   │   ├── label.py         # 标签管理
│   │   │   ├── tape_*.py        # 磁带 CRUD
│   │   │   └── tape_statistics.py
│   │   ├── scheduler.py         # 计划任务 API
│   │   ├── system/              # 系统管理 API
│   │   │   ├── database.py      # 数据库配置
│   │   │   ├── logs.py          # 日志查询
│   │   │   ├── statistics.py    # 系统统计
│   │   │   ├── notification.py  # 通知配置
│   │   │   ├── env_config.py    # 环境配置
│   │   │   └── file_system.py   # 文件系统
│   │   ├── recovery.py          # 恢复管理 API
│   │   ├── wechat.py            # 微信 API
│   │   └── tools.py             # 工具 API
│   ├── middleware/              # 中间件
│   ├── templates/               # HTML 模板
│   └── static/                  # 静态资源（CSS/JS）
│
├── scripts/                     # 脚本工具
├── services/                    # 服务模块
├── tests/                       # 测试用例
├── docs/                        # 文档目录
├── logs/                        # 日志目录
└── temp/                        # 临时文件目录
    ├── backup/                  # 备份临时目录
    ├── compress/                # 压缩临时目录
    ├── output/                  # 输出目录
    └── recovery/                # 恢复临时目录
```

</details>

<details>
<summary><b>API 文档</b></summary>

系统提供完整的 RESTful API，启动后可访问交互式文档：

- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc

**主要 API 端点**：

| 方法 | 端点 | 说明 |
|------|------|------|
| GET/POST | `/api/backup/tasks` | 备份任务 CRUD |
| POST | `/api/backup/tasks/{id}/run` | 立即执行任务 |
| GET | `/api/recovery/sets` | 搜索备份集 |
| POST | `/api/recovery/restore` | 创建恢复任务 |
| GET/POST | `/api/tape/*` | 磁带管理操作 |
| GET/POST | `/api/scheduler/tasks` | 计划任务管理 |
| GET | `/api/system/statistics` | 系统统计信息 |

</details>

<details>
<summary><b>运行测试</b></summary>

```bash
# 运行所有测试
pytest -v

# 运行特定测试文件
pytest tests/test_backup.py -v

# 生成覆盖率报告
pytest --cov=. --cov-report=html
```

</details>

<details>
<summary><b>常见问题</b></summary>

**Q: 数据库连接失败？**
```bash
# 检查 openGauss 状态
gs_ctl status -D /path/to/data

# 测试连接
gsql -d taf_backup -U taf_user -h localhost
```

**Q: 磁带设备无法识别？**
```bash
# 扫描 SCSI 设备
lsscsi -g

# 查看磁带设备
ls -la /dev/nst* /dev/sg*

# 设置权限
sudo chmod 666 /dev/nst0 /dev/sg2
```

**Q: LTFS 挂载失败？**
```bash
# 检查 LTFS 安装
which mkltfs ltfs

# 检查磁带状态
mt -f /dev/nst0 status
```

</details>

---

## English Documentation

<details>
<summary><b>Project Overview</b></summary>

**TAF (Tape Archive File)** is an enterprise-level tape backup system built in Python, designed exclusively for **Linux** platforms and developed on **openEuler**. The system provides intelligent backup strategies, tape lifecycle management, LTFS filesystem support, and comprehensive RESTful APIs.

### Key Features

- **Intelligent Backup Strategies** - Full, Incremental, Differential, Mirror, Archive backups
- **Multiple Compression Algorithms** - Zstandard, PGZip, 7-Zip, Tar
- **Tape Lifecycle Management** - Automated inventory, format, erase, expiration
- **Scheduled Task Management** - Cron-style scheduled backups
- **Modern Web Interface** - Dark tech-themed, responsive design
- **openGauss Database** - Production-grade database with native SQL
- **High Performance** - Async processing, batch operations, connection pooling
- **DingTalk/WeChat Notifications** - Real-time backup status and alerts
- **LTFS Support** - Linux LTFS tape filesystem
- **SMB Network Paths** - Backup from SMB/CIFS network shares

</details>

<details>
<summary><b>System Requirements</b></summary>

| Component | Requirement | Notes |
|-----------|-------------|-------|
| **OS** | Linux (openEuler 22+ / Ubuntu 20.04+ / CentOS 7+) | Linux only |
| **Python** | 3.8+ | Conda recommended |
| **Database** | openGauss 5.x+ | Required for production |
| **Memory** | 4GB+ | 8GB recommended |
| **Disk Space** | 50GB+ | For temp files and logs |
| **Tape Drive** | LTO Drive (SCSI) | LTO-4 and above |

**Optional Components**:
- Redis - High-performance task storage and caching
- LTFS - Tape filesystem support
- 7-Zip - 7z compression format support

</details>

<details>
<summary><b>Quick Start</b></summary>

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/TAF.git
cd TAF

# 2. Create Python environment
conda create -n taf python=3.9
conda activate taf

# 3. Install dependencies
pip install -r requirements.txt
pip install aiosqlite zstandard

# 4. Configure openGauss database
sudo su - omm
gsql -d postgres
CREATE DATABASE taf_backup;
CREATE USER taf_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE taf_backup TO taf_user;
ALTER USER taf_user WITH SYSID;

# 5. Configure environment variables
cp .env.sample .env
# Edit .env file to set database connection and other configs

# 6. Start the system
python main.py

# 7. Access Web UI
# http://localhost:8080
```

</details>

<details>
<summary><b>Environment Variables</b></summary>

```ini
# openGauss Database Configuration
DATABASE_URL=opengauss://taf_user:password@localhost:5432/taf_backup
DB_HOST=localhost
DB_PORT=5432
DB_USER=taf_user
DB_PASSWORD=password
DB_DATABASE=taf_backup

# Connection Pool Configuration
DB_POOL_SIZE=40
DB_MAX_OVERFLOW=80
DB_POOL_TIMEOUT=30.0

# Web Service Configuration
WEB_PORT=8080
WEB_HOST=0.0.0.0

# Compression Configuration
COMPRESSION_METHOD=zstd
COMPRESSION_THREADS=4

# Tape Device Configuration (Linux)
TAPE_DEVICE_PATH=/dev/nst0
SG_DEVICE_PATH=/dev/sg2

# LTFS Configuration
LTFS_BINARY_PATH=/usr/local/bin/ltfs
LTFS_MOUNT_POINT=/mnt/ltfs

# DingTalk Notification Configuration
DINGTALK_API_URL=http://localhost:5555
DINGTALK_API_KEY=your-api-key
DINGTALK_DEFAULT_PHONE=13800000000

# WeChat Notification Configuration (Optional)
WECHAT_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/web/send
WECHAT_ENABLED=false
WECHAT_REPORT_INTERVAL=30

# SMB/CIFS Network Path Configuration
SMB_USERNAME=administrator
SMB_PASSWORD=your-password
SMB_DOMAIN=DOMAIN
SMB_MOUNT_BASE=/mnt/smb
```

</details>

<details>
<summary><b>Project Structure</b></summary>

```
TAF/
├── main.py                      # Main entry point
├── requirements.txt             # Python dependencies
├── .env.sample                  # Environment config template
├── CHANGELOG.md                 # Version changelog
├── CLAUDE.md                    # Claude AI development guide
│
├── config/                      # Configuration management
│   ├── settings.py              # System settings
│   ├── database.py              # Database connection manager
│   ├── database_init.py         # Database initialization
│   ├── redis_db.py              # Redis connection manager
│   └── config_manager.py        # Configuration manager
│
├── models/                      # Data models
│   ├── backup.py                # Backup task and set models
│   ├── tape.py                  # Tape models
│   ├── scheduled_task.py        # Scheduled task models
│   ├── user.py                  # User models
│   ├── system_log.py            # System log models
│   ├── system_config.py         # System config models
│   ├── data_classes.py          # Data class definitions
│   ├── notification_user.py     # Notification user models
│   └── base.py                  # Base model class
│
├── backup/                      # Backup processing module
│   ├── backup_engine.py         # Backup engine (main controller)
│   ├── compressor.py            # Compression processor
│   ├── compression_worker.py    # Parallel compression worker
│   ├── backup_db.py             # Backup database operations
│   ├── file_scanner.py          # File scanner
│   ├── tape_handler.py          # Tape handler
│   ├── memory_db_writer.py      # In-memory database writer
│   ├── file_group_prefetcher.py # File group prefetcher
│   ├── final_dir_monitor.py     # Final directory monitor
│   ├── backup_scanner.py        # Backup scan coordinator
│   ├── backup_task_manager.py   # Backup task manager
│   ├── backup_notifier.py       # Backup notifier
│   ├── concurrent_dir_scanner.py # Parallel directory scanner
│   ├── sequential_dir_scanner.py # Sequential directory scanner
│   ├── file_move_worker.py      # File move worker
│   └── utils.py                 # Backup utility functions
│
├── tape/                        # Tape management module
│   ├── tape_manager.py          # Tape manager
│   ├── tape_operations.py       # Tape operations
│   └── tape_cartridge.py        # Tape cartridge class
│
├── utils/                       # Utility modules
│   ├── scheduler/               # Scheduled task scheduler
│   │   ├── scheduler.py         # Task scheduler
│   │   ├── task_storage.py      # Task storage (openGauss)
│   │   ├── task_executor.py     # Task executor
│   │   ├── task_status_checker.py # Task status checker
│   │   ├── task_unlocker.py     # Task lock releaser
│   │   ├── schedule_calculator.py # Schedule calculator
│   │   ├── action_handlers.py   # Action handlers
│   │   ├── db_utils.py          # Database utility functions
│   │   └── redis_task_storage.py # Redis task storage
│   ├── opengauss/               # openGauss related
│   │   └── guard.py            # openGauss connection guard
│   ├── linux_tape.py            # Linux native tape operations
│   ├── libltfs_wrapper.py       # LTFS wrapper
│   ├── tape_tools.py            # Tape tools collection
│   ├── dingtalk_notifier.py     # DingTalk notifier
│   ├── wechat_notifier.py       # WeChat notifier
│   ├── network_path.py          # Network path handler
│   ├── log_utils.py             # Logging utilities
│   ├── datetime_utils.py        # Date/time utilities
│   └── production_guard.py      # Production environment guard
│
├── recovery/                    # Recovery module
│   └── recovery_engine.py       # Recovery engine
│
├── web/                         # Web application
│   ├── app.py                   # FastAPI application entry
│   ├── api/                     # RESTful API
│   │   ├── backup/              # Backup management API
│   │   │   ├── operations.py    # Backup operations
│   │   │   ├── sets.py          # Backup sets
│   │   │   ├── tasks_*.py       # Task CRUD
│   │   │   └── backup_statistics.py
│   │   ├── tape/                # Tape management API
│   │   │   ├── device.py        # Device management
│   │   │   ├── operations.py    # Tape operations
│   │   │   ├── label.py         # Label management
│   │   │   ├── tape_*.py        # Tape CRUD
│   │   │   └── tape_statistics.py
│   │   ├── scheduler.py         # Scheduled task API
│   │   ├── system/              # System management API
│   │   │   ├── database.py      # Database config
│   │   │   ├── logs.py          # Log query
│   │   │   ├── statistics.py    # System statistics
│   │   │   ├── notification.py  # Notification config
│   │   │   ├── env_config.py    # Environment config
│   │   │   └── file_system.py   # File system
│   │   ├── recovery.py          # Recovery management API
│   │   ├── wechat.py            # WeChat API
│   │   └── tools.py             # Tools API
│   ├── middleware/              # Middleware
│   ├── templates/               # HTML templates
│   └── static/                  # Static resources (CSS/JS)
│
├── scripts/                     # Script utilities
├── services/                    # Service modules
├── tests/                       # Test cases
├── docs/                        # Documentation
├── logs/                        # Log directory
└── temp/                        # Temporary files directory
    ├── backup/                  # Backup temp directory
    ├── compress/                # Compression temp directory
    ├── output/                  # Output directory
    └── recovery/                # Recovery temp directory
```

</details>

<details>
<summary><b>API Documentation</b></summary>

The system provides complete RESTful APIs with interactive documentation:

- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc

**Main API Endpoints**:

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET/POST | `/api/backup/tasks` | Backup task CRUD |
| POST | `/api/backup/tasks/{id}/run` | Execute task immediately |
| GET | `/api/recovery/sets` | Search backup sets |
| POST | `/api/recovery/restore` | Create recovery task |
| GET/POST | `/api/tape/*` | Tape management operations |
| GET/POST | `/api/scheduler/tasks` | Scheduled task management |
| GET | `/api/system/statistics` | System statistics |

</details>

<details>
<summary><b>Running Tests</b></summary>

```bash
# Run all tests
pytest -v

# Run specific test file
pytest tests/test_backup.py -v

# Generate coverage report
pytest --cov=. --cov-report=html
```

</details>

<details>
<summary><b>Troubleshooting</b></summary>

**Q: Database connection failed?**
```bash
# Check openGauss status
gs_ctl status -D /path/to/data

# Test connection
gsql -d taf_backup -U taf_user -h localhost
```

**Q: Tape device not detected?**
```bash
# Scan SCSI devices
lsscsi -g

# List tape devices
ls -la /dev/nst* /dev/sg*

# Set permissions
sudo chmod 666 /dev/nst0 /dev/sg2
```

**Q: LTFS mount failed?**
```bash
# Check LTFS installation
which mkltfs ltfs

# Check tape status
mt -f /dev/nst0 status
```

</details>

---

## 技术架构 | Architecture

### 分层架构 | Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Web Layer (FastAPI + Jinja2)                          │
├─────────────────────────────────────────────────────────┤
│  Business Logic Layer (Backup, Recovery, Scheduler)    │
├─────────────────────────────────────────────────────────┤
│  Data Access Layer (Native SQL + Connection Pool)      │
├─────────────────────────────────────────────────────────┤
│  Hardware Layer (LTFS, SCSI, Tape Drive)               │
└─────────────────────────────────────────────────────────┘
```

### 备份数据流 | Backup Data Flow

```
Source Files → FileScanner → MemoryDBWriter (in-memory SQLite)
                                    ↓
                        Batch sync to openGauss
                                    ↓
        CompressionWorker ← FileGroupPrefetcher
                ↓
          Compressor (zstd/pgzip)
                ↓
    temp/output/ → FinalDirMonitor → TapeHandler → Tape
```

### 关键技术 | Key Technologies

- **FastAPI** - 现代化 Web 框架 | Modern web framework
- **openGauss** - 企业级数据库 | Enterprise-grade database
- **asyncpg** - 异步 PostgreSQL 连接 | Async PostgreSQL adapter
- **LTFS** - 磁带文件系统 | Tape filesystem
- **Zstandard** - 高性能压缩 | High-performance compression

---

## 开发指南 | Development Guide

### 添加压缩方法 | Add Compression Method

1. 在 `backup/compressor.py` 添加压缩函数 | Add compression function
2. 在 `config/settings.py` 添加配置 | Add configuration
3. 更新 `recovery/recovery_engine.py` 支持解压 | Update recovery engine

### 添加 API 端点 | Add API Endpoint

1. 在 `web/api/` 创建/修改文件 | Create/modify file
2. 定义 FastAPI 路由和模型 | Define route and models
3. 在 `web/app.py` 注册路由 | Register route

---

## 版本历史 | Version History

当前版本 | Current Version: **v0.2.3**

详见 | See [CHANGELOG.md](CHANGELOG.md)

---

## 许可证 | License

MIT License - 详见 | See [LICENSE](LICENSE)

---

## 贡献 | Contributing

欢迎贡献！请遵循 | Welcome contributions! Please follow:

1. Fork 项目 | Fork the project
2. 创建特性分支 | Create feature branch
3. 提交更改 | Commit changes
4. 推送到分支 | Push to branch
5. 创建 Pull Request | Create Pull Request

---

## 联系方式 | Contact

- **项目地址 | Repository**: https://github.com/yourusername/TAF
- **问题反馈 | Issues**: GitHub Issues
- **开发平台 | Development Platform**: openEuler 22.03 LTS

---

## 致谢 | Acknowledgments

- **openEuler** - 企业级 Linux 操作系统 | Enterprise Linux OS
- **openGauss** - 企业级数据库 | Enterprise database
- **FastAPI** - 现代化 Web 框架 | Modern web framework
- **LTFS** - 磁带文件系统 | Tape filesystem

---

<div align="center">

**TAF - Enterprise Tape Backup System for Linux**

*Designed on openEuler, Built for openGauss*

在 openEuler 上设计，为 openGauss 构建

</div>
# TAF-OpenEuler
