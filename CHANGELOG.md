# 更新日志

## [0.6.0] - 2026-04-02

### 改进

#### 系统稳定性
- 优化备份引擎和压缩管线，提升大文件处理性能
- 统一钉钉通知入口，所有通知走 `utils/notify.py:notify()`
- 工具页面代码质量审查和修复
- 清理无效 `.env` 参数（36项），精简配置模板

#### 安全与运维
- 添加单实例启动检测，防止重复进程
- 磁带弹出操作增加钉钉通知
- 设备路径校验，只允许 `/dev/sg*/nst*/st*` 格式

---

## [0.2.3] - 2025-12-08

### 改进

#### 代码优化和修复
- ✅ 优化备份引擎和磁带处理逻辑
- ✅ 改进调度器任务执行和错误处理
- ✅ 优化前端界面和用户体验
- ✅ 更新依赖包版本

---

## [0.2.2] - 2025-12-06

### 新增

#### 任务集完成钉钉通知
- ✅ `backup/backup_engine.py` 添加任务集完成钉钉通知
  - 当所有条件满足时（扫描完成、预取完成、压缩完成、文件移动到final目录完成）发送钉钉通知
  - 通知内容包含：备份名称、文件数量、总大小、耗时、备份速度（GB/小时）
  - 自动计算任务耗时并格式化显示（秒/分钟/小时）
  - 自动计算备份速度（GB/小时），保留2位小数

#### 立即运行失败弹窗提醒
- ✅ `web/static/js/modules/system/scheduler-api.js` 添加失败检查
  - 检查API返回的 `success` 字段，如果为 `false` 则弹窗提示
  - 统一错误处理，确保所有失败情况都能弹窗提醒
- ✅ `web/static/js/pages/backup.js` 添加失败检查
  - 在 `runSchedulerTask` 函数中检查 `success` 字段
  - 确保备份页面和调度器页面都能正确显示错误信息

#### 任务锁获取失败弹窗提醒
- ✅ `utils/scheduler/task_executor.py` 手动运行时获取锁失败抛出异常
  - 如果是手动运行且获取任务锁失败，抛出 `RuntimeError` 异常
  - 异常信息：`"任务已在执行中，无法重复运行"`
- ✅ `utils/scheduler/scheduler.py` 捕获锁获取失败异常
  - 在创建后台任务后等待0.1秒检查任务是否因锁失败而立即退出
  - 如果检测到锁失败异常，从运行列表中移除并重新抛出异常
- ✅ `web/api/scheduler.py` 返回明确的错误信息
  - 捕获 `RuntimeError` 异常，如果是锁获取失败，返回 HTTP 409 状态码
  - 错误信息：`"任务已在执行中，无法重复运行"`

### 改进

#### 压缩并行批次数量动态调整
- ✅ `backup/compression_worker.py` 根据扫描状态动态调整并行批次数量
  - 扫描阶段（`scan_status` 不是 `"completed"`）：并行批次数量 -1，减少同时运行的压缩任务数
  - 扫描结束后（`scan_status == "completed"`）：恢复为配置的并行批次数量
  - 在多个关键位置检查扫描状态：等待文件组时、启动任务后、超时异常时、每10次循环时
  - 确保扫描完成后能及时恢复并行批次数量，充分利用系统资源

#### 钉钉通知增强
- ✅ `utils/dingtalk_notifier.py` 添加备份速度显示
  - 在备份完成通知中添加"备份速度"字段（GB/小时）
  - 自动计算并格式化显示备份速度

---

## [0.2.1] - 2025-12-06

### 新增

#### 压缩文件名唯一性保证
- ✅ `backup/compressor.py` 添加压缩文件名记录机制
  - 在 `Compressor` 类中添加 `_last_archive_info` 字典，记录每个备份集的上次压缩文件名和序号
  - 生成文件名时自动检查文件是否已存在，如果存在则增加序号（格式：`backup_{set_id}_{timestamp}_{序号:04d}.tar.zst`）
  - 最多尝试 1000 次生成唯一文件名，如果仍失败则使用包含微秒的精确时间戳
  - 记录本次使用的文件名和序号，便于追踪和调试
  - 日志中显示文件名、序号和上次文件名信息

### 改进

#### zstd 线程数动态调整优化
- ✅ `backup/compressor.py` 优化 zstd 线程数控制逻辑
  - 修复线程数调整未生效的问题：调整逻辑从 `compression_threads` 改为直接调整 `zstd_threads`
  - 扫描阶段（`scan_status` 不是 `"completed"`）：线程数 -1，减少与扫描的 IO 竞争
  - 扫描结束后（`scan_status == "completed"`）：恢复正常线程数，充分利用系统资源
  - 添加详细的日志记录，显示扫描状态和线程数调整情况

---

## [0.2.0] - 2025-12-04

### 新增

#### 文件组预取器去重机制
- ✅ `backup/file_group_prefetcher.py` 添加去重逻辑
  - 对 `fetch_pending_files_grouped_by_size` 返回的文件组进行去重处理
  - 相同 `file_path` 只保留第一个，重复部分不计入容量和文件数
  - 所有队列统计（`queued_files_count`、`total_queued_files_count`、`total_queued_size`）使用去重后的值
  - 日志中明确显示去重信息：`去重（数量）` 格式，即使没有重复也显示 `去重（0）`

#### 查询条件统一优化
- ✅ `backup/backup_db.py` 统一查询条件
  - 将所有 `is_copy_success = FALSE OR is_copy_success IS NULL` 替换为 `is_copy_success IS DISTINCT FROM TRUE`
  - 语义一致但更简洁，与更新条件保持一致
  - 确保查询和更新逻辑的一致性

#### 文件处理统计增强
- ✅ `backup/backup_db.py` 添加本次文件累计统计
  - 在 `fetch_pending_files_grouped_by_size` 中添加 `total_files_processed` 累计变量
  - 日志中显示"本次文件累计"信息，便于跟踪本次函数调用处理的文件数
  - 累计数是去重后的文件数，确保统计准确性

#### 测试工具
- ✅ 新增 `tests/check_duplicate_paths_in_table.py`
  - 检查指定表中是否有重复路径
  - 先全部加载到内存，然后在内存中快速统计重复路径
  - 支持按重复次数分组统计和状态分布分析
- ✅ 新增 `tests/analyze_duplicate_paths_in_prefetcher.py`
  - 分析文件组预取器为什么会有重复路径
  - 模拟查询逻辑，检查查询结果中的重复情况
  - 分析数据库中所有记录的重复情况和混合状态

### 改进

#### 文件组预取器去重与日志优化
- ✅ `backup/file_group_prefetcher.py`
  - 在主循环、全库扫描后重新检索、全库扫描后再次调用等所有位置添加去重逻辑
  - 日志格式统一为 `去重（数量）`，替代之前的 `（去重后）` 格式
  - 确保重复路径不会影响文件数和容量统计

#### 数据库查询优化
- ✅ `backup/backup_db.py`
  - `fetch_pending_files_grouped_by_size` 在内存中去重，相同 `file_path` 只保留 `id` 最小的记录
  - 添加重复路径统计和日志输出，便于诊断问题
  - 优化查询条件，使用 `IS DISTINCT FROM TRUE` 提高可读性

#### 恢复引擎修复
- ✅ `recovery/recovery_engine.py`
  - 修复 `get_top_level_directories` 和 `get_directory_contents` 中的硬编码表名问题
  - 使用 `get_backup_files_table_by_set_id` 动态获取正确的表名（`backup_files_xxx`）
  - 解决 openGauss 模式下 `relation "backup_files" does not exist` 错误

### 修复

#### 事务提交与验证
- ✅ `backup/backup_db.py`
  - 确保 `mark_files_as_queued` 中每批次更新后都执行 `commit()`
  - 添加二次校验逻辑，确认 `is_copy_success` 已成功设置为 `TRUE`
  - 如果校验失败，自动重试一次批量更新

---

## [0.1.35] - 2025-12-04

### 新增

#### 简洁扫描直写模式（openGauss）
- ✅ 新增 `backup/simple_scanner.py` 简洁扫描器
  - 完全对齐 `test_scan_direct_write.py` 的扫描与写库流程
  - 使用 `os.scandir` 顺序扫描 + 单连接同步批量 `INSERT`
  - 分表名从内存读取（`backup_task.backup_files_table`），必要时回退数据库查询一次
  - 单线程同步模式，扫描和写入在同一循环中完成，便于排查与压测
  - 详细统计扫描/写入/排除/错误等指标并输出日志

#### 压缩并行批次与进度聚合（openGauss 模式）
- ✅ 为 openGauss 模式引入压缩并行批次配置
  - 设置项 `COMPRESSION_PARALLEL_BATCHES`（默认 2），预取队列容量为 `parallel_batches + 1`
  - 系统配置 API 与前端新增对应表单项，支持在“系统配置 → 常规/备份策略”中调整
- ✅ `FileGroupPrefetcher` 并发预取增强
  - 使用 `asyncio.Queue(maxsize = parallel_batches + 1)` 管理文件组
  - 精细统计：当前队列文件数、累计入队文件数、累计字节数、预取循环次数等
  - 针对“扫描完成但不足一组”的场景，增加全库补偿扫描与遗漏文件重检逻辑
  - 提前为入队文件设置 `is_copy_success = TRUE`，避免重复检索和状态不一致
- ✅ `CompressionWorker` 并行压缩与进度聚合
  - openGauss 模式下按 `parallel_batches` 控制同时运行的压缩任务数
  - 为每个批次维护 `compress_progress` 字典，并在内存中聚合多任务进度
  - 新增 `get_aggregated_compression_progress()`，返回聚合进度和各任务明细列表
  - 进度更新任务 `_update_compression_progress_periodically()` 改为优先使用内存统计，
    在 openGauss 模式下不再强依赖数据库统计结果

#### openGauss 任务调度与运行记录
- ✅ `utils/scheduler/task_storage.py`
  - 为 openGauss 增加 `task_runs` 运行记录表的创建与读写逻辑（`record_run_start` / `record_run_end`）
  - 使用原生 SQL + `JSONB` 存储任务执行结果与错误信息
  - 新增基于表 `task_locks` 的任务并发锁实现，支持 `is_active` 字段与自动迁移
  - 修复异常路径下事务未提交 / 未回滚导致的长事务锁表问题（显式 `commit()` / `rollback()`）
  - 关键修复：获取锁失败时统一返回 `False`，防止任务在锁获取失败时仍然继续执行
- ✅ `utils/scheduler/task_executor.py`
  - 执行前先通过 `acquire_task_lock` 获取任务锁，失败时记录系统日志并直接跳过执行
  - openGauss 模式下所有状态更新（`RUNNING` / `ACTIVE` / `ERROR` 等）统一使用原生 SQL，
    并在每次执行后显式 `commit()` 与校验 `transaction_status`
  - 执行成功/失败路径分别更新 `total_runs`/`success_runs`/`failure_runs`/`average_duration`，
    并重新计算 `next_run_time`
  - 极大增强任务执行前后日志：输出执行 ID、耗时、统计信息以及详细错误堆栈

#### 环境与系统配置扩展
- ✅ `web/api/system/env_config.py` / `web/static/js/modules/system/system.js` / `web/templates/system/_tab_general.html`
  - 新增扫描等待超时配置：`SCAN_WAIT_TIMEOUT`，在“常规 → 扫描配置”中可视化编辑
  - 新增压缩并行批次配置：`COMPRESSION_PARALLEL_BATCHES`，UI 与 API 完整打通
  - 扩展 SQLite 优化参数在系统配置页的加载与保存（缓存大小 / 页面大小 / 日志模式 / 同步策略）
  - 新增“磁带自动格式化”开关 `ENABLE_TAPE_FORMAT_BEFORE_FULL`，控制是否在完整备份前自动调用格式化命令
  - `loadAllSystemConfig()` / `saveEnvConfigSection()` 同步支持新增所有字段，保证 .env 与 UI 双向同步

#### openGauss 连接池与多表支持（调度器通用）
- ✅ `utils/scheduler/db_utils.py`
  - 引入基于 psycopg3 `AsyncConnectionPool` 的 openGauss 连接池（binary protocol），
    出错时自动回退到 asyncpg
  - 新增 `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` / `DB_MAX_INACTIVE_CONNECTION_LIFETIME` /
    `DB_ACQUIRE_TIMEOUT` / `DB_QUERY_DOP` 等配置的统一读取与应用
  - 在连接创建时执行 `SET query_dop = N`，并通过 `reset_connection` 与
    连接释放逻辑在返回池前彻底清理事务状态
  - 为池连接获取/释放增加 watchdog 监控与超时保护，附带连接池状态日志（used/idle/waiting）
  - 新增多表方案辅助函数 `get_backup_files_table_by_set_id()`，根据 `backup_set_id`
    决定物理表名，避免直接访问基础表 `backup_files`

### 改进

#### 扫描状态与检索阶段保护
- ✅ `backup/backup_db.py`
  - 修复扫描完成后仍被错误标记为 `retrieving` 的问题
  - 仅在当前 `scan_status` 不为 `retrieving` 且不为 `completed` 时，才更新为 `retrieving`
  - 避免压缩阶段或后续操作覆盖“扫描完成”状态

#### 压缩循环与统计更新
- ✅ `backup/compression_worker.py`
  - 压缩任务开始前创建 `OpenGaussDBScheduler`，将 chunk 信息与压缩统计统一交给后台调度器批量写库
  - `_compress_file_group` 中仅维护内存统计与必要字段，openGauss 模式下通过调度器异步更新
  - 更新 `backup_tasks` 的 `processed_files` / `processed_bytes` / `compressed_bytes` /
    `progress_percent` 时先从数据库读取当前值，再累加，避免多进程/多实例并发覆盖
  - 压缩完成日志增加整体统计输出（处理组数 / 文件数 / 原始大小 / 压缩大小 / 压缩率）

#### 文件组预取鲁棒性
- ✅ `backup/file_group_prefetcher.py`
  - 明确停止条件：扫描完成 + 预取至少执行一次 + 队列空 + 所有压缩任务完成 + 结束信号已入队
  - 针对 BufferError / 大结果集等异常增加重试与降级日志，引导调优 `MAX_FILE_SIZE` 与批次大小
  - 记录更详细的性能数据：单次检索耗时 / 累计耗时 / 平均耗时 / 最后处理 ID / 队列状态

---

## [0.1.34] - 2025-12-02

### 优化

#### 扫描性能优化
- ✅ 删除扫描阶段的 file_metadata 和 tags JSON 序列化
  - 扫描阶段不再创建和序列化 file_metadata 和 tags 字段
  - 压缩阶段会更新 file_metadata（添加 tape_file_path 等恢复所需信息）
  - 每个文件减少 2 次 json.dumps() 调用，显著提升扫描速度
  - 预计性能提升 5-10%（取决于文件数量）
- ✅ 将 Path 对象替换为 os.path 函数
  - 使用 `os.path.exists()`, `os.path.isfile()`, `os.path.isdir()` 替代 Path 对象方法
  - 使用 `os.path.basename()` 替代 `Path().name`
  - 使用 `os.path.dirname()` 替代 `Path().parent`
  - 使用 `os.path.abspath()` 替代 `Path().resolve()`
  - os.path 函数是 C 实现，比 Path 对象更快
  - 每个文件减少 2-3 次 Path 对象创建，预计性能提升 3-5%
- ✅ 优化路径处理逻辑
  - 目录队列使用字符串而非 Path 对象
  - 直接使用 entry.path 字符串，避免不必要的对象转换
  - 减少内存分配和对象创建开销

### 技术细节
- `backup/simple_scanner.py`：
  - 删除 file_metadata 和 tags 的 JSON 序列化代码
  - 从 SQL INSERT 语句中移除 file_metadata 和 tags 字段
  - 将 Path 对象操作替换为 os.path 函数
  - 优化目录队列使用字符串而非 Path 对象
- `backup/backup_scanner.py`：
  - 删除 file_metadata 和 tags 的 JSON 序列化代码
  - 从 SQL INSERT 语句中移除 file_metadata 和 tags 字段

## [0.1.33] - 2025-11-30

### 修改

#### openGauss 同步状态报告优化
- ✅ 修复同步状态报告间隔过长的问题
  - 在批次同步完成后也检查状态报告条件，确保即使批次耗时很长也能及时报告状态
  - 状态报告间隔不会超过30秒（或在批次完成后立即报告）
  - 提升同步进度的可见性和监控能力

#### openGauss 数据库连接和事务管理优化
- ✅ 添加异常时连接回滚逻辑，避免长事务锁表
  - 在检查表存在、executemany 执行、验证数据等操作的异常处理中添加回滚逻辑
  - 确保异常时连接处于干净状态，避免长时间持有事务锁
  - 防止 openGauss 长事务锁表问题
- ✅ 确保空列表直接返回，不执行 SQL
  - `_insert_files_to_opengauss` 方法开头检查 `file_data_map` 是否为空
  - 如果为空，直接返回空列表，不执行任何数据库操作
  - 提升性能，避免无意义的 SQL 查询
- ✅ 确保异步连接复用，避免连接泄漏
  - 所有数据库操作（fetchrow、executemany、fetchval）都在同一个 `async with get_opengauss_connection() as conn:` 块中
  - 复用同一连接，避免连接泄漏
- ✅ 确保显式提交事务（psycopg3 binary protocol）
  - `executemany` 在 `psycopg3_compat` 中已经显式提交事务
  - 验证事务状态，确保已提交
  - 如果事务状态仍为 INTRANS，会重试提交

### 技术细节
- `backup/memory_db_writer.py`：
  - 修复 `_sync_to_opengauss` 方法中状态报告间隔过长的问题
  - 在批次同步完成后检查是否需要报告状态
  - 添加多处异常处理时的连接回滚逻辑
  - 确保空列表直接返回，不执行 SQL
  - 确保所有数据库操作复用同一连接

## [0.1.32] - 2025-11-27

### 新增

#### 文件组预取器（openGauss模式）
- ✅ 实现文件组预取功能，压缩和搜索并行执行
  - 新增 `backup/file_group_prefetcher.py` 模块，实现 `FileGroupPrefetcher` 类
  - 在压缩任务开始前预取文件组，放入队列供压缩任务使用
  - 压缩任务从队列获取文件组，不再直接查询数据库
  - 内存中同时存在 N+1 组文件：N 个正在压缩的，1 个待压缩的（N=COMPRESSION_PARALLEL_BATCHES）
  - 大幅提升压缩效率，减少数据库查询等待时间
  - 支持 openGauss 模式，自动检测并使用预取器

#### 数据库连接辅助工具
- ✅ 新增统一的数据库连接辅助函数
  - 新增 `utils/db_connection_helper.py` 模块
  - 统一处理 psycopg2 和 psycopg3 的连接创建
  - 在 openGauss 模式下优先使用 psycopg3 binary protocol
  - 自动降级到 psycopg2（如果 psycopg3 不可用）
  - 简化数据库连接管理，提高代码复用性

#### psycopg3 兼容层
- ✅ 新增调度器 psycopg3 兼容支持
  - 新增 `utils/scheduler/psycopg3_compat.py` 模块
  - 为调度器提供 psycopg3 兼容接口
  - 支持调度器在 openGauss 模式下使用 psycopg3
  - 保持与现有代码的兼容性

#### 备份集查询 API
- ✅ 新增备份集查询接口
  - 新增 `web/api/backup/sets.py` 模块
  - 提供 `GET /api/backup/backup-sets` 接口，支持备份集列表查询
  - 支持按备份组过滤、分页查询
  - 支持 openGauss、SQLite、Redis 三种数据库模式
  - 统一返回格式，便于前端使用

#### 备份设置管理 API
- ✅ 新增备份设置管理接口
  - 新增 `web/api/backup/settings.py` 模块
  - 提供备份相关配置的查询和更新接口
  - 支持后台更新开关等配置项管理
  - 与系统配置页面集成

#### 磁带历史清理功能
- ✅ 新增磁带操作历史清理接口
  - 新增 `web/api/tape/tape_history_clean.py` 模块
  - 提供 `DELETE /api/tape/history/clean` 接口，支持清理历史记录
  - 支持按时间范围清理，避免历史数据无限增长
  - 记录清理操作日志

### 改进

#### 数据库连接管理优化
- ✅ 优化数据库连接创建和管理逻辑
  - 统一使用 `get_psycopg_connection()` 辅助函数
  - 改进连接池配置和错误处理
  - 提升数据库操作的稳定性和性能

#### 压缩工作流优化
- ✅ 优化压缩工作流程
  - 集成文件组预取器，减少数据库查询等待
  - 改进压缩任务调度和资源管理
  - 提升整体压缩性能

#### API 接口优化
- ✅ 优化备份和磁带管理 API
  - 改进错误处理和日志记录
  - 统一返回格式和状态码
  - 提升 API 响应速度和稳定性

### 技术细节

#### 文件组预取器实现
- 使用 `asyncio.Queue` 管理文件组队列
- 队列容量为 `parallel_batches + 1`
- 预取任务在后台持续运行，直到所有文件组处理完成
- 支持超时和取消机制

#### 数据库连接辅助
- 优先尝试使用 psycopg3（如果可用）
- 自动降级到 psycopg2
- 统一连接参数和错误处理
- 支持连接池和直接连接两种模式

#### API 接口
- 所有新 API 支持 openGauss、SQLite、Redis 三种模式
- 使用原生 SQL 查询，提升性能
- 统一的错误处理和日志记录
>>>>>>> 08e29b4055a41952230040797de258a6a131ca5d

## [0.1.31] - 2025-11-27

### 修改

#### 多进程压缩进度聚合显示
- ✅ 实现多进程压缩进度聚合显示功能
  - 新增 `BackupDB.get_compressed_files_count()` 方法，支持从数据库查询已压缩文件数（聚合所有进程的进度）
  - 在 `CompressionWorker` 中添加 `_update_compression_progress_periodically()` 后台任务，每5秒从数据库查询并更新压缩进度
  - 支持 openGauss、SQLite、Redis 三种数据库模式
  - 前端显示所有压缩进程的聚合进度，而非单个进程的进度
  - 解决多进程并发压缩时进度显示不准确的问题

### 技术细节
- `backup/backup_db.py`：新增 `get_compressed_files_count()` 方法，查询已压缩文件数
- `backup/compression_worker.py`：新增 `_update_compression_progress_periodically()` 后台任务，定期更新压缩进度
- `backup/sqlite_backup_db.py`：新增 `get_compressed_files_count_sqlite()` 方法
- `backup/redis_backup_db.py`：新增 `get_compressed_files_count_redis()` 方法
- 进度更新格式：`[压缩文件中...] {已压缩数}/{总文件数} 个文件 ({百分比}%)`

## [0.1.30] - 2025-11-25

### 修改

#### 压缩进度刷新频率优化
- ✅ 压缩进度刷新频率从每 2 秒调整为每 5 秒
  - 降低数据库写入压力，减少不必要的进度更新
  - 保持 UI 响应性的同时优化性能

#### 后台更新功能位置调整
- ✅ "后台更新"开关从 `/backup` 页面移动到 `/system` → "备份策略"页签
  - 统一管理备份相关配置，提升用户体验
  - 与内存数据库、扫描配置等备份策略设置集中管理
  - 配置读取和保存逻辑与系统配置页面统一

### 技术细节
- `compression_worker.py`：压缩进度更新间隔从 2 秒改为 5 秒
- `web/templates/system/_tab_backup.html`：新增后台更新开关控件
- `web/static/js/modules/system/system.js`：集成后台更新配置的读取和保存
- `web/api/backup/settings.py`：新增后台更新配置 API 接口

## [0.1.29] - 2025-11-25

### 修改

#### 后台扫描顺序执行模式
- ✅ 修改后台扫描为顺序执行，不使用队列机制
  - 扫描完一个批次后，直接同步写入数据库，等待全部写入完成后再继续扫描下一个批次
  - 批次大小由 `SCAN_UPDATE_INTERVAL` 配置决定（默认2000个文件）
  - 移除了 `BatchDBWriter` 的队列和后台 worker，改为直接同步写入
  - 确保 openGauss 模式下使用 psycopg3 binary protocol
  - 在 openGauss 模式下，所有状态更新后显式调用 `commit()` 方法

#### 批量写入字段修复
- ✅ 修复批量写入数据库时的字段内容问题
  - 使用 `CAST` 替代 `::` 进行 JSON 类型转换，提高兼容性
  - 增强 `file_metadata` 和 `tags` 的类型验证，确保始终是有效的 JSON 字符串
  - 修复参数顺序问题，确保所有字段正确映射

#### UI 样式优化
- ✅ 优化月度备份计划模板的显示样式
  - 将所有模板任务统一使用浅灰色背景（`table-light`），不再使用刺眼的黄色
  - 调整 pending 状态的边框颜色为灰色，使整体更柔和协调
  - "等待执行"徽章使用浅灰色半透明背景，更柔和

#### 压缩线程后台更新与 UI 按钮
- ✅ 压缩线程新增后台标记 `is_copy_success` 模式
  - 新增环境变量 `ENABLE_BACKGROUND_COPY_UPDATE`（默认关闭），可通过 `/backup` 页面的“后台更新”按钮即时切换并写入 `.env`
  - 开启后，`mark_files_as_copied` 在后台异步执行，主压缩循环可以立即继续下一批
  - 所有后台任务在退出前都会 `await` 完成，确保 `is_copy_success` 最终置为 `TRUE`
  - 验证流程抽取为 `_verify_is_copy_success`，后台模式完成后同样执行全量校验
- ✅ “后台更新”开关移动到 `/system` → “备份策略”页签，同步接入环境配置读取/保存，便于集中管理备份策略
- ✅ 压缩进度刷新频率调整为每 5 秒一次，降低数据库压力
- ✅ `/api/backup/settings/background-copy` 新增查询/更新接口，并在 `/system` → “备份策略”页签提供开关控件，支持一键读取/保存配置

### 技术细节
- `BatchDBWriter.write_batch_sync()`：新增同步写入方法，不使用队列
- `backup_scanner.py`：所有扫描器都改为使用 `write_batch_sync()` 直接写入
- `backup_db.py`：移除队列相关代码，简化批量写入逻辑
- `backup.js`：优化模板任务的显示样式，使用更柔和的颜色

## [0.1.28] - 2025-11-24

### 修复

#### recovery 页面数据库读取问题（openGauss 模式）
- ✅ 修复 recovery 页面在 openGauss 模式下的数据库读取问题
  - 修复 `fetchval` 方法缺少错误状态检查的问题
  - 添加连接错误状态（INERROR）检查，确保查询前连接处于正常状态
  - 改进所有 recovery 查询方法的错误处理和日志记录
  - 添加详细的异常捕获和错误日志，便于排查问题

#### recovery 页面无法显示已扫描文件
- ✅ 修复 recovery 页面无法显示已扫描文件的问题
  - 问题原因：查询条件要求 `is_copy_success = TRUE`，但扫描阶段写入的文件 `is_copy_success = False`
  - 解决方案：移除所有 recovery 查询中的 `is_copy_success` 限制条件
  - 修复范围：
    - `get_backup_set_files()` - openGauss 和 SQLite 模式
    - `get_top_level_directories()` - openGauss 和 SQLite 模式（3处）
    - `get_directory_contents()` - openGauss 和 SQLite 模式（2处）
  - 效果：recovery 页面现在可以显示所有已扫描的文件，无论是否已复制到磁带

### 改进

#### recovery 查询错误处理优化
- ✅ 优化 recovery 引擎的查询错误处理
  - 为所有数据库查询添加异常捕获和详细日志
  - 区分查询失败和数据不存在的情况
  - 查询失败时返回合理的默认值（空列表），避免页面崩溃
  - 添加错误堆栈记录，便于快速定位问题

## [0.1.27] - 2025-11-24

### 新增功能

#### 内存数据库配置选项
- ✅ 在系统配置页面添加"是否使用内存数据库"选项
  - 可在 `/system` 页面的"备份策略"标签页中配置
  - 配置项保存到 `.env` 文件（`USE_MEMORY_DB`）
  - 默认启用（`USE_MEMORY_DB=True`），性能最优
  - 禁用后直接写入数据库，按批次顺序写入，不丢弃内容，循环处理

#### 不使用内存数据库时的直接写入模式
- ✅ 实现不使用内存数据库时的直接写入数据库模式
  - 当 `USE_MEMORY_DB=False` 时，扫描文件线程直接写入数据库
  - 批次大小由 `SCAN_UPDATE_INTERVAL` 控制
  - 按批次顺序写入，不丢弃内容，循环处理
  - 不使用超时检测，确保数据完整性

### 修复

#### 批量写入数据库字段内容不对
- ✅ 修复批量写入数据库的字段内容，参照内存数据库的同步
  - 统一字段映射逻辑，使用 `_build_file_record_fields()` 方法
  - 确保字段顺序和内容与内存数据库同步完全一致
  - 包括 `directory_path`、`display_name`、`file_owner`、`file_group`、`version`、`tags` 等字段
  - 修复 `file_metadata` 和 `tags` 的 JSON 格式处理

#### openGauss 模式下数据未写入问题
- ✅ 修复 openGauss 模式下数据未写入 `backup_files` 表的问题
  - 修复 `_process_batch` 中的事务提交逻辑
  - 验证 `executemany` 执行后的事务状态
  - 如果状态仍为 `INTRANS`，再次提交确保数据持久化
  - 修复 `get_opengauss_connection` 的 `finally` 块，避免在 `IDLE` 状态下执行 `rollback()`

#### 保存"使用内存数据库"状态失败
- ✅ 修复保存"使用内存数据库"状态失败的问题
  - 修复前端 JavaScript 中 checkbox 值的收集逻辑
  - 确保 `false` 值能正确传递到后端
  - 修复后端 API 对 `use_memory_db` 字段的处理

### 改进

#### 批量写入器事务管理优化
- ✅ 优化 `BatchDBWriter` 的事务管理
  - 在 `_batch_insert` 和 `_batch_update` 后验证事务状态
  - 如果 `executemany` 后状态仍为 `INTRANS`，自动重试提交
  - 添加详细的日志记录，便于调试和排查问题

#### 连接释放逻辑优化
- ✅ 优化 `get_opengauss_connection` 的连接释放逻辑
  - 如果状态是 `IDLE`（已提交），不执行 `rollback()`
  - 只在 `INTRANS` 或 `INERROR` 状态下才清理事务
  - 避免已提交的数据被回滚

## [0.1.26] - 2025-11-24

### 修复

#### 扫描进度更新事务提交问题
- ✅ 修复 `update_scan_progress_only` 在 openGauss 模式下缺少显式提交事务的问题
  - 后台扫描任务更新 `total_files` 和 `total_bytes` 后，其他连接（如 API 查询）无法立即看到更新
  - 原因：psycopg3 默认 `autocommit=False`，需要显式提交事务
  - 解决：在 `update_scan_progress_only` 方法中添加显式 `commit()` 调用
  - 效果：UI 可以实时显示更新后的总文件数和总字节数

#### API 查询缺少 operation_stage 字段
- ✅ 修复 API 查询中缺少 `operation_stage` 字段的问题
  - 之前 API 从 `description` 字段解析操作阶段，可能不准确
  - 解决：在 SQL 查询中添加 `operation_stage` 字段
  - 优化 `_build_stage_info` 函数，优先使用数据库中的 `operation_stage` 字段
  - 效果：UI 可以准确接收到"写入磁带"和"完成"等状态更新

### 改进

#### 状态更新流程优化
- ✅ 优化任务状态更新流程
  - "写入磁带"状态：由 `TapeFileMover` 或 `CompressionWorker` 更新 `operation_stage = "copy"`
  - "完成"状态：由 `CompressionWorker._finalize_backup_set_and_notify` 更新 `status = COMPLETED`
  - 所有状态更新都会显式提交事务，确保 UI 可以实时接收

## [0.1.25] - 2025-11-24

### 改进

#### openGauss 连接池事务管理优化
- ✅ 优化 psycopg3 连接池的事务状态管理
  - 在连接返回池前确保连接处于干净状态（IDLE）
  - 在 `get_opengauss_connection()` 的 `finally` 块中检查并清理事务状态
  - 如果检测到 `INTRANS` 状态，先尝试提交而不是直接回滚（避免数据丢失）
  - 即使状态为 `IDLE`，也执行回滚以确保连接完全干净

#### 只读查询后自动提交事务
- ✅ 修复 `fetchrow`、`fetch`、`fetchval` 方法的事务处理
  - 查询完成后检查连接的事务状态
  - 如果处于 `INTRANS` 状态，自动提交事务（只读查询应自动提交）
  - 确保连接返回池时处于 `IDLE` 状态，避免连接池警告

#### executemany 事务提交验证
- ✅ 增强 `executemany` 方法的事务提交验证
  - 执行前检查事务状态
  - `commit()` 后等待并验证状态是否变为 `IDLE`
  - 如果状态仍为 `INTRANS`，重试提交并刷新连接状态
  - 添加详细的日志记录，便于调试

#### 连接池重置函数优化
- ✅ 改进连接池的 `reset_connection` 函数
  - 检测到 `INTRANS` 时先尝试提交，而不是直接回滚
  - 提交后验证状态，确保已变为 `IDLE`
  - 添加详细的日志记录（DEBUG 级别）

### 修复

#### psycopg3 连接池警告日志
- ✅ 降低 psycopg3 连接池的警告日志级别
  - 将 `psycopg.pool` 的日志级别设置为 `ERROR`
  - 避免在 INFO 级别时显示连接重置警告
  - 这些警告是连接池的正常清理行为，不影响功能

#### 设备缓存保存事务提交
- ✅ 修复 `_save_cached_devices` 方法的事务提交
  - 在 `INSERT`/`UPDATE` 操作后显式调用 `commit()`
  - 确保设备缓存正确保存到数据库

#### 连接池 reset_connection 执行顺序问题
- ✅ 修复连接池 `reset_connection` 的执行时机
  - psycopg3 内部的 `_reset_connection` 在我们的函数之前执行
  - 在 `get_opengauss_connection()` 的 `finally` 块中提前清理事务状态
  - 确保连接返回池前处于干净状态

### 技术细节

#### 事务状态管理
- psycopg3 默认 `autocommit=False`，即使只读查询也会启动事务
- 查询后需要显式提交或回滚，否则连接会保持 `INTRANS` 状态
- 连接返回池时，如果处于 `INTRANS` 状态，连接池会回滚并打印警告

#### 连接池重置流程
1. `get_opengauss_connection()` 的 `finally` 块：检查并清理事务状态
2. `conn.release()` 或 `pool.putconn()`：释放连接回池
3. psycopg3 内部的 `_reset_connection`：再次检查并清理（如果仍有问题会回滚）
4. 我们的 `reset_connection`：最后清理（此时状态应该已经是 `IDLE`）

#### 日志级别调整
- `psycopg.pool`：设置为 `ERROR`，避免 WARNING 级别日志
- `[连接池重置]`：设置为 `DEBUG`，避免 INFO 级别日志
- 功能不受影响，只是减少了不必要的警告日志

## [0.1.24] - 2025-11-22

### 改进

#### 文件移动可靠性增强
- ✅ 改进压缩文件从temp目录移动到final目录的可靠性
  - 压缩完成后等待1秒，确保文件句柄完全释放
  - 移动前检查文件大小稳定性（连续3次检查大小不变）
  - 额外等待0.5秒，确保文件句柄完全释放
  - 验证目标文件大小与源文件一致，确保移动成功
  - 如果源文件删除失败，最多重试3次（递增等待时间）

#### 文件移动错误处理优化
- ✅ 优化文件移动失败时的处理逻辑
  - 如果移动已成功（目标文件存在且大小匹配），但源文件删除失败，允许跳过删除操作
  - 删除失败时只记录警告，不中断备份流程
  - 提供详细的错误信息，便于排查问题
  - 提示可稍后手动删除源文件

### 修复

#### 文件移动后源文件残留问题
- ✅ 修复文件移动后源文件仍存在的问题
  - 原因：`shutil.move()` 在跨文件系统时会先复制再删除，如果删除失败源文件会残留
  - 解决：添加文件大小验证、源文件存在检查、重试删除机制
  - 如果移动成功但删除失败，允许跳过删除操作，不影响备份流程

### 技术细节

#### 文件移动流程
1. 压缩完成后等待1秒，确保文件句柄释放
2. 检查文件大小稳定性（连续3次不变）
3. 额外等待0.5秒
4. 执行 `shutil.move()` 移动文件
5. 验证目标文件存在且大小匹配
6. 如果源文件仍存在，尝试删除（最多3次重试）
7. 如果删除失败但移动成功，记录警告并继续

#### 错误处理策略
- 移动失败：抛出异常，中断流程
- 大小不匹配：抛出异常，中断流程
- 删除失败但移动成功：记录警告，继续执行

## [0.1.23] - 2025-11-22

### 新增

#### 备份卡片显示优化
- ✅ 添加每小时处理GB数徽章显示
  - 在备份卡片右上角显示每小时处理的GB数（仅数字）
  - 根据处理速度自动判断颜色：≥80G/小时显示绿色，<80G/小时显示黄色
  - 鼠标悬停显示详细信息（已处理数据/已用时间）
  - 使用已处理数据/已用时间计算处理速度

#### 压缩进度大小信息显示
- ✅ 在压缩阶段显示文件组总容量
  - "当前阶段"和"本批次压缩进度"中显示当前文件组的总容量（压缩前）
  - 格式：`压缩文件中 1/1 个文件 (100.0%) 14.19G`
  - 显示的是正在压缩的文件组的原始总大小，而非累计压缩大小
  - 后端传递 `current_compression_progress.group_size_bytes` 字段

### 改进

#### 写入磁带阶段徽章显示逻辑
- ✅ 优化写入磁带阶段徽章的显示效果
  - 向磁带移动时：红色徽章闪烁（`pulse-badge`）
  - 移动完成后：红色徽章不闪烁（熄灭）
  - 任务完成时：绿色徽章亮起（`bg-success`）
  - 根据 `operation_status` 自动判断是否正在移动

#### API响应模型扩展
- ✅ 扩展 `BackupTaskResponse` 模型
  - 添加 `current_compression_progress` 字段，包含当前压缩进度信息
  - 包含文件组大小（`group_size_bytes`）等运行时信息
  - 从运行中的任务对象获取实时压缩进度

### 修复

#### 文件扫描器导入修复
- ✅ 修复 `NameError: name 'time' is not defined` 错误
  - 在 `backup/file_scanner.py` 中添加 `import time`
  - 修复顺序扫描和并发扫描中的时间计算问题

#### 压缩进度信息键检查
- ✅ 修复 `KeyError: 'current'` 错误
  - 在访问 `current_compression_progress` 字典键之前添加存在性检查
  - 确保即使压缩进度信息不完整也不会抛出异常
  - 修复两处：构建压缩进度信息和定期更新压缩进度

#### 压缩进度信息传递
- ✅ 修复压缩进度信息无法传递到前端的问题
  - 在 `BackupTaskManager.get_task_status` 中从运行中的任务对象获取 `current_compression_progress`
  - 在 API 响应中包含压缩进度信息
  - 确保前端能够正确显示文件组大小

### 技术细节

#### 压缩进度信息结构
- `current_compression_progress` 包含：
  - `current`: 当前已压缩文件数
  - `total`: 文件组总文件数
  - `percent`: 压缩进度百分比
  - `group_size_bytes`: 文件组的原始总大小（压缩前）

#### 徽章显示逻辑
- 写入磁带阶段徽章根据任务状态和操作状态动态切换
- 使用 `operation_status` 判断是否正在移动文件
- 任务完成时所有阶段徽章显示完成状态

## [0.1.22] - 2025-11-22

### 新增

#### 顺序目录扫描器
- ✅ 实现优化的顺序目录扫描器（SequentialDirScanner）
  - 新建 `backup/sequential_dir_scanner.py` 模块
  - 基于 `os.scandir` 顺序执行，去掉不必要的检测
  - 保留排除项检查功能
  - 简化错误处理，提升扫描速度
  - 专用于 openGauss 模式下不启用多线程的场景

#### 多线程扫描选项
- ✅ 添加多线程扫描配置选项
  - 新增 `USE_SCAN_MULTITHREAD` 配置项（默认启用）
  - 在系统设置 → 备份标签页中添加"启用多线程扫描"复选框
  - 仅当扫描方法为"默认扫描（os.scandir）"时有效
  - 勾选多线程：使用并发目录扫描（ConcurrentDirScanner）
  - 不勾选多线程：使用顺序目录扫描（SequentialDirScanner，优化版）

### 改进

#### openGauss 模式同步流程优化
- ✅ 优化内存数据库同步到 openGauss 的流程
  - openGauss 模式下使用更激进的同步策略
  - 批次触发阈值从 100% 降低到 50%
  - 同步间隔检查时间缩短为配置值的一半（最少 5 秒）
  - 内存阈值降低，更早触发同步
  - 超时时间从 60 秒缩短到 30 秒
  - 完成检查阈值从 98% 降低到 95%，间隔从 30 秒缩短到 15 秒
  - 尽快将内存数据库记录复制到 openGauss

#### 日志级别优化
- ✅ 优化内存数据库同步相关日志级别
  - 关键信息（启动、同步开始/完成、错误）保持 INFO 级别
  - 详细调试信息（定期同步触发、批次详情、检查点等）改为 DEBUG 级别
  - 减少日志噪音，提高关键信息的可见性

#### 并发扫描器队列超时优化
- ✅ 修复 `asyncio.Queue.put()` 超时问题
  - 增加超时时间：有大小限制的队列从 10 秒增加到 60 秒
  - 无限制队列使用 30 秒超时
  - 添加队列状态检查，接近满（90%）时记录警告
  - 改进超时错误信息，包含队列大小和批次大小等详细信息

#### 扫描方式选择逻辑优化
- ✅ 优化文件扫描器的扫描方式选择逻辑
  - openGauss 模式 + 扫描方法=default + 启用多线程 → 使用 ConcurrentDirScanner
  - openGauss 模式 + 扫描方法=default + 不启用多线程 → 使用 SequentialDirScanner（新实现）
  - 其他情况 → 使用原有的顺序扫描代码
  - 根据配置自动选择最优扫描方式

### 修复

#### 并发扫描器异步队列调用修复
- ✅ 修复 `RuntimeWarning: coroutine 'Queue.put' was never awaited` 警告
  - 使用 `isinstance(path_queue, asyncio.Queue)` 直接检测队列类型
  - 确保 `asyncio.Queue` 必须使用 `asyncio.run_coroutine_threadsafe()` 调用
  - 只有在确认不是 `asyncio.Queue` 时才直接调用同步的 `put()` 方法

### 技术细节

#### 顺序扫描器实现
- 使用 `os.scandir` 顺序遍历目录
- 去掉路径解析缓存、大型目录结构检测等不必要的开销
- 保留排除项检查，确保功能完整性
- 简化错误处理，只记录必要的错误信息

#### 多线程选项配置
- 配置项：`USE_SCAN_MULTITHREAD`（布尔值，默认 True）
- UI 位置：系统设置 → 备份标签页
- 显示条件：仅当扫描方法选择"默认扫描（os.scandir）"时显示
- 联动效果：勾选多线程时显示线程数输入框，不勾选时隐藏

#### 同步流程优化
- openGauss 模式下的同步触发条件更加宽松
- 更频繁的同步检查，减少数据延迟
- 保持 SQLite 模式的原有逻辑不变，确保向后兼容

## [0.1.21] - 2025-01-20

### 新增

#### 并发目录扫描功能
- ✅ 实现openGauss模式下的高性能并发目录扫描
  - 新建 `backup/concurrent_dir_scanner.py` 模块，实现并发目录扫描器
  - 使用 `ThreadPoolExecutor` 实现多线程并发扫描多个目录
  - 线程安全的数据结构管理（队列、锁、统计）
  - 自动检测 `asyncio.Queue` 和 `queue.Queue` 类型
  - 仅在 openGauss 模式下且 `SCAN_THREADS > 1` 时自动启用
  - 预期性能提升：2-5倍（根据线程数）

- ✅ 配置项支持
  - 新增 `SCAN_THREADS` 配置项（默认4，范围1-16）
  - 在系统设置 → 备份标签页中添加"目录扫描并发线程数"输入框
  - 支持通过 `.env` 文件和 UI 界面配置
  - 使用 `SCAN_UPDATE_INTERVAL` 作为批次大小

- ✅ 集成到文件扫描器
  - 在 `backup/file_scanner.py` 中集成并发扫描器
  - 自动检测 openGauss 模式和线程数配置
  - 向后兼容：非 openGauss 模式或线程数为1时使用顺序扫描

### 改进

#### 配置项保存优化
- ✅ 修复目录扫描并发线程数无法保存的问题
  - 改进前端 JS 中 `scan_threads` 的处理逻辑
  - 修复 `parseInt(...) || null` 在空值时的处理问题
  - 确保值能正确解析和保存到 `.env` 文件

- ✅ 配置项保存检查
  - 创建配置项保存检查清单文档
  - 检查所有备份策略、内存数据库、SQLite 配置项的保存逻辑
  - 确保所有配置项都能正确保存

### 文档

- ✅ 创建并发目录扫描实现说明文档 (`docs/并发目录扫描实现说明.md`)
  - 详细的实现说明和架构介绍
  - 配置项说明和使用方式
  - 性能提升预期和技术细节
  - 故障排查指南

- ✅ 创建配置项保存检查清单文档 (`docs/配置项保存检查清单.md`)
  - 已修复问题说明
  - 配置项检查清单
  - 问题排查方法

### 技术细节

#### 并发扫描实现
- 使用 `ThreadPoolExecutor` 管理线程池
- 线程安全的 `queue.Queue` 管理待扫描目录
- `threading.Lock` 保护共享数据结构
- 批次文件收集和异步队列提交
- 路径缓存机制优化性能

#### 性能优化
- 并发扫描多个目录，充分利用 I/O 等待时间
- 批次提交机制，减少异步操作开销
- 智能队列检测，支持异步和同步队列

## [0.1.20] - 2025-11-19

### 改进

#### 数据库操作全面原生化
- ✅ 移除所有 SQLAlchemy ORM 使用，全面采用原生 SQL
  - openGauss 分支：使用 `asyncpg` 原生 SQL 查询
  - SQLite 分支：使用 `aiosqlite` 原生 SQL 查询
  - 两个数据库分支完全独立，互不影响
  - 提升数据库操作性能和稳定性

- ✅ 修复的文件列表（共 10 个文件）:
  - `web/api/system/logs.py` - SQLite 分支改为原生 SQL
  - `web/api/system/statistics.py` - SQLite 分支改为原生 SQL
  - `web/api/scheduler.py` - 备份任务模板验证改为原生 SQL
  - `web/api/backup_statistics.py` - SQLite 分支改为原生 SQL
  - `web/api/tape/crud.py` - 所有 SQLite 分支改为原生 SQL
    - `_check_tape_exists_sqlite` - 磁带存在性检查
    - `_count_serial_numbers_sqlite` - 序列号统计
    - `create_tape` - 磁带创建/更新
    - `check_tape_exists` - 磁带存在性查询
    - `list_tapes` - 磁带列表查询
    - `get_tape_inventory` - 磁带库存统计
    - `get_tape_drive_history` - 磁带操作历史

#### 同步持续时间显示修复
- ✅ 修复 openGauss 模式下同步持续时间显示为 0.0 秒的问题
  - 在 `_sync_to_opengauss` 函数中正确设置 `_sync_start_time`
  - 与 SQLite 模式保持一致的时间记录机制
  - 确保同步持续时间正确显示

#### 压缩日志格式优化
- ✅ 优化压缩处理速度日志显示格式
  - 从 "最近1000个文件平均处理时间: 0.004秒/文件 (总耗时: 4.2秒)"
  - 改为 "最近1000个文件耗时: 4.2秒，xxx文件/秒"
  - 更直观地显示处理速度和总耗时

### 技术细节

#### 原生 SQL 实现
- openGauss: 使用 `asyncpg` 连接池，参数化查询使用 `$1, $2, ...` 占位符
- SQLite: 使用 `aiosqlite` 连接，参数化查询使用 `?` 占位符
- 所有枚举类型转换在应用层处理，确保数据库兼容性
- JSON 字段手动序列化/反序列化，避免 ORM 自动处理问题

#### 性能优化
- 批量操作使用 `executemany` 减少数据库交互次数
- 连接池复用减少连接开销
- 原生 SQL 查询性能优于 ORM 查询

## [0.1.19] - 2025-11-17

### 改进

#### 扫描错误处理增强
- ✅ 移除流式扫描中的路径长度检查
  - 不再检查路径长度是否超过 Windows 限制（260 字符）
  - 所有路径都会被处理，不会因为路径过长而跳过
  - 简化了 `format_path_for_log` 函数，仅用于日志显示截断
  - 移除了 `path_too_long_count` 统计和相关的日志输出

- ✅ 增强扫描错误处理机制
  - 所有权限错误（`PermissionError`, `OSError`）被捕获并记录，不会中止扫描
  - 所有IO错误（`FileNotFoundError`, `IOError`）被捕获并记录，不会中止扫描
  - 所有其他异常（`Exception`）被捕获并记录，不会中止扫描
  - `os.scandir()` 调用被包装在 try-except 中，确保目录无法打开时不会中止整个扫描
  - 所有错误都记录到日志（前20个详细记录，避免日志过多）
  - 所有错误都跳过继续扫描，确保扫描过程不会因个别文件/目录的错误而中止
  - 适用于流式扫描、压缩扫描和普通扫描三种扫描模式

#### UI 优化
- ✅ 处理速度显示优化
  - 处理速度（G/小时）显示在右上角进度徽章中，替换百分比显示
  - 仅显示数字（如 "12.37"），不带单位，鼠标悬停显示完整信息
  - 移除了底部显示的处理速度行，界面更简洁
  - 如果无法计算速度，回退到显示压缩进度或百分比

### 修复

- ✅ 修复扫描过程中权限不足导致扫描中止的问题
  - 权限不足的文件/目录现在会被记录到日志并跳过，不会中止整个扫描过程
  - 任何其他错误（IO错误、文件不存在等）也不会中止扫描
  - 确保扫描过程能够完整完成，即使遇到大量权限错误

## [0.1.18] - 2025-11-17

### 改进

#### 压缩循环和文件移动架构重构
- ✅ 将压缩循环逻辑分离到独立的 `CompressionWorker` 类
  - 创建 `backup/compression_worker.py`，包含完整的压缩循环逻辑
  - 严格按照流程顺序执行：检索文件 → 压缩 → 更新数据库 → 循环
  - 检索所有非 `is_copy_success` 的文件，超阈值的跳过不修改 `is_copy_success`
  - 压缩完成后立即更新 `is_copy_success`，确保数据一致性
  - 所有参数、日志、错误处理等细节与原始逻辑完全一致
  - 独立的后台线程，不阻塞主流程
  - 支持压缩进度实时更新和任务取消

- ✅ 将文件移动逻辑分离到独立的 `FileMoveWorker` 类
  - 创建 `backup/file_move_worker.py`，包含文件移动逻辑
  - 独立的后台线程，与压缩线程互不影响、互不阻塞
  - 支持多个文件分次提交，正确排队顺序处理
  - 顺序执行：temp → final → 磁带（不能并行移动）
  - 即使第二个文件提交时第一个文件还在移动中，也能正确排队处理
  - 使用 `asyncio.Queue` 实现任务队列，确保顺序执行

- ✅ 重构 `backup_engine.py`
  - 删除原有的压缩循环代码（约 500 行）
  - 简化为创建和启动两个 worker，等待压缩完成
  - 从 worker 获取统计信息（`processed_files`, `total_size`）
  - 代码结构更清晰，职责分离更明确
  - 异常处理时也能正确获取已处理的统计信息

- ✅ 增强 `compressor.py` 返回信息
  - 添加 `temp_path` 和 `final_path` 字段（Path 对象）
  - 添加 `compression_method` 字段，标识使用的压缩方法（`pgzip`, `7zip_command`, `py7zr`, `tar`, `zstd`）
  - 便于文件移动 worker 正确处理文件路径
  - 恢复引擎可根据 `compression_method` 和文件扩展名自动选择解压方式

### 技术细节

- 压缩循环流程（严格按照顺序）：
  1. 检索所有非 `is_copy_success` 的文件，超阈值的跳过不修改 `is_copy_success`
  2. 压缩文件组（使用配置的压缩方法：pgzip/7zip/tar/zstd）
  3. 修改数据库 `is_copy_success`
  4. 循环到步骤 1

- 文件移动流程（顺序执行）：
  1. 从队列获取文件移动任务
  2. 将文件从 `temp` 目录移动到 `final` 目录（等待完成）
  3. 将文件加入磁带移动队列（顺序执行，一个完成后再处理下一个）
  4. 循环到步骤 1

- 压缩方法选择机制：
  - 通过 `COMPRESSION_METHOD` 配置项选择（`.env` 或 UI 设置）
  - 支持的方法：`pgzip` (默认) → `.tar.gz`, `7zip_command`/`py7zr` → `.7z`, `tar` → `.tar`, `zstd` → `.tar.zst`
  - 恢复时根据文件扩展名自动识别解压方法

## [0.1.17] - 2025-11-17

## [0.1.16] - 2025-11-17

### 改进

#### 检查点文件自动清理机制
- ✅ 实现检查点文件的自动清理功能
  - 添加 `checkpoint_retention_hours` 参数（默认 24 小时），控制检查点文件保留时间
  - 每次创建检查点时自动清理超过保留期的旧文件
  - 程序停止时清理所有检查点文件
  - 同时清理临时目录中可能遗留的检查点文件（通过文件名模式匹配）
  - 防止检查点文件无限累积占用磁盘空间
  - 清理操作记录到日志（DEBUG 级别），失败时记录警告但不影响主流程

#### 文件移动和压缩并行执行
- ✅ 实现文件移动和压缩的完全并行执行
  - 压缩完成后，文件从 `temp` 目录移动到 `final` 目录改为后台任务，不阻塞压缩循环
  - 文件加入移动队列改为后台任务，等待文件移动到 `final` 目录完成后再加入队列
  - 压缩循环可以立即处理下一个文件组，无需等待文件移动和队列操作完成
  - 大幅提升压缩效率，压缩和文件移动可以同时进行

#### 压缩进度显示优化
- ✅ 优化备份任务卡片中的压缩进度显示
  - 压缩进度显示当前文件组的进度（如 "814/1637"），而非整个任务的进度
  - 进度信息从 `operation_status` 中解析，格式为 "当前文件数/文件组总数"
  - 右上角进度圆圈显示当前文件组的压缩进度，更直观地反映当前压缩状态
  - 进度圆圈样式调整为圆角矩形，支持显示 "xxx/yyy" 格式

#### 压缩方法扩展
- ✅ 添加 `tar` 和 `zstd` 作为独立的压缩方法
  - `tar`：仅打包，不压缩，生成 `.tar` 文件
  - `zstd`：使用 Zstandard 压缩，生成 `.tar.zst` 文件
  - 在系统设置 → 压缩配置中添加对应的配置选项
  - 恢复页面支持根据文件扩展名自动选择解压方式（`.tar`、`.tar.zst`、`.zst`）
  - 支持配置 Zstandard 压缩线程数（`ZSTD_THREADS`）

### 修复

#### 计划任务立即执行按钮修复
- ✅ 修复计划任务页面"立即执行"按钮无响应的问题
  - 将事件监听器从 `tasksTableBody` 改为全局事件委托（`document`）
  - 确保动态更新的表格内容也能正确绑定事件处理器
  - 修复按钮点击无反应的问题

#### 程序阻塞问题修复
- ✅ 修复程序阻塞需要终端回车才能继续的问题
  - 所有子进程创建时添加 `stdin=asyncio.subprocess.DEVNULL`，防止等待标准输入
  - 修复 ITDT 接口、磁带操作、LTFS 工具等子进程的阻塞问题
  - 确保程序在非交互环境下正常运行

#### Ctrl+C 中断问题修复
- ✅ 修复 Ctrl+C 无法中断程序的问题
  - 增强信号处理，主动取消所有 asyncio 任务
  - 在压缩、文件移动、文件复制等操作中添加 `asyncio.CancelledError` 处理
  - 确保程序能够响应 Ctrl+C 信号并优雅退出

#### 压缩阻塞问题修复
- ✅ 修复压缩完成后阻塞的问题
  - 将 `_mark_files_as_copied` 从单个文件操作改为批量操作
  - 使用 `executemany` 进行批量 UPDATE 和 INSERT，大幅减少数据库交互次数
  - 从 N 次数据库调用减少到 3 次（SELECT + UPDATE + INSERT），消除 7 分钟阻塞
  - 添加详细的压缩进度日志，便于定位问题

#### 文件分组逻辑错误修复
- ✅ 修复 `NameError: name 'sorted_files' is not defined` 错误
  - 移除 `fetch_pending_files_grouped_by_size` 中未使用的旧分组逻辑代码
  - 确保函数正确返回单个文件组，避免引用未定义的变量

#### 备份计划模板样式修复
- ✅ 修复备份计划模板颜色和文字可读性问题
  - 设置深色背景（`rgba(13, 22, 38, 0.95)`）与主界面协调
  - 标签文字设置为白色（`#ffffff`），描述文字设置为浅灰色（`#adb5bd`）
  - 关闭按钮添加 `btn-close-white` 类，提高可见性

#### 重复确认对话框修复
- ✅ 修复备份页面点击"立即运行"弹出两次确认对话框的问题
  - 移除 `backup.js` 中的重复 `confirm` 对话框
  - 统一由 `scheduler.js` 的 `runTask` 函数处理确认逻辑

## [0.1.15] - 2025-11-16

### 改进

#### 后台扫描性能优化（扫描与写库分离）
- ✅ 实现扫描线程与数据库写入线程分离架构
  - 扫描线程专注于文件系统遍历，不再直接写数据库
  - 创建专门的数据库写入 worker（异步后台任务），从队列中读取文件信息并写入 `backup_files`
  - 使用 `asyncio.Queue` 作为生产者-消费者队列，队列容量设为无限（`maxsize=0`）
  - 扫描端只需将 `file_info` 放入队列，不等待数据库写入完成
  - 大幅提升扫描速度，目标达到 1000+ 文件/秒的扫描速度
  - 数据库写入在后台异步进行，不影响扫描性能

- ✅ 数据库写入 worker 日志增强
  - 每写入 1000 条记录或间隔 60 秒输出进度日志
  - 显示已写入文件记录数和当前扫描累计文件数
  - 收到停止信号时输出累计写入统计
  - 便于监控数据库写入进度和性能

#### 日志系统统一
- ✅ 统一控制台和日志文件的日志级别
  - 控制台 handler 不再固定为 `INFO` 级别，改为使用配置的 `LOG_LEVEL`
  - 屏幕输出和 `application.log` 文件内容完全一致
  - 通过系统设置中的 `LOG_LEVEL` 统一控制所有日志输出
  - 支持 `DEBUG`、`INFO`、`WARNING`、`ERROR` 等标准级别

#### 扫描进度统计配置化
- ✅ 添加扫描进度更新间隔配置（文件数）
  - 新增 `SCAN_UPDATE_INTERVAL` 配置项（默认 500），控制每处理多少个文件更新一次数据库统计
  - 在系统设置 → 备份标签页中添加"后台扫描进度更新间隔（文件数）"输入框
  - 支持通过 `.env` 文件和 UI 界面配置
  - 仅用于更新 `backup_tasks.total_files` 和 `total_bytes`，不影响文件记录写入

- ✅ 添加扫描进度统计间隔配置（时间）
  - 新增 `SCAN_LOG_INTERVAL_SECONDS` 配置项（默认 60 秒），控制进度日志输出频率
  - 在系统设置 → 备份标签页中添加"后台扫描进度统计间隔（秒）"输入框
  - 控制"后台扫描任务：已扫描 N 个文件..."日志的输出频率
  - 避免日志过多，同时确保进度可见性

#### 文件分组逻辑优化
- ✅ 实现容差机制和等待机制
  - 文件组大小允许 5% 容差范围：`[max_file_size - tolerance, max_file_size + tolerance]`
  - 如果组大小小于最小目标大小且扫描未完成，返回空列表等待更多文件
  - 最多循环 6 次等待，避免无限等待
  - 如果单个文件超过最大大小（含容差），单独成组处理
  - 如果添加文件会导致超过上限，跳过该文件（下次查询时仍可检索到）

- ✅ 移除文件大小降序排序
  - 不再对文件按大小降序排序，按数据库返回顺序处理
  - 简化逻辑，提高分组效率

#### 压缩配置优化
- ✅ PGZip 块大小默认值调整
  - 将 `PGZIP_BLOCK_SIZE` 默认值从 `1G` 改为 `1M`
  - 系统设置 UI 中默认值同步更新为 `1M`
  - 支持 `1M`、`128K`、`1G` 等多种格式，正确解析并转换为字节数

#### 流式扫描日志恢复
- ✅ 恢复流式扫描关键进度日志为 INFO 级别
  - 检测到大型目录结构时的日志
  - 当前正在扫描目录的进度日志
  - 目录树遍历完成的总结日志
  - 便于监控大规模目录结构的扫描进度

### 修复

#### 后台扫描任务错误修复
- ✅ 修复 `NameError: free variable 'time' referenced before assignment` 错误
  - 移除函数内部的重复 `import time` 语句
  - 统一使用模块顶部的全局 `import time`
  - 确保 `sync_scan_worker` 函数中能正确访问 `time` 模块

#### 日志输出问题修复
- ✅ 修复屏幕无日志输出的问题
  - 确保控制台 handler 使用配置的日志级别，而非固定 `INFO`
  - 屏幕和日志文件输出内容完全一致
  - 恢复流式扫描的关键进度日志为 INFO 级别

## [0.1.14] - 2025-11-15

### 修复

#### 数据库字段缺失问题
- ✅ 修复 `backup_tasks` 表缺少 `operation_stage` 字段导致的错误
  - 在 `models/backup.py` 中添加 `operation_stage` 字段定义
  - 在 `config/database.py` 的字段迁移逻辑中添加 `operation_stage` 字段
  - openGauss 数据库：`_migrate_missing_columns` 方法自动检测并添加缺失字段
  - PostgreSQL 数据库：`_migrate_missing_columns_postgresql` 方法自动检测并添加缺失字段
  - 重启应用后自动为现有数据库添加缺失字段，无需手动执行 SQL
  - 修复 `column "operation_stage" of relation "backup_tasks" does not exist` 错误

#### 事件循环冲突问题
- ✅ 修复多线程环境下更新任务阶段时的 `RuntimeError: Task got Future attached to a different loop` 错误
  - 重构 `update_task_stage` 方法，分为 `update_task_stage_async`（异步）和 `update_task_stage`（同步包装）
  - 同步方法支持 `main_loop` 参数，使用 `asyncio.run_coroutine_threadsafe` 在主事件循环中执行
  - `TapeFileMover` 初始化时保存主事件循环引用，并传递给数据库更新操作
  - 确保数据库操作在主事件循环中执行，避免事件循环冲突

## [0.1.13] - 2025-11-15

### 改进

#### 压缩文件移动流程优化
- ✅ 优化压缩完成后的文件移动逻辑
  - 压缩函数现在返回实际生成的完整文件路径（`archive_path`）
  - `compress_file_group` 优先使用压缩函数返回的路径，而非预设路径
  - 确保所有压缩方法（pgzip、7zip_command、py7zr）都正确返回文件路径
  - 文件移动时使用绝对路径，避免路径问题
- ✅ 添加详细的压缩和移动日志
  - 记录压缩函数返回的文件路径
  - 记录文件移动前后的路径和大小
  - 记录文件加入移动队列的状态
- ✅ 修复压缩完成后文件未被移动的问题
  - 确保压缩完成后文件正确移动到 `final_dir`
  - 确保文件正确加入 `TapeFileMover` 队列
  - 添加文件存在性检查日志

#### 关键阶段日志输出
- ✅ 添加关键阶段的日志输出（以 WARNING 级别输出，确保可见性）
  - `[扫描文件中...]` - 后台扫描任务开始时
  - `[扫描完成] 共 X 个文件，总大小 XX` - 扫描完成时
  - `[开始压缩] 批次 #X，X 个文件` - 开始压缩每个批次时
  - `[压缩完成] 文件组 X，大小: XX` - 每个文件组压缩完成时
  - `[文件已移动到正式目录] 文件名，大小: XX` - 文件从 temp 目录移动到 final 目录时
  - `[加入移动队列] 文件组 X，等待移动到磁带机` - 文件成功加入移动队列时
  - `[文件已移动到磁带机] 文件名 (文件组 X)` - 文件成功移动到磁带机时
  - `[移动到磁带机失败] 文件组 X，错误: XX` - 移动失败时

#### 写入磁带徽章状态显示
- ✅ 优化备份页面卡片中的阶段徽章显示
  - "写入磁带"徽章在写入阶段（`operation_stage === 'copy'`）显示为黄色警告色并带有脉冲动画
  - 写入完成后徽章恢复为普通样式（绿色或灰色）
  - 添加 `pulse-badge` CSS 动画效果，提供视觉反馈

#### 日志格式优化
- ✅ 优化扫描日志格式，明确区分不同扫描任务的计数
  - 后台扫描：`[后台扫描] 流式扫描：已扫描 X 个路径（包含目录），Y 个目录...`
  - 压缩扫描：`[压缩扫描]：已扫描 X 个文件（当前源路径，仅为当前源路径的文件数），找到...`
  - 每 10000 个文件输出一次进度日志（避免日志过多）
  - 明确说明后台扫描统计的是路径（包含目录），压缩扫描统计的是文件（仅当前源路径）

#### 路径过长和权限错误的处理
- ✅ 优化长路径和权限错误的处理
  - 在扫描阶段直接跳过路径超过 260 字符（Windows 限制）的文件
  - 跳过权限错误的文件，避免写入数据库
  - 添加详细的错误日志记录（仅记录前 10-20 个错误，避免日志过多）

### 修复

#### 压缩完成后文件移动问题
- ✅ 修复压缩完成后文件未被移动到磁带的问题
  - 修复 `compress_result['archive_path']` 未被正确使用的问题
  - 确保所有压缩方法都返回正确的 `archive_path`
  - 修复文件移动逻辑，确保使用压缩函数返回的实际路径

#### 文件移动队列状态更新
- ✅ 修复文件移动到磁带时的状态更新
  - `TapeFileMover` 现在正确传递 `backup_task` 对象
  - 写入开始时更新 `operation_stage` 为 `'copy'`
  - 写入完成后更新 `operation_stage` 为 `'finalize'`

## [0.1.12] - 2025-11-14

### 改进

#### 数据库初始化自动补齐压缩字段
- ✅ `config/database.py` 增加字段迁移逻辑
  - openGauss 初始化时使用 psycopg2 自动检测并补齐 `backup_tasks.compressed_bytes`、`backup_sets.compressed_bytes`、`backup_sets.compression_ratio`
  - PostgreSQL 初始化时使用 SQLAlchemy Inspector 逐表检查，缺失字段会通过 `ALTER TABLE ... ADD COLUMN ...` 自动补齐
  - SQLite 环境跳过字段迁移逻辑，保持原有行为
  - 添加字段注释（若数据库支持）以便后续维护
- ✅ 启动流程现在可自动修复缺失字段
  - 无需手动执行 SQL 脚本即可保证压缩统计相关字段齐全
  - 避免升级后因旧表缺少字段导致的 API 报错或压缩统计缺失

## [0.1.11] - 2025-11-13

### 修复

#### 计划任务页面路由修复
- ✅ 修复了 `/scheduler` 路由返回 404 的问题
  - 调整了路由注册顺序，将页面路由移到 API 路由之前，确保页面路由优先匹配
  - 更新了 `AuthMiddleware` 的 `EXCLUDED_PATHS`，添加了所有页面路由（`/backup`、`/recovery`、`/tape`、`/tapedrive`、`/scheduler`、`/tools`、`/system`）
  - 添加了调试日志，便于诊断路由问题
  - 修复了路由处理函数中的错误处理逻辑

### 改进

#### 计划任务页面独立化
- ✅ 将计划任务管理从系统设置页面移动到独立的导航链接和页面
  - 在 `base.html` 中添加了"计划"导航链接，位于"工具"链接左侧
  - 创建了独立的 `scheduler.html` 页面，包含完整的计划任务管理功能
  - 从 `system.html` 中移除了计划任务标签页和相关 JavaScript
  - 从 `system/_tabs_nav.html` 中移除了计划任务标签
  - 从 `system/_modals.html` 中移除了计划任务相关模态框（现在直接包含在 `scheduler.html` 中）
  - 添加了新的 FastAPI 路由 `/scheduler` 用于服务计划任务管理页面

#### 路由注册优化
- ✅ 优化了路由注册顺序
  - 页面路由（`@app.get()`）在 API 路由（`app.include_router()`）之前注册
  - 确保页面路由优先匹配，避免与 API 路由冲突
  - 提高了路由匹配的准确性和性能

#### 中间件配置优化
- ✅ 改进了认证中间件的路径排除逻辑
  - 将所有页面路由添加到 `EXCLUDED_PATHS`，避免 HTML 页面被认证中间件拦截
  - 添加了调试日志，记录跳过认证检查的路径
  - 改进了路径匹配逻辑，确保页面路由能够正确跳过认证

### 技术细节

#### 路由匹配优先级
- FastAPI 按照路由注册顺序匹配路由
- 页面路由先注册，确保精确匹配（如 `/scheduler`）优先于 API 路由的前缀匹配（如 `/api/scheduler/*`）
- 这样可以避免路由冲突和 404 错误

#### 中间件执行顺序
- 中间件按照添加顺序执行（从后往前）
- `AuthMiddleware` 检查路径是否在 `EXCLUDED_PATHS` 中
- 如果路径匹配，则跳过认证检查，继续处理请求
- 页面路由在 `EXCLUDED_PATHS` 中，因此不会被认证中间件拦截

## [0.1.10] - 2025-11-13

### 修复

#### 数据库连接池全面重构
- ✅ 将所有数据库连接从旧模式迁移到连接池模式
  - 修复了78处使用旧连接模式（`conn = await get_opengauss_connection()` + `await conn.close()`）的代码
  - 统一使用新的连接池模式（`async with get_opengauss_connection() as conn:`）
  - 修复了 `TypeError: object _AsyncGeneratorContextManager can't be used in 'await' expression` 错误
  - 解决了 `WinError 121: 信号灯超时时间已到` 连接超时问题

- ✅ 修复的文件列表（共19个文件）:
  - `backup/backup_engine.py` - 6处
  - `backup/backup_db.py` - 9处
  - `backup/backup_task_manager.py` - 3处
  - `backup/tape_handler.py` - 1处
  - `web/api/backup.py` - 8处
  - `web/api/system/notification.py` - 4处
  - `web/api/system/statistics.py` - 6处
  - `web/api/system/logs.py` - 1处
  - `web/api/system/tape_config.py` - 1处
  - `web/api/scheduler.py` - 2处
  - `web/api/tape/crud.py` - 1处
  - `recovery/recovery_engine.py` - 6处
  - `tape/tape_manager.py` - 2处
  - `tape/tape_operations.py` - 1处
  - `utils/log_utils.py` - 2处
  - `utils/scheduler/task_storage.py` - 12处
  - `utils/scheduler/scheduler.py` - 1处
  - `utils/scheduler/task_executor.py` - 6处
  - `utils/scheduler/action_handlers.py` - 5处

- ✅ 应用关闭时正确释放连接池资源
  - 在 `main.py` 的 `shutdown()` 方法中添加了关闭 openGauss 连接池的逻辑
  - 优化了关闭顺序：先关闭连接池，再关闭数据库管理器
  - 确保所有数据库连接在应用关闭时正确释放

### 改进

#### 连接池配置可配置化
- ✅ 添加了连接池超时时间配置项
  - `DB_POOL_TIMEOUT: float = 30.0` - 连接池连接超时时间（秒）
  - `DB_COMMAND_TIMEOUT: float = 60.0` - 命令超时时间（秒）
  - `DB_ACQUIRE_TIMEOUT: float = 10.0` - 从连接池获取连接的超时时间（秒）
  - 所有超时时间都可以通过配置文件（`.env`）调整

- ✅ 连接池参数使用配置值
  - 连接池创建时使用配置的超时参数
  - 获取连接时使用配置的超时参数
  - 日志输出包含实际使用的超时值，便于监控和调试

#### 连接池错误处理优化
- ✅ 改进了连接池错误处理
  - 正确处理 `UNLISTEN statement is not yet supported` 警告（openGauss 限制）
  - 将 openGauss 限制性警告降级为 DEBUG 级别日志
  - 其他错误使用 WARNING 级别日志
  - 确保连接释放不会因为 openGauss 限制而失败

### 技术细节

#### 连接池实现
- 使用 `asyncpg.create_pool()` 创建连接池
- 连接池参数：
  - `min_size`: 最小连接数（pool_size // 2）
  - `max_size`: 最大连接数（pool_size + max_overflow）
  - `timeout`: 连接超时时间（可配置）
  - `command_timeout`: 命令超时时间（可配置）
  - `max_queries`: 每个连接的最大查询数（50000）
  - `max_inactive_connection_lifetime`: 非活跃连接的最大生命周期（300秒）

#### 连接获取机制
- 使用 `async with get_opengauss_connection() as conn:` 获取连接
- 自动管理连接的获取和释放
- 支持连接获取超时重试（最多3次，指数退避）
- 连接池关闭时自动重新创建

#### 使用场景
- 适用于所有使用 openGauss 数据库的场景
- 特别适用于高并发场景（连接池可以复用连接）
- 解决了大量并发请求时的连接超时问题

## [0.1.9] - 2025-11-12

### 改进

#### 文件扫描性能优化（智能批次阈值）
- ✅ 根据目录数量智能匹配批次阈值
  - 目录数量 >= 50000：使用10个文件/路径作为批次阈值
  - 目录数量 >= 10000：使用25个文件/路径作为批次阈值
  - 目录数量 >= 1000：使用50个文件/路径作为批次阈值
  - 目录数量 < 1000：使用100个文件/路径作为批次阈值
  - 适用于独立扫描任务和压缩扫描任务

- ✅ 强制提交间隔优化
  - 强制提交间隔从5分钟（300秒）调整为20分钟（1200秒）
  - 队列等待超时时间从5分钟（300秒）调整为20分钟（1200秒）
  - 心跳日志间隔从5分钟（300秒）调整为20分钟（1200秒）
  - 警告阈值从10分钟（600秒）调整为30分钟（1800秒）

- ✅ 扫描任务优化
  - 独立扫描任务（后台扫描）使用智能批次阈值
  - 压缩扫描任务（流式扫描）使用智能批次阈值
  - 日志中显示批次阈值信息，便于监控

### 技术细节

#### 智能批次阈值规则
- 根据待扫描目录数量动态调整批次阈值
- 目录数量越多，批次阈值越小，确保进度更新更频繁
- 对于大量目录结构（如40.5万个目录），使用更小的批次阈值（10个文件）

#### 强制提交机制
- 即使没有达到批次阈值，每20分钟也会强制提交一次批次
- 确保在文件分布稀疏的情况下，也能定期更新进度
- 队列等待超时时间与强制提交间隔一致（20分钟）

#### 使用场景
- 特别适用于包含大量子目录的目录结构
- 对于文件分布稀疏的场景，确保进度更新更频繁
- 减少不必要的批次提交，提高性能

## [0.1.8] - 2025-11-12

### 改进

#### 文件扫描器性能优化（大量目录处理）
- ✅ 使用 `os.scandir()` 替代 `rglob()` 以提高性能
  - `os.scandir()` 在 Windows 上比 `rglob()` 更快，特别是在处理大量目录时
  - 内存占用更低，不会一次性加载所有路径
  - 使用迭代方式逐个目录扫描，避免内存溢出

- ✅ 迭代式目录遍历实现
  - 使用 `dirs_to_scan` 队列管理待扫描目录
  - 使用 `scanned_dirs` 集合避免重复扫描
  - 按需处理，减少内存压力

- ✅ 大型目录结构检测与自适应日志
  - 自动检测大型目录结构（超过1万个目录）
  - 大型目录结构下使用更频繁的日志输出：
    - 目录日志：每30秒（而非2分钟）
    - 进度日志：每30秒（而非1分钟）
  - 日志中包含待扫描目录数量，便于监控进度

- ✅ 任务卡片显示优化
  - 修复任务卡片中源路径、目标和运行状态显示问题
  - 在 `update_task_status` 中同时更新 `source_paths` 和 `tape_id`（当状态为 RUNNING 时）
  - 在 `get_task_status` 中返回完整的任务信息（包括 `source_paths`、`tape_id`、`description`）
  - 添加 `update_task_fields` 方法用于更新特定字段
  - 确保任务卡片正确显示源路径、磁带ID和操作状态

- ✅ 错误处理增强
  - 目录权限错误单独处理，不影响整体扫描
  - 错误计数与限制（最多记录20个权限错误）
  - 路径过长检测与跳过
  - 改进的异常处理和日志记录

- ✅ 性能监控改进
  - 显示待扫描目录数量
  - 显示目录计数
  - 显示扫描速度（路径/秒）
  - 显示权限错误和路径过长错误计数

### 技术细节

#### 优化效果
- 扫描速度提升：`os.scandir()` 比 `rglob()` 快约2-3倍
- 内存占用降低：迭代处理，不一次性加载所有路径
- 支持大规模目录结构：已测试支持40.5万个目录
- 更好的进度可见性：更频繁的日志输出，便于监控

#### 使用场景
- 特别适用于包含大量子目录的目录结构（如 `D:\备份\天正协同备份\tbmdata\data\ftpdata` 中有40.5万个目录）
- 首次扫描可能需要较长时间，但不会因为内存问题而失败
- 后续扫描会更快（部分目录可能已缓存）

## [0.1.7] - 2025-11-12

### 改进

#### 备份任务中止功能完善
- ✅ 完善 Ctrl+C 中止任务功能
  - 在 `execute_backup_task` 方法中添加 `KeyboardInterrupt` 和 `CancelledError` 异常处理
  - 在 `_perform_backup` 方法中添加 `KeyboardInterrupt` 和 `CancelledError` 异常处理
  - 在 `_scan_for_progress_update` 方法中添加 `KeyboardInterrupt` 和 `CancelledError` 异常处理
  - 任务被中止时正确更新状态为 `CANCELLED`
  - 任务被中止时正确更新扫描进度为 `[已取消]`
  - 记录错误信息为"用户中止任务（Ctrl+C）"或"任务被取消"

- ✅ 后台扫描任务能够被正确取消
  - 在 `finally` 块中确保后台扫描任务被正确取消
  - 等待后台扫描任务取消完成（超时 5 秒）
  - 即使被取消也保存已扫描的文件数和字节数
  - 记录取消日志，便于追踪

- ✅ 流式扫描循环能够被正确中止
  - 在流式扫描循环中添加取消检查
  - 每批次检查任务是否被取消
  - 检测到取消信号时立即退出循环
  - 记录中止日志

- ✅ 资源清理机制完善
  - 在 `finally` 块中清理后台扫描任务
  - 等待任务取消完成（超时 5 秒）
  - 记录清理日志
  - 确保资源正确释放

- ✅ 任务状态更新优化
  - 任务被中止时状态更新为 `CANCELLED`
  - 错误信息记录为"用户中止任务（Ctrl+C）"或"任务被取消"
  - 扫描进度显示为 `[已取消]`
  - 即使被中止也保存已处理的文件数和字节数

- ✅ 异常处理完善
  - 添加 `KeyboardInterrupt` 专门处理（用户按 Ctrl+C）
  - 添加 `asyncio.CancelledError` 专门处理（任务被取消）
  - 重新抛出异常，让上层处理
  - 记录详细的错误日志和堆栈跟踪

#### 后台扫描任务优化
- ✅ 移除独立线程的判断条件
  - 取消 `if backup_task and backup_task.source_paths:` 判断条件
  - 直接在流式扫描和压缩循环之前启动后台扫描任务
  - 与流式扫描和压缩循环共用前置条件
  - 确保后台扫描任务总是能够启动

- ✅ 后台扫描任务事件循环修复
  - 修复后台线程中无法使用 `asyncio.get_running_loop()` 的问题
  - 在启动后台线程之前获取主事件循环
  - 在后台线程中使用主事件循环提交批次
  - 确保批次能够正确提交到队列

- ✅ 日志输出改进
  - 将关键日志从 `debug` 改为 `info` 级别
  - 添加更多日志信息（目录名、批次数量、文件数、字节数等）
  - 每批次提交时记录日志
  - 目录遍历完成时记录日志
  - 收到完成信号时记录日志

- ✅ 错误处理改进
  - 改进异常捕获和处理
  - 添加错误日志（包含堆栈跟踪）
  - 确保任务正常完成
  - 即使失败也尝试更新已扫描的进度

## [0.1.6] - 2025-11-12

### 改进

#### 备份管理页面显示优化
- ✅ 删除压缩包进度显示
  - 移除"压缩包进度: 125800/125800"的显示
  - 简化任务卡片信息展示

- ✅ 添加数据量显示
  - 新增"数据量"字段，显示已处理文件大小（GB单位）
  - 使用 `formatBytesToGB` 函数格式化显示
  - 实时更新数据量信息

- ✅ 添加已用时间显示
  - 新增"已用时间"字段，显示任务执行时长
  - 支持显示格式：天、小时、分钟、秒
  - 运行中的任务实时更新已用时间
  - 已完成任务显示总耗时

- ✅ 改进进度计算逻辑
  - 优先使用后端提供的实际总字节数（`total_bytes_actual`字段）
  - 后端未提供时自动预估总字节数：
    - 策略1：基于平均文件大小预估（已处理文件数 × 平均文件大小）
    - 策略2：基于当前进度反推（已处理字节数 ÷ 进度百分比）
    - 策略3：保守倍数估算（已处理字节数 × 3）
  - 基于数据量的进度计算（80%权重）+ 文件数进度（20%权重）
  - 分阶段进度计算：扫描阶段（0-70%）、压缩+写入阶段（70-100%）
  - 确保进度计算更准确，反映实际备份进度

## [0.1.5] - 2025-12-20

### 修复

#### 备份页面卡片显示修复
- ✅ 修复进度计算逻辑
  - 进度计算改为：`已处理文件数（累计）/总扫描文件数 * 100`
  - 修复前端进度条显示，确保准确反映文件处理进度
  - 修复卡片更新时的进度计算逻辑

- ✅ 修复压缩包进度显示
  - 修复 `estimated_archive_count` 传递问题
  - 确保第一批压缩完成后正确保存和传递预计压缩包总数
  - 改进 `estimated_archive_count` 的计算逻辑，基于总扫描文件数和平均文件大小
  - 添加调试日志，便于追踪压缩包进度

- ✅ 修复压缩率传递和显示
  - 确保 `compression_ratio` 正确计算和返回（如 3.7%）
  - 修复 `get_backup_tasks` API 返回 `compressed_bytes` 和 `compression_ratio`
  - 修复前端压缩率显示逻辑，确保正确显示百分比格式

- ✅ 修复开始时间和完成时间传递
  - 确保 `started_at` 和 `completed_at` 都保存到数据库
  - 修复 `_update_task_status` 方法，在状态变更时正确保存时间字段
  - 确保 `get_task_status` 和 `get_backup_tasks` API 都返回时间字段
  - 修复前端时间显示，确保开始时间和完成时间正确显示

- ✅ 修复API返回字段完整性
  - `get_backup_tasks` API 添加 `compressed_bytes` 和 `compression_ratio` 字段
  - `get_task_status` API 添加 `completed_at` 字段
  - 确保所有必需字段都正确传递到前端

### 改进

#### 备份任务状态更新优化
- ✅ 改进 `_update_task_status` 方法
  - 根据任务状态自动更新 `started_at`（RUNNING状态）和 `completed_at`（COMPLETED/FAILED状态）
  - 使用动态SQL构建UPDATE语句，只更新必要的字段
  - 确保时间字段在状态变更时正确保存

## [0.1.4] - 2025-12-19

### 改进

#### 计划任务调度配置可编辑
- ✅ 允许直接编辑计划任务的调度配置时间
  - 移除时间输入框的 `readonly` 属性，支持直接输入时间
  - 添加时间格式验证（HH:MM格式，如 02:00）
  - 实时验证和格式化，自动同步到隐藏输入框
  - 支持每日、每周、每月、每年任务的时间编辑
  - 添加时间格式提示和标签说明

- ✅ 改进调度配置界面
  - 添加字段标签和格式说明
  - 改进布局，使配置项更清晰
  - 时间输入框支持直接编辑或使用时间选择器

#### 定时检查任务优化
- ✅ 取消保留期检查的定时任务
  - 移除默认的保留期检查定时任务（`RETENTION_CHECK_CRON`）
  - 改为在打开磁带管理页面时自动检查一次
  - API接口 `GET /api/tape/list` 会自动执行保留期检查
  - 备份开始前已经检查了保留期，避免重复检查

- ✅ 废弃月度备份默认任务
  - 移除默认的月度备份任务注册（`MONTHLY_BACKUP_CRON`）
  - 计划任务的执行时间由用户在Web界面创建时设置
  - 不再使用配置文件中的默认Cron表达式
  - 所有计划任务通过Web界面创建和管理

#### 磁带管理页面状态显示修复
- ✅ 修复正在格式化的磁带不应显示为可用
  - 修复统计API：`MAINTENANCE` 状态的磁带不计入可用数量
  - 修复前端显示：确保状态值统一为小写，CSS样式正确应用
  - 正在格式化的磁带显示为"格式化中"（黄色警告样式），而不是"可用"

### 文档

- ✅ 更新定时检查说明文档
  - 标记保留期检查任务为"已取消定时执行"
  - 说明保留期检查改为页面打开时触发
  - 标记月度备份任务为"已废弃"
  - 更新检查频率总结表

## [0.1.3] - 2025-11-11

### 新增

#### 恢复页面使用真实数据
- ✅ 恢复引擎从数据库查询真实备份集数据
  - `search_backup_sets()`: 从数据库查询备份集列表，支持过滤条件
  - `get_backup_set_files()`: 从数据库查询备份集文件列表
  - `get_backup_groups()`: 从数据库查询备份组列表
  - `_get_backup_set_info()`: 从数据库查询备份集详细信息
  - 支持 openGauss 和 SQLAlchemy 两种数据库查询方式

- ✅ 恢复页面前端使用 API 获取真实数据
  - 删除硬编码的示例数据
  - 添加 `loadBackupGroups()` 从 API 加载备份组
  - 添加 `loadBackupSets()` 从 API 加载备份集列表
  - 添加 `loadBackupSetFiles()` 从 API 加载文件列表
  - 添加 `buildFileTree()` 动态构建文件树
  - 添加文件搜索和类型过滤功能
  - 添加恢复任务创建功能

#### 首页使用真实数据
- ✅ Dashboard API 提供真实统计数据
  - `get_system_statistics()`: 从数据库查询系统统计信息
  - 备份任务统计（总数、运行中、已完成、失败）
  - 磁带库存统计（总数、可用、使用中、过期、在线/离线）
  - 存储统计（总容量、已用容量、使用率）
  - 最近备份活动列表
  - 存储使用趋势（最近30天）
  - 成功率统计（总体、本月、上月对比）

- ✅ 首页前端使用 API 获取真实数据
  - 删除所有硬编码的示例数据
  - 添加 `loadDashboardData()` 从 API 加载统计数据
  - 添加 `renderStorageTrend()` 渲染存储趋势图表
  - 添加 `renderRecentBackups()` 渲染最近备份活动列表
  - 实时更新所有统计卡片（备份任务、存储、磁带、成功率等）
  - 自动刷新机制（每30秒刷新一次）
  - 数据库连接状态检查

#### 计划任务支持恢复操作
- ✅ 完善 RecoveryActionHandler 实现
  - 从配置中获取恢复参数（backup_set_id, files, target_path）
  - 调用恢复引擎创建和执行恢复任务
  - 添加错误处理和日志记录
  - 返回详细的执行结果

### 改进

#### 备份引擎流式处理优化
- ✅ 实现智能流式备份处理
  - 添加配置项 `SCAN_BATCH_SIZE`（文件数阈值）和 `SCAN_BATCH_SIZE_BYTES`（字节数阈值）
  - 修改 `_perform_backup()` 实现流式处理：扫描和压缩循环执行
  - 添加 `_scan_source_files_streaming()` 异步生成器，分批返回文件
  - 达到批次阈值时立即开始压缩，不再等待所有文件扫描完成
  - 降低内存占用，提升大文件备份性能

- ✅ 7z压缩多线程支持
  - 修改 `_compress_file_group()` 使用 `py7zr.SevenZipFile` 替代 `tarfile`
  - 启用多线程压缩（使用 `COMPRESSION_THREADS` 配置）
  - 确保压缩完成判断：使用 `with` 语句确保 `archive.close()` 完成
  - 检查文件大小稳定，使用 `completed` 标志确保压缩操作完成

## [0.1.2] - 2025-11-10

### 改进

#### 计划任务格式化功能优化
- ✅ 计划任务格式化改为后台异步执行，不阻塞API响应
  - 使用 `asyncio.create_task()` 后台执行格式化任务
  - API立即返回，提升用户体验
  - 与磁盘管理页面格式化逻辑保持一致

- ✅ 添加当月判断和卷标自动更新功能
  - 判断当前磁带卷标是否为当月
  - 如果是当月，根据当前系统时间更新卷标
  - 如果不是当月，根据当前系统时间生成新卷标
  - 确保卷标始终与当前系统时间一致

- ✅ 复用磁盘管理的完整格式化逻辑
  - 状态更新：格式化前设置为 `MAINTENANCE`，成功后设置为 `AVAILABLE`，失败设置为 `ERROR`
  - 读取实际值：格式化成功后读取实际卷标和序列号，并更新数据库
  - 错误处理：格式化失败时发送钉钉通知
  - 与磁盘管理页面功能完全一致

- ✅ 序列号格式统一
  - 从 `YYMMNN` 格式改为 `TPMMNN` 格式
  - 与磁盘管理页面的序列号格式保持一致

#### 格式化阻塞问题修复
- ✅ 修复格式化完成后可能阻塞的问题
  - 为 `fsutil` 命令添加10秒超时，防止命令卡住
  - 改进输出读取逻辑，添加单次读取超时（1秒）和总超时（30秒）
  - 确保格式化完成后能正常退出，不会无限等待

- ✅ 优化命令输出读取机制
  - 使用分块读取（每次4KB），避免一次性读取大量数据
  - 添加超时保护，防止读取操作卡住
  - 改进错误处理和日志记录

#### 文档更新
- ✅ 创建格式化流程详解文档 (`docs/格式化流程详解.md`)
  - 详细的格式化执行流程说明
  - 顺序执行机制说明
  - 超时机制说明
  - 状态流转图

- ✅ 创建格式化阻塞问题分析文档 (`docs/格式化阻塞问题分析.md`)
  - 问题分析
  - 修复方案
  - 超时设置说明

- ✅ 创建计划任务格式化功能分析文档 (`docs/计划任务格式化功能分析.md`)
  - 功能对比分析
  - 潜在问题分析
  - 改进建议

- ✅ 创建计划任务格式化改进总结文档 (`docs/计划任务格式化改进总结.md`)
  - 改进内容总结
  - 执行流程说明
  - 与磁盘管理的对比

## [0.1.1] - 2025-11-08

### 改进

#### 磁带格式化操作优化
- ✅ 将磁带格式化操作改为后台任务，避免阻塞前端
  - 使用FastAPI的`BackgroundTasks`执行格式化操作
  - API立即返回响应，前端不再等待格式化完成
  - 格式化完成后在操作历史中记录日志
  - 提升用户体验，避免长时间等待

#### 计划任务源选择器统一
- ✅ 统一计划任务源选择器行为
  - 无论备份目标是"磁带机"还是"存储"，源选择按钮都调用存储选择器（文件浏览器）
  - 添加代码注释说明统一行为
  - 简化用户操作流程

## [0.1.0] - 2025-11-04

### 新增

#### ITDT集成支持
- ✅ 集成IBM Tape Diagnostic Tool (ITDT)支持
  - 创建ITDT接口类 (`tape/itdt_interface.py`)
  - 支持ITDT命令行模式操作
  - 实现所有核心磁带操作（加载、卸载、倒带、擦除、格式化等）
  - 支持设备扫描和状态查询
  - 支持诊断测试功能

#### 磁带操作接口切换
- ✅ 支持ITDT和SCSI接口切换
  - 添加配置项 `TAPE_INTERFACE_TYPE` 选择接口类型
  - 修改 `TapeOperations` 类支持多接口切换
  - 保持API接口不变，后端可切换实现
  - ITDT接口作为推荐方式（更稳定、更标准）

#### ITDT配置支持
- ✅ 添加ITDT相关配置项
  - `TAPE_INTERFACE_TYPE`: 接口类型选择（"itdt" 或 "scsi"）
  - `ITDT_PATH`: ITDT可执行文件路径
  - `ITDT_LOG_LEVEL`: 日志级别（Errors|Warnings|Information|Debug）
  - `ITDT_LOG_PATH`: 日志文件路径
  - `ITDT_RESULT_PATH`: 结果文件路径

#### 文档更新
- ✅ 创建ITDT集成方案文档 (`docs/ITDT集成方案.md`)
  - 详细的ITDT命令分析
  - 架构设计方案
  - 实现步骤和时间表
  - 风险评估和缓解措施
- ✅ 更新README.md
  - 添加ITDT安装和配置说明
  - 添加接口选择说明
  - 更新版本历史

### 改进

#### 磁带操作稳定性
- ✅ ITDT接口提供更稳定的磁带操作
  - 使用IBM官方工具，兼容性更好
  - 标准化命令接口，错误处理更完善
  - 支持更多诊断和测试功能

#### 系统可扩展性
- ✅ 接口抽象设计，便于未来扩展
  - 统一的接口抽象
  - 支持多接口并存
  - 便于添加新的磁带操作接口

### 技术细节

#### ITDT支持的操作
- 基本操作：load, unload, rewind, erase, format
- 读写操作：write, read, weof (write filemark)
- 定位操作：fsf, fsr, bsf, bsr (forward/backward space)
- 状态查询：tur, qrypos, devinfo, logsense
- 设备扫描：scan, qrypath
- 诊断测试：standardtest, systemtest, rwtest

#### 兼容性
- 支持Windows和Linux平台
- 保持与现有SCSI接口的兼容性
- API接口保持不变，切换透明

## [0.0.14] - 2025-11-04

### 修复

#### 计划任务系统JavaScript错误修复
- ✅ 修复`getActionConfig`方法中的null检查问题
  - 修复`Cannot read properties of null (reading 'value')`错误
  - 增强元素查找函数，支持全局和面板内查找
  - 添加安全的值获取函数`val()`，处理元素不存在的情况
  - 添加安全的选中状态获取函数`checked()`，使用可选链操作符
  - 添加元素可见性检查函数`isElementVisibleAndExists()`
  - 在所有DOM元素访问前添加存在性检查，避免null引用错误

#### 模态框aria-hidden警告修复
- ✅ 修复目录浏览器模态框的aria-hidden警告
  - 优化模态框显示逻辑，确保焦点管理正确
  - 改善无障碍访问体验

## [0.0.13] - 2025-11-04

### 改进

#### 磁带添加/更新逻辑优化
- ✅ 将“磁带标签”明确为磁带身份的唯一标识
- ✅ 当从磁带机读取到已有标签时：
  - 标签输入框设为只读，禁用月份选择与“生成标签”按钮
  - 其他字段可编辑
- ✅ 保存逻辑调整：
  - 若数据库已存在该标签则自动执行更新（PUT /api/tape/update/{tape_id}）
  - 若不存在则执行创建（POST /api/tape/create）
  - 模态框标题与确认按钮文案随模式自动切换（添加/更新）
- ✅ 仅前端逻辑调整（`web/templates/tape.html`），兼容后端现有接口

## [0.0.12] - 2025-11-04

### 修复

#### 计划任务系统修复
- ✅ 修复时间格式解析问题
  - 支持多种时间格式：`2025-11-04 17:05:00`、`2025/11/04 17:05:00`、`2025-11-04 17:05`、`2025/11/04 17:05`
  - 使用链式try-except处理不同格式，提高兼容性
  
- ✅ 修复openGauss数据库操作问题
  - `get_task`、`update_task`、`delete_task` 方法对openGauss使用原生SQL查询（asyncpg）
  - 避免SQLAlchemy版本解析错误：`Could not determine version from string '(openGauss-lite 7.0.0-RC1...)'`
  - 正确处理枚举类型（使用CAST）和JSON字段
  - 确保整数字段（total_runs、success_runs、failure_runs）不为None
  
- ✅ 修复API路由匹配问题
  - 调整路由定义顺序：更具体的路径（如`/tasks/{task_id}/disable`）必须在通用路径（如`/tasks/{task_id}`）之前定义
  - 修复`POST /api/scheduler/tasks/{task_id}/disable`和`DELETE /api/scheduler/tasks/{task_id}`返回404的问题
  - 确保FastAPI能够正确匹配路由

- ✅ 增强错误日志
  - 所有数据库操作错误都包含详细的堆栈跟踪信息
  - 便于调试和排查问题

## [0.0.11] - 2025-11-04

### 改进

#### 计划任务界面全面优化
- ✅ 优化计划任务弹出窗口布局
  - 任务名称、备份目标和备份类型一行三个字段（各占 col-md-4）
  - 调度类型、任务动作类型和调度配置一行三个字段（各占 col-md-4）
  - 备份源路径和目标路径一行显示（各占 col-md-6）
  - 任务描述和排除模式一行显示（各占 col-md-6）
  
- ✅ 备份目标配置优化
  - 备份目标选择改为下拉框（磁带机/存储），默认磁带机
  - 备份目标选择"磁带机"时：显示磁带机选择区域，隐藏目标路径
  - 备份目标选择"存储"时：显示目标路径，隐藏磁带机选择
  - 磁带机和存储共用同一位置，根据选择动态切换显示
  
- ✅ 目标磁带机选择优化
  - 改为下拉框 + 添加按钮方式（类似目录添加逻辑）
  - 下拉框包含"自动选择磁带机"、"全部磁带机"和所有可用磁带机
  - 选择后点击"添加"按钮添加到列表（深色背景卡片显示）
  - 支持添加多个磁带机
  - 选择"全部磁带机"时自动清除其他选择
  - 已选择"全部磁带机"时不允许添加单个磁带机
  
- ✅ 目标路径和源路径优化
  - 目标路径支持多选（类似源路径）
  - 目标路径列表使用深色背景卡片显示
  - 浏览按钮文字改为"浏览"（不再显示"浏览并添加"）
  - 合并浏览和添加功能：浏览选择后自动添加，无需再点击添加按钮
  
- ✅ 排除模式优化
  - 预填常用排除项：*.bak, *.tmp, *.log, *.swp, *.cache, Thumbs.db, .DS_Store, $RECYCLE.BIN, System Volume Information, pagefile.sys, $*
  - 删除提示文字"支持通配符模式，每行一个"

## [0.0.10] - 2025-11-03

### 改进

#### API路由优化
- ✅ 优化磁带管理API路由命名
  - `PUT /api/tape/{tape_id}` 改为 `PUT /api/tape/update/{tape_id}`
  - `GET /api/tape/{tape_id}` 改为 `GET /api/tape/show/{tape_id}`
  - 更新所有相关前端调用

#### 磁带管理界面优化
- ✅ 优化磁带扫描和标签读取流程
  - 合并"读取磁带标签"功能到"从磁带机扫描信息"按钮
  - 扫描时并行执行设备扫描和读取磁带标签
  - 优化扫描结果模态框显示格式：
    - 设备信息：标题在上，关键信息（厂商、型号、产品）显示在下方
    - 磁带信息：标题在上，关键信息（磁带标签、序列号、创建日期）对齐显示
  - 序列号使用后端Python `uuid.uuid4()`生成（HEX格式：全大写无连字符，32字符）
  - 添加后端UUID生成API：`GET /api/tape/generate-uuid`
  - 删除序列号刷新按钮，序列号由扫描结果自动生成
  - "应用信息"按钮仅在获得磁带标签后启用
  - 优化设备信息显示：隐藏"Unknown"序列号，代码块使用深底白字样式

#### 计划任务界面优化
- ✅ 优化计划任务弹出窗口布局
  - 任务名称和备份类型并排显示（第一行标签，第二行输入框）

## [0.0.9] - 2025-11-02

### 新增

- 计划任务目录浏览能力
  - 新增后端文件系统浏览接口：GET `/api/system/file-system/drives`、GET `/api/system/file-system/list?path=...`
  - Windows 使用 WinAPI 获取盘符，Linux 获取挂载点，统一返回结构
  - 前端新增左右分栏目录浏览器（驱动器/网络路径 + 目录/文件列表）
  - 支持输入网络路径（`\\server\share`、`//server/share`、`smb://...`）

- 备份管理与计划任务联动
  - 计划任务可选择备份模板执行，避免重复配置
  - 执行时自动生成备份记录并关联模板，防止同模板同日重复执行

### 改进

- 计划任务 UI 优化
  - 备份源路径支持多选与样式优化（卡片式、网络标签、路径截断）
  - 目录浏览器深色主题适配，文本高对比度，交互高亮
  - 表单布局优化：
    - “备份类型 + 目标磁带机”同一行两列
    - “启用压缩 + 启用加密 + 启用任务”同一行三列，并移动到“排除模式”上方

- 依赖与加载顺序
  - 全局引入 `axios.min.js`，修复 `axios is not defined` 问题（base 模板统一加载）
  - `scheduler.js` 作为 ES Module 导入并兼容全局 axios

- 系统与数据库
  - 数据库初始化引入所有模型，确保新表与枚举自动创建
  - 移除应用启动时的路由清单调试日志

### 修复

- SQLAlchemy Declarative 冲突：将 `metadata` 字段重命名为 `task_metadata`
- openGauss 版本解析导致任务加载报错的容错处理（记录日志，不阻塞表创建）


所有重要的变更都会记录在此文件中。

本文档遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/) 格式。

## [0.0.8] - 2025-11-01

### 新增

#### 磁带创建和管理逻辑优化
- ✅ 完善磁带创建逻辑：月份选择默认当前月，创建和过期日期仅计算年月
  - 创建日期和过期日期计算只考虑年月，忽略具体日期
  - 过期判断仅比较年月：当前年月 >= 过期年月
  - 月份选择器默认选中当前月，添加提示文本
  - 实现磁带标签读取后自动填充：如果数据库已存在，禁用标签输入框并更新其他信息
  
#### 磁带标签保持不变机制
- ✅ 实现擦除和格式化后保留磁带标签
  - 擦除操作后自动重新写入原始标签，保留创建日期和过期日期
  - 格式化操作前读取现有标签，格式化成功后重新写入
  - 确保磁带标签在整个生命周期内保持恒定

### 修复

#### LTFS文件系统标签支持
- ✅ 修复Windows下LTFS环境中无法读取磁带标签的问题
  - 实现双模式标签读取/写入：优先使用LTFS文件系统（`O:\TAPE_LABEL.txt`），失败回退到SCSI磁带头
  - Windows系统且配置了LTFS盘符时，自动从文件系统读取标签
  - 兼容IBM LTFS环境中磁带设备被独占的情况
  - 解析LTFS格式的标签文件（TAPE_xxx, Created, Capacity字段）
  - 写入标签时同样优先写入LTFS文件系统
  
#### 时区感知datetime处理
- ✅ 修复过期判断时区问题：使用timezone-aware datetime进行比较
  - 数据库返回的是timezone-aware datetime，需要使用相同的类型进行比较
  - 统一使用datetime.now(timezone.utc)进行过期判断

## [0.0.7] - 2025-10-31

### 移除

#### UUID功能完全移除
- ✅ 移除所有UUID相关功能和代码
  - 从数据库模型中删除`tape_uuid`和`set_uuid`字段
  - 删除物理UUID读取相关的所有SCSI接口方法
  - 移除UUID读取失败的错误对话框
  - 删除磁带列表中的UUID显示
  - 简化磁带创建流程，取消UUID验证要求
  - 清理所有UUID相关的Windows Storage API和VPD读取代码

## [0.0.6] - 2025-10-30

### 新增

#### 磁带管理扫描结果模态框
- ✅ 添加磁带设备扫描结果详情显示
  - 扫描后显示完整设备信息（厂商、型号、序列号、设备类型、建议容量等）
  - 提供"应用信息"按钮自动填充表单
  - 改善用户体验

#### 磁带编辑功能
- ✅ 实现磁带记录编辑功能
  - 支持编辑磁带序列号、类型、容量、位置、备注
  - 主键(tape_id)和标签不允许修改
  - 添加PUT `/api/tape/{tape_id}` 更新API

#### 物理磁带标签读写
- ✅ 实现磁带物理标签自动读写功能
  - 创建磁带时自动写入标签到物理磁带
  - 读取磁带标签以检测是否已格式化
  - **独立的写入标签API**：POST `/api/tape/write-label`允许用户修改现有磁带的卷标
  - 智能格式化：仅未格式化磁带才格式化
  - 使用SCSI READ(16)/WRITE(16)命令直接操作磁带头部
  - 修复读写操作错误处理以检查success字段
  - **WMI到DOS路径映射**：自动将WMI DeviceID转换为Windows DOS设备路径
  - **磁带设备路径验证**：优先使用\\.\TAPEn路径，通过INQUIRY命令验证设备类型确保准确性

### 修复

#### IBM LTO检测修复
- ✅ 修复WMI识别vendor字段错误导致LTO检测失败
  - vendor为"LTO"时从model或path中提取IBM信息
  - 从model中提取LTO代数并设置建议容量
  - Windows/Linux平台全面修复

#### 磁带容量单位转换
- ✅ 修复TB转GB容量计算错误
  - 18TB应转换为18432GB（18 * 1024）
  - 更新前后端容量转换逻辑

#### 数据库枚举值大小写
- ✅ 修复PostgreSQL/OpenGauss枚举值区分大小写问题
  - 使用全大写枚举值（AVAILABLE, IN_USE, FULL等）
  - 统一前后端状态值格式

#### 磁带创建API兼容性
- ✅ 修复磁带创建API在OpenGauss上的兼容性问题
  - 使用psycopg2直接连接避免SQLAlchemy版本解析
  - 修复tape status枚举值大小写问题
  - `/api/tape/list`、`/api/tape/create`、`/api/tape/check`全面优化
  - 明确指定health_score默认值为100，确保新磁带显示健康而非严重

#### 磁带显示问题
- ✅ 修复磁带卡片显示异常
  - health_score为空时默认100分
  - 状态映射支持大小写（AVAILABLE/available等）
  - 添加过期时间显示
  - 类型和容量显示为"LTO-9 / 18.0 TB"格式
  - 使用容量显示为"实际使用/实际最大容量"

### 改进

#### 磁带默认值优化
- ✅ 添加磁带默认位置为"机房"
  - 新磁带创建时自动填充位置为"机房"

#### 磁带下拉选择器
- ✅ 修复磁带容量下拉选择器值匹配问题
  - 统一选项值格式（18TB而非18 TB）
  - 确保自动填充功能正常工作

#### 调试信息增强
- ✅ 添加数据库枚举类型创建和检查调试日志
  - 显示枚举类型值信息
  - 便于排查数据库兼容性问题

## [0.0.5] - 2025-10-29

### 新增

#### 模态框拖拽功能
- ✅ 实现所有模态框的可拖拽功能
  - 模态框内容区域可移动
  - 移除模态框透明度，改为不透明深色背景(#0d1626)
  - 改善模态框的用户交互体验

#### 数据库自动初始化
- ✅ OpenGauss数据库自动创建和权限配置
  - 自动检测数据库是否存在，不存在则创建
  - 自动设置数据库所有者权限
  - 自动配置public schema权限和默认权限
  - 使用psycopg2直接执行DDL语句

#### 磁带管理数据库集成
- ✅ 磁带列表API支持数据库查询
  - `/api/tape/list` - 从数据库获取所有磁带记录
  - `/api/tape/inventory` - 从数据库聚合统计信息
  - 支持磁带状态、位置、健康状态筛选
  - 支持磁带搜索功能

#### 磁带操作数据库同步
- ✅ 磁带操作自动同步到数据库
  - `load_tape` - 加载磁带时更新状态为in_use
  - `unload_tape` - 卸载磁带时更新状态为available
  - `erase_tape` - 擦除磁带时重置磁带信息
  - `write_data` - 写入数据后更新used_bytes和write_count

#### 磁带管理UI动态加载
- ✅ 磁带管理页面完全动态化
  - 统计卡片：磁带总数、可用磁带、已满磁带、错误磁带
  - 磁带列表：支持状态、位置、健康状态筛选
  - 磁带搜索：按标签、序列号、ID搜索
  - 磁带详情：动态显示完整磁带信息

### 修复

#### OpenGauss兼容性
- ✅ 修复OpenGauss版本字符串解析问题
  - 使用psycopg2直接连接，绕过SQLAlchemy版本解析
  - 修复DATETIME类型在OpenGauss中不支持的问题，改用TIMESTAMP
  - 修复ENUM类型创建问题，使用SQLAlchemy引擎生成SQL
  - 移除无效的server_version_check参数

#### 数据库初始化
- ✅ 修复数据库未初始化导致的RuntimeError
  - 统一使用全局db_manager实例
  - 修复main.py中创建新DatabaseManager实例的问题
  - 使用依赖注入get_db()获取数据库会话

#### 模态框显示
- ✅ 彻底禁用模态框遮罩层
  - 设置.modal-backdrop { display: none !important; }
  - 移除所有模态框的data-bs-backdrop属性或设为false
  - 设置模态框z-index为9999确保显示在最上层

#### 系统启动容错
- ✅ 系统启动时数据库连接失败不退出
  - 组件初始化失败时记录警告日志继续启动
  - 允许用户通过Web界面修复配置
  - 提供清晰的错误提示信息

### 改进

#### 磁带机配置页面
- ✅ 自动扫描磁带设备
  - 页面加载时自动触发设备扫描
  - 连接状态和已检测设备并排显示
  - 提高用户体验

#### 数据库健康检查
- ✅ OpenGauss数据库健康检查优化
  - 使用psycopg2直接连接，避免SQLAlchemy版本解析
  - 异步健康检查使用原生SQL语句
  - 添加5秒连接超时设置

#### API性能优化
- ✅ 减少重复API调用
  - `loadTapeStatistics`使用已加载的磁带数据
  - 优化DOMContentLoaded事件处理顺序
  - 提升页面加载速度

## [0.0.4] - 2025-10-28

### 修复

#### 模态框显示问题
- ✅ 修复所有模态框(modal)被遮罩层遮挡的问题
  - 设置模态框 z-index 为 9999
  - 设置遮罩层 z-index 为 9998
  - 确保所有弹窗(添加磁带、扫描磁带、添加通知人员等)正常显示

#### 配置保存优化
- ✅ 修复通知设置保存后被重载覆盖的问题
- ✅ 修复数据库配置保存后被重载覆盖的问题
- ✅ 修复磁带机配置保存后被重载覆盖的问题
- ✅ 保存配置后不再自动重载，保持用户输入内容

#### 数据库支持
- ✅ 添加 OpenGauss 数据库方言支持
  - 安装 `opengauss-sqlalchemy>=2.4.0` 依赖包
  - 支持在系统设置中选择 OpenGauss 数据库类型

#### 配置测试优化
- ✅ 数据库测试从输入框读取配置信息
- ✅ 通知测试从输入框读取配置信息
- ✅ 测试前不需要保存配置，实时验证

### 改进

#### UI统一性增强
- ✅ 统一所有页面使用 console-panel 和 service-card 样式
- ✅ 首页、备份管理、恢复管理、磁带管理等页面视觉一致
- ✅ 首页磁带设备状态动态加载
- ✅ 提高文字亮度，改善深色背景下的可读性

#### 导航优化
- ✅ 磁带机配置提升为独立的顶级导航项
- ✅ 系统设置移除磁带机配置选项卡
- ✅ 系统设置默认显示"常规设置"选项卡
- ✅ 优化选中标签的文字颜色和背景

## [0.0.3] - 2025-10-28

### 新增

#### 用户界面全面升级
- ✅ 应用现代暗色科技主题界面
  - 从.sample目录引入完整的UI组件和样式
  - 深色背景色(#0a0e17)配合紫色系主题
  - 响应式布局，支持各种屏幕尺寸
- ✅ 丰富的视觉背景效果
  - 粒子动画效果(particles.js)
  - 电路板图案覆盖层
  - 数据流动画背景
  - 毛玻璃模糊效果(backdrop-filter)
- ✅ 优化的导航栏设计
  - 固定顶部导航栏
  - 半透明背景与模糊效果
  - 悬停和激活状态的平滑过渡动画
  - 用户头像和版本信息显示
- ✅ 控制台面板(console-panel)样式
  - 半透明卡片的毛玻璃效果
  - 圆角边框和阴影
  - 悬停时的上浮动画效果
- ✅ 服务卡片(service-card)组件
  - 顶部彩色渐变条
  - 图标容器样式
  - 状态指示器可视化
- ✅ 首页完全重构
  - 系统状态卡片展示
  - 快速操作按钮
  - 系统信息表格
  - 响应式栅格布局

### 改进

- ✅ 配色方案更新
  - 主色调从蓝色改为紫色(#8b7cf6)
  - 更适合磁带备份系统的科技感
  - 统一的色彩变量系统
- ✅ 字体系统优化
  - 使用AlimamaDaoLiTi字体
  - 统一的基础字体大小(0.9rem)
  - 导航、标题、正文分层字体大小
- ✅ 静态资源整理
  - 引入Bootstrap Icons图标库
  - 引入Particles.js动画库
  - 引入Marked.js Markdown渲染
  - 整理CSS/JS/图片资源结构

### 技术细节

#### 前端资源
- `web/static/css/ai.css` - 主样式文件(紫色主题)
- `web/static/js/components/backgroundEffects.js` - 背景效果
- `web/static/js/vendor/` - 第三方库文件
- `web/static/img/` - Logo和装饰图片

#### 模板更新
- `web/templates/base.html` - 基础模板全面重构
- `web/templates/index.html` - 首页应用新UI风格

#### UI特性
- CSS变量系统支持主题定制
- 响应式设计，移动端友好
- 动画过渡效果，提升用户体验
- 毛玻璃和模糊效果，现代感强

## [0.0.2] - 2025-10-28

### 新增

#### 磁带机配置增强
- ✅ 最大卷大小单位改为GB显示（更用户友好）
- ✅ 已检测设备显示磁盘容量和LTO代数信息
- ✅ 设备列表增强显示（厂商、型号、路径、容量、状态）

#### 通知系统增强
- ✅ 完整的通知事件配置界面
  - 备份相关：成功、开始、失败
  - 恢复相关：成功、失败
  - 磁带相关：更换、过期、错误
  - 系统相关：容量预警、系统错误
- ✅ 通知人员管理界面
  - 添加通知人员模态框
  - 支持多人员通知配置

#### 备份任务创建增强
- ✅ 新增备份类型支持
  - 完整备份 (Full Backup)
  - 增量备份 (Incremental)
  - 差异备份 (Differential)
  - 镜像备份 (Mirror) - 新增
  - 归档备份 (Archive) - 新增
  - 快照备份 (Snapshot) - 新增
- ✅ 源路径选择改进
  - 输入框支持多路径
  - 浏览按钮（待实现文件选择对话框）
- ✅ 备份目标选择
  - 磁盘存储
  - 磁带机（可选择具体磁带）

#### SCSI接口增强
- ✅ 新增磁带SCSI操作命令
  - format_tape - 格式化磁带
  - erase_tape - 擦除磁带
  - load_unload - 加载/卸载
  - space_blocks - 按块定位
  - write_filemarks - 写入文件标记
  - set_mark - 设置磁带标记
- ✅ 新增磁带操作API端点
  - POST /api/tape/format - 格式化
  - POST /api/tape/rewind - 倒带
  - POST /api/tape/space - 定位

## [0.0.1] - 2025-10-28

### 新增

#### 配置管理
- ✅ 新增 `.env.sample` 配置模板文件
- ✅ 新增 `SystemConfig` 数据模型（`models/system_config.py`）
- ✅ 新增 `SystemConfigManager` 配置管理器（`config/config_manager.py`）
- ✅ 新增 Web界面数据库配置功能
- ✅ 支持多数据库类型（SQLite、PostgreSQL、openGauss、MySQL）
- ✅ 配置参数自动从.env文件加载
- ✅ 数据库配置支持Web界面修改

#### 文档
- ✅ `README.md` - 项目说明文档
- ✅ `docs/系统架构.md` - 系统架构说明
- ✅ `docs/使用说明.md` - 用户使用指南
- ✅ `docs/开发说明.md` - 开发指南
- ✅ `docs/配置管理说明.md` - 配置管理说明
- ✅ `docs/数据库配置说明.md` - 数据库配置说明
- ✅ `docs/数据库配置测试说明.md` - 数据库配置测试指南
- ✅ `docs/配置参数存储规划.md` - 配置存储策略
- ✅ `docs/配置系统优化总结.md` - 配置系统优化总结
- ✅ `docs/仅使用OpenGauss的可行性说明.md` - OpenGauss使用说明
- ✅ `docs/Redis和Celery使用说明.md` - Redis/Celery使用说明
- ✅ `docs/IBM磁带机API使用示例.md` - IBM磁带机API示例
- ✅ `docs/IBM磁带机快速开始指南.md` - IBM磁带机快速开始
- ✅ `docs/IBM磁带机集成说明.md` - IBM磁带机集成说明

#### 核心功能
- ✅ 备份引擎（`backup/backup_engine.py`）
- ✅ 恢复引擎（`recovery/recovery_engine.py`）
- ✅ 磁带管理器（`tape/tape_manager.py`）
- ✅ 磁带操作（`tape/tape_operations.py`）
- ✅ SCSI接口（`tape/scsi_interface.py`）
- ✅ 核心备份处理器（`mcp/core.py`）
- ✅ 计划任务调度器（`utils/scheduler.py`）
- ✅ 钉钉通知器（`utils/dingtalk_notifier.py`）
- ✅ 日志管理器（`utils/logger.py`）

#### Web界面
- ✅ 主入口（`web/app.py`）
- ✅ 备份管理API（`web/api/backup.py`）
- ✅ 恢复管理API（`web/api/recovery.py`）
- ✅ 磁带管理API（`web/api/tape.py`）
- ✅ 系统管理API（`web/api/system.py`）
- ✅ 用户管理API（`web/api/user.py`）
- ✅ 认证中间件（`web/middleware/auth_middleware.py`）
- ✅ 日志中间件（`web/middleware/logging_middleware.py`）
- ✅ HTML模板（`web/templates/`）
  - index.html - 首页
  - backup.html - 备份管理
  - recovery.html - 恢复管理
  - tape.html - 磁带管理
  - system.html - 系统设置
  - base.html - 基础模板

#### 数据模型
- ✅ 基础模型（`models/base.py`）
- ✅ 备份模型（`models/backup.py`）
- ✅ 磁带模型（`models/tape.py`）
- ✅ 用户模型（`models/user.py`）
- ✅ 系统日志模型（`models/system_log.py`）
- ✅ 系统配置模型（`models/system_config.py`）

#### 静态资源
- ✅ CSS样式（`web/static/css/main.css`）
- ✅ JavaScript（`web/static/js/main.js`）
- ✅ Markdown渲染支持

#### 测试
- ✅ 测试框架配置（`tests/conftest.py`）
- ✅ 备份功能测试（`tests/test_backup.py`）
- ✅ 磁带功能测试（`tests/test_tape.py`）
- ✅ 配置功能测试（`tests/test_config.py`）

#### 文档
- ✅ `SCSI接口实现分析报告.md` - SCSI接口分析

### 修改

#### 配置优化
- ✅ 脱敏敏感配置信息（密码、密钥）
- ✅ 补充配置参数（ENVIRONMENT, WEB_HOST, ENABLE_CORS等）
- ✅ 统一Base模型引用
- ✅ 优化配置加载逻辑
- ✅ 数据库文件路径标准化（移动到data目录）
- ✅ 版本号从1.0.0调整为0.0.1
- ✅ 完善.env和.env.sample文件

#### 数据库
- ✅ 优化openGauss版本检测处理
- ✅ 支持异步和同步数据库操作
- ✅ 添加数据库连接池配置
- ✅ 改进数据库健康检查
- ✅ 支持SQLite数据库文件路径配置

### 安全

- ✅ 敏感信息脱敏处理
- ✅ 密码字段加密存储
- ✅ JWT令牌认证
- ✅ 输入验证和SQL注入防护

### 文档

- ✅ 完善README.md项目说明
- ✅ 添加详细的配置说明文档
- ✅ 补充开发和使用指南
- ✅ 创建系统架构文档

### 已知问题

- ⚠️ 磁带设备检测功能需要实际硬件测试
- ⚠️ 部分核心功能需要实际业务验证
- ⚠️ 计划任务持久化待完善

### 待办事项

- 🔲 标准LOG SENSE解析优化
- 🔲 MODE SENSE/SELECT完善
- 🔲 UI SCSI状态显示
- 🔲 实现Celery分布式任务（可选）
- 🔲 添加配置加密功能
- 🔲 完善Web界面交互
- 🔲 增加更多测试用例
- 🔲 实现配置版本管理

### 版本管理

- ✅ 新增CHANGELOG.md版本管理文件
- ✅ 实现版本API接口（GET /api/system/version）
- ✅ 添加UI版本显示和弹窗功能
- ✅ 优化数据库文件路径管理（data/taf_backup.db）
- ✅ 配置参数脱敏和标准化

#### SCSI接口重构

- ✅ 完善Windows SCSI Pass Through完整实现
  - 完整实现SCSI_PASS_THROUGH结构填充
  - 正确执行DeviceIoControl调用
  - 支持数据双向传输
- ✅ 修复Linux SG_IO导入问题
  - 优化平台特定导入逻辑
  - 确保fcntl正确可用
- ✅ 实现READ/WRITE SCSI命令
  - READ(16) 和 WRITE(16)完整实现
  - 支持64位LBA寻址
  - 替换旧式READ/WRITE(6)
- ✅ 添加SCSI命令重试机制
  - 指数退避重试策略
  - 智能错误类型判断
  - 自动处理临时性错误
- ✅ 实现设备热插拔监控
  - 设备连接/断开自动检测
  - 状态变化事件通知
  - 监控任务管理
- ✅ 优化SCSI接口架构
  - 代码结构优化
  - 错误处理增强
  - 日志记录完善

#### 磁带机配置UI

- ✅ 新增磁带机配置标签页
  - 设备路径配置
  - 块大小和卷大小配置
  - 磁带池配置
- ✅ 实现配置API
  - GET /api/system/tape/config - 获取配置
  - POST /api/system/tape/test - 测试连接
  - PUT /api/system/tape/config - 保存配置
  - GET /api/system/tape/scan - 扫描设备
- ✅ 设备扫描和测试
  - 实时扫描磁带设备
  - 连接状态测试
  - 设备列表显示

#### Bug修复

- ✅ 数据库健康检查修复（text导入）
- ✅ Recovery API Request参数修复
- ✅ 数据库配置密码自动填充
- ✅ 磁带连接测试逻辑优化
- ✅ 错误处理改进

#### 文档

- ✅ `SCSI接口重构总结.md` - SCSI重构文档
- ✅ `Build_Summary_20241101.md` - 构建总结

## [未发布]

### 计划

- 标准LOG SENSE解析（需要IBM文档参考）
- MODE SENSE/SELECT完整实现
- UI SCSI状态显示增强
- 配置加密功能
- 配置导入/导出
- 配置变更历史
- 配置版本回滚
- 更多监控指标
- 性能优化

---

**企业级磁带备份系统**
项目地址: https://github.com/grigs28/TAF
版本：v0.1.32

