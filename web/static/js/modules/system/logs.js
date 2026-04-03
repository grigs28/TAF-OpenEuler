// 系统日志管理
// System Logs Management

let currentLogPage = 0;
let logPageSize = 50;
let totalLogCount = 0;

// 分类和操作类型的中文映射
const categoryLabels = {
    'system': '系统', 'backup': '备份', 'recovery': '恢复', 'tape': '磁带',
    'user': '用户', 'security': '安全', 'scheduler': '计划任务',
    'api': 'API', 'database': '数据库', 'performance': '性能', 'web': 'Web'
};

const levelLabels = {
    'debug': '调试', 'info': '信息', 'warning': '警告',
    'error': '错误', 'critical': '严重'
};

const operationLabels = {
    'create': '创建', 'update': '更新', 'delete': '删除', 'read': '读取',
    'execute': '执行', 'config': '配置', 'export': '导出', 'import': '导入',
    'login': '登录', 'logout': '登出',
    'tape_load': '加载磁带', 'tape_unload': '卸载磁带', 'tape_eject': '弹出磁带',
    'tape_scan': '扫描磁带', 'tape_read_label': '读取标签', 'tape_write_label': '写入标签',
    'tape_erase': '擦除磁带', 'tape_format': '格式化磁带', 'tape_mount': '挂载磁带',
    'tape_unmount': '卸载磁带', 'tape_verify': '验证磁带', 'tape_rewind': '回绕磁带',
    'tape_position': '定位磁带',
    'backup_start': '备份开始', 'backup_complete': '备份完成',
    'backup_failed': '备份失败', 'backup_cancel': '备份取消',
    'recovery_start': '恢复开始', 'recovery_complete': '恢复完成',
    'recovery_failed': '恢复失败',
    'scheduler_create': '创建计划任务', 'scheduler_update': '更新计划任务',
    'scheduler_delete': '删除计划任务', 'scheduler_execute': '执行计划任务'
};

// 初始化系统日志
document.addEventListener('DOMContentLoaded', function() {
    var logsTab = document.getElementById('logs-tab');
    if (logsTab) {
        logsTab.addEventListener('shown.bs.tab', function() {
            loadLogFilterOptions();
            loadSystemLogs();
        });

        if (logsTab.classList.contains('active')) {
            loadLogFilterOptions();
            loadSystemLogs();
        }
    }

    // 筛选器变化自动刷新
    ['logCategory', 'logLevel', 'logOperationType'].forEach(function(id) {
        var el = document.getElementById(id);
        if (el) {
            el.addEventListener('change', function() {
                currentLogPage = 0;
                loadSystemLogs();
            });
        }
    });

    // 刷新日志按钮
    var refreshLogsBtn = document.getElementById('refreshLogsBtn');
    if (refreshLogsBtn) {
        refreshLogsBtn.addEventListener('click', function() {
            loadLogFilterOptions();
            loadSystemLogs();
        });
    }

    // 日志级别切换
    loadCurrentLogLevel();
    var logLevelToggle = document.getElementById('logLevelToggle');
    if (logLevelToggle) {
        logLevelToggle.addEventListener('click', function(e) {
            var btn = e.target.closest('button[data-level]');
            if (!btn) return;
            var level = btn.getAttribute('data-level');
            if (!level) return;
            // 保存到后端，成功后再高亮按钮
            fetch('/api/system/env-config', {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({log_level: level})
            }).then(function(r) { return r.json(); }).then(function(result) {
                if (result.success) {
                    logLevelToggle.querySelectorAll('button').forEach(function(b) { b.classList.remove('active'); });
                    btn.classList.add('active');
                    console.log('日志级别已切换为: ' + level);
                } else {
                    alert('切换失败: ' + (result.detail || '未知错误'));
                    loadCurrentLogLevel();
                }
            }).catch(function(err) {
                alert('切换失败: ' + err.message);
                loadCurrentLogLevel();
            });
        });
    }

    // 导出Excel按钮
    var exportLogsBtn = document.getElementById('exportLogsBtn');
    if (exportLogsBtn) {
        exportLogsBtn.addEventListener('click', function() {
            var params = new URLSearchParams();
            var category = document.getElementById('logCategory');
            var level = document.getElementById('logLevel');
            var opType = document.getElementById('logOperationType');
            if (category && category.value) params.set('category', category.value);
            if (level && level.value) params.set('level', level.value);
            if (opType && opType.value) params.set('operation_type', opType.value);
            window.open('/api/system/logs/export?' + params.toString(), '_blank');
        });
    }

    // 清空日志按钮
    var clearLogsBtn = document.getElementById('clearLogsBtn');
    if (clearLogsBtn) {
        clearLogsBtn.addEventListener('click', function() {
            if (confirm('确定要清空系统日志吗？此操作不可恢复！')) {
                clearSystemLogs();
            }
        });
    }

    // 分页按钮
    var logPrevPage = document.getElementById('logPrevPage');
    var logNextPage = document.getElementById('logNextPage');
    var logPageSizeSelect = document.getElementById('logPageSize');

    if (logPrevPage) {
        logPrevPage.addEventListener('click', function() {
            if (currentLogPage > 0) {
                currentLogPage--;
                loadSystemLogs();
            }
        });
    }

    if (logNextPage) {
        logNextPage.addEventListener('click', function() {
            currentLogPage++;
            loadSystemLogs();
        });
    }

    if (logPageSizeSelect) {
        logPageSizeSelect.addEventListener('change', function() {
            logPageSize = parseInt(this.value, 10);
            currentLogPage = 0;
            loadSystemLogs();
        });
    }
});

// 加载筛选器选项（从数据库实际存在的值）
async function loadLogFilterOptions() {
    try {
        var response = await fetch('/api/system/log-filters');
        var result = await response.json();
        if (!result.success) return;

        // 保留当前选中值
        var catSelect = document.getElementById('logCategory');
        var lvlSelect = document.getElementById('logLevel');
        var opSelect = document.getElementById('logOperationType');

        var curCat = catSelect ? catSelect.value : '';
        var curLvl = lvlSelect ? lvlSelect.value : '';
        var curOp = opSelect ? opSelect.value : '';

        // 填充分类
        if (catSelect && result.categories) {
            catSelect.options.length = 1;
            result.categories.forEach(function(val) {
                var opt = document.createElement('option');
                opt.value = val;
                opt.textContent = categoryLabels[val] || val;
                catSelect.appendChild(opt);
            });
            catSelect.value = curCat;
        }

        // 填充级别
        if (lvlSelect && result.levels) {
            lvlSelect.options.length = 1;
            result.levels.forEach(function(val) {
                var opt = document.createElement('option');
                opt.value = val;
                opt.textContent = levelLabels[val] || val;
                lvlSelect.appendChild(opt);
            });
            lvlSelect.value = curLvl;
        }

        // 填充操作类型
        if (opSelect && result.operation_types) {
            opSelect.options.length = 1;
            result.operation_types.forEach(function(val) {
                var opt = document.createElement('option');
                opt.value = val;
                opt.textContent = operationLabels[val] || val;
                opSelect.appendChild(opt);
            });
            opSelect.value = curOp;
        }
    } catch (err) {
        console.error('加载日志筛选选项失败:', err);
    }
}

// 加载系统日志
async function loadSystemLogs() {
    const logContainer = document.getElementById('logContainer');
    if (!logContainer) return;
    
    // 显示加载中
    logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-hourglass-split me-2"></i>加载中...</div>';
    
    try {
        // 获取筛选条件
        const category = document.getElementById('logCategory')?.value || '';
        const level = document.getElementById('logLevel')?.value || '';
        const operationType = document.getElementById('logOperationType')?.value || '';
        
        // 构建查询参数
        const params = new URLSearchParams({
            limit: logPageSize.toString(),
            offset: (currentLogPage * logPageSize).toString()
        });
        
        if (category) params.append('category', category);
        if (level) params.append('level', level);
        if (operationType) params.append('operation_type', operationType);
        
        const response = await fetch(`/api/system/logs?${params.toString()}`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const result = await response.json();
        
        if (result.success) {
            const logs = result.logs || [];
            const total = result.total !== undefined ? result.total : 0;
            displaySystemLogs(logs, total);
            updateLogPagination(total, logs.length);
        } else {
            logContainer.innerHTML = `<div class="text-center text-danger py-5">
                <i class="bi bi-exclamation-triangle me-2"></i>加载失败：${result.message || result.detail || '未知错误'}
            </div>`;
        }
    } catch (error) {
        console.error('加载系统日志失败:', error);
        logContainer.innerHTML = `<div class="text-center text-danger py-5">
            <i class="bi bi-exclamation-triangle me-2"></i>加载失败：${error.message}
        </div>`;
    }
}

// 显示系统日志
function displaySystemLogs(logs, total) {
    const logContainer = document.getElementById('logContainer');
    if (!logContainer) return;
    
    if (logs.length === 0) {
        logContainer.innerHTML = '<div class="text-center text-muted py-5"><i class="bi bi-inbox me-2"></i>暂无日志</div>';
        return;
    }
    
    let html = '';
    logs.forEach(log => {
        const timestamp = log.timestamp ? new Date(log.timestamp).toLocaleString('zh-CN') : '未知时间';
        const level = log.level || 'info';
        const levelClass = `bar-${level}`;
        const levelBadge = getLevelBadge(level);
        const category = log.category || 'system';
        const categoryBadge = getCategoryBadge(category);
        
        // 根据日志类型显示不同内容
        if (log.type === 'operation') {
            // 操作日志
            const operationName = log.operation_name || log.operation_type || '未知操作';
            const success = log.success !== false;
            const successBadge = success 
                ? '<span class="badge bg-success">成功</span>' 
                : '<span class="badge bg-danger">失败</span>';
            
            html += `
                <div class="log-entry mb-2 d-flex">
                    <div class="log-bar ${levelClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="content-line d-flex align-items-center flex-wrap">
                            ${levelBadge}
                            ${categoryBadge}
                            ${successBadge}
                            <span class="message">${operationName}</span>
                            <small class="text-muted ms-auto">${timestamp}</small>
                        </div>
                        ${log.operation_description ? `<div class="text-muted small mt-1">${log.operation_description}</div>` : ''}
                        ${log.error_message ? `<div class="text-danger small mt-1">错误：${log.error_message}</div>` : ''}
                        ${log.result_message ? `<div class="text-success small mt-1">${log.result_message}</div>` : ''}
                    </div>
                </div>
            `;
        } else {
            // 系统日志
            const message = log.message || '无消息';
            const module = log.module || '';
            const functionName = log.function || '';
            
            html += `
                <div class="log-entry mb-2 d-flex">
                    <div class="log-bar ${levelClass}"></div>
                    <div class="content flex-grow-1">
                        <div class="content-line d-flex align-items-center flex-wrap">
                            ${levelBadge}
                            ${categoryBadge}
                            <span class="message">${message}</span>
                            <small class="text-muted ms-auto">${timestamp}</small>
                        </div>
                        ${module || functionName ? `<div class="text-muted small mt-1">${module}${functionName ? '.' + functionName : ''}</div>` : ''}
                        ${log.stack_trace ? `<div class="text-danger small mt-1" style="white-space: pre-wrap; font-family: monospace;">${log.stack_trace}</div>` : ''}
                    </div>
                </div>
            `;
        }
    });
    
    logContainer.innerHTML = html;
}

// 获取级别徽章
function getLevelBadge(level) {
    const badges = {
        'debug': '<span class="badge bg-secondary">调试</span>',
        'info': '<span class="badge bg-info">信息</span>',
        'warning': '<span class="badge bg-warning text-dark">警告</span>',
        'error': '<span class="badge bg-danger">错误</span>',
        'critical': '<span class="badge bg-dark">严重</span>'
    };
    return badges[level] || badges['info'];
}

// 获取分类徽章
function getCategoryBadge(category) {
    const badges = {
        'system': '<span class="badge bg-primary">系统</span>',
        'backup': '<span class="badge bg-success">备份</span>',
        'recovery': '<span class="badge bg-info">恢复</span>',
        'tape': '<span class="badge bg-warning text-dark">磁带</span>',
        'user': '<span class="badge bg-secondary">用户</span>',
        'security': '<span class="badge bg-danger">安全</span>',
        'scheduler': '<span class="badge bg-purple">计划任务</span>',
        'api': '<span class="badge bg-cyan">API</span>',
        'database': '<span class="badge bg-orange">数据库</span>'
    };
    return badges[category] || '<span class="badge bg-secondary">' + category + '</span>';
}

// 更新日志分页信息
function updateLogPagination(total, returnedCount) {
    const paginationInfo = document.getElementById('logPaginationInfo');
    const logPrevPage = document.getElementById('logPrevPage');
    const logNextPage = document.getElementById('logNextPage');

    totalLogCount = total;

    if (paginationInfo) {
        if (total === 0) {
            paginationInfo.textContent = '暂无数据';
        } else {
            const start = currentLogPage * logPageSize + 1;
            const end = currentLogPage * logPageSize + returnedCount;
            paginationInfo.textContent = `显示 ${start}-${end} 条，共 ${total} 条`;
        }
    }

    if (logPrevPage) {
        logPrevPage.disabled = currentLogPage === 0;
    }

    if (logNextPage) {
        const hasMore = (currentLogPage + 1) * logPageSize < total;
        logNextPage.disabled = !hasMore || returnedCount === 0;
    }
}

// 清空系统日志
async function clearSystemLogs() {
    try {
        // 注意：这里需要后端提供清空日志的API
        // 目前先提示用户
        alert('清空日志功能需要后端API支持，请联系管理员');
    } catch (error) {
        console.error('清空系统日志失败:', error);
        alert('清空失败：' + error.message);
    }
}

// 加载当前日志级别并高亮按钮
async function loadCurrentLogLevel() {
    try {
        var response = await fetch('/api/system/env-config');
        var result = await response.json();
        if (result.success && result.config) {
            var currentLevel = (result.config.log_level || 'INFO').toUpperCase();
            var toggle = document.getElementById('logLevelToggle');
            if (toggle) {
                toggle.querySelectorAll('button').forEach(function(btn) {
                    btn.classList.remove('active');
                    if (btn.getAttribute('data-level') === currentLevel) {
                        btn.classList.add('active');
                    }
                });
            }
        }
    } catch (err) {
        console.error('加载日志级别失败:', err);
    }
}
