// 系统设置JavaScript
// 数据库配置
document.addEventListener('DOMContentLoaded', function() {
    const dbType = document.getElementById('dbType');
    if (dbType) {
        loadDatabaseConfig();
        loadSystemConfig();
        
        // 数据库类型切换
        dbType.addEventListener('change', function() {
            const dbTypeVal = this.value;
            const sqliteConfig = document.getElementById('sqliteConfig');
            const serverDbConfig = document.getElementById('serverDbConfig');
            const redisConfig = document.getElementById('redisConfig');
            const redisSpecificConfig = document.getElementById('redisSpecificConfig');
            const dbDatabaseGroup = document.getElementById('dbDatabaseGroup');
            const dbUserGroup = document.getElementById('dbUserGroup');
            const dbIndexGroup = document.getElementById('dbIndexGroup');
            
            if (dbTypeVal === 'sqlite') {
                sqliteConfig.style.display = 'block';
                serverDbConfig.style.display = 'none';
                redisConfig.style.display = 'none';
                if (redisSpecificConfig) redisSpecificConfig.style.display = 'none';
            } else if (dbTypeVal === 'redis') {
                sqliteConfig.style.display = 'none';
                serverDbConfig.style.display = 'none';  // Redis不使用serverDbConfig
                redisConfig.style.display = 'block';
                if (redisSpecificConfig) redisSpecificConfig.style.display = 'block';
                // Redis使用专门的配置项，不使用serverDbConfig
                if (dbDatabaseGroup) dbDatabaseGroup.style.display = 'none';
                if (dbUserGroup) dbUserGroup.style.display = 'none';
                if (dbIndexGroup) dbIndexGroup.style.display = 'none';
            } else {
                sqliteConfig.style.display = 'none';
                serverDbConfig.style.display = 'block';
                redisConfig.style.display = 'none';
                if (redisSpecificConfig) redisSpecificConfig.style.display = 'none';
                // 其他数据库需要用户名和数据库名
                if (dbDatabaseGroup) dbDatabaseGroup.style.display = 'block';
                if (dbUserGroup) dbUserGroup.style.display = 'block';
                if (dbIndexGroup) dbIndexGroup.style.display = 'none';
            }
        });
        
        // 扫描方法切换
        const scanMethod = document.getElementById('scanMethod');
        if (scanMethod) {
            scanMethod.addEventListener('change', function() {
                const esExePathGroup = document.getElementById('esExePathGroup');
                const scanMultithreadGroup = document.getElementById('scanMultithreadGroup');
                const scanThreadsGroup = document.getElementById('scanThreadsGroup');
                const useScanMultithread = document.getElementById('useScanMultithread');
                
                if (esExePathGroup) {
                    if (this.value === 'es') {
                        esExePathGroup.style.display = 'block';
                        // ES方法时隐藏多线程选项
                        if (scanMultithreadGroup) scanMultithreadGroup.style.display = 'none';
                    } else {
                        esExePathGroup.style.display = 'none';
                        // 默认方法时显示多线程选项
                        if (scanMultithreadGroup) scanMultithreadGroup.style.display = 'block';
                        // 根据多线程选项显示/隐藏线程数输入
                        if (useScanMultithread && scanThreadsGroup) {
                            scanThreadsGroup.style.display = useScanMultithread.checked ? 'block' : 'none';
                        }
                    }
                }
            });
        }
        
        // 多线程选项切换
        const useScanMultithread = document.getElementById('useScanMultithread');
        const scanThreadsGroup = document.getElementById('scanThreadsGroup');
        if (useScanMultithread && scanThreadsGroup) {
            useScanMultithread.addEventListener('change', function() {
                scanThreadsGroup.style.display = this.checked ? 'block' : 'none';
            });
        }
        
        // ES工具路径浏览按钮
        const browseEsExePathBtn = document.getElementById('browseEsExePath');
        if (browseEsExePathBtn) {
            browseEsExePathBtn.addEventListener('click', function() {
                // 创建隐藏的文件输入元素
                const input = document.createElement('input');
                input.type = 'file';
                input.accept = '.exe';
                input.style.display = 'none';
                
                input.addEventListener('change', function(e) {
                    const file = e.target.files[0];
                    if (file) {
                        const esExePathInput = document.getElementById('esExePath');
                        if (esExePathInput) {
                            // 在浏览器环境中，file.path不可用，使用file.name
                            // 用户可能需要手动调整路径为服务器端的完整路径
                            const fileName = file.name;
                            // 如果当前路径存在，尝试保持目录部分，只替换文件名
                            const currentPath = esExePathInput.value;
                            if (currentPath && currentPath.includes('\\')) {
                                const dirPath = currentPath.substring(0, currentPath.lastIndexOf('\\') + 1);
                                esExePathInput.value = dirPath + fileName;
                            } else {
                                // 如果没有当前路径，使用默认路径
                                esExePathInput.value = `E:\\app\\TAF\\ITDT\\ES\\${fileName}`;
                            }
                            // 提示用户可能需要手动调整路径
                            if (!file.path) {
                                console.log('提示：请确认文件路径是否正确，必要时请手动编辑为服务器端的完整路径');
                            }
                        }
                    }
                    // 清理临时元素
                    document.body.removeChild(input);
                });
                
                // 添加到DOM并触发点击
                document.body.appendChild(input);
                input.click();
            });
        }
        
        // Redis配置文件浏览按钮
        const browseRedisConfigBtn = document.getElementById('browseRedisConfigBtn');
        if (browseRedisConfigBtn) {
            browseRedisConfigBtn.addEventListener('click', function() {
                // 创建隐藏的文件输入元素
                const input = document.createElement('input');
                input.type = 'file';
                input.accept = '.conf';
                input.style.display = 'none';
                
                input.addEventListener('change', function(e) {
                    const file = e.target.files[0];
                    if (file) {
                        const redisConfigFilePathInput = document.getElementById('redisConfigFilePath');
                        if (redisConfigFilePathInput) {
                            // 在浏览器环境中，file.path不可用，使用file.name
                            // 用户可能需要手动调整路径为服务器端的完整路径
                            const fileName = file.name;
                            // 如果当前路径存在，尝试保持目录部分，只替换文件名
                            const currentPath = redisConfigFilePathInput.value;
                            if (currentPath && currentPath.includes('\\')) {
                                const dirPath = currentPath.substring(0, currentPath.lastIndexOf('\\') + 1);
                                redisConfigFilePathInput.value = dirPath + fileName;
                            } else if (currentPath && currentPath.includes('/')) {
                                const dirPath = currentPath.substring(0, currentPath.lastIndexOf('/') + 1);
                                redisConfigFilePathInput.value = dirPath + fileName;
                            } else {
                                // 如果没有当前路径，使用默认路径
                                redisConfigFilePathInput.value = fileName;
                            }
                            // 提示用户可能需要手动调整路径
                            if (!file.path) {
                                console.log('提示：请确认文件路径是否正确，必要时请手动编辑为服务器端的完整路径');
                            }
                        }
                    }
                    // 清理临时元素
                    document.body.removeChild(input);
                });
                
                // 添加到DOM并触发点击
                document.body.appendChild(input);
                input.click();
            });
        }

        // Redis配置文件自动检测按钮
        const detectRedisConfigBtn = document.getElementById('detectRedisConfigBtn');
        if (detectRedisConfigBtn) {
            detectRedisConfigBtn.addEventListener('click', async function() {
                const btn = this;
                const originalHtml = btn.innerHTML;
                btn.disabled = true;
                btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-1"></i>检测中...';
                
                try {
                    const response = await fetch('/api/system/database/redis/config-file');
                    const result = await response.json();
                    
                    if (result.success && result.config_file_path) {
                        const redisConfigFilePathInput = document.getElementById('redisConfigFilePath');
                        if (redisConfigFilePathInput) {
                            redisConfigFilePathInput.value = result.config_file_path;
                            alert('✅ Redis配置文件路径已自动检测: ' + result.config_file_path);
                        }
                    } else {
                        alert('⚠️ ' + (result.message || '未找到Redis配置文件，请手动指定路径'));
                    }
                } catch (error) {
                    alert('❌ 自动检测失败：' + error.message);
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = originalHtml;
                }
            });
        }

        // 测试数据库连接
        const testDbConnectionBtn = document.getElementById('testDbConnectionBtn');
        if (testDbConnectionBtn) {
            testDbConnectionBtn.addEventListener('click', async function() {
                const btn = this;
                const originalHtml = btn.innerHTML;
                btn.disabled = true;
                btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>测试中...';
                
                try {
                    const config = getDatabaseConfig();
                    const response = await fetch('/api/system/database/test', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(config)
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        alert('✅ 数据库连接测试成功！');
                    } else {
                        alert('❌ 数据库连接测试失败：' + result.message);
                    }
                } catch (error) {
                    alert('❌ 测试失败：' + error.message);
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = originalHtml;
                }
            });
        }
    }
});

// 加载系统常规配置（包含磁带自动格式化选项）
async function loadSystemConfig() {
    try {
        const response = await fetch('/api/system/config');
        const data = await response.json();
        if (!data) return;

        const enableTapeFormatCheckbox = document.getElementById('enableTapeFormatBeforeFull');
        if (enableTapeFormatCheckbox && typeof data.enable_tape_format_before_full === 'boolean') {
            enableTapeFormatCheckbox.checked = data.enable_tape_format_before_full;
        }
    } catch (e) {
        console.error('加载系统配置失败:', e);
    }
}

// 获取数据库配置
function getDatabaseConfig() {
    const dbType = document.getElementById('dbType').value;
    const config = {
        db_type: dbType,
        pool_size: parseInt(document.getElementById('poolSize').value),
        max_overflow: parseInt(document.getElementById('maxOverflow').value)
    };
    
    if (dbType === 'sqlite') {
        // 内存数据库（本地SQLite）只需要路径
        config.db_path = document.getElementById('dbPath').value;
    } else {
        // 仅支持 openGauss（服务器数据库）
        config.db_host = document.getElementById('dbHost').value;
        config.db_port = parseInt(document.getElementById('dbPort').value);
        config.db_user = document.getElementById('dbUser').value;
        config.db_password = document.getElementById('dbPassword').value;
        config.db_database = document.getElementById('dbDatabase').value;
    }
    
    return config;
}

// 保存数据库配置（统一由右上角按钮调用）
async function saveDatabaseConfigSection() {
    const dbTypeElement = document.getElementById('dbType');
    if (!dbTypeElement) {
        return true;
    }
    const config = getDatabaseConfig();
    
    // 验证必填字段 - 如果配置不完整，跳过保存（允许用户只保存其他配置）
    if (config.db_type === 'sqlite') {
        if (!config.db_path || config.db_path.trim() === '') {
            // SQLite 路径为空，跳过保存数据库配置（不抛出错误）
            console.log('SQLite数据库路径为空，跳过保存数据库配置');
            return true;
        }
    } else {
        // openGauss：需要完整的主机/端口/用户名/密码/数据库名
        if (!config.db_host || config.db_host.trim() === '' ||
            !config.db_port ||
            !config.db_user || config.db_user.trim() === '' ||
            !config.db_password || config.db_password.trim() === '' ||
            !config.db_database || config.db_database.trim() === '') {
            console.log('openGauss 数据库配置不完整，跳过保存数据库配置');
            return true;
        }
    }
    
    const response = await fetch('/api/system/database/config', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config)
    });
    
    const result = await response.json();
    if (!response.ok || !result.success) {
        throw new Error(result.message || '保存数据库配置失败');
    }
    return true;
}

// 加载数据库配置
async function loadDatabaseConfig() {
    try {
        const response = await fetch('/api/system/database/config');
        const config = await response.json();
        
        if (config) {
            document.getElementById('dbType').value = config.db_type || 'sqlite';

            if (config.db_type === 'sqlite') {
                // 如果配置中有路径，使用配置的路径；否则使用默认路径
                const defaultPath = 'data\\backup_system.db';
                document.getElementById('dbPath').value = config.db_path || defaultPath;
                document.getElementById('sqliteConfig').style.display = 'block';
                document.getElementById('serverDbConfig').style.display = 'none';

                // 加载 SQLite 配置（从环境配置中加载）
                loadSQLiteConfig();
            } else {
                // openGauss 配置
                if (config.db_host) document.getElementById('dbHost').value = config.db_host;
                if (config.db_port) document.getElementById('dbPort').value = config.db_port;
                if (config.db_user) document.getElementById('dbUser').value = config.db_user;
                if (config.db_password !== undefined) document.getElementById('dbPassword').value = config.db_password;
                if (config.db_database) document.getElementById('dbDatabase').value = config.db_database;
                document.getElementById('sqliteConfig').style.display = 'none';
                document.getElementById('serverDbConfig').style.display = 'block';
            }
            
            if (config.pool_size) document.getElementById('poolSize').value = config.pool_size;
            if (config.max_overflow) document.getElementById('maxOverflow').value = config.max_overflow;
            
            document.getElementById('currentPoolSize').textContent = config.pool_size || '10';
            document.getElementById('currentMaxOverflow').textContent = config.max_overflow || '20';
        }
        
        // 加载数据库状态
        loadDatabaseStatus();
    } catch (error) {
        console.error('加载数据库配置失败:', error);
    }
}

// 加载 SQLite 配置
async function loadSQLiteConfig() {
    try {
        const response = await fetch('/api/system/env-config');
        const result = await response.json();
        
        if (result.success && result.config) {
            const config = result.config;
            
            // SQLite 配置
            if (config.sqlite_cache_size) {
                const sqliteCacheSizeInput = document.getElementById('sqliteCacheSize');
                if (sqliteCacheSizeInput) sqliteCacheSizeInput.value = config.sqlite_cache_size;
            }
            if (config.sqlite_page_size) {
                const sqlitePageSizeSelect = document.getElementById('sqlitePageSize');
                if (sqlitePageSizeSelect) sqlitePageSizeSelect.value = config.sqlite_page_size;
            }
            if (config.sqlite_timeout) {
                const sqliteTimeoutInput = document.getElementById('sqliteTimeout');
                if (sqliteTimeoutInput) sqliteTimeoutInput.value = config.sqlite_timeout;
            }
            if (config.sqlite_journal_mode) {
                const sqliteJournalModeSelect = document.getElementById('sqliteJournalMode');
                if (sqliteJournalModeSelect) sqliteJournalModeSelect.value = config.sqlite_journal_mode;
            }
            if (config.sqlite_synchronous) {
                const sqliteSynchronousSelect = document.getElementById('sqliteSynchronous');
                if (sqliteSynchronousSelect) sqliteSynchronousSelect.value = config.sqlite_synchronous;
            }
        }
    } catch (error) {
        console.error('加载 SQLite 配置失败:', error);
    }
}

// 加载数据库状态
async function loadDatabaseStatus() {
    try {
        const response = await fetch('/api/system/database/status');
        const status = await response.json();
        
        if (status) {
            const statusIndicator = document.querySelector('#dbStatus .status-indicator');
            const statusText = document.getElementById('dbStatusText');
            const dbTypeText = document.getElementById('dbTypeText');
            
            statusIndicator.className = 'status-indicator';
            
            if (status.status === 'online') {
                statusIndicator.classList.add('status-online');
                statusText.textContent = '已连接';
                statusText.className = 'text-success';
            } else if (status.status === 'offline') {
                statusIndicator.classList.add('status-offline');
                statusText.textContent = '未连接';
                statusText.className = 'text-danger';
            } else {
                statusIndicator.classList.add('status-warning');
                statusText.textContent = status.message || '未知';
                statusText.className = 'text-warning';
            }
            
            dbTypeText.textContent = status.db_type || 'Unknown';
        }
    } catch (error) {
        console.error('加载数据库状态失败:', error);
    }
}

// 定期刷新数据库状态
setInterval(loadDatabaseStatus, 30000); // 每30秒刷新一次

// 处理URL hash，自动打开对应的标签页
document.addEventListener('DOMContentLoaded', function() {
    const hash = window.location.hash;
    if (hash) {
        // 移除#号，获取标签页ID
        const tabId = hash.substring(1);
        // 查找对应的tab按钮（ID格式为 {tabId}-tab）
        const tabButton = document.getElementById(tabId + '-tab');
        if (tabButton) {
            // 使用Bootstrap的tab API切换标签页
            const tab = new bootstrap.Tab(tabButton);
            tab.show();
        }
    }
});

// ===== 通知配置JavaScript =====
document.addEventListener('DOMContentLoaded', function() {
    // 加载通知配置
    loadNotificationConfig();
    
    // 测试通知
    const testNotificationBtn = document.getElementById('testNotificationBtn');
    if (testNotificationBtn) {
        testNotificationBtn.addEventListener('click', async function() {
            const btn = this;
            const originalHtml = btn.innerHTML;
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>发送中...';
            
            try {
                const phone = document.getElementById('dingtalkDefaultPhone').value;
                const response = await fetch(`/api/system/notification/test?phone=${phone}`, {
                    method: 'POST'
                });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ ' + result.message);
                } else {
                    alert('❌ ' + result.message);
                }
            } catch (error) {
                alert('❌ 测试失败：' + error.message);
            } finally {
                btn.disabled = false;
                btn.innerHTML = originalHtml;
            }
        });
    }
    
});

// 加载通知配置
function loadNotificationConfig() {
    fetch('/api/system/notification/config')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.config) {
                const config = data.config;
                if (config.dingtalk_api_url) {
                    document.getElementById('dingtalkApiUrl').value = config.dingtalk_api_url;
                }
                if (config.dingtalk_api_key) {
                    document.getElementById('dingtalkApiKey').value = config.dingtalk_api_key;
                }
                if (config.dingtalk_default_phone) {
                    document.getElementById('dingtalkDefaultPhone').value = config.dingtalk_default_phone;
                    document.getElementById('defaultPhoneDisplay').textContent = config.dingtalk_default_phone;
                }
            }
        })
        .catch(error => console.error('加载通知配置失败:', error));
    
    // 加载通知事件配置
    fetch('/api/system/notification/events')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.events) {
                const events = data.events;
                document.getElementById('notifyBackupSuccess').checked = events.notify_backup_success ?? true;
                document.getElementById('notifyBackupStarted').checked = events.notify_backup_started ?? true;
                document.getElementById('notifyBackupFailed').checked = events.notify_backup_failed ?? true;
                document.getElementById('notifyRecoverySuccess').checked = events.notify_recovery_success ?? true;
                document.getElementById('notifyRecoveryFailed').checked = events.notify_recovery_failed ?? true;
                document.getElementById('notifyTapeChange').checked = events.notify_tape_change ?? true;
                document.getElementById('notifyTapeExpired').checked = events.notify_tape_expired ?? true;
                document.getElementById('notifyTapeError').checked = events.notify_tape_error ?? true;
                document.getElementById('notifyCapacityWarning').checked = events.notify_capacity_warning ?? true;
                document.getElementById('notifySystemError').checked = events.notify_system_error ?? true;
                document.getElementById('notifySystemStarted').checked = events.notify_system_started ?? true;
            }
        })
        .catch(error => console.error('加载通知事件配置失败:', error));
}

// 保存通知配置（统一由右上角按钮调用）
async function saveNotificationSection() {
    const apiUrlInput = document.getElementById('dingtalkApiUrl');
    if (!apiUrlInput) {
        return true;
    }
    const config = {
        dingtalk_api_url: apiUrlInput.value,
        dingtalk_api_key: document.getElementById('dingtalkApiKey').value,
        dingtalk_default_phone: document.getElementById('dingtalkDefaultPhone').value
    };
    
    const response = await fetch('/api/system/notification/config', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config)
    });
    const result = await response.json();
    if (!response.ok || !result.success) {
        throw new Error(result.message || '保存通知配置失败');
    }
    
    const eventConfig = {
        notify_backup_success: document.getElementById('notifyBackupSuccess')?.checked ?? true,
        notify_backup_started: document.getElementById('notifyBackupStarted')?.checked ?? true,
        notify_backup_failed: document.getElementById('notifyBackupFailed')?.checked ?? true,
        notify_recovery_success: document.getElementById('notifyRecoverySuccess')?.checked ?? true,
        notify_recovery_failed: document.getElementById('notifyRecoveryFailed')?.checked ?? true,
        notify_tape_change: document.getElementById('notifyTapeChange')?.checked ?? true,
        notify_tape_expired: document.getElementById('notifyTapeExpired')?.checked ?? true,
        notify_tape_error: document.getElementById('notifyTapeError')?.checked ?? true,
        notify_capacity_warning: document.getElementById('notifyCapacityWarning')?.checked ?? true,
        notify_system_error: document.getElementById('notifySystemError')?.checked ?? true,
        notify_system_started: document.getElementById('notifySystemStarted')?.checked ?? true
    };
    
    const eventResponse = await fetch('/api/system/notification/events', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(eventConfig)
    });
    const eventResult = await eventResponse.json();
    if (!eventResponse.ok || !eventResult.success) {
        throw new Error(eventResult.message || '保存通知事件配置失败');
    }
    
    const defaultPhoneDisplay = document.getElementById('defaultPhoneDisplay');
    if (defaultPhoneDisplay && config.dingtalk_default_phone) {
        defaultPhoneDisplay.textContent = config.dingtalk_default_phone;
    }
    return true;
}

// 添加通知人员
async function addNotificationUser() {
    const phone = document.getElementById('notificationPhone').value.trim();
    const name = document.getElementById('notificationName').value.trim();
    const remark = document.getElementById('notificationRemark').value.trim();
    
    if (!phone || phone.length !== 11) {
        alert('请输入正确的11位手机号');
        return;
    }
    
    if (!name) {
        alert('请输入姓名');
        return;
    }
    
    try {
        const response = await fetch('/api/system/notification/users', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                phone: phone,
                name: name,
                remark: remark || null,
                enabled: true
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('✅ 通知人员添加成功');
            // 重新加载通知人员列表
            await loadNotificationUsers();
            
            // 清空表单
            document.getElementById('notificationPhone').value = '';
            document.getElementById('notificationName').value = '';
            document.getElementById('notificationRemark').value = '';
            
            // 关闭模态框
            const modal = bootstrap.Modal.getInstance(document.getElementById('addNotificationUserModal'));
            modal.hide();
        } else {
            alert('❌ 添加失败：' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('❌ 添加失败：' + error.message);
    }
}

// 加载通知人员列表
async function loadNotificationUsers() {
    try {
        const response = await fetch('/api/system/notification/users');
        const result = await response.json();
        
        const container = document.getElementById('notificationUsers');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (result.success && result.users && result.users.length > 0) {
            result.users.forEach(user => {
                addNotificationUserToList(user.id, user.phone, user.name, user.remark, user.enabled);
            });
        } else {
            container.innerHTML = '<p class="text-muted">暂无通知人员</p>';
        }
    } catch (error) {
        console.error('加载通知人员列表失败:', error);
    }
}

// 添加通知人员到列表显示
function addNotificationUserToList(userId, phone, name, remark, enabled) {
    const container = document.getElementById('notificationUsers');
    if (!container) return;
    
    const userDiv = document.createElement('div');
    userDiv.className = 'alert alert-light mb-2 border';
    userDiv.id = `notification-user-${userId}`;
    userDiv.innerHTML = `
        <div class="d-flex justify-content-between align-items-center">
            <div class="form-check">
                <input class="form-check-input" type="checkbox" ${enabled ? 'checked' : ''} 
                       onchange="toggleNotificationUser(${userId}, this.checked)">
                <label class="form-check-label">
                    <strong>${name || '未命名'}</strong> <small class="text-muted">${phone}</small>
                    ${remark ? `<br><small class="text-muted">${remark}</small>` : ''}
                </label>
            </div>
            <div>
                <button class="btn btn-sm btn-danger" onclick="deleteNotificationUser(${userId})">
                    <i class="bi bi-trash"></i> 删除
                </button>
            </div>
        </div>
    `;
    container.appendChild(userDiv);
}

// 切换通知人员启用状态
async function toggleNotificationUser(userId, enabled) {
    try {
        // 先获取用户信息
        const getResponse = await fetch('/api/system/notification/users');
        const getResult = await getResponse.json();
        
        if (!getResult.success || !getResult.users) {
            alert('❌ 获取通知人员信息失败');
            return;
        }
        
        const user = getResult.users.find(u => u.id === userId);
        if (!user) {
            alert('❌ 通知人员不存在');
            return;
        }
        
        // 更新用户信息
        const response = await fetch(`/api/system/notification/users/${userId}`, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                phone: user.phone,
                name: user.name,
                remark: user.remark || null,
                enabled: enabled
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            console.log(`通知人员 ${user.name} ${enabled ? '已启用' : '已禁用'}`);
        } else {
            alert('❌ 更新失败：' + (result.message || '未知错误'));
            // 恢复复选框状态
            const checkbox = document.querySelector(`#notification-user-${userId} input[type="checkbox"]`);
            if (checkbox) checkbox.checked = !enabled;
        }
    } catch (error) {
        alert('❌ 更新失败：' + error.message);
        // 恢复复选框状态
        const checkbox = document.querySelector(`#notification-user-${userId} input[type="checkbox"]`);
        if (checkbox) checkbox.checked = !enabled;
    }
}

// 删除通知人员
async function deleteNotificationUser(userId) {
    if (!confirm('确定要删除该通知人员吗？')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/system/notification/users/${userId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.success) {
            alert('✅ 通知人员删除成功');
            // 重新加载通知人员列表
            await loadNotificationUsers();
        } else {
            alert('❌ 删除失败：' + (result.message || '未知错误'));
        }
    } catch (error) {
        alert('❌ 删除失败：' + error.message);
    }
}

// 页面加载时加载通知人员列表
document.addEventListener('DOMContentLoaded', function() {
    // 当通知设置标签页激活时加载通知人员列表
    const notificationTab = document.getElementById('notification-tab');
    if (notificationTab) {
        notificationTab.addEventListener('shown.bs.tab', function() {
            loadNotificationUsers();
        });
        
        // 如果标签页已经激活，立即加载
        if (notificationTab.classList.contains('active')) {
            loadNotificationUsers();
        }
    }
});

// 系统信息标签页的版本链接
document.addEventListener('DOMContentLoaded', function() {
    const aboutVersionLink = document.getElementById('about-version-link');
    if (aboutVersionLink) {
        aboutVersionLink.addEventListener('click', function(e) {
            e.preventDefault();
            loadChangelog();
        });
    }
});

// ===== 统一系统配置管理 =====
// 加载所有系统配置
async function loadAllSystemConfig() {
    try {
        const response = await fetch('/api/system/env-config');
        const result = await response.json();
        
        if (result.success && result.config) {
            const config = result.config;
            
            // 应用配置
            if (config.app_name) {
                const appNameInput = document.getElementById('appName');
                if (appNameInput) appNameInput.value = config.app_name;
            }
            if (config.log_level) {
                const logLevelSelect = document.getElementById('logLevel');
                if (logLevelSelect) logLevelSelect.value = (config.log_level || 'INFO').toUpperCase();
            } else {
                const logLevelSelect = document.getElementById('logLevel');
                if (logLevelSelect) logLevelSelect.value = 'INFO';
            }
            
            // Web服务配置
            if (config.web_host) {
                const webHostInput = document.getElementById('webHost');
                if (webHostInput) webHostInput.value = config.web_host;
            }
            if (config.web_port) {
                const webPortInput = document.getElementById('webPort');
                if (webPortInput) webPortInput.value = config.web_port;
            }
            if (config.enable_cors !== undefined) {
                const enableCorsCheckbox = document.getElementById('enableCors');
                if (enableCorsCheckbox) enableCorsCheckbox.checked = config.enable_cors;
            }
            
            // ITDT工具配置
            if (config.itdt_path) {
                const itdtPathInput = document.getElementById('itdtPath');
                if (itdtPathInput) itdtPathInput.value = config.itdt_path;
            }
            if (config.itdt_device_path) {
                const itdtDevicePathInput = document.getElementById('itdtDevicePath');
                if (itdtDevicePathInput) itdtDevicePathInput.value = config.itdt_device_path;
            }
            
            // LTFS工具配置
            if (config.ltfs_tools_dir) {
                const ltfsToolsDirInput = document.getElementById('ltfsToolsDir');
                if (ltfsToolsDirInput) ltfsToolsDirInput.value = config.ltfs_tools_dir;
            }
            if (config.tape_drive_letter) {
                const tapeDriveLetterInput = document.getElementById('tapeDriveLetter');
                if (tapeDriveLetterInput) tapeDriveLetterInput.value = config.tape_drive_letter;
            }
            
            // 备份策略配置
            if (config.default_retention_months) {
                const defaultRetentionMonthsInput = document.getElementById('defaultRetentionMonths');
                if (defaultRetentionMonthsInput) defaultRetentionMonthsInput.value = config.default_retention_months;
            }
            if (config.auto_erase_expired !== undefined) {
                const autoEraseExpiredCheckbox = document.getElementById('autoEraseExpired');
                if (autoEraseExpiredCheckbox) autoEraseExpiredCheckbox.checked = config.auto_erase_expired;
            }
            if (config.max_file_size) {
                // 将字节转换为GB
                const maxFileSizeGB = config.max_file_size / (1024 * 1024 * 1024);
                const maxFileSizeGBInput = document.getElementById('maxFileSizeGB');
                if (maxFileSizeGBInput) maxFileSizeGBInput.value = maxFileSizeGB.toFixed(1);
            }
            if (config.backup_compress_dir) {
                const backupCompressDirInput = document.getElementById('backupCompressDir');
                if (backupCompressDirInput) backupCompressDirInput.value = config.backup_compress_dir;
            }
            if (config.scan_update_interval) {
                const scanUpdateIntervalInput = document.getElementById('scanUpdateInterval');
                if (scanUpdateIntervalInput) scanUpdateIntervalInput.value = config.scan_update_interval;
            }
            if (config.scan_log_interval_seconds) {
                const scanLogIntervalInput = document.getElementById('scanLogIntervalSeconds');
                if (scanLogIntervalInput) scanLogIntervalInput.value = config.scan_log_interval_seconds;
            }
            if (config.scan_wait_timeout) {
                const scanWaitTimeoutInput = document.getElementById('scanWaitTimeout');
                if (scanWaitTimeoutInput) scanWaitTimeoutInput.value = config.scan_wait_timeout;
            }
            if (config.scan_method) {
                const scanMethodInput = document.getElementById('scanMethod');
                if (scanMethodInput) {
                    scanMethodInput.value = config.scan_method;
                    // 触发change事件以显示/隐藏ES路径输入框
                    scanMethodInput.dispatchEvent(new Event('change'));
                }
            }
            if (config.es_exe_path) {
                const esExePathInput = document.getElementById('esExePath');
                if (esExePathInput) esExePathInput.value = config.es_exe_path;
            }
            if (config.use_scan_multithread !== undefined) {
                const useScanMultithreadInput = document.getElementById('useScanMultithread');
                if (useScanMultithreadInput) {
                    useScanMultithreadInput.checked = config.use_scan_multithread;
                    // 触发change事件以显示/隐藏线程数输入
                    useScanMultithreadInput.dispatchEvent(new Event('change'));
                }
            }
            if (config.scan_threads) {
                const scanThreadsInput = document.getElementById('scanThreads');
                if (scanThreadsInput) scanThreadsInput.value = config.scan_threads;
            }
            if (config.use_checkpoint !== undefined) {
                const useCheckpointInput = document.getElementById('useCheckpoint');
                if (useCheckpointInput) useCheckpointInput.checked = config.use_checkpoint;
            }
            if (config.enable_background_copy_update !== undefined) {
                const enableBackgroundCopyUpdateInput = document.getElementById('enableBackgroundCopyUpdate');
                if (enableBackgroundCopyUpdateInput) {
                    enableBackgroundCopyUpdateInput.checked = config.enable_background_copy_update;
                }
            }
            if (config.compression_parallel_batches) {
                const compressionParallelBatchesInput = document.getElementById('compressionParallelBatches');
                if (compressionParallelBatchesInput) compressionParallelBatchesInput.value = config.compression_parallel_batches;
            }
            if (config.compression_batches_reduction !== undefined) {
                const compressionBatchesReductionInput = document.getElementById('compressionBatchesReduction');
                if (compressionBatchesReductionInput) compressionBatchesReductionInput.value = config.compression_batches_reduction;
            }

            // 内存数据库配置
            if (config.use_memory_db !== undefined) {
                const useMemoryDbCheckbox = document.getElementById('useMemoryDb');
                if (useMemoryDbCheckbox) {
                    useMemoryDbCheckbox.checked = config.use_memory_db;
                    // 触发change事件以显示/隐藏配置项
                    useMemoryDbCheckbox.dispatchEvent(new Event('change'));
                }
            }
            if (config.memory_db_max_files) {
                const memoryDbMaxFilesInput = document.getElementById('memoryDbMaxFiles');
                if (memoryDbMaxFilesInput) memoryDbMaxFilesInput.value = config.memory_db_max_files;
            }
            if (config.memory_db_sync_batch_size) {
                const memoryDbSyncBatchSizeInput = document.getElementById('memoryDbSyncBatchSize');
                if (memoryDbSyncBatchSizeInput) memoryDbSyncBatchSizeInput.value = config.memory_db_sync_batch_size;
            }
            if (config.memory_db_sync_interval) {
                const memoryDbSyncIntervalInput = document.getElementById('memoryDbSyncInterval');
                if (memoryDbSyncIntervalInput) memoryDbSyncIntervalInput.value = config.memory_db_sync_interval;
            }
            if (config.memory_db_checkpoint_interval) {
                const memoryDbCheckpointIntervalInput = document.getElementById('memoryDbCheckpointInterval');
                if (memoryDbCheckpointIntervalInput) memoryDbCheckpointIntervalInput.value = config.memory_db_checkpoint_interval;
            }
            if (config.memory_db_checkpoint_retention_hours) {
                const memoryDbCheckpointRetentionHoursInput = document.getElementById('memoryDbCheckpointRetentionHours');
                if (memoryDbCheckpointRetentionHoursInput) memoryDbCheckpointRetentionHoursInput.value = config.memory_db_checkpoint_retention_hours;
            }
            
            // SQLite 配置
            if (config.sqlite_cache_size) {
                const sqliteCacheSizeInput = document.getElementById('sqliteCacheSize');
                if (sqliteCacheSizeInput) sqliteCacheSizeInput.value = config.sqlite_cache_size;
            }
            if (config.sqlite_page_size) {
                const sqlitePageSizeSelect = document.getElementById('sqlitePageSize');
                if (sqlitePageSizeSelect) sqlitePageSizeSelect.value = config.sqlite_page_size;
            }
            if (config.sqlite_timeout) {
                const sqliteTimeoutInput = document.getElementById('sqliteTimeout');
                if (sqliteTimeoutInput) sqliteTimeoutInput.value = config.sqlite_timeout;
            }
            if (config.sqlite_journal_mode) {
                const sqliteJournalModeSelect = document.getElementById('sqliteJournalMode');
                if (sqliteJournalModeSelect) sqliteJournalModeSelect.value = config.sqlite_journal_mode;
            }
            if (config.sqlite_synchronous) {
                const sqliteSynchronousSelect = document.getElementById('sqliteSynchronous');
                if (sqliteSynchronousSelect) sqliteSynchronousSelect.value = config.sqlite_synchronous;
            }
            
            console.log('系统配置加载完成');
        }
    } catch (error) {
        console.error('加载系统配置失败:', error);
    }
}

// 保存所有系统配置
async function saveEnvConfigSection() {
    try {
        // 收集所有配置
        const config = {
            // 应用配置
            app_name: document.getElementById('appName')?.value || null,
            
            // Web服务配置
            web_host: document.getElementById('webHost')?.value || null,
            web_port: parseInt(document.getElementById('webPort')?.value) || null,
            enable_cors: document.getElementById('enableCors')?.checked || null,
            
            // ITDT工具配置
            itdt_path: document.getElementById('itdtPath')?.value || null,
            itdt_device_path: document.getElementById('itdtDevicePath')?.value || null,
            
            // LTFS工具配置
            ltfs_tools_dir: document.getElementById('ltfsToolsDir')?.value || null,
            tape_drive_letter: document.getElementById('tapeDriveLetter')?.value || null,
            
            // 备份策略配置
            default_retention_months: parseInt(document.getElementById('defaultRetentionMonths')?.value) || null,
            auto_erase_expired: document.getElementById('autoEraseExpired')?.checked || null,
            max_file_size: parseFloat(document.getElementById('maxFileSizeGB')?.value) * 1024 * 1024 * 1024 || null,
            backup_compress_dir: document.getElementById('backupCompressDir')?.value || null,
            scan_update_interval: (() => {
                const input = document.getElementById('scanUpdateInterval');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_log_interval_seconds: (() => {
                const input = document.getElementById('scanLogIntervalSeconds');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_wait_timeout: (() => {
                const input = document.getElementById('scanWaitTimeout');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            scan_method: document.getElementById('scanMethod')?.value || null,
            es_exe_path: document.getElementById('esExePath')?.value || null,
            use_scan_multithread: document.getElementById('useScanMultithread')?.checked ?? null,
            scan_threads: (() => {
                const input = document.getElementById('scanThreads');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            use_checkpoint: document.getElementById('useCheckpoint')?.checked || null,
            enable_background_copy_update: (() => {
                const checkbox = document.getElementById('enableBackgroundCopyUpdate');
                if (!checkbox) return undefined;
                return checkbox.checked === true;
            })(),
            compression_parallel_batches: (() => {
                const input = document.getElementById('compressionParallelBatches');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),
            compression_batches_reduction: (() => {
                const input = document.getElementById('compressionBatchesReduction');
                if (!input) return null;
                const value = input.value;
                if (value === '' || value === null || value === undefined) return null;
                const parsed = parseInt(value);
                return isNaN(parsed) ? null : parsed;
            })(),

            // 内存数据库配置
            use_memory_db: (() => {
                const checkbox = document.getElementById('useMemoryDb');
                if (!checkbox) return undefined;
                // 明确返回布尔值，确保 false 也能被发送
                return checkbox.checked === true;
            })(),
            memory_db_max_files: parseInt(document.getElementById('memoryDbMaxFiles')?.value) || null,
            memory_db_sync_batch_size: parseInt(document.getElementById('memoryDbSyncBatchSize')?.value) || null,
            memory_db_sync_interval: parseInt(document.getElementById('memoryDbSyncInterval')?.value) || null,
            memory_db_checkpoint_interval: parseInt(document.getElementById('memoryDbCheckpointInterval')?.value) || null,
            memory_db_checkpoint_retention_hours: parseInt(document.getElementById('memoryDbCheckpointRetentionHours')?.value) || null,
            
            // SQLite 配置
            sqlite_cache_size: parseInt(document.getElementById('sqliteCacheSize')?.value) || null,
            sqlite_page_size: parseInt(document.getElementById('sqlitePageSize')?.value) || null,
            sqlite_timeout: parseFloat(document.getElementById('sqliteTimeout')?.value) || null,
            sqlite_journal_mode: document.getElementById('sqliteJournalMode')?.value || null,
            sqlite_synchronous: document.getElementById('sqliteSynchronous')?.value || null,
            
            log_level: document.getElementById('logLevel')?.value || null,

            // 磁带自动格式化配置
            enable_tape_format_before_full: (() => {
                const checkbox = document.getElementById('enableTapeFormatBeforeFull');
                if (!checkbox) return undefined;
                return checkbox.checked === true;
            })(),
        };
        
        // 移除 null 值
        Object.keys(config).forEach(key => {
            if (config[key] === null || config[key] === undefined) {
                delete config[key];
            }
        });
        
        const response = await fetch('/api/system/env-config', {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(config)
        });
        
        const result = await response.json();
        if (!response.ok || !result.success) {
            throw new Error(result.message || '保存失败');
        }
        return true;
    } catch (error) {
        throw error;
    }
}

async function saveAllSystemConfig() {
    try {
        await saveEnvConfigSection();
        await saveDatabaseConfigSection();
        await saveNotificationSection();
        if (typeof saveCompressionConfig === 'function') {
            await saveCompressionConfig({silent: true, reload: false});
        }
        alert('✅ 配置已保存（部分配置需要重启服务后生效）');
        return true;
    } catch (error) {
        console.error('保存系统配置失败:', error);
        alert('❌ 保存失败：' + error.message);
        return false;
    }
}

// 页面加载时加载配置
document.addEventListener('DOMContentLoaded', function() {
    // 加载所有系统配置
    loadAllSystemConfig();
    
    // 保存设置按钮
    const saveSettingsBtn = document.getElementById('saveSettingsBtn');
    if (saveSettingsBtn) {
        saveSettingsBtn.addEventListener('click', async function() {
            const btn = this;
            const originalHtml = btn.innerHTML;
            btn.disabled = true;
            btn.innerHTML = '<i class="bi bi-arrow-clockwise spinner me-2"></i>保存中...';
            
            try {
                // 保存所有配置
                await saveAllSystemConfig();
            } finally {
                btn.disabled = false;
                btn.innerHTML = originalHtml;
            }
        });
    }
    
    // 重启系统按钮
    const restartSystemBtn = document.getElementById('restartSystemBtn');
    if (restartSystemBtn) {
        restartSystemBtn.addEventListener('click', function() {
            if (confirm('确定要重启系统吗？这将中断所有正在运行的任务。')) {
                alert('重启功能暂未实现，请手动重启服务。');
            }
        });
    }
});

