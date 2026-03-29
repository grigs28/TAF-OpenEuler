/**
 * 计划任务目录浏览器模块
 * Scheduler Directory Browser Module
 */

import { escapeHtml, formatFileSize, showMessage } from './scheduler-utils.js';
import { SchedulerAPI } from './scheduler-api.js';

/**
 * 目录浏览器类
 */
export class DirectoryBrowser {
    constructor() {
        this.browserType = null; // 'backup', 'backup_target', 'recovery'
        this.currentBrowserPath = null;
        this.eventsInitialized = false; // 标记事件是否已初始化
        this.modalInstance = null; // 缓存模态框实例
    }

    /**
     * 显示目录浏览器
     */
    async showDirectoryBrowser(type) {
        this.browserType = type;
        this.currentBrowserPath = null;

        // 打开模态框
        const modalEl = document.getElementById('directoryBrowserModal');

        // 获取或创建模态框实例（避免重复创建）
        this.modalInstance = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);

        // 确保隐藏时移除焦点，避免 aria-hidden 焦点保留
        modalEl.addEventListener('hidden.bs.modal', () => {
            if (document.activeElement) {
                document.activeElement.blur();
            }
        }, { once: true });

        // 重置目录列表为初始状态
        const container = document.getElementById('directoryItemsList');
        if (container) {
            container.innerHTML = `
                <div class="text-center text-muted py-5">
                    <i class="bi bi-folder-x display-4 d-block mb-3"></i>
                    <p>请选择驱动器或输入网络路径</p>
                </div>
            `;
        }

        // 清空选中的路径
        const selectedPathInput = document.getElementById('selectedPathInput');
        if (selectedPathInput) {
            selectedPathInput.value = '';
        }

        // 清空网络路径输入框
        const networkPathInput = document.getElementById('networkPathInput');
        if (networkPathInput) {
            networkPathInput.value = '';
        }

        // 清空当前路径显示
        const browserCurrentPath = document.getElementById('browserCurrentPath');
        if (browserCurrentPath) {
            browserCurrentPath.value = '';
        }

        // 重置面包屑
        this.updateBreadcrumb(null);

        // 显示模态框
        this.modalInstance.show();

        // 加载驱动器列表
        await this.loadDrives();

        // 只在第一次设置事件监听（避免重复绑定）
        if (!this.eventsInitialized) {
            this.setupDirectoryBrowserEvents();
            this.eventsInitialized = true;
        }
    }
    
    /**
     * 加载驱动器列表
     */
    async loadDrives() {
        try {
            const drivesData = await SchedulerAPI.loadDrives();
            const drives = drivesData.drives || [];
            
            const drivesList = document.getElementById('drivesList');
            if (!drivesList) return;
            
            drivesList.innerHTML = drives.map(drive => `
                <a href="#" class="list-group-item list-group-item-action" data-path="${escapeHtml(drive.path)}">
                    <i class="bi bi-hdd me-2"></i>
                    <strong>${escapeHtml(drive.name)}</strong>
                    <small class="text-muted d-block">${escapeHtml(drive.path)}</small>
                </a>
            `).join('');
            
            // 绑定点击事件
            drivesList.querySelectorAll('a').forEach(item => {
                item.addEventListener('click', async (e) => {
                    e.preventDefault();
                    const path = item.getAttribute('data-path');
                    await this.browseDirectory(path);
                });
            });
            
        } catch (error) {
            console.error('加载驱动器列表失败:', error);
            showMessage('加载驱动器列表失败', 'error');
        }
    }
    
    /**
     * 浏览目录
     * @param {string} path - 要浏览的路径
     * @param {boolean} rethrowOnError - 是否在错误时重新抛出异常（用于外部处理错误显示）
     */
    async browseDirectory(path, rethrowOnError = false) {
        try {
            this.currentBrowserPath = path;
            document.getElementById('browserCurrentPath').value = path;
            document.getElementById('selectedPathInput').value = path;
            this.updateBreadcrumb(path);

            const response = await SchedulerAPI.browseDirectory(path);
            this.renderDirectoryItems(response.items || []);

        } catch (error) {
            console.error('浏览目录失败:', error);
            if (error.response?.status === 403) {
                showMessage('无权限访问该路径', 'error');
            } else if (error.response?.status === 404) {
                showMessage('路径不存在', 'error');
            } else {
                showMessage('浏览目录失败', 'error');
            }
            // 如果需要外部处理错误显示，重新抛出异常
            if (rethrowOnError) {
                throw error;
            }
        }
    }
    
    /**
     * 渲染目录项列表
     */
    renderDirectoryItems(items) {
        const container = document.getElementById('directoryItemsList');
        if (!container) return;
        
        if (items.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-5">
                    <i class="bi bi-folder-x display-4 d-block mb-3"></i>
                    <p>当前目录为空</p>
                </div>
            `;
            return;
        }
        
        // 分离目录和文件
        const directories = items.filter(item => item.type === 'directory');
        const files = items.filter(item => item.type === 'file');
        
        let html = '';
        
        // 显示目录（双击可进入，带选择框）
        if (directories.length > 0) {
            html += '<div class="mb-3"><h6 class="text-muted small"><i class="bi bi-folder me-1"></i>目录</h6></div>';
            html += '<div class="list-group mb-3">';
            directories.forEach(item => {
                const icon = item.name === '..' ? 'bi-arrow-90deg-up' : 'bi-folder-fill';
                const cssClass = item.name === '..' ? 'text-muted' : '';
                const itemId = `dir-item-${item.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
                html += `
                    <div class="list-group-item list-group-item-action ${cssClass}" 
                         data-path="${escapeHtml(item.path)}" data-type="directory"
                         style="cursor: pointer;">
                        <div class="d-flex align-items-center">
                            ${item.name !== '..' ? `
                            <input type="checkbox" class="form-check-input me-3" id="${itemId}" 
                                   data-path="${escapeHtml(item.path)}" data-name="${escapeHtml(item.name)}"
                                   style="width: 18px; height: 18px; cursor: pointer; flex-shrink: 0;">
                            ` : '<span style="width: 18px; margin-right: 12px;"></span>'}
                            <a href="#" class="flex-grow-1 text-decoration-none ${cssClass}" 
                               data-path="${escapeHtml(item.path)}" data-type="directory"
                               style="display: flex; align-items: center;">
                                <i class="bi ${icon} me-2 text-primary"></i>
                                <span class="flex-grow-1">${escapeHtml(item.name)}</span>
                                ${item.name !== '..' ? '<small class="text-muted">目录</small>' : ''}
                            </a>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
        }
        
        // 显示文件（带选择框）
        if (files.length > 0) {
            html += '<div class="mb-3"><h6 class="text-muted small"><i class="bi bi-file-earmark me-1"></i>文件</h6></div>';
            html += '<div class="list-group">';
            files.forEach(item => {
                const sizeStr = item.size ? formatFileSize(item.size) : '';
                const itemId = `file-item-${item.path.replace(/[^a-zA-Z0-9]/g, '_')}`;
                html += `
                    <div class="list-group-item" style="cursor: pointer;">
                        <div class="d-flex align-items-center">
                            <input type="checkbox" class="form-check-input me-3" id="${itemId}"
                                   data-path="${escapeHtml(item.path)}" data-name="${escapeHtml(item.name)}"
                                   style="width: 18px; height: 18px; cursor: pointer; flex-shrink: 0;">
                            <i class="bi bi-file-earmark me-2 text-secondary"></i>
                            <span class="flex-grow-1">${escapeHtml(item.name)}</span>
                            <small class="text-muted">${sizeStr}</small>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
        }
        
        container.innerHTML = html;
        
        // 显示/隐藏全选和清空按钮
        const selectAllBtn = document.getElementById('selectAllBtn');
        const clearAllBtn = document.getElementById('clearAllBtn');
        const hasSelectableItems = directories.filter(d => d.name !== '..').length > 0 || files.length > 0;
        if (selectAllBtn) selectAllBtn.style.display = hasSelectableItems ? 'inline-block' : 'none';
        if (clearAllBtn) clearAllBtn.style.display = hasSelectableItems ? 'inline-block' : 'none';
        
        // 绑定目录点击事件（双击进入）
        container.querySelectorAll('a[data-type="directory"]').forEach(item => {
            item.addEventListener('dblclick', async (e) => {
                e.preventDefault();
                const path = item.getAttribute('data-path');
                await this.browseDirectory(path);
            });
            
            // 单击选中路径（同时勾选复选框）
            item.addEventListener('click', (e) => {
                // 如果点击的是复选框，不处理（复选框有自己的change事件）
                if (e.target.type === 'checkbox') {
                    return;
                }
                e.preventDefault();
                
                const listItem = item.closest('.list-group-item');
                const checkbox = listItem?.querySelector('input[type="checkbox"]');
                const path = item.getAttribute('data-path');
                
                // 如果是".."目录，不处理复选框
                if (path && !path.includes('..') && checkbox) {
                    // 切换复选框状态
                    checkbox.checked = !checkbox.checked;
                    // 触发change事件以更新选中路径
                    checkbox.dispatchEvent(new Event('change'));
                } else {
                    // 移除之前的选中状态
                    container.querySelectorAll('a').forEach(a => a.classList.remove('active'));
                    // 添加选中状态
                    item.classList.add('active');
                    // 更新选中的路径
                    if (path && item.closest('.list-group-item').querySelector('.bi-arrow-90deg-up') === null) {
                        document.getElementById('selectedPathInput').value = path;
                    }
                }
            });
        });
        
        // 绑定复选框变化事件，支持多选
        container.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                this.updateSelectedPaths();
            });
            
            // 点击复选框时阻止事件冒泡
            checkbox.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        });
        
        // 绑定文件项点击事件（切换复选框）
        container.querySelectorAll('.list-group-item').forEach(item => {
            const checkbox = item.querySelector('input[type="checkbox"]');
            if (checkbox && !item.querySelector('a[data-type="directory"]')) {
                // 文件项：单击切换复选框
                item.addEventListener('click', (e) => {
                    if (e.target.type !== 'checkbox') {
                        checkbox.checked = !checkbox.checked;
                        checkbox.dispatchEvent(new Event('change'));
                    }
                });
            }
        });
    }
    
    /**
     * 更新选中的路径（多选）
     */
    updateSelectedPaths() {
        const container = document.getElementById('directoryItemsList');
        if (!container) return;
        
        const checkedBoxes = container.querySelectorAll('input[type="checkbox"]:checked');
        const selectedPaths = Array.from(checkedBoxes).map(cb => cb.getAttribute('data-path'));
        
        // 更新选中路径输入框（多个路径用换行分隔）
        const selectedPathInput = document.getElementById('selectedPathInput');
        if (selectedPathInput) {
            if (selectedPaths.length > 0) {
                selectedPathInput.value = selectedPaths.join('\n');
            } else {
                // 如果没有选中任何复选框，使用单击选中的路径
                const activeItem = container.querySelector('a.active');
                if (activeItem) {
                    const path = activeItem.getAttribute('data-path');
                    if (path && !path.includes('..')) {
                        selectedPathInput.value = path;
                    }
                }
            }
        }
    }
    
    /**
     * 获取所有选中的路径（多选）
     */
    getSelectedPaths() {
        const container = document.getElementById('directoryItemsList');
        if (!container) return [];
        
        const checkedBoxes = container.querySelectorAll('input[type="checkbox"]:checked');
        return Array.from(checkedBoxes).map(cb => cb.getAttribute('data-path'));
    }
    
    /**
     * 更新面包屑导航
     */
    updateBreadcrumb(path) {
        const breadcrumb = document.getElementById('currentPathBreadcrumb');
        if (!breadcrumb) return;

        breadcrumb.textContent = path || '-';
    }

    /**
     * 渲染网络路径错误信息
     */
    renderNetworkPathError(path, error) {
        const container = document.getElementById('directoryItemsList');
        if (!container) return;

        // 获取错误消息
        let errorMsg = '未知错误';
        if (error.response?.data?.detail) {
            errorMsg = error.response.data.detail;
        } else if (error.message) {
            errorMsg = error.message;
        }

        // 创建错误提示元素
        container.innerHTML = '';

        const errorDiv = document.createElement('div');
        errorDiv.className = 'text-center py-5';

        const icon = document.createElement('i');
        icon.className = 'bi bi-exclamation-triangle text-warning display-4 d-block mb-3';
        errorDiv.appendChild(icon);

        const titleP = document.createElement('p');
        titleP.className = 'text-warning mb-2';
        titleP.textContent = '无法浏览网络路径';
        errorDiv.appendChild(titleP);

        const pathP = document.createElement('p');
        pathP.className = 'text-info small mb-2';
        pathP.style.wordBreak = 'break-all';
        pathP.textContent = path;
        errorDiv.appendChild(pathP);

        const errorP = document.createElement('p');
        errorP.className = 'text-muted small mb-3';
        errorP.textContent = errorMsg;
        errorDiv.appendChild(errorP);

        const hintP = document.createElement('p');
        hintP.className = 'text-muted small';
        const hintIcon = document.createElement('i');
        hintIcon.className = 'bi bi-info-circle me-1';
        hintP.appendChild(hintIcon);
        hintP.appendChild(document.createTextNode('路径已保留，您可以点击"确定"按钮使用此路径，或检查网络连接和SMB配置后重试'));
        errorDiv.appendChild(hintP);

        container.appendChild(errorDiv);
    }
    
    /**
     * 设置目录浏览器事件
     */
    setupDirectoryBrowserEvents() {
        // 刷新按钮
        const refreshBtn = document.getElementById('refreshBrowserBtn');
        if (refreshBtn) {
            refreshBtn.onclick = () => {
                if (this.currentBrowserPath) {
                    this.browseDirectory(this.currentBrowserPath);
                }
            };
        }
        
        // 添加网络路径按钮
        const addNetworkBtn = document.getElementById('addNetworkPathBtn');
        if (addNetworkBtn) {
            addNetworkBtn.onclick = async () => {
                const networkPath = document.getElementById('networkPathInput').value.trim();
                if (networkPath) {
                    // 验证网络路径格式
                    if (networkPath.startsWith('\\\\') || networkPath.startsWith('//') || networkPath.startsWith('smb://')) {
                        // 规范化路径：将 // 和 smb:// 转换为 \\ 格式
                        let normalizedPath = networkPath;
                        if (networkPath.startsWith('smb://')) {
                            normalizedPath = '\\\\' + networkPath.substring(6).replace(/\//g, '\\');
                        } else if (networkPath.startsWith('//')) {
                            normalizedPath = '\\\\' + networkPath.substring(2).replace(/\//g, '\\');
                        }

                        document.getElementById('networkPathInput').value = normalizedPath;
                        document.getElementById('selectedPathInput').value = normalizedPath;
                        document.getElementById('browserCurrentPath').value = normalizedPath;
                        this.updateBreadcrumb(normalizedPath);

                        // 尝试浏览该网络路径（rethrowOnError=true 以便捕获错误并显示详细信息）
                        try {
                            await this.browseDirectory(normalizedPath, true);
                        } catch (error) {
                            console.error('浏览网络路径失败:', error);
                            // 在右侧显示错误信息
                            this.renderNetworkPathError(normalizedPath, error);
                            // 即使浏览失败，也保留路径让用户可以确认使用
                            showMessage('无法浏览该网络路径，但您仍可以确认使用此路径', 'warning');
                        }
                    } else {
                        showMessage('请输入正确的网络路径格式（如 \\\\server\\share 或 //server/share）', 'error');
                    }
                }
            };
        }
        
        // 全选按钮
        const selectAllBtn = document.getElementById('selectAllBtn');
        if (selectAllBtn) {
            selectAllBtn.onclick = () => {
                const container = document.getElementById('directoryItemsList');
                if (container) {
                    container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                        cb.checked = true;
                    });
                    this.updateSelectedPaths();
                }
            };
        }
        
        // 清空按钮
        const clearAllBtn = document.getElementById('clearAllBtn');
        if (clearAllBtn) {
            clearAllBtn.onclick = () => {
                const container = document.getElementById('directoryItemsList');
                if (container) {
                    container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                        cb.checked = false;
                    });
                    this.updateSelectedPaths();
                }
            };
        }
        
        // 确认按钮（浏览后自动添加，支持多选）
        const confirmBtn = document.getElementById('confirmPathBtn');
        if (confirmBtn) {
            confirmBtn.onclick = () => {
                // 优先使用复选框选中的路径（多选）
                const selectedPaths = this.getSelectedPaths();
                const selectedPathInput = document.getElementById('selectedPathInput');
                const inputValue = selectedPathInput?.value.trim();
                
                if (selectedPaths.length > 0) {
                    // 如果有复选框选中的路径，使用这些路径
                    selectedPaths.forEach(path => {
                        if (path) {
                            // 触发自定义事件，让主模块处理路径添加
                            const event = new CustomEvent('pathSelected', {
                                detail: {
                                    path: path,
                                    type: this.browserType
                                }
                            });
                            document.dispatchEvent(event);
                        }
                    });
                } else if (inputValue) {
                    // 如果没有复选框选中，使用输入框的值（可能是多行）
                    const paths = inputValue.split('\n').map(p => p.trim()).filter(p => p);
                    paths.forEach(path => {
                        // 触发自定义事件，让主模块处理路径添加
                        const event = new CustomEvent('pathSelected', {
                            detail: {
                                path: path,
                                type: this.browserType
                            }
                        });
                        document.dispatchEvent(event);
                    });
                }
                
                // 关闭模态框
                const modalEl = document.getElementById('directoryBrowserModal');
                const modal = bootstrap.Modal.getInstance(modalEl);
                if (modal) {
                    modal.hide();
                }
                
                // 清空选中的复选框
                const container = document.getElementById('directoryItemsList');
                if (container) {
                    container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                        cb.checked = false;
                    });
                }
            };
        }
        
        // 选中路径输入框（现在是textarea）Ctrl+Enter确认
        const selectedPathInput = document.getElementById('selectedPathInput');
        if (selectedPathInput) {
            selectedPathInput.addEventListener('keydown', (e) => {
                // Ctrl+Enter 或 Cmd+Enter 确认
                if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                    e.preventDefault();
                    if (confirmBtn) {
                        confirmBtn.click();
                    }
                }
            });
        }
    }
}

