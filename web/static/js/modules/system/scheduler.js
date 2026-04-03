/**
 * 计划任务管理模块（主模块）
 * Scheduled Task Management Module (Main)
 * Version: 2.0.0 (Refactored - Modular Architecture)
 * Last updated: 2025-12-20
 */

// 导入子模块
import { showMessage } from './scheduler-utils.js';
import { SchedulerAPI } from './scheduler-api.js';
import { PathManager } from './scheduler-path-manager.js';
import { DirectoryBrowser } from './scheduler-directory-browser.js';
import { ScheduleConfigManager } from './scheduler-schedule-config.js';
import { ActionConfigManager } from './scheduler-action-config.js';
import { SchedulerUI } from './scheduler-ui.js';
import { FormManager } from './scheduler-form.js';

// 在控制台输出版本信息
console.log('SchedulerManager v2.0.0 (Modular Architecture) - Loaded at', new Date().toISOString());

const SchedulerManager = {
    // 任务列表数据
    tasks: [],
    loading: false,
    
    // 当前编辑的任务
    currentTask: null,
    
    // 子模块实例
    pathManager: new PathManager(),
    directoryBrowser: new DirectoryBrowser(),
    
    // 调度类型选项
    scheduleTypes: [
        { value: 'once', label: '一次性任务' },
        { value: 'interval', label: '间隔任务' },
        { value: 'daily', label: '每日任务' },
        { value: 'weekly', label: '每周任务' },
        { value: 'monthly', label: '每月任务' },
        { value: 'yearly', label: '每年任务' },
        { value: 'cron', label: 'Cron表达式' }
    ],
    
    // 任务动作类型选项
    actionTypes: [
        { value: 'backup', label: '备份任务' },
        { value: 'recovery', label: '恢复任务' },
        { value: 'cleanup', label: '清理任务' },
        { value: 'health_check', label: '健康检查' },
        { value: 'retention_check', label: '保留期检查' },
        { value: 'custom', label: '自定义任务' }
    ],
    
    /**
     * 初始化
     */
    async init() {
        await this.loadTasks();
        await this.loadTapeDevices();
        this.setupEventListeners();
        this.setupActionTypeChange();
        this.setupDatePickers();
        this.setupDirectoryBrowsers();
        
        // 每30秒刷新一次任务列表
        setInterval(() => {
            if (document.getElementById('scheduler-tab')?.classList.contains('active')) {
                this.loadTasks();
            }
        }, 30000);
    },
    
    /**
     * 设置事件监听器
     */
    setupEventListeners() {
        // 添加任务按钮
        const addBtn = document.getElementById('addScheduledTaskBtn');
        if (addBtn) {
            addBtn.addEventListener('click', () => this.showAddTaskModal());
        }
        
        // 保存任务表单
        const saveForm = document.getElementById('scheduledTaskForm');
        if (saveForm) {
            saveForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.saveTask();
            });
        }
        
        // 关闭模态框时重置表单
        const taskModal = document.getElementById('scheduledTaskModal');
        if (taskModal) {
            taskModal.addEventListener('hidden.bs.modal', () => {
                this.resetForm();
            });
        }
        
        // 调度类型改变时更新配置表单
        const scheduleTypeSelect = document.getElementById('scheduleType');
        if (scheduleTypeSelect) {
            scheduleTypeSelect.addEventListener('change', function() {
                console.log('Schedule type changed to:', this.value);
                ScheduleConfigManager.updateScheduleConfigForm(this.value, {});
                // 确保调度配置容器可见
                const configContent = document.getElementById('scheduleConfigContent');
                if (configContent) {
                    configContent.style.display = 'block';
                    configContent.style.visibility = 'visible';
                }
            });
        }
        
        // 添加源路径按钮
        const addSourcePathBtn = document.getElementById('browseSourcePathBtn');
        if (addSourcePathBtn) {
            addSourcePathBtn.addEventListener('click', () => this.directoryBrowser.showDirectoryBrowser('backup'));
        }
        
        // 浏览目标路径按钮
        const browseTargetPathBtn = document.getElementById('browseTargetPathBtn');
        if (browseTargetPathBtn) {
            browseTargetPathBtn.addEventListener('click', () => this.directoryBrowser.showDirectoryBrowser('backup_target'));
        }
        
        // 目标路径输入框回车添加
        const targetPathInput = document.getElementById('backupTargetPath');
        if (targetPathInput) {
            targetPathInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const path = targetPathInput.value.trim();
                    if (path) {
                        this.pathManager.addTargetPath(path);
                        targetPathInput.value = '';
                    }
                }
            });
        }
        
        // 添加磁带机按钮
        const addTapeDeviceBtn = document.getElementById('addTapeDeviceBtn');
        if (addTapeDeviceBtn) {
            addTapeDeviceBtn.addEventListener('click', () => this.pathManager.addTapeDevice());
        }

        // 使用全局事件委托处理表格中的操作按钮（与backup.js保持一致）
        // 这样可以确保即使表格内容动态更新，事件也能正确绑定
        document.addEventListener('click', (e) => {
            const button = e.target.closest('button');
            if (!button) return;

            // 只处理计划任务表格中的按钮（检查是否在scheduledTasksTableBody内）
            const tasksTableBody = document.getElementById('scheduledTasksTableBody');
            if (!tasksTableBody || !tasksTableBody.contains(button)) return;

            // 立即运行按钮
            if (button.classList.contains('btn-action-run')) {
                e.preventDefault();
                e.stopPropagation();
                e.stopImmediatePropagation(); // 阻止其他监听器处理
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.runTask(taskId);
                }
                return;
            }

            // 禁用按钮
            if (button.classList.contains('btn-action-disable')) {
                e.preventDefault();
                e.stopPropagation();
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.disableTask(taskId);
                }
                return;
            }

            // 启用按钮
            if (button.classList.contains('btn-action-enable')) {
                e.preventDefault();
                e.stopPropagation();
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.enableTask(taskId);
                }
                return;
            }

            // 编辑按钮
            if (button.classList.contains('btn-action-edit')) {
                e.preventDefault();
                e.stopPropagation();
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.editTask(taskId);
                }
                return;
            }

            // 解锁按钮
            if (button.classList.contains('btn-action-unlock')) {
                e.preventDefault();
                e.stopPropagation();
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.unlockTask(taskId);
                }
                return;
            }

            // 删除按钮
            if (button.classList.contains('btn-action-delete')) {
                e.preventDefault();
                e.stopPropagation();
                e.stopImmediatePropagation(); // 阻止其他监听器处理
                const taskId = parseInt(button.getAttribute('data-task-id'));
                if (!isNaN(taskId)) {
                    this.deleteTask(taskId);
                }
                return;
            }
        });
        
        // 源路径输入框回车添加
        const sourcePathInput = document.getElementById('backupSourcePath');
        if (sourcePathInput) {
            sourcePathInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const path = sourcePathInput.value.trim();
                    if (path) {
                        this.pathManager.addSourcePath(path);
                        sourcePathInput.value = '';
                    }
                }
            });
        }
        
        // 监听路径选择事件（从目录浏览器）
        document.addEventListener('pathSelected', (e) => {
            const { path, type } = e.detail;
            if (type === 'backup') {
                this.pathManager.addSourcePath(path);
            } else if (type === 'backup_target') {
                this.pathManager.addTargetPath(path);
            }
        });
    },
    
    /**
     * 设置任务动作类型切换
     */
    setupActionTypeChange() {
        const actionTypeSelect = document.getElementById('actionType');
        if (actionTypeSelect) {
            actionTypeSelect.addEventListener('change', () => {
                ActionConfigManager.updateActionConfigForm(actionTypeSelect.value);
            });
        }
        
        // 备份目标类型切换
        const backupTargetTypeSelect = document.getElementById('backupTargetType');
        if (backupTargetTypeSelect) {
            backupTargetTypeSelect.addEventListener('change', () => {
                ActionConfigManager.updateBackupTargetConfig(backupTargetTypeSelect.value);
            });
        }

        // 验证类型切换：控制磁带设备容器显隐
        const verifyTypeSelect = document.getElementById('verifyType');
        if (verifyTypeSelect) {
            verifyTypeSelect.addEventListener('change', () => {
                const tapeContainer = document.getElementById('verifyTapeDeviceContainer');
                if (tapeContainer) {
                    tapeContainer.style.display = verifyTypeSelect.value === 'tape' ? 'block' : 'none';
                }
            });
        }

        // 验证源路径管理
        window._verifySourcePaths = [];
        window._renderVerifySourcePaths = () => {
            const list = document.getElementById('verifySourcePathsList');
            if (!list) return;
            list.textContent = '';
            window._verifySourcePaths.forEach((p, i) => {
                const div = document.createElement('div');
                div.className = 'd-flex align-items-center mb-1';
                const badge = document.createElement('span');
                badge.className = 'badge bg-secondary me-2';
                badge.textContent = '📁';
                const pathSpan = document.createElement('span');
                pathSpan.className = 'text-light flex-grow-1';
                pathSpan.textContent = p;
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'btn btn-sm btn-outline-danger';
                btn.textContent = '✕';
                btn.addEventListener('click', () => {
                    window._verifySourcePaths.splice(i, 1);
                    window._renderVerifySourcePaths();
                });
                div.appendChild(badge);
                div.appendChild(pathSpan);
                div.appendChild(btn);
                list.appendChild(div);
            });
        };
        const verifySourceInput = document.getElementById('verifySourcePath');
        if (verifySourceInput) {
            verifySourceInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    const val = verifySourceInput.value.trim();
                    if (val && !window._verifySourcePaths.includes(val)) {
                        window._verifySourcePaths.push(val);
                        window._renderVerifySourcePaths();
                        verifySourceInput.value = '';
                    }
                }
            });
        }
        
        // 备份类型顶部选择器同步
        const backupTaskTypeHeader = document.getElementById('backupTaskTypeHeader');
        const backupTaskType = document.getElementById('backupTaskType');
        if (backupTaskTypeHeader && backupTaskType) {
            backupTaskTypeHeader.addEventListener('change', () => {
                backupTaskType.value = backupTaskTypeHeader.value;
            });
            backupTaskType.addEventListener('change', () => {
                backupTaskTypeHeader.value = backupTaskType.value;
            });
        }
    },
    
    /**
     * 设置日期时间选择器
     */
    setupDatePickers() {
        // 一次性任务的日期时间选择器
        const onceDateTimeBtn = document.getElementById('onceDateTimeBtn');
        const onceDateTimeInput = document.getElementById('onceDateTime');
        const onceDateTimeHidden = document.getElementById('onceDateTimeHidden');
        
        if (onceDateTimeBtn && onceDateTimeInput && onceDateTimeHidden) {
            onceDateTimeBtn.addEventListener('click', () => {
                onceDateTimeHidden.showPicker();
            });
            
            onceDateTimeHidden.addEventListener('change', () => {
                const value = onceDateTimeHidden.value;
                if (value) {
                    // 格式化为可读格式
                    const date = new Date(value);
                    const formatted = date.toLocaleString('zh-CN', {
                        year: 'numeric',
                        month: '2-digit',
                        day: '2-digit',
                        hour: '2-digit',
                        minute: '2-digit'
                    });
                    onceDateTimeInput.value = formatted;
                }
            });
        }
        
        // 时间选择器
        const timeButtons = [
            { btn: 'dailyTimeBtn', input: 'dailyTime', hidden: 'dailyTimeHidden' },
            { btn: 'weeklyTimeBtn', input: 'weeklyTime', hidden: 'weeklyTimeHidden' },
            { btn: 'monthlyTimeBtn', input: 'monthlyTime', hidden: 'monthlyTimeHidden' },
            { btn: 'yearlyTimeBtn', input: 'yearlyTime', hidden: 'yearlyTimeHidden' }
        ];
        
        timeButtons.forEach(({ btn, input, hidden }) => {
            const btnEl = document.getElementById(btn);
            const inputEl = document.getElementById(input);
            const hiddenEl = document.getElementById(hidden);
            
            if (btnEl && inputEl && hiddenEl) {
                // 保存原始值
                const originalInputValue = inputEl.value;
                const originalHiddenValue = hiddenEl.value;
                
                // 移除旧的事件监听器（通过克隆节点）
                const newBtn = btnEl.cloneNode(true);
                btnEl.parentNode.replaceChild(newBtn, btnEl);
                const newInput = inputEl.cloneNode(true);
                // 恢复原始值
                newInput.value = originalInputValue;
                inputEl.parentNode.replaceChild(newInput, inputEl);
                const newHidden = hiddenEl.cloneNode(true);
                // 恢复原始值并确保d-none类被保留
                newHidden.value = originalHiddenValue;
                if (hiddenEl.classList.contains('d-none')) {
                    newHidden.classList.add('d-none');
                }
                hiddenEl.parentNode.replaceChild(newHidden, hiddenEl);
                
                // 重新获取元素
                const newBtnEl = document.getElementById(btn);
                const newInputEl = document.getElementById(input);
                const newHiddenEl = document.getElementById(hidden);
                
                // 确保显示元素可见
                if (newBtnEl) {
                    newBtnEl.style.display = '';
                    newBtnEl.style.visibility = 'visible';
                    console.log(`Time picker button ${btn} is visible`);
                }
                if (newInputEl) {
                    newInputEl.style.display = '';
                    newInputEl.style.visibility = 'visible';
                    console.log(`Time input ${input} is visible`);
                }
                
                // 确保隐藏的time输入框保留d-none类
                if (newHiddenEl && newHiddenEl.classList.contains('d-none')) {
                    console.log(`Hidden time input ${hidden} has d-none class (correct)`);
                }
                
                // 按钮点击：打开时间选择器
                newBtnEl.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    console.log(`[${btn}] Time picker button clicked, hidden element exists:`, !!newHiddenEl);
                    console.log(`[${btn}] Hidden element classes:`, newHiddenEl.classList.toString());
                    console.log(`[${btn}] Hidden element display:`, window.getComputedStyle(newHiddenEl).display);
                    
                    // 确保隐藏输入框可以显示选择器（临时移除d-none类）
                    const wasHidden = newHiddenEl.classList.contains('d-none');
                    console.log(`[${btn}] Was hidden:`, wasHidden);
                    
                    if (wasHidden) {
                        newHiddenEl.classList.remove('d-none');
                        // 临时设置为在屏幕外但仍可交互（使用fixed定位）
                        newHiddenEl.style.position = 'fixed';
                        newHiddenEl.style.top = '50%';
                        newHiddenEl.style.left = '50%';
                        newHiddenEl.style.transform = 'translate(-50%, -50%)';
                        newHiddenEl.style.width = '200px';
                        newHiddenEl.style.height = '40px';
                        newHiddenEl.style.opacity = '0.01'; // 几乎透明但可见（便于调试，可选）
                        newHiddenEl.style.pointerEvents = 'auto';
                        newHiddenEl.style.zIndex = '10000';
                        console.log(`[${btn}] Temporarily shown hidden input for picker`);
                    }
                    
                    // 使用setTimeout确保DOM更新完成
                    setTimeout(() => {
                        try {
                            // 先尝试showPicker方法（现代浏览器）
                            if (typeof newHiddenEl.showPicker === 'function') {
                                console.log(`[${btn}] Using showPicker() method`);
                                const pickerResult = newHiddenEl.showPicker();
                                // 检查返回值是否为Promise
                                if (pickerResult && typeof pickerResult.catch === 'function') {
                                    pickerResult.catch(err => {
                                        console.warn(`[${btn}] showPicker() failed:`, err);
                                        // showPicker失败，尝试点击
                                        console.log(`[${btn}] Falling back to focus/click`);
                                        newHiddenEl.focus();
                                        setTimeout(() => {
                                            newHiddenEl.click();
                                        }, 100);
                                    });
                                } else {
                                    // showPicker返回的不是Promise，直接使用备用方案
                                    console.log(`[${btn}] showPicker() did not return a Promise, using focus/click`);
                                    newHiddenEl.focus();
                                    setTimeout(() => {
                                        newHiddenEl.click();
                                    }, 100);
                                }
                            } else {
                                // 不支持showPicker，直接聚焦并点击
                                console.log(`[${btn}] showPicker() not supported, using focus/click`);
                                newHiddenEl.focus();
                                setTimeout(() => {
                                    newHiddenEl.click();
                                }, 100);
                            }
                        } catch (err) {
                            console.error(`[${btn}] Error opening time picker:`, err);
                            // 备用方案：直接聚焦到隐藏输入框
                            newHiddenEl.focus();
                            setTimeout(() => {
                                newHiddenEl.click();
                            }, 100);
                        }
                    }, 50);
                });
                
                // 时间选择器变化：同步到显示输入框
                newHiddenEl.addEventListener('change', () => {
                    const value = newHiddenEl.value;
                    console.log(`[${hidden}] Time picker changed to:`, value);
                    if (value) {
                        const [hours, minutes] = value.split(':');
                        newInputEl.value = `${hours}:${minutes}`;
                        console.log(`[${input}] Updated display input to:`, newInputEl.value);
                    }
                    
                    // 延迟重新隐藏输入框，确保选择器完全关闭
                    setTimeout(() => {
                        // 重新隐藏输入框（恢复d-none类）
                        if (!newHiddenEl.classList.contains('d-none')) {
                            newHiddenEl.classList.add('d-none');
                            // 清除所有临时样式
                            newHiddenEl.style.position = '';
                            newHiddenEl.style.top = '';
                            newHiddenEl.style.left = '';
                            newHiddenEl.style.transform = '';
                            newHiddenEl.style.width = '';
                            newHiddenEl.style.height = '';
                            newHiddenEl.style.opacity = '';
                            newHiddenEl.style.pointerEvents = '';
                            newHiddenEl.style.zIndex = '';
                            console.log(`[${hidden}] Re-hidden time input after selection`);
                        }
                    }, 300);
                });
                
                // 显示输入框直接输入：验证格式并同步到隐藏输入框
                newInputEl.addEventListener('input', (e) => {
                    const value = e.target.value.trim();
                    // 验证时间格式 HH:MM 或 H:MM
                    const timePattern = /^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$/;
                    if (timePattern.test(value)) {
                        // 格式正确，同步到隐藏输入框
                        const [hours, minutes] = value.split(':');
                        newHiddenEl.value = `${hours.padStart(2, '0')}:${minutes}`;
                        // 移除错误样式
                        newInputEl.classList.remove('is-invalid');
                    } else if (value.length > 0) {
                        // 格式错误，添加错误样式
                        newInputEl.classList.add('is-invalid');
                    } else {
                        // 清空时移除错误样式
                        newInputEl.classList.remove('is-invalid');
                    }
                });
                
                // 失去焦点时验证并格式化
                newInputEl.addEventListener('blur', (e) => {
                    const value = e.target.value.trim();
                    const timePattern = /^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$/;
                    if (value && timePattern.test(value)) {
                        // 格式正确，标准化格式（补零）
                        const [hours, minutes] = value.split(':');
                        const formatted = `${hours.padStart(2, '0')}:${minutes}`;
                        newInputEl.value = formatted;
                        newHiddenEl.value = `${formatted}:00`;
                        newInputEl.classList.remove('is-invalid');
                    } else if (value && !timePattern.test(value)) {
                        // 格式错误，恢复默认值或显示提示
                        showMessage('时间格式不正确，请使用 HH:MM 格式（如 02:00）', 'warning');
                        // 恢复为隐藏输入框的值
                        if (newHiddenEl.value) {
                            const [h, m] = newHiddenEl.value.split(':');
                            newInputEl.value = `${h}:${m}`;
                        } else {
                            newInputEl.value = '02:00';
                            newHiddenEl.value = '02:00:00';
                        }
                        newInputEl.classList.remove('is-invalid');
                    }
                });
            }
        });
    },
    
    /**
     * 设置目录浏览器
     */
    setupDirectoryBrowsers() {
        // 恢复任务的目录浏览器
        const browseRecoveryBtn = document.getElementById('browseRecoveryPathBtn');
        if (browseRecoveryBtn) {
            browseRecoveryBtn.addEventListener('click', () => {
                this.directoryBrowser.showDirectoryBrowser('recovery');
            });
        }
    },
    
    /**
     * 加载任务列表
     */
    async loadTasks() {
        try {
            this.loading = true;
            this.tasks = await SchedulerAPI.loadTasks();
            SchedulerUI.renderTasksTable(this.tasks, this.scheduleTypes);
        } catch (error) {
            console.error('加载计划任务失败:', error);
        } finally {
            this.loading = false;
        }
    },
    
    /**
     * 加载磁带机设备列表
     */
    async loadTapeDevices() {
        try {
            const devices = await SchedulerAPI.loadTapeDevices();
            this.pathManager.setTapeDevices(devices);
            this.pathManager.renderTapeDevicesSelect();
        } catch (error) {
            console.error('加载磁带机设备失败:', error);
        }
    },
    
    /**
     * 显示添加任务模态框
     */
    showAddTaskModal() {
        this.currentTask = null;
        this.resetForm();
        // 默认选择备份任务
        const actionTypeEl = document.getElementById('actionType');
        if (actionTypeEl) {
            actionTypeEl.value = 'backup';
            ActionConfigManager.updateActionConfigForm('backup');
        }
        // 默认选择每月任务
        const scheduleTypeSelect = document.getElementById('scheduleType');
        if (scheduleTypeSelect) {
            scheduleTypeSelect.value = 'monthly';
        }
        // 顶部备份类型与表单内备份类型同步默认值
        const backupTaskType = document.getElementById('backupTaskType');
        const backupTaskTypeHeader = document.getElementById('backupTaskTypeHeader');
        if (backupTaskType && backupTaskTypeHeader) {
            backupTaskTypeHeader.value = backupTaskType.value || 'full';
        }
        // 初始化备份目标默认显示（磁带机）
        const backupTargetType = document.getElementById('backupTargetType');
        if (backupTargetType) {
            if (!backupTargetType.value) backupTargetType.value = 'tape';
            ActionConfigManager.updateBackupTargetConfig(backupTargetType.value);
        }
        
        // 确保时间选择器已初始化（模态框显示后）
        const modalEl = document.getElementById('scheduledTaskModal');
        const modal = new bootstrap.Modal(modalEl);
        
        // 监听模态框显示事件，确保时间选择器正确初始化
        modalEl.addEventListener('shown.bs.modal', () => {
            // 重新初始化时间选择器（确保DOM已完全渲染）
            this.setupDatePickers();
            // 更新调度配置表单（显示每月任务配置面板）
            if (scheduleTypeSelect && scheduleTypeSelect.value) {
                ScheduleConfigManager.updateScheduleConfigForm(scheduleTypeSelect.value, {});
            }
            
            // 自动选择第一个磁带机
            if (backupTargetType && backupTargetType.value === 'tape' && this.pathManager.tapeDevices.length > 0) {
                const tapeDeviceSelect = document.getElementById('backupTapeDeviceSelect');
                if (tapeDeviceSelect && tapeDeviceSelect.options.length > 1) {
                    // 选择第一个实际磁带机（跳过"请选择"选项）
                    tapeDeviceSelect.value = tapeDeviceSelect.options[1].value;
                    // 自动添加到列表
                    this.pathManager.addTapeDevice();
                }
            }
        }, { once: true }); // 只执行一次
        
        modal.show();
    },
    
    /**
     * 编辑任务
     */
    async editTask(taskId) {
        try {
            this.currentTask = await SchedulerAPI.getTask(taskId);
            FormManager.fillForm(this.currentTask, this.pathManager);
            
            const modal = new bootstrap.Modal(document.getElementById('scheduledTaskModal'));
            modal.show();
        } catch (error) {
            console.error('加载任务详情失败:', error);
        }
    },
    
    /**
     * 保存任务
     */
    async saveTask() {
        try {
            const success = await FormManager.saveTask(this.currentTask, this.pathManager);
            if (success) {
                // 关闭模态框
                const modalEl = document.getElementById('scheduledTaskModal');
                const modal = bootstrap.Modal.getInstance(modalEl);
                if (modal) {
                    modal.hide();
                }
                // 刷新任务列表
                await this.loadTasks();
            } else {
                // 保存失败，但不关闭模态框，让用户看到错误信息
                console.warn('保存任务失败，请检查错误信息');
            }
        } catch (error) {
            console.error('保存任务时发生异常:', error);
            // 确保错误信息已显示（SchedulerAPI.saveTask 会显示）
        }
    },
    
    /**
     * 删除任务
     */
    async deleteTask(taskId) {
        if (!confirm('确定要删除这个计划任务吗？')) {
            return;
        }
        
        try {
            await SchedulerAPI.deleteTask(taskId);
            alert('任务已删除');
            await this.loadTasks();
        } catch (error) {
            console.error('删除任务失败:', error);
            alert('删除任务失败: ' + (error.message || '未知错误'));
        }
    },
    
    /**
     * 运行任务 - 与backup.js保持一致
     */
    async runTask(taskId) {
        if (!confirm('确定要立即运行此计划任务吗？')) return;

        try {
            await SchedulerAPI.runTask(taskId);
            await this.loadTasks();
        } catch (error) {
            console.error('运行任务失败:', error);
            // SchedulerAPI.runTask 已经显示 alert，这里不需要重复
            // 但如果需要额外的错误处理，可以在这里添加
        }
    },
    
    /**
     * 解锁任务
     */
    async unlockTask(taskId) {
        try {
            if (!confirm('确定要解锁此任务吗？这将释放任务锁并重置状态。')) {
                return;
            }
            await SchedulerAPI.unlockTask(taskId);
            await this.loadTasks();
            // 显示成功消息（SchedulerAPI.unlockTask 已经显示 alert，这里不需要重复）
        } catch (error) {
            console.error('解锁任务失败:', error);
            alert('解锁任务失败: ' + (error.message || '未知错误'));
        }
    },
    
    /**
     * 启用任务
     */
    async enableTask(taskId) {
        try {
            await SchedulerAPI.enableTask(taskId);
            await this.loadTasks();
        } catch (error) {
            console.error('启用任务失败:', error);
        }
    },
    
    /**
     * 禁用任务
     */
    async disableTask(taskId) {
        try {
            await SchedulerAPI.disableTask(taskId);
            await this.loadTasks();
        } catch (error) {
            console.error('禁用任务失败:', error);
        }
    },
    
    /**
     * 重置表单
     */
    resetForm() {
        FormManager.resetForm(this.pathManager);
        this.currentTask = null;
    },
    
    // 向后兼容：保留旧的方法名
    removeSourcePath(index) {
        this.pathManager.removeSourcePath(index);
    },
    
    removeTargetPath(index) {
        this.pathManager.removeTargetPath(index);
    },
    
    removeTapeDevice(index) {
        this.pathManager.removeTapeDevice(index);
    }
};

// 将 SchedulerManager 暴露到全局作用域（向后兼容）
window.SchedulerManager = SchedulerManager;
window.pathManager = SchedulerManager.pathManager;
window.schedulerManager = SchedulerManager;

// 注意：调度类型改变事件监听器现在在 setupEventListeners() 方法中设置

// 导出（ES6模块）
export { SchedulerManager };
