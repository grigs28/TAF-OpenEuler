/**
 * 计划任务动作配置模块
 * Scheduler Action Config Module
 */

import { showMessage, isElementVisible } from './scheduler-utils.js';

/**
 * 动作配置管理器类
 */
export class ActionConfigManager {
    /**
     * 更新动作配置表单显示
     */
    static updateActionConfigForm(actionType) {
        // 隐藏所有配置面板
        document.querySelectorAll('.action-config-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        
        // 隐藏顶部备份类型选择器
        const backupTaskTypeRow = document.getElementById('backupTaskTypeRow');
        if (backupTaskTypeRow) {
            backupTaskTypeRow.style.display = 'none';
        }
        
        // 显示对应的配置面板
        const panelMap = {
            'backup': 'backupActionConfig',
            'recovery': 'recoveryActionConfig',
            'cleanup': 'cleanupActionConfig',
            'health_check': 'healthCheckActionConfig',
            'retention_check': 'retentionCheckActionConfig',
            'verify': 'verifyActionConfig',
            'custom': 'customActionConfig'
        };
        
        const panelId = panelMap[actionType];
        if (panelId) {
            const panel = document.getElementById(panelId);
            if (panel) {
                panel.style.display = 'block';
            }
        }
        
        // 如果是备份任务，显示顶部备份类型选择器
        if (actionType === 'backup') {
            if (backupTaskTypeRow) {
                backupTaskTypeRow.style.display = 'block';
            }
            // 初始化备份目标配置显示
            const backupTargetType = document.getElementById('backupTargetType');
            if (backupTargetType) {
                this.updateBackupTargetConfig(backupTargetType.value);
            }
        }
    }
    
    /**
     * 更新备份目标配置显示
     */
    static updateBackupTargetConfig(targetType) {
        const pathContainer = document.getElementById('backupTargetPathContainer');
        const tapeDeviceContainer = document.getElementById('backupTapeDeviceContainer');
        
        // 根据目标类型显示不同的内容
        if (targetType === 'tape') {
            // 磁带机：显示磁带机选择，隐藏目标路径
            if (pathContainer) pathContainer.style.display = 'none';
            if (tapeDeviceContainer) {
                tapeDeviceContainer.style.display = 'block';
            }
        } else if (targetType === 'storage') {
            // 存储：显示目标路径，隐藏磁带机选择
            if (pathContainer) pathContainer.style.display = 'block';
            if (tapeDeviceContainer) tapeDeviceContainer.style.display = 'none';
        }
    }
    
    /**
     * 获取动作配置
     */
    static getActionConfig(actionType, pathManager) {
        try {
            let config = {};
            const panelMap = {
                'backup': 'backupActionConfig',
                'recovery': 'recoveryActionConfig',
                'cleanup': 'cleanupActionConfig',
                'health_check': 'healthCheckActionConfig',
                'retention_check': 'retentionCheckActionConfig',
                'custom': 'customActionConfig'
            };
            const panelId = panelMap[actionType];
            const panel = panelId ? document.getElementById(panelId) : null;

            // 增强的元素查找函数，支持全局和面板内查找
            const findElement = (selector) => {
                // 先在面板内查找
                if (panel) {
                    const element = panel.querySelector(selector);
                    if (element) return element;
                }
                // 如果面板内没找到，尝试全局查找（处理动态显示的header元素）
                return document.querySelector(selector);
            };

            // 安全的值获取函数，处理元素不存在的情况
            const val = (selector, def = '') => {
                const element = findElement(selector);
                if (!element) return def;
                return element.value ?? def;
            };

            // 安全的选中状态获取函数
            const checked = (selector) => {
                const element = findElement(selector);
                return !!element?.checked;
            };

            // 检查元素是否可见且存在
            const isElementVisibleAndExists = (selector) => {
                const element = findElement(selector);
                if (!element) {
                    console.debug(`Element not found: ${selector}`);
                    return false;
                }
                const style = window.getComputedStyle(element);
                const isVisible = style.display !== 'none' && style.visibility !== 'hidden';
                return isVisible;
            };
            
            switch (actionType) {
                case 'backup':
                    if (pathManager.backupSourcePaths.length === 0) {
                        showMessage('请至少添加一个备份源路径', 'error');
                        return null;
                    }

                    // 获取备份目标类型，安全地获取
                    const targetTypeElement = findElement('#backupTargetType');
                    let targetType = 'tape'; // 默认值
                    if (targetTypeElement) {
                        targetType = targetTypeElement.value || 'tape';
                    }

                    // 获取备份任务类型 - 智能查找
                    let taskType = 'full'; // 默认值
                    // 优先从header获取（通常可见）
                    if (isElementVisibleAndExists('#backupTaskTypeHeader')) {
                        taskType = val('#backupTaskTypeHeader', 'full');
                    } else {
                        // 如果header不可见，尝试在面板内查找
                        taskType = val('#backupTaskType', 'full');
                    }

                    // 获取压缩、加密选项 - 只有元素存在且可见时才获取
                    let compressionEnabled = true; // 默认值
                    let encryptionEnabled = false; // 默认值

                    if (isElementVisibleAndExists('#backupCompression')) {
                        compressionEnabled = checked('#backupCompression');
                    }
                    if (isElementVisibleAndExists('#backupEncryption')) {
                        encryptionEnabled = checked('#backupEncryption');
                    }

                    config = {
                        source_paths: pathManager.backupSourcePaths,
                        task_type: taskType,
                        compression_enabled: compressionEnabled,
                        encryption_enabled: encryptionEnabled,
                        target_type: targetType
                    };

                    // 根据目标类型获取相应配置
                    if (targetType === 'tape') {
                        // 磁带目标：获取磁带设备列表（返回数组）
                        if (pathManager.backupTapeDevices.length === 0) {
                            showMessage('请至少添加一个目标磁带机', 'error');
                            return null;
                        }
                        // 保存为数组格式，同时兼容单个设备的情况
                        config.tape_devices = pathManager.backupTapeDevices;
                        config.tape_device = pathManager.backupTapeDevices.length === 1 
                            ? pathManager.backupTapeDevices[0] 
                            : pathManager.backupTapeDevices.join(',');
                    } else {
                        // 存储目标：获取目标路径
                        if (pathManager.backupTargetPaths.length === 0) {
                            showMessage('请至少添加一个目标路径', 'error');
                            return null;
                        }
                        // 保存为数组格式，同时兼容单个路径的情况
                        config.target_paths = pathManager.backupTargetPaths;
                        config.target_path = pathManager.backupTargetPaths.length === 1 
                            ? pathManager.backupTargetPaths[0] 
                            : pathManager.backupTargetPaths.join(',');
                    }

                    // 获取排除模式配置 - 只有可见时才获取
                    if (isElementVisibleAndExists('#backupExcludePatterns')) {
                        const excludeText = val('#backupExcludePatterns', '').trim();
                        if (excludeText) {
                            config.exclude_patterns = excludeText.split('\n').filter(p => p.trim());
                        }
                    }
                    break;
                    
                case 'recovery':
                    // 安全获取恢复配置
                    let backupSetId = '';
                    let targetPath = '';

                    if (isElementVisibleAndExists('#recoveryBackupSetId')) {
                        backupSetId = val('#recoveryBackupSetId', '').trim();
                    }
                    if (isElementVisibleAndExists('#recoveryTargetPath')) {
                        targetPath = val('#recoveryTargetPath', '').trim();
                    }

                    config = {
                        backup_set_id: backupSetId,
                        target_path: targetPath
                    };

                    if (!config.backup_set_id) {
                        showMessage('请输入备份集ID', 'error');
                        return null;
                    }
                    if (!config.target_path) {
                        showMessage('请选择或填写恢复目标路径', 'error');
                        return null;
                    }
                    break;

                case 'cleanup':
                    // 安全获取清理配置
                    let retentionDays = 180; // 默认值
                    if (isElementVisibleAndExists('#cleanupRetentionDays')) {
                        retentionDays = parseInt(val('#cleanupRetentionDays', '180')) || 180;
                    }
                    config = {
                        retention_days: retentionDays
                    };
                    break;

                case 'health_check':
                case 'retention_check':
                    config = {};
                    break;

                case 'verify':
                    config = {
                        verify_type: val('#verifyType', 'directory'),
                        verify_percent: parseFloat(val('#verifyPercent', '1')) || 1,
                        source_paths: window._verifySourcePaths || [],
                    };
                    const verifyTapeDevice = val('#verifyTapeDevice', '');
                    if (verifyTapeDevice) {
                        config.tape_device = verifyTapeDevice;
                    }
                    if (config.verify_type === 'directory' && config.source_paths.length === 0) {
                        showMessage('请至少添加一个验证源路径', 'error');
                        return null;
                    }
                    break;

                case 'custom':
                    // 安全获取自定义配置
                    let customConfigText = '';
                    if (isElementVisibleAndExists('#customActionConfigJson')) {
                        customConfigText = val('#customActionConfigJson', '').trim();
                    }

                    if (customConfigText) {
                        try {
                            config = JSON.parse(customConfigText);
                        } catch (e) {
                            showMessage('自定义配置必须是有效的JSON格式', 'error');
                            return null;
                        }
                    }
                    break;
            }

            return config;

        } catch (error) {
            console.error('Error in getActionConfig:', error);
            console.error('Stack trace:', error.stack);
            showMessage('获取动作配置时发生错误: ' + error.message, 'error');
            return null;
        }
    }
    
    /**
     * 填充动作配置
     */
    static fillActionConfig(actionType, config, pathManager) {
        switch (actionType) {
            case 'backup':
                // 填充源路径
                if (config.source_paths) {
                    pathManager.backupSourcePaths = config.source_paths;
                    pathManager.renderSourcePathsList();
                }
                
                // 填充备份任务类型
                const backupTaskTypeHeader = document.getElementById('backupTaskTypeHeader');
                const backupTaskType = document.getElementById('backupTaskType');
                if (config.task_type) {
                    if (backupTaskTypeHeader) backupTaskTypeHeader.value = config.task_type;
                    if (backupTaskType) backupTaskType.value = config.task_type;
                }
                
                // 填充备份目标类型
                const backupTargetTypeSelect = document.getElementById('backupTargetType');
                if (backupTargetTypeSelect) {
                    const targetType = config.target_type || 'tape';
                    backupTargetTypeSelect.value = targetType;
                    this.updateBackupTargetConfig(targetType);
                    
                    if (targetType === 'tape') {
                        // 填充磁带设备
                        if (config.tape_devices) {
                            pathManager.backupTapeDevices = config.tape_devices;
                            pathManager.renderTapeDevicesList();
                        }
                    } else {
                        // 填充目标路径
                        if (config.target_paths) {
                            pathManager.backupTargetPaths = config.target_paths;
                            pathManager.renderTargetPathsList();
                        }
                    }
                }
                
                // 填充排除模式
                if (config.exclude_patterns) {
                    const excludePatternsEl = document.getElementById('backupExcludePatterns');
                    if (excludePatternsEl) {
                        excludePatternsEl.value = config.exclude_patterns.join('\n');
                    }
                }
                break;
                
            case 'recovery':
                if (config.backup_set_id) {
                    const backupSetIdEl = document.getElementById('recoveryBackupSetId');
                    if (backupSetIdEl) backupSetIdEl.value = config.backup_set_id;
                }
                if (config.target_path) {
                    const targetPathEl = document.getElementById('recoveryTargetPath');
                    if (targetPathEl) targetPathEl.value = config.target_path;
                }
                break;
                
            case 'cleanup':
                if (config.retention_days) {
                    const retentionDaysEl = document.getElementById('cleanupRetentionDays');
                    if (retentionDaysEl) retentionDaysEl.value = config.retention_days;
                }
                break;
                
            case 'verify':
                if (config.verify_type) {
                    const verifyTypeEl = document.getElementById('verifyType');
                    if (verifyTypeEl) verifyTypeEl.value = config.verify_type;
                }
                if (config.verify_percent) {
                    const verifyPercentEl = document.getElementById('verifyPercent');
                    if (verifyPercentEl) verifyPercentEl.value = config.verify_percent;
                }
                if (config.source_paths) {
                    window._verifySourcePaths = config.source_paths;
                    if (window._renderVerifySourcePaths) window._renderVerifySourcePaths();
                }
                if (config.tape_device) {
                    const verifyTapeDeviceEl = document.getElementById('verifyTapeDevice');
                    if (verifyTapeDeviceEl) verifyTapeDeviceEl.value = config.tape_device;
                }
                break;

            case 'custom':
                if (config) {
                    const customConfigEl = document.getElementById('customActionConfigJson');
                    if (customConfigEl) {
                        customConfigEl.value = JSON.stringify(config, null, 2);
                    }
                }
                break;
        }
    }
}

