/**
 * 计划任务表单处理模块
 * Scheduler Form Module
 */

import { showMessage, safeGetValue } from './scheduler-utils.js';
import { ScheduleConfigManager } from './scheduler-schedule-config.js';
import { ActionConfigManager } from './scheduler-action-config.js';
import { SchedulerAPI } from './scheduler-api.js';

/**
 * 表单管理器类
 */
export class FormManager {
    /**
     * 填充表单
     */
    static fillForm(task, pathManager) {
        document.getElementById('taskName').value = task.task_name || '';
        document.getElementById('taskDescription').value = task.description || '';
        document.getElementById('scheduleType').value = task.schedule_type || '';
        document.getElementById('actionType').value = task.action_type || '';
        document.getElementById('taskEnabled').checked = task.enabled !== false;
        
        // 更新调度配置
        ScheduleConfigManager.updateScheduleConfigForm(task.schedule_type, task.schedule_config);
        
        // 更新动作类型，触发配置表单显示
        ActionConfigManager.updateActionConfigForm(task.action_type);
        
        // 填充动作配置
        ActionConfigManager.fillActionConfig(task.action_type, task.action_config || {}, pathManager);
    }
    
    /**
     * 重置表单
     */
    static resetForm(pathManager) {
        // 重置基本字段
        document.getElementById('taskName').value = '';
        document.getElementById('taskDescription').value = '';
        document.getElementById('scheduleType').value = '';
        document.getElementById('actionType').value = '';
        document.getElementById('taskEnabled').checked = true;
        
        // 重置调度配置面板
        document.querySelectorAll('.schedule-config-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        
        // 重置动作配置面板
        document.querySelectorAll('.action-config-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        
        // 清空路径管理器
        if (pathManager) {
            pathManager.clearAll();
        }
        
        // 重置备份任务类型
        const backupTaskType = document.getElementById('backupTaskType');
        const backupTaskTypeHeader = document.getElementById('backupTaskTypeHeader');
        if (backupTaskType) backupTaskType.value = 'full';
        if (backupTaskTypeHeader) backupTaskTypeHeader.value = 'full';
        
        // 重置备份目标类型
        const backupTargetType = document.getElementById('backupTargetType');
        if (backupTargetType) backupTargetType.value = 'tape';
        
        // 重置磁带设备选择
        const tapeDeviceSelect = document.getElementById('backupTapeDevice') || document.getElementById('backupTapeDeviceSelect');
        if (tapeDeviceSelect) {
            tapeDeviceSelect.value = '';
        }
        
        // 重置所有隐藏的输入框
        const hiddenInputs = [
            'onceDateTimeHidden',
            'dailyTimeHidden',
            'weeklyTimeHidden',
            'monthlyTimeHidden',
            'yearlyTimeHidden'
        ];
        hiddenInputs.forEach(id => {
            const input = document.getElementById(id);
            if (input) {
                input.value = '';
            }
        });
        
        // 重置显示的输入框
        const onceDateTime = document.getElementById('onceDateTime');
        if (onceDateTime) onceDateTime.value = '';
        
        const timeInputs = ['dailyTime', 'weeklyTime', 'yearlyTime'];
        timeInputs.forEach(id => {
            const input = document.getElementById(id);
            if (input) input.value = '02:00';
        });
        // 每月任务默认时间为 00:02
        const monthlyTime = document.getElementById('monthlyTime');
        const monthlyTimeHidden = document.getElementById('monthlyTimeHidden');
        if (monthlyTime) monthlyTime.value = '00:02';
        if (monthlyTimeHidden) monthlyTimeHidden.value = '00:02';
        // 每月任务默认日期为 28
        const monthlyDay = document.getElementById('monthlyDay');
        if (monthlyDay) monthlyDay.value = '28';
    }
    
    /**
     * 验证表单
     */
    static validateForm() {
        console.log('=== validateForm start ===');
        const taskName = safeGetValue('taskName');
        const scheduleType = safeGetValue('scheduleType');
        const actionType = safeGetValue('actionType');
        console.log('Form values:', { taskName, scheduleType, actionType });

        if (!taskName) {
            showMessage('任务名称不能为空', 'error');
            return false;
        }

        if (!scheduleType) {
            showMessage('请选择调度类型', 'error');
            return false;
        }

        if (!actionType) {
            showMessage('请选择任务动作类型', 'error');
            return false;
        }

        // 验证调度配置
        const scheduleConfig = ScheduleConfigManager.getScheduleConfig();
        console.log('scheduleConfig:', scheduleConfig);
        if (!scheduleConfig || Object.keys(scheduleConfig).length === 0) {
            showMessage('请设置调度配置', 'error');
            return false;
        }

        // 针对不同调度类型进行特定验证
        switch (scheduleType) {
            case 'once':
                if (!scheduleConfig.datetime) {
                    showMessage('请选择一次性任务的执行时间', 'error');
                    return false;
                }
                break;
            case 'interval':
                if (!scheduleConfig.interval || scheduleConfig.interval <= 0) {
                    showMessage('请设置有效的间隔时间', 'error');
                    return false;
                }
                break;
            case 'daily':
                if (!scheduleConfig.time) {
                    showMessage('请设置每日任务的执行时间', 'error');
                    return false;
                }
                break;
            case 'weekly':
                if (!scheduleConfig.time || scheduleConfig.day_of_week === undefined) {
                    showMessage('请设置每周任务的执行时间和日期', 'error');
                    return false;
                }
                break;
            case 'monthly':
                console.log('Validating monthly:', { time: scheduleConfig.time, day_of_month: scheduleConfig.day_of_month });
                if (!scheduleConfig.time || !scheduleConfig.day_of_month) {
                    console.error('Monthly validation failed:', { time: scheduleConfig.time, day_of_month: scheduleConfig.day_of_month });
                    showMessage('请设置每月任务的执行时间和日期', 'error');
                    return false;
                }
                break;
            case 'yearly':
                if (!scheduleConfig.time || !scheduleConfig.month || !scheduleConfig.day) {
                    showMessage('请设置每年任务的执行时间、月份和日期', 'error');
                    return false;
                }
                break;
            case 'cron':
                if (!scheduleConfig.cron) {
                    showMessage('请输入有效的Cron表达式', 'error');
                    return false;
                }
                break;
        }

        return true;
    }
    
    /**
     * 获取表单数据
     */
    static getFormData(pathManager) {
        const taskNameElement = document.getElementById('taskName');
        const taskDescriptionElement = document.getElementById('taskDescription');
        const scheduleTypeElement = document.getElementById('scheduleType');
        const actionTypeElement = document.getElementById('actionType');
        const taskEnabledElement = document.getElementById('taskEnabled');

        const formData = {
            task_name: taskNameElement ? taskNameElement.value.trim() : '',
            description: taskDescriptionElement ? taskDescriptionElement.value.trim() : '',
            schedule_type: scheduleTypeElement ? scheduleTypeElement.value : '',
            schedule_config: ScheduleConfigManager.getScheduleConfig(),
            action_type: actionTypeElement ? actionTypeElement.value : '',
            enabled: taskEnabledElement ? taskEnabledElement.checked : false
        };
        
        // 获取动作配置
        const actionConfig = ActionConfigManager.getActionConfig(formData.action_type, pathManager);
        if (actionConfig === null) {
            return null; // 验证失败
        }
        
        formData.action_config = actionConfig;
        
        return formData;
    }
    
    /**
     * 保存任务
     */
    static async saveTask(currentTask, pathManager) {
        // 验证表单
        if (!this.validateForm()) {
            return false;
        }
        
        // 获取表单数据
        const formData = this.getFormData(pathManager);
        if (!formData) {
            return false; // 验证失败
        }
        
        // 如果是编辑模式，添加任务ID
        if (currentTask && currentTask.id) {
            formData.id = currentTask.id;
        }
        
        try {
            await SchedulerAPI.saveTask(formData);
            return true;
        } catch (error) {
            console.error('保存任务失败:', error);
            // SchedulerAPI.saveTask 已经显示了错误信息，这里不需要再次显示
            // 但确保错误被正确抛出，以便调用者知道保存失败
            return false;
        }
    }
}

