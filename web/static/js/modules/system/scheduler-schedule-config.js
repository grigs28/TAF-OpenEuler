/**
 * 计划任务调度配置模块
 * Scheduler Schedule Config Module
 */

import { safeGetValue, safeGetIntValue, isElementVisible } from './scheduler-utils.js';

/**
 * 调度配置管理器类
 */
export class ScheduleConfigManager {
    /**
     * 更新调度配置表单显示
     */
    static updateScheduleConfigForm(scheduleType, config) {
        console.log('updateScheduleConfigForm called with scheduleType:', scheduleType);
        
        // 隐藏所有配置面板
        document.querySelectorAll('.schedule-config-panel').forEach(panel => {
            panel.style.display = 'none';
        });
        
        // 显示对应的配置面板
        const panel = document.getElementById(`${scheduleType}Config`);
        console.log('Panel element:', panel, 'for scheduleType:', scheduleType);
        
        if (panel) {
            // 确保面板可见
            panel.style.display = 'block';
            panel.style.visibility = 'visible';
            panel.style.opacity = '1';
            
            // 确保面板内的所有输入框、按钮和标签都可见
            panel.querySelectorAll('input, select, button, label, .input-group, .form-label, small').forEach(el => {
                // 跳过隐藏的time输入框（有d-none类），但要确保它们仍然隐藏
                if (el.classList.contains('d-none')) {
                    // 保持d-none类的元素隐藏，但确保在需要时可以显示
                    el.style.display = 'none';
                } else {
                    el.style.display = '';
                    el.style.visibility = 'visible';
                    el.style.opacity = '1';
                }
            });
            
            // 确保input-group内的元素可见
            panel.querySelectorAll('.input-group').forEach(group => {
                group.style.display = 'flex';
                group.style.visibility = 'visible';
            });
            
            console.log('Panel displayed, children count:', panel.children.length);
            console.log('Panel HTML:', panel.innerHTML.substring(0, 200));
            
            // 特别检查时间选择器元素
            const timeInput = panel.querySelector('input[type="text"][id*="Time"]');
            const timeBtn = panel.querySelector('button[id*="TimeBtn"]');
            const timeHidden = panel.querySelector('input[type="time"]');
            console.log('Time input elements:', { timeInput, timeBtn, timeHidden });
            
            // 填充配置值
            if (config) {
                switch (scheduleType) {
                    case 'once':
                        if (config.datetime) {
                            // 将API格式转换为datetime-local格式
                            // API格式: YYYY-MM-DD HH:MM:SS
                            // datetime-local格式: YYYY-MM-DDTHH:MM
                            const dateTime = config.datetime.replace(' ', 'T').substring(0, 16);
                            const hiddenInput = document.getElementById('onceDateTimeHidden');
                            const displayInput = document.getElementById('onceDateTime');
                            if (hiddenInput) {
                                hiddenInput.value = dateTime;
                            }
                            if (displayInput && dateTime) {
                                const date = new Date(dateTime);
                                // 验证日期是否有效
                                if (isNaN(date.getTime())) {
                                    // 如果日期无效，清空显示字段
                                    displayInput.value = '';
                                    console.warn('无效的日期格式:', dateTime);
                                } else {
                                    const formatted = date.toLocaleString('zh-CN', {
                                        year: 'numeric',
                                        month: '2-digit',
                                        day: '2-digit',
                                        hour: '2-digit',
                                        minute: '2-digit'
                                    });
                                    displayInput.value = formatted;
                                }
                            }
                        }
                        break;
                    case 'interval':
                        document.getElementById('intervalValue').value = config.interval || '';
                        document.getElementById('intervalUnit').value = config.unit || 'minutes';
                        break;
                    case 'daily':
                        const dailyTime = config.time || '02:00:00';
                        const dailyTimeHidden = document.getElementById('dailyTimeHidden');
                        const dailyTimeDisplay = document.getElementById('dailyTime');
                        if (dailyTimeHidden) {
                            dailyTimeHidden.value = dailyTime.substring(0, 5);
                        }
                        if (dailyTimeDisplay) {
                            dailyTimeDisplay.value = dailyTime.substring(0, 5);
                        }
                        break;
                    case 'weekly':
                        document.getElementById('weeklyDay').value = config.day_of_week || 0;
                        const weeklyTime = config.time || '02:00:00';
                        const weeklyTimeHidden = document.getElementById('weeklyTimeHidden');
                        const weeklyTimeDisplay = document.getElementById('weeklyTime');
                        if (weeklyTimeHidden) {
                            weeklyTimeHidden.value = weeklyTime.substring(0, 5);
                        }
                        if (weeklyTimeDisplay) {
                            weeklyTimeDisplay.value = weeklyTime.substring(0, 5);
                        }
                        break;
                    case 'monthly':
                        document.getElementById('monthlyDay').value = config.day_of_month || 28;
                        const monthlyTime = config.time || '00:02:00';
                        const monthlyTimeHidden = document.getElementById('monthlyTimeHidden');
                        const monthlyTimeDisplay = document.getElementById('monthlyTime');
                        if (monthlyTimeHidden) {
                            monthlyTimeHidden.value = monthlyTime.substring(0, 5);
                        }
                        if (monthlyTimeDisplay) {
                            monthlyTimeDisplay.value = monthlyTime.substring(0, 5);
                        }
                        break;
                    case 'yearly':
                        document.getElementById('yearlyMonth').value = config.month || 1;
                        document.getElementById('yearlyDay').value = config.day || 1;
                        const yearlyTime = config.time || '02:00:00';
                        const yearlyTimeHidden = document.getElementById('yearlyTimeHidden');
                        const yearlyTimeDisplay = document.getElementById('yearlyTime');
                        if (yearlyTimeHidden) {
                            yearlyTimeHidden.value = yearlyTime.substring(0, 5);
                        }
                        if (yearlyTimeDisplay) {
                            yearlyTimeDisplay.value = yearlyTime.substring(0, 5);
                        }
                        break;
                    case 'cron':
                        document.getElementById('cronExpression').value = config.cron || '';
                        break;
                }
            }
        }
    }
    
    /**
     * 获取调度配置
     */
    static getScheduleConfig() {
        // 安全获取调度类型
        const scheduleTypeElement = document.getElementById('scheduleType');
        if (!scheduleTypeElement) {
            console.error('Schedule type element not found');
            return {};
        }

        const scheduleType = scheduleTypeElement.value;
        let config = {};
        console.log('getScheduleConfig: scheduleType =', scheduleType);

        switch (scheduleType) {
            case 'once':
                // 优先从隐藏字段获取（如果可用）
                const onceDateTimeHidden = document.getElementById('onceDateTimeHidden');
                let dateTimeValue = null;

                console.log('onceDateTimeHidden element:', onceDateTimeHidden);
                console.log('onceDateTimeHidden value:', onceDateTimeHidden?.value);

                if (onceDateTimeHidden && onceDateTimeHidden.value) {
                    // 从隐藏字段获取（datetime-local格式）
                    dateTimeValue = onceDateTimeHidden.value;
                    console.log('Got dateTime from hidden input:', dateTimeValue);
                }

                // 如果隐藏字段没有值，尝试从显示字段获取
                if (!dateTimeValue) {
                    const onceDateTimeDisplay = document.getElementById('onceDateTime');
                    console.log('onceDateTime display element:', onceDateTimeDisplay);
                    console.log('onceDateTime display value:', onceDateTimeDisplay?.value);

                    if (onceDateTimeDisplay && onceDateTimeDisplay.value) {
                        const displayValue = onceDateTimeDisplay.value;
                        if (!displayValue.includes('Invalid')) {
                            // 尝试解析显示字段的日期格式
                            try {
                                const date = new Date(displayValue);
                                if (!isNaN(date.getTime())) {
                                    // 转换为 YYYY-MM-DDTHH:MM 格式
                                    const year = date.getFullYear();
                                    const month = String(date.getMonth() + 1).padStart(2, '0');
                                    const day = String(date.getDate()).padStart(2, '0');
                                    const hours = String(date.getHours()).padStart(2, '0');
                                    const minutes = String(date.getMinutes()).padStart(2, '0');
                                    dateTimeValue = `${year}-${month}-${day}T${hours}:${minutes}`;
                                    console.log('Parsed dateTime from display:', dateTimeValue);
                                }
                            } catch (e) {
                                console.warn('无法解析日期:', displayValue, e);
                            }
                        }
                    }
                }

                if (dateTimeValue && !dateTimeValue.includes('Invalid')) {
                    // datetime-local格式: YYYY-MM-DDTHH:MM
                    // 需要转换为: YYYY-MM-DD HH:MM:SS
                    const dateTime = dateTimeValue.replace('T', ' ') + ':00';
                    // 验证最终日期格式
                    const date = new Date(dateTime.replace(' ', 'T'));
                    if (!isNaN(date.getTime())) {
                        config = {
                            datetime: dateTime
                        };
                        console.log('Final once config:', config);
                    } else {
                        console.warn('无效的日期格式:', dateTime);
                    }
                } else {
                    console.warn('No valid dateTimeValue for once task');
                }
                break;

            case 'interval':
                if (isElementVisible('intervalConfig')) {
                    config = {
                        interval: safeGetIntValue('intervalValue', 30),
                        unit: safeGetValue('intervalUnit', 'minutes')
                    };
                }
                break;

            case 'daily':
                // 无论面板是否可见，都尝试获取时间值
                // 优先从隐藏的time输入框获取（如果可用）
                const dailyTimeHiddenEl = document.getElementById('dailyTimeHidden');
                const dailyTimeDisplayEl = document.getElementById('dailyTime');
                let dailyTime = '02:00:00'; // 默认值
                
                if (dailyTimeHiddenEl && dailyTimeHiddenEl.value) {
                    // 从隐藏输入框获取（格式: HH:MM）
                    dailyTime = dailyTimeHiddenEl.value;
                    // 如果不是完整格式，添加秒数
                    if (dailyTime.split(':').length === 2) {
                        dailyTime = dailyTime + ':00';
                    }
                } else if (dailyTimeDisplayEl && dailyTimeDisplayEl.value) {
                    // 从显示输入框获取（格式: HH:MM）
                    const displayValue = dailyTimeDisplayEl.value.trim();
                    dailyTime = displayValue + ':00';
                }
                
                config = {
                    time: dailyTime
                };
                console.log('Daily schedule config:', config);
                break;

            case 'weekly':
                // 无论面板是否可见，都尝试获取时间值
                const weeklyTimeHiddenEl = document.getElementById('weeklyTimeHidden');
                const weeklyTimeDisplayEl = document.getElementById('weeklyTime');
                let weeklyTime = '02:00:00'; // 默认值
                
                if (weeklyTimeHiddenEl && weeklyTimeHiddenEl.value) {
                    weeklyTime = weeklyTimeHiddenEl.value;
                    if (weeklyTime.split(':').length === 2) {
                        weeklyTime = weeklyTime + ':00';
                    }
                } else if (weeklyTimeDisplayEl && weeklyTimeDisplayEl.value) {
                    weeklyTime = weeklyTimeDisplayEl.value.trim() + ':00';
                }
                
                config = {
                    day_of_week: safeGetIntValue('weeklyDay', 0),
                    time: weeklyTime
                };
                console.log('Weekly schedule config:', config);
                break;

            case 'monthly':
                // 无论面板是否可见，都尝试获取时间值
                const monthlyTimeHiddenEl = document.getElementById('monthlyTimeHidden');
                const monthlyTimeDisplayEl = document.getElementById('monthlyTime');
                let monthlyTime = '00:02:00'; // 默认值 00:02
                
                if (monthlyTimeHiddenEl && monthlyTimeHiddenEl.value) {
                    monthlyTime = monthlyTimeHiddenEl.value;
                    if (monthlyTime.split(':').length === 2) {
                        monthlyTime = monthlyTime + ':00';
                    }
                } else if (monthlyTimeDisplayEl && monthlyTimeDisplayEl.value) {
                    monthlyTime = monthlyTimeDisplayEl.value.trim() + ':00';
                }
                
                config = {
                    day_of_month: safeGetIntValue('monthlyDay', 28), // 默认值 28
                    time: monthlyTime
                };
                console.log('Monthly schedule config:', config);
                break;

            case 'yearly':
                // 无论面板是否可见，都尝试获取时间值
                const yearlyTimeHiddenEl = document.getElementById('yearlyTimeHidden');
                const yearlyTimeDisplayEl = document.getElementById('yearlyTime');
                let yearlyTime = '02:00:00'; // 默认值
                
                if (yearlyTimeHiddenEl && yearlyTimeHiddenEl.value) {
                    yearlyTime = yearlyTimeHiddenEl.value;
                    if (yearlyTime.split(':').length === 2) {
                        yearlyTime = yearlyTime + ':00';
                    }
                } else if (yearlyTimeDisplayEl && yearlyTimeDisplayEl.value) {
                    yearlyTime = yearlyTimeDisplayEl.value.trim() + ':00';
                }
                
                config = {
                    month: safeGetIntValue('yearlyMonth', 1),
                    day: safeGetIntValue('yearlyDay', 1),
                    time: yearlyTime
                };
                console.log('Yearly schedule config:', config);
                break;

            case 'cron':
                if (isElementVisible('cronConfig')) {
                    config = {
                        cron: safeGetValue('cronExpression')
                    };
                }
                break;
        }
        
        return config;
    }
    
    /**
     * 格式化调度配置显示
     */
    static formatScheduleConfig(scheduleType, config) {
        if (!config) return '-';
        
        switch (scheduleType) {
            case 'once':
                return config.datetime || '-';
            case 'interval':
                return `每${config.interval}${this.getUnitLabel(config.unit)}`;
            case 'daily':
                return `每天 ${config.time || '-'}`;
            case 'weekly':
                const weekdays = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'];
                return `${weekdays[config.day_of_week] || '-'} ${config.time || '-'}`;
            case 'monthly':
                return `每月${config.day_of_month}号 ${config.time || '-'}`;
            case 'yearly':
                return `${config.month}月${config.day}日 ${config.time || '-'}`;
            case 'cron':
                return `<code>${config.cron || '-'}</code>`;
            default:
                return '-';
        }
    }
    
    /**
     * 获取单位标签
     */
    static getUnitLabel(unit) {
        const units = {
            'minutes': '分钟',
            'hours': '小时',
            'days': '天'
        };
        return units[unit] || unit;
    }
}

