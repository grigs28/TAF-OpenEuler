import { initSystemMonitor } from './components/system_monitor.js';
import { initVersionHistory } from './components/versionHistory.js';
import { initReadmeModal } from './components/readmeModal.js';
import { initConfigModal } from './components/configModal.js';
import { logger } from './components/logger.js';

// 创建Vue应用实例
const app = Vue.createApp({
    delimiters: ['[[', ']]'],  // 设置自定义分隔符
    data() {
        return {
            version: '0.42',  // 当前版本号
            currentTab: 'ai-nav',  // 修改默认标签页为AI导航
            models: [],
            systemLogs: [], // 修改 logs 为 systemLogs
            newModelId: '',
            selectedModel: null,
            downloading: false,
            readmeContent: '',
            readmeLoading: false,
            statusCheckInterval: null,
            downloadLogBuffer: null,
            runLogRealTime: true,
            auth: {
                username: '',
                password: '',
                isAuthenticated: false,
                token: ''
            },
            showLoginModal: false,
            loginError: '',
            ws: null,
            wsReconnectAttempts: 0,
            maxReconnectAttempts: 5,
            showVersionModal: false,
            modelSource: 'modelscope', // 默认使用ModelScope
            downloadList: [], // 下载列表
            localIp: '127.0.0.1', // 添加本地IP属性
            versionModal: null,
            showVersionDialog: false,
            // 添加状态映射
            statusMap: {
                'downloading': '下载中',
                'stopped': '已停止',
                'stopping': '停止中',
                'success': '已完成',
                'error': '错误'
            },
            aiNavItems: [], // 添加AI导航数据数组
            searchQuery: '', // 添加搜索查询字段
            modelListInterval: null,
            websiteSettings: {
                title: '',
                theme: 'light',
                description: '',
                keywords: '',
                icp_number: '',
                analytics_code: '',
                primary_color: '#3a7bd5', // 默认主色调
                secondary_color: '#00d2ff' // 默认次要色调
            }
        }
    },

    computed: {
        // 添加过滤后的AI导航项目计算属性
        filteredAINavItems() {
            if (!this.searchQuery) {
                return this.aiNavItems;
            }
            const query = this.searchQuery.toLowerCase();
            return this.aiNavItems.filter(item => {
                return item.name.toLowerCase().includes(query) ||
                    item.description.toLowerCase().includes(query) ||
                    item.tags.some(tag => tag.toLowerCase().includes(query));
            });
        }
    },

    mounted() {
        // SSO 自动登录检测
        this.ssoAutoLogin();

        this.init();
        this.initWebSocket();

        // 请求当前下载状态
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: 'get_download_status'
            }));
        }

        this.loadWebsiteSettings();

        // 等待DOM更新后再初始化模态框
        this.$nextTick(() => {
            const versionModalEl = document.getElementById('versionModal');
            if (versionModalEl) {
                this.versionModal = new bootstrap.Modal(versionModalEl);
            }
        });
    },

    beforeUnmount() {
        if (this.statusCheckInterval) {
            clearInterval(this.statusCheckInterval);
        }
        if (this.statsInterval) {
            clearInterval(this.statsInterval);
        }
        this.stopModelListPolling();
    },

    methods: {
        // 初始化方法
        async init() {
            try {
                await Promise.all([
                    this.fetchModels(),
                    this.fetchAINavigation()
                ]);
                // 根据当前标签页启动相应的轮询
                if (this.currentTab === 'monitor') {
                    this.startMonitoring();
                } else if (this.currentTab === 'list') {
                    this.startModelListPolling();
                }
            } catch (error) {
                console.error('初始化失败:', error);
            }
        },
        getAINavIcon(tags) {
            // 根据标签返回对应的图标类名
            const iconMap = {
                chat: 'bi-chat-left-text',
                image: 'bi-image',
                video: 'bi-camera-reels',
                audio: 'bi-mic',
                code: 'bi-code-slash',
                translate: 'bi-translate',
                search: 'bi-search',
                writing: 'bi-pencil',
                analysis: 'bi-bar-chart',
                // 添加更多标签映射...
            };

            // 查找匹配的标签
            for (const tag of tags) {
                if (iconMap[tag.toLowerCase()]) {
                    return iconMap[tag.toLowerCase()];
                }
            }

            // 默认图标
            return 'bi-stars';
        },
        getGpuColClass(gpuCount) {
            if (gpuCount >= 4) {
                return 'col-md-3'; // 每行4个
            } else if (gpuCount === 3) {
                return 'col-md-4'; // 每行3个
            } else if (gpuCount === 2) {
                return 'col-md-6'; // 每行2个
            } else {
                return 'col-md-12'; // 每行1个
            }
        },
        // 获取AI导航数据
        async fetchAINavigation() {
            try {
                const response = await fetch('/api/ai-navigation');
                if (response.ok) {
                    this.aiNavItems = await response.json();
                }
            } catch (error) {
                console.error('获取AI导航数据失败:', error);
            }
        },

        // 获取模型列表
        async fetchModels() {
            try {
                const response = await fetch('/api/models');
                if (response.ok) {
                    this.models = await response.json();
                }
            } catch (error) {
                console.error('获取模型列表失败:', error);
            }
        },

        // 获取系统状态
        async fetchStats() {
            try {
                const response = await fetch('/api/system_stats');
                if (!response.ok) throw new Error('获取系统状态失败');
                const data = await response.json();

                // 直接更新 stats 对象，保持响应性
                this.stats = {
                    system: {
                        cpu: data.cpu?.percent || 0,
                        memory: {
                            used: data.memory?.used || 0,
                            total: data.memory?.total || 0,
                            percent: data.memory?.percent || 0
                        },
                        disks: data.disks || []
                    },
                    gpus: data.gpus || [],
                    timestamp: data.timestamp || new Date().toLocaleTimeString()
                };
            } catch (error) {
                console.error('获取系统状态失败:', error);
            }
        },

        // 启动模型列表定时刷新
        startModelListPolling() {
            if (this.modelListInterval) clearInterval(this.modelListInterval);
            this.loadModels();
            this.modelListInterval = setInterval(() => {
                this.loadModels();
            }, 3000);
        },

        // 停止模型列表定时刷新
        stopModelListPolling() {
            if (this.modelListInterval) {
                clearInterval(this.modelListInterval);
                this.modelListInterval = null;
            }
        },

        // 切换标签页
        switchTab(tab) {
            this.currentTab = tab;
            // 只保留模型列表和日志的轮询逻辑
            if (tab === 'list') {
                this.startModelListPolling();
            } else {
                this.stopModelListPolling();
            }
            if (tab === 'logs') {
                this.$nextTick(() => {
                    this.scrollToBottom();
                });
            }
        },

        async validateToken() {
            const authState = await loginUtils.validateToken();
            this.auth = { ...this.auth, ...authState };
        },        

        startServiceMonitor(modelId) {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                this.addLog('WebSocket未连接，无法启动监控', 'warning');
                return;
            }

            this.ws.send(JSON.stringify({
                type: 'start_monitor',
                modelId: modelId,
                authToken: this.auth.token
            }));
        },

        stopServiceMonitor(modelId) {
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
                this.addLog('WebSocket未连接，无法停止监控', 'warning');
                return;
            }

            this.ws.send(JSON.stringify({
                type: 'stop_monitor',
                modelId: modelId,
                authToken: this.auth.token
            }));
        },
        // 初始化WebSocket连接
        initWebSocket() {
            if (this.wsReconnectAttempts >= this.maxReconnectAttempts) {
                this.addLog('WebSocket重连次数过多，请刷新页面重试', 'error');
                return;
            }

            // 如果已经有连接，先关闭它
            if (this.ws) {
                this.ws.close();
                this.ws = null;
            }

            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            this.ws = new WebSocket(`${protocol}//${window.location.host}`);

            this.ws.onopen = () => {
                this.addLog('WebSocket连接已建立', 'success');
                this.wsReconnectAttempts = 0;  // 重置重连计数

                // 等待连接完全建立后再发送消息
                setTimeout(() => {
                    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                        // 发送认证信息
                        if (this.auth.isAuthenticated) {
                            this.ws.send(JSON.stringify({
                                type: 'auth',
                                token: this.auth.token
                            }));
                        }

                        // 请求本地IP地址
                        this.ws.send(JSON.stringify({
                            type: 'get_local_ip'
                        }));

                        // 重新订阅所有正在下载的模型
                        this.downloadList.forEach(item => {
                            if (item.status === 'downloading') {
                                this.ws.send(JSON.stringify({
                                    type: 'subscribe_download',
                                    modelId: item.modelId,
                                    authToken: this.auth.token
                                }));
                            }
                        });
                    }
                }, 500);  // 增加延迟时间到500ms
            };

            this.ws.onclose = (event) => {
                console.log('WebSocket连接关闭:', event);
                this.ws = null;
                
                // 如果不是正常关闭，尝试重连
                if (!event.wasClean) {
                    this.wsReconnectAttempts++;
                    this.addLog(`WebSocket连接断开，正在尝试重连 (${this.wsReconnectAttempts}/${this.maxReconnectAttempts})...`, 'warning');
                    
                    // 使用指数退避策略进行重连
                    const delay = Math.min(1000 * Math.pow(2, this.wsReconnectAttempts - 1), 30000);
                    setTimeout(() => {
                        if (this.wsReconnectAttempts < this.maxReconnectAttempts) {
                            this.initWebSocket();
                        }
                    }, delay);
                }
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket错误:', error);
                this.addLog('WebSocket连接错误', 'error');
            };

            this.ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    console.log('WebSocket 消息:', data); // 添加调试日志
                    
                    switch (data.type) {
                        case 'local_ip':
                            this.localIp = data.ip;
                            break;
                        case 'download_log':
                            // 只处理日志消息，不更新状态
                            if (data.message) {
                                const ip = this.localIp;
                                const modelId = data.modelId || '未知';
                                logger.addLog(`[${ip}][${modelId}] ${data.message}`, data.type === 'error' ? 'error' : 'info', this);
                            }
                            break;
                        case 'run_log':
                            this.handleRunLog(data);
                            break;
                        case 'service_status':
                            // 直接在这里处理服务状态更新
                            if (data.modelId && data.status) {
                                const model = this.models.find(m => m.id === data.modelId);
                                if (model) {
                                    model.running = data.status === 'running';
                                    if (data.status === 'running') {
                                        console.log('更新前的模型数据:', model);
                                        if (data.max_model_len !== undefined) {
                                            model.max_model_len = data.max_model_len;
                                            console.log('更新 max_model_len:', model.max_model_len);
                                        }
                                        if (data.port !== undefined) {
                                            model.port = data.port;
                                            console.log('更新 port:', model.port);
                                        }
                                        console.log('更新后的模型数据:', model);
                                        console.log('服务状态更新:', {
                                            modelId: data.modelId,
                                            status: data.status,
                                            max_model_len: data.max_model_len,
                                            port: data.port
                                        });
                                        // 强制更新视图
                                        this.$forceUpdate();
                                    }
                                }
                            }
                            this.handleServiceStatus(data);
                            break;
                        case 'download_complete':
                            // 更新下载状态
                            const completeIndex = this.downloadList.findIndex(item => item.modelId === data.modelId);
                            if (completeIndex !== -1) {
                                this.downloadList[completeIndex].status = 'success';
                                this.downloadList[completeIndex].progress = 100;
                                logger.addLog(`[${this.localIp}][${data.modelId}] 下载完成`, 'success', this);
                            }
                            break;
                        case 'download_failed':
                            // 处理下载失败
                            const failIndex = this.downloadList.findIndex(item => item.modelId === data.modelId);
                            if (failIndex !== -1) {
                                this.downloadList[failIndex].status = 'error';
                                this.downloadList[failIndex].progress = 0;
                                logger.addLog(`[${this.localIp}][${data.modelId}] 下载失败: ${data.message}`, 'error', this);
                                this.$forceUpdate();
                            }
                            break;
                        case 'download_progress':
                            // 处理下载进度更新
                            if (data.modelId && data.progress !== undefined) {
                                const index = this.downloadList.findIndex(item => item.modelId === data.modelId);
                                if (index !== -1) {
                                    // 更新现有下载项
                                    this.downloadList[index].progress = data.progress;
                                    this.downloadList[index].status = 'downloading';
                                } else {
                                    // 添加新的下载项
                                    this.downloadList.push({
                                        modelId: data.modelId,
                                        progress: data.progress,
                                        status: 'downloading',
                                        source: data.source || 'modelscope'
                                    });
                                }
                                this.$forceUpdate();
                            }
                            break;
                        case 'download_stopped':
                            // 处理下载停止的消息
                            const stopIndex = this.downloadList.findIndex(d => d.modelId === data.modelId);
                            if (stopIndex !== -1) {
                                this.downloadList[stopIndex].status = 'stopped';
                                this.downloadList[stopIndex].progress = 0;
                                logger.addLog(`模型 ${data.modelId} 下载已停止`, 'warning', this);
                                // 强制更新视图
                                this.$forceUpdate();
                            }
                            break;
                        case 'download_status':
                            // 处理下载状态响应
                            if (data.downloads) {
                                this.downloadList = data.downloads.map(item => ({
                                    modelId: item.modelId,
                                    progress: item.progress,
                                    status: item.status,
                                    source: item.source || 'modelscope'
                                }));
                            }
                            break;
                        case 'monitor_started':
                            logger.addLog(`已启动服务日志监控: ${data.modelId}`, 'success', this);
                            break;

                        case 'monitor_stopped':
                            logger.addLog(`已停止服务日志监控: ${data.modelId}`, 'info', this);
                            break;

                        case 'log_stream':
                            logger.addLog(`[${data.stream_id}] ${data.message}`, 'info', this);
                            break;
                    }
                } catch (e) {
                    console.error('处理WebSocket消息失败:', e);
                    logger.addLog(`处理WebSocket消息失败: ${e.message}`, 'error', this);
                }
            };
        },

        // 关闭WebSocket连接
        closeWebSocket() {
            if (this.ws) {
                this.ws.close();
                this.ws = null;
            }
        },

        // 处理下载请求
        handleDownload() {
            if (!this.auth.isAuthenticated) {
                this.showLoginModal = true;
                this.addLog('请先登录后再下载模型', 'warning');
                return;
            }

            if (!this.newModelId.trim()) {
                this.addLog('请输入模型标识', 'warning');
                return;
            }

            this.addLog(`开始下载模型 ${this.newModelId}...`, 'info');
            this.downloading = true;  // 设置下载状态
            this._sendDownloadRequest();
        },

        _sendDownloadRequest() {
            try {
                // 确保WebSocket连接是打开的
                if (this.ws.readyState !== WebSocket.OPEN) {
                    this.addLog('WebSocket连接未就绪，正在重连...', 'warning');
                    this.initWebSocket();
                    setTimeout(() => this._sendDownloadRequest(), 1000);
                    return;
                }

                // 发送下载请求
                this.ws.send(JSON.stringify({
                    type: 'download_request',
                    modelId: this.newModelId,
                    authToken: this.auth.token
                }));
            } catch (e) {
                this.addLog(`下载请求发送失败: ${e.message}`, 'error');
                this.downloading = false;
                this.newModelId = '';
            }
        },

        // 处理下载完成
        handleDownloadComplete(data) {
            this.addLog(`模型 ${data.modelId} 下载完成`, 'success');
            this.downloading = false;
            this.newModelId = '';
            this.loadModels();
        },

        // 加载模型列表
        async loadModels() {
            try {
                console.log('正在加载模型列表...'); // 添加调试日志
                const res = await fetch('/api/models');
                if (!res.ok) throw new Error(await res.text());
                const data = await res.json();

                // 直接使用后端返回的数据
                this.models = data;

                console.log('模型列表加载完成:', this.models.length); // 添加调试日志
            } catch (error) {
                console.error('加载模型列表失败:', error);
                this.addLog('加载模型列表失败: ' + error.message, 'error');
            }
        },

        // 确认删除模型
        confirmDeleteModel(model) {
            if (!this.auth.isAuthenticated) {
                this.$refs.loginModal.show();
                this.addLog('请先登录后再删除模型', 'warning');
                return;
            }
            // 移除下载检查，允许在下载时删除其他模型
            if (model.running) {
                alert(`⚠️ 无法删除: 模型 ${model.id} 正在运行中，请先停止！`);
                this.addLog(`无法删除: 模型 ${model.id} 正在运行中`, 'warning');
                return;
            }

            if (confirm(`⚠️确定要永久删除模型 ${model.id} 吗？此操作不可恢复！`)) {
                this.addLog(`确定要永久删除模型 ${model.id} 吗？此操作不可恢复！`);
                this.deleteModel(model);
            }
        },

        // 启用服务
        async enableService(model) {
            if (!this.auth.isAuthenticated) {
                this.$refs.loginModal.show();
                this.addLog('请先登录后再启用服务', 'warning');
                return;
            }

            if (!confirm(`确定要启用模型 ${model.id} 的服务吗？此操作将创建并启用该模型的服务。`)) {
                return;
            }

            try {
                this.addLog(`正在为模型 ${model.id} 启用服务...`, 'info');
                const res = await fetch('/api/enable_service', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + this.auth.token
                    },
                    body: JSON.stringify({ modelId: model.id })
                });

                if (!res.ok) {
                    if (res.status === 401) {
                        loginUtils.logout();
                        throw new Error('会话已过期，请重新登录');
                    }
                    throw new Error(await res.text());
                }

                const data = await res.json();
                this.addLog(data.message, data.status === 'success' ? 'success' : 'error');
                if (data.status === 'success') {
                    this.loadModels();
                }
            } catch (e) {
                this.addLog(`启用服务失败: ${e.message}`, 'error');
            }
        },

        // 删除模型方法
        async deleteModel(model) {
            try {
                if (!this.auth.isAuthenticated || !this.auth.token) {
                    this.$refs.loginModal.show();
                    throw new Error('请先登录');
                }

                this.addLog(`正在删除模型 ${model.id}...`, 'info');
                const res = await fetch('/api/delete_model', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + this.auth.token
                    },
                    body: JSON.stringify({ modelId: model.id })
                });

                if (!res.ok) {
                    if (res.status === 401) {
                        loginUtils.logout();
                        throw new Error('会话已过期，请重新登录');
                    }
                    throw new Error(await res.text());
                }

                this.addLog(`模型 ${model.id} 已删除`, 'success');
                this.loadModels();

                // 重置相关下载状态
                if (this.newModelId === model.id) {
                    this.downloading = false;
                    this.newModelId = '';
                }

            } catch (e) {
                this.addLog(`删除模型失败: ${e.message}`, 'error');
            }
        },

        // 确认停止模型
        async confirmStopModel(model) {
            if (model.run_mode === 'service') {
                if (!confirm(`确定要停止模型 ${model.id} 的服务吗？此操作将停止该模型的服务。`)) {
                    return;
                }
                // 如果是service模式，使用systemctl stop
                try {
                    const response = await fetch('/api/stop_service', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': `Bearer ${this.auth.token}`
                        },
                        body: JSON.stringify({ modelId: model.id })
                    });

                    const result = await response.json();
                    if (result.status === 'success') {
                        this.addLog('模型已停止', 'success');
                        this.loadModels();
                    } else {
                        this.addLog(result.message || '停止服务失败', 'error');
                    }
                } catch (error) {
                    console.error('停止模型失败:', error);
                    this.addLog('停止模型失败', 'error');
                }
            } else {
                // 原有的停止逻辑
                if (confirm(`确定要停止模型 ${model.id} 吗？`)) {
                    this.addLog(`正在停止模型 ${model.id}...`, 'info');

                    fetch('/api/stop', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization': 'Bearer ' + this.auth.token
                        },
                        body: JSON.stringify({ modelId: model.id })
                    })
                        .then(response => {
                            if (!response.ok) {
                                return response.text().then(text => { throw new Error(text) });
                            }
                            return response.json();
                        })
                        .then(data => {
                            if (data.status === 'stopped') {
                                this.addLog(`模型 ${model.id} 已停止`, 'success');
                                this.loadModels();
                            } else {
                                this.addLog(data.message || `停止命令已发送`, 'info');
                            }
                        })
                        .catch(error => {
                            this.addLog(`停止失败: ${error.message}`, 'error');
                            if (error.message.includes('认证')) {
                                this.$refs.loginModal.show();
                            }
                        });
                }
            }
        },

        // 停止模型
        async stopModel(model) {
            try {
                // 双重检查认证状态
                if (!this.auth.isAuthenticated || !this.auth.token) {
                    this.$refs.loginModal.show();
                    throw new Error('请先登录');
                }

                this.addLog(`正在停止模型 ${model.id}...`, 'info');

                const res = await fetch('/api/stop', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + this.auth.token
                    },
                    body: JSON.stringify({ modelId: model.id })
                });

                // 特殊处理401状态码
                if (res.status === 401) {
                    loginUtils.logout();
                    throw new Error('会话已过期，请重新登录');
                }

                if (!res.ok) throw new Error(await res.text());

                const data = await res.json();
                if (data.status === 'stopped') {
                    this.addLog(`模型 ${model.id} 已停止`, 'success');
                    this.loadModels();
                } else {
                    this.addLog(`模型 ${model.id} 未在运行`, 'warning');
                }
            } catch (e) {
                this.addLog(`停止模型失败: ${e.message}`, 'error');
                if (e.message.includes('登录')) {
                    this.$refs.loginModal.show();
                }
            }
        },

        // 处理运行日志
        handleRunLog(data) {
            const logData = typeof data === 'string' ? { message: data, real_time: false } : data;
            const type = logData.message.toLowerCase().includes('error') ? 'error' : 'info';
            this.addLog(logData.message, type);

            // 检查是否包含 max_model_len 的日志消息
            if (logData.message.includes('Maximum') && logData.message.includes('concurrency')) {
                const model = this.models.find(m => m.id === logData.modelId);
                if (model) {
                    // 从日志中提取 max_model_len
                    const maxLenMatch = logData.message.match(/Maximum.*?concurrency for (\d+),?\d*\s*tokens/);
                    if (maxLenMatch) {
                        const maxLen = parseInt(maxLenMatch[1]);
                        model.max_model_len = maxLen;
                        console.log('从日志中提取的 max_model_len:', maxLen);
                        model.running = true;
                        // 强制更新视图
                        this.$forceUpdate();
                    }
                }
            }
            // 检查是否包含端口信息的日志消息
            else if (logData.message.includes('Starting vLLM API server on')) {
                const model = this.models.find(m => m.id === logData.modelId);
                if (model) {
                    const portMatch = logData.message.match(/http:\/\/[^:]+:(\d+)/);
                    if (portMatch) {
                        const port = parseInt(portMatch[1]);
                        model.port = port;
                        console.log('从日志中提取的端口:', port);
                        // 强制更新视图
                        this.$forceUpdate();
                    }
                }
            }
        },

        // 处理下载日志
        handleDownloadLog(data) {
            // 过滤掉WebSocket错误消息
            if (data.type === 'error') {
                console.error('WebSocket错误:', data.message);
                return;
            }

            // 格式化时间
            const now = new Date();
            const timeStr = now.toLocaleTimeString();
            const ip = this.localIp;
            const modelId = data.modelId || '未知';

            // 如果有消息，添加到运行日志
            if (data.message) {
                this.addLog(`[${ip}][${modelId}] ${data.message}`, data.type === 'error' ? 'error' : 'info');
            }
        },

        // 清理ANSI控制字符
        cleanAnsi(text) {
            const ansiRegex = /[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]/g;
            return text.replace(ansiRegex, '');
        },

        // 刷新下载日志缓冲区
        flushDownloadLogBuffer(immediate = false) {
            if (this.downloadLogBuffer?.length > 0) {
                if (immediate && this.downloadLogInterval) {
                    clearInterval(this.downloadLogInterval);
                    this.downloadLogInterval = null;
                }

                this.downloadLogBuffer.forEach(log => {
                    this.addLog(log.message, log.type);
                });

                this.downloadLogBuffer = [];
            }
        },

        // 处理服务状态
        handleServiceStatus(data) {
            if (data.status === 'running') {
                this.addLog('服务启动成功: ' + data.message, 'success');
                if (this.statusCheckInterval) {
                    clearInterval(this.statusCheckInterval);
                }
                this.loadModels();
            } else if (data.status === 'failed') {
                this.addLog('服务启动失败: ' + data.message, 'error');
            }
        },

        handleModelStatus(data) {
            if (data.model_id && data.status) {
                const model = this.models.find(m => m.id === data.model_id);
                if (model) {
                    model.running = data.status === 'running';
                    if (data.status === 'running') {
                        model.max_model_len = data.max_model_len;
                        model.port = data.port;
                        console.log(`模型 ${model.id} 状态更新:`, {
                            status: data.status,
                            max_model_len: data.max_model_len,
                            port: data.port
                        });
                    }
                }
            }
        },

        addLog(message, type = 'info') {
            logger.addLog(message, type, this);
        },

        // 显示模型说明文档
        async showReadme(model) {
            this.$refs.readmeModal.show(model);
        },

        // 确认下载模型
        async confirmDownloadModel() {
            if (!this.auth.isAuthenticated) {
                this.showLoginModal = true;
                this.addLog('请先登录后再下载模型', 'warning');
                return;
            }

            if (!this.newModelId) {
                this.addLog('请输入模型标识', 'warning');
                return;
            }

            try {
                // 处理多个模型的情况
                const modelIds = this.newModelId.split(',').map(id => id.trim()).filter(id => id);

                if (modelIds.length === 0) {
                    this.addLog('请输入有效的模型标识', 'warning');
                    return;
                }

                // 确保WebSocket连接是打开的
                if (this.ws.readyState !== WebSocket.OPEN) {
                    this.addLog('WebSocket连接未就绪，正在重连...', 'warning');
                    this.initWebSocket();
                    setTimeout(() => this.confirmDownloadModel(), 1000);
                    return;
                }

                // 为每个模型创建下载任务
                for (const modelId of modelIds) {
                    this.addLog(`开始下载模型 ${modelId}...`, 'info');

                    // 添加到下载列表
                    this.downloadList.push({
                        modelId: modelId,
                        source: this.modelSource,
                        status: 'downloading',
                        progress: 0
                    });

                    // 发送下载请求
                    this.ws.send(JSON.stringify({
                        type: 'download_request',
                        modelId: modelId,
                        source: this.modelSource,
                        authToken: this.auth.token
                    }));
                }

                this.newModelId = '';
                this.loadModels();
            } catch (error) {
                this.addLog(`下载失败: ${error.message}`, 'error');
                if (error.message.includes('登录')) {
                    this.showLoginModal = true;
                }
            }
        },

        // 停止下载
        async stopDownload(item) {
            try {
                if (!this.auth.isAuthenticated) {
                    this.showLoginModal = true;
                    this.addLog('请先登录后再停止下载', 'warning');
                    return;
                }

                // 确保WebSocket连接是打开的
                if (this.ws.readyState !== WebSocket.OPEN) {
                    this.addLog('WebSocket连接未就绪，正在重连...', 'warning');
                    this.initWebSocket();
                    setTimeout(() => this.stopDownload(item), 1000);
                    return;
                }

                // 发送停止下载请求
                this.ws.send(JSON.stringify({
                    type: 'stop_download',
                    modelId: item.modelId,
                    source: item.source,
                    authToken: this.auth.token
                }));

                // 更新下载状态
                const index = this.downloadList.findIndex(d => d.modelId === item.modelId);
                if (index !== -1) {
                    this.downloadList[index].status = 'stopping';
                    this.downloadList[index].progress = 0;
                    this.addLog(`正在停止下载模型 ${item.modelId}...`, 'warning');
                }
            } catch (error) {
                this.addLog(`停止下载失败: ${error.message}`, 'error');
            }
        },

        // 显示配置弹窗
        async showConfig(model) {
            console.log('showConfig called with model:', model);
            if (!this.auth.isAuthenticated) {
                console.log('User not authenticated, showing login modal');
                this.$refs.loginModal.show();
                this.addLog('请先登录后再加载配置', 'warning');
                return;
            }

            this.selectedModel = model;
            try {
                console.log('Loading config for model:', model.id);
                // 使用新的配置模态框组件
                const configModal = this.$refs.configModal;
                console.log('configModal ref:', configModal);
                if (configModal && configModal.$ && configModal.$.ctx && typeof configModal.$.ctx.show === 'function') {
                    configModal.$.ctx.show(model, this.stats);
                } else {
                    console.error('configModal ref (no show):', configModal);
                    this.addLog('无法显示配置：组件未正确暴露 show 方法', 'error');
                }
            } catch (e) {
                console.error('Error in showConfig:', e);
                this.addLog(`加载配置失败: ${e.message}`, 'error');
            }
        },

        // 滚动到底部
        scrollToBottom() {
            this.$nextTick(() => {
                const el = document.querySelector('.console');
                if (el) el.scrollTop = el.scrollHeight;
            });
        },

        /* ========== 前缀颜色生成方法 ========== */
        // 生成基于字符串的稳定哈希颜色
        stringToColor(str) {
            let hash = 0;
            for (let i = 0; i < str.length; i++) {
                hash = str.charCodeAt(i) + ((hash << 5) - hash);
            }

            // 生成柔和色调 (HSL颜色空间)
            const h = Math.abs(hash) % 360;
            const s = 70 + Math.abs(hash) % 15; // 饱和度 70-85%
            const l = 80 + Math.abs(hash) % 10; // 亮度 80-90%

            return `hsl(${h}, ${s}%, ${l}%)`;
        },

        // 计算对比文本颜色（确保可读性）
        getContrastTextColor(bgColor) {
            const match = bgColor.match(/hsl\((\d+),\s*(\d+)%,\s*(\d+)%/);
            if (match) {
                const lightness = parseInt(match[3]);
                return lightness > 85 ? '#333' : '#fff';
            }
            return '#333';
        },

        // 格式化模型名称
        formatModelName(modelId) {
            const parts = modelId.split('/');
            if (parts.length > 1) {
                const prefix = parts[0].toLowerCase(); // 忽略大小写
                let color;

                // 根据前缀设置固定颜色
                if (prefix === 'qwen') {
                    color = '#1e88e5'; // 蓝色
                } else if (prefix === 'deepseek-ai') {
                    color = '#43a047'; // 绿色
                } else {
                    // 其他前缀使用随机颜色
                    color = this.stringToColor(prefix);
                }

                const textColor = this.getContrastTextColor(color);

                return `<span class="model-name-prefix" style="color: ${color}; font-weight: bold;">${parts[0]}</span>/${parts.slice(1).join('/')}`;
            }

            // 没有前缀的情况
            const bgColor = this.stringToColor(modelId);
            const textColor = this.getContrastTextColor(bgColor);
            return `<span class="model-name-prefix" style="color: ${textColor};">${modelId}</span>`;
        },

        // 调整阴影颜色使更协调
        adjustShadowColor(bgColor) {
            return bgColor.replace(/\d+%\)/, '30%)');
        },

        // 复制到剪贴板
        copyToClipboard(text) {
            try {
                // 如果传入的是模型对象，使用其id属性
                const textToCopy = typeof text === 'object' ? text.id : text;

                // 创建临时文本区域
                const textArea = document.createElement('textarea');
                textArea.value = textToCopy;
                textArea.style.position = 'fixed';
                textArea.style.left = '-999999px';
                textArea.style.top = '-999999px';
                document.body.appendChild(textArea);

                // 选择并复制文本
                textArea.focus();
                textArea.select();
                document.execCommand('copy');

                // 清理
                document.body.removeChild(textArea);

                this.addLog('已复制到剪贴板', 'success');
            } catch (err) {
                this.addLog('复制失败: ' + err.message, 'error');
            }
        },

        async updateVersion() {
            if (!this.newVersionContent.trim()) {
                this.addLog('请输入版本更新内容', 'warning');
                return;
            }

            try {
                const res = await fetch('/api/update_version', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': 'Bearer ' + this.auth.token
                    },
                    body: JSON.stringify({
                        change_content: this.newVersionContent
                    })
                });

                if (!res.ok) {
                    throw new Error(await res.text());
                }

                const data = await res.json();
                this.version = data.version;
                this.addLog(`版本已更新到 ${data.version}`, 'success');
                this.newVersionContent = '';
                this.showVersionModal = false;

                // 重新加载版本信息
                await this.loadVersionInfo();

                // 保存当前版本号到localStorage
                localStorage.setItem('lastVersion', this.version);

            } catch (e) {
                this.addLog(`更新版本失败: ${e.message}`, 'error');
            }
        },

        async loadVersionInfo() {
            try {
                const res = await fetch('/api/version');
                const data = await res.json();
                this.version = data.version;
                // 确保按版本号降序排序
                this.versionLogs = data.changelog.sort((a, b) => parseFloat(b.version) - parseFloat(a.version));
            } catch (e) {
                console.error('加载版本信息失败:', e);
                this.addLog('加载版本信息失败', 'error');
            }
        },

        // 获取模型状态显示
        getModelStatus(model) {
            if (model.running) {
                return '<span class="badge bg-success">运行中</span>';
            } else {
                return '<span class="badge bg-secondary">已停止</span>';
            }
        },

        // 获取下载状态显示
        getDownloadStatus(status) {
            return this.statusMap[status] || status;
        },

        // 获取下载状态样式
        getDownloadStatusClass(status) {
            const statusClassMap = {
                'downloading': 'bg-primary',
                'stopped': 'bg-secondary',
                'stopping': 'bg-warning',
                'success': 'bg-success',
                'error': 'bg-danger'
            };
            return statusClassMap[status] || 'bg-secondary';
        },

        // 确认启动模型
        confirmStartModel(model) {
            if (!this.auth.isAuthenticated) {
                this.$refs.loginModal.show();
                this.addLog('请先登录后再运行模型', 'warning');
                return;
            }
            // 直接显示配置界面
            this.showConfig(model);
        },

        // 添加禁用服务函数
        async disableService(model) {
            if (!confirm(`确定要禁用模型 ${model.id} 的服务吗？此操作将停止并禁用该模型的服务。`)) {
                return;
            }

            try {
                const response = await fetch('/api/disable_service', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${this.auth.token}`
                    },
                    body: JSON.stringify({ modelId: model.id })
                });

                const result = await response.json();
                if (result.status === 'success') {
                    this.addLog('服务已禁用', 'success');
                    this.loadModels();
                } else {
                    this.addLog(result.message || '禁用服务失败', 'error');
                }
            } catch (error) {
                console.error('禁用服务失败:', error);
                this.addLog('禁用服务失败', 'error');
            }
        },

        // 添加过滤AI导航的方法
        filterAINavigation() {
            // 这个方法会在输入时自动触发，因为我们使用了计算属性
            // 不需要额外的实现，但保留这个方法以保持与模板的一致性
        },

        onLoginSuccess({ username, token }) {
            this.auth.username = username;
            this.auth.isAuthenticated = true;
            this.auth.token = token || 'sso';
            this.addLog('登录成功', 'success');
            this.closeWebSocket();
            this.initWebSocket();
            this.loadModels();
        },

        async ssoAutoLogin() {
            try {
                const resp = await fetch('/api/yz/user', { credentials: 'same-origin' });
                const data = await resp.json();
                if (data.ok) {
                    this.auth.username = data.display_name || data.username;
                    this.auth.isAuthenticated = true;
                    this.auth.token = 'sso';
                    if (window.location.search.indexOf('sso=1') !== -1) {
                        window.history.replaceState(null, '', window.location.pathname);
                    }
                }
            } catch (e) {
                console.log('SSO 检测:', e);
            }
        },

        async loadWebsiteSettings() {
            try {
                // 仅在管理员页面调用 /api/admin/website-settings
                if (window.location.pathname.includes('/admin')) {
                    const response = await fetch('/api/admin/website-settings');
                    if (response.ok) {
                        const data = await response.json();
                        this.websiteSettings = { ...this.websiteSettings, ...data };
                    } else {
                        console.warn('加载网站设置失败，使用默认设置');
                    }
                } else {
                    // 非管理员页面使用默认设置
                    console.log('非管理员页面，使用默认网站设置');
                }
            } catch (error) {
                console.error('加载网站设置失败:', error);
            }
        },
    }
});

// 注册 system-monitor 组件
initSystemMonitor(app);

// 在创建应用实例后，但在挂载之前初始化组件
initVersionHistory(app);
initReadmeModal(app);
initConfigModal(app);

// 等待 DOM 加载完成后再挂载应用
document.addEventListener('DOMContentLoaded', () => {
    app.mount('#app');
});
