import { logger } from './components/logger.js';
import { initVersionHistory } from './components/versionHistory.js';

// 创建Vue应用实例
const app = Vue.createApp({
    delimiters: ['[[', ']]'],
    data() {
        return {
            currentTab: 'users',
            users: [],
            navigation: [],
            servers: [],
            systemLogs: [],
            apiKeys: [],
            modelDbList: [],
            websiteSettings: {
                title: '',
                theme: 'light',
                description: '',
                keywords: '',
                icp_number: '',
                analytics_code: '',
                primary_color: '#3a7bd5',
                secondary_color: '#00d2ff'
            },
            showUserModal: false,
            showNavModal: false,
            showServerModal: false,
            showApiModal: false,
            showEditModelDbModal: false,
            selectedModelIds: new Set(),
            userForm: {
                id: '',
                username: '',
                password: '',
                role: 'user'
            },
            navForm: {
                id: '',
                name: '',
                url: '',
                description: '',
                icon: '',
                tags: '',
                sort_order: 0
            },
            serverForm: {
                id: '',
                name: '',
                ip: '',
                port: '',
                status: 'online'
            },
            editModelDbForm: {
                model_id: '',
                ip: '',
                run_mode: '',
                pid: '',
                status: ''
            },
            isEditingUser: false,
            isEditingNav: false,
            isEditingServer: false,
            serverPingInterval: null,
            auth: {
                username: '',
                password: '',
                isAuthenticated: false,
                token: ''
            },
            showLoginModal: false,
            loginError: '',
            isAdmin: false,
            version: '0.42'
        }
    },

    computed: {
        sortedNavigation() {
            return [...this.navigation].sort((a, b) => a.sort_order - b.sort_order);
        },
        groupedModelDbList() {
            const groups = {};
            this.modelDbList.forEach(item => {
                if (!groups[item.ip]) {
                    groups[item.ip] = [];
                }
                groups[item.ip].push(item);
            });
            return groups;
        }
    },

    mounted() {
        const path = window.location.pathname;
        if (path.startsWith('/admin')) {
            // SSO 模式下通过 cookie 验证权限
            fetch('/api/yz/user', { credentials: 'same-origin' })
                .then(r => r.json())
                .then(data => {
                    if (data.ok) {
                        this.auth.username = data.display_name || data.username;
                        this.auth.isAuthenticated = true;
                        this.auth.token = 'sso';
                    }
                })
                .catch(() => {});
        }
    },

    beforeUnmount() {
        if (this.serverPingInterval) {
            clearInterval(this.serverPingInterval);
        }
    },

    methods: {
        

        // 初始化方法
        async init() {
            if (!this.auth.isAuthenticated || !this.isAdmin || !this.auth.token) {
                return;
            }

            try {
                await Promise.all([
                    this.fetchUsers(),
                    this.fetchNavigation(),
                    this.fetchServers(),
                    this.fetchSystemLogs(),
                    this.fetchApiKeys(),
                    this.loadWebsiteSettings(),
                    this.loadModelDbList()
                ]);
            } catch (error) {
                console.error('初始化失败:', error);
                if (error.status === 401) {
                    this.showLoginModal = true;
                }
            }
        },

        // 获取用户列表
        async fetchUsers() {
            try {
                const response = await fetch('/api/admin/users', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.users = await response.json();
                }
            } catch (error) {
                console.error('获取用户列表失败:', error);
            }
        },

        // 获取导航列表
        async fetchNavigation() {
            try {
                const response = await fetch('/api/admin/navigation', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.navigation = await response.json();
                }
            } catch (error) {
                console.error('获取导航列表失败:', error);
            }
        },

        // 获取服务器列表
        async fetchServers() {
            try {
                const response = await fetch('/api/admin/servers', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.servers = await response.json();
                }
            } catch (error) {
                console.error('获取服务器列表失败:', error);
            }
        },

        // 获取系统日志
        async fetchSystemLogs() {
            try {
                const response = await fetch('/api/admin/logs', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.systemLogs = await response.json();
                }
            } catch (error) {
                console.error('获取系统日志失败:', error);
            }
        },

        // 获取API密钥列表
        async fetchApiKeys() {
            try {
                const response = await fetch('/api/admin/api-keys', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.apiKeys = await response.json();
                }
            } catch (error) {
                console.error('获取API密钥列表失败:', error);
            }
        },

        // 加载网站设置
        async loadWebsiteSettings() {
            try {
                const response = await fetch('/api/admin/website-settings', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.websiteSettings = await response.json();
                }
            } catch (error) {
                console.error('加载网站设置失败:', error);
            }
        },

        // SSO 模式下通过 cookie 验证
        async validateToken() {
            try {
                const resp = await fetch('/api/yz/user', { credentials: 'same-origin' });
                const data = await resp.json();
                if (data.ok) {
                    this.auth.isAuthenticated = true;
                    this.auth.username = data.display_name || data.username;
                    this.auth.token = 'sso';
                    this.isAdmin = data.is_admin === 1;
                    return;
                }
            } catch (error) {
                console.error('SSO 验证失败:', error);
            }
            this.showLoginModal = true;
        },

        // 切换标签页
        async switchTab(tab) {
            this.currentTab = tab;
            if (tab === 'servers') {
                await this.loadData();
                await this.pingServers();
                if (this.serverPingInterval) clearInterval(this.serverPingInterval);
                this.serverPingInterval = setInterval(this.pingServers, 5000);
            } else {
                if (this.serverPingInterval) {
                    clearInterval(this.serverPingInterval);
                    this.serverPingInterval = null;
                }
            }
        },

        // 格式化日期
        formatDate(date) {
            if (!date) return '';
            return new Date(date).toLocaleString();
        },

        // 登录成功回调
        onLoginSuccess({ username, token, is_admin }) {
            this.auth.username = username;
            this.auth.token = token;
            this.auth.isAuthenticated = true;
            this.isAdmin = is_admin || false;
            localStorage.setItem('authToken', token);
            this.showLoginModal = false;

            const urlParams = new URLSearchParams(window.location.search);
            const redirect = urlParams.get('redirect');
            if (redirect) {
                window.location.href = redirect;
            } else {
                this.init();
            }
        },

        // 切换API密钥显示
        toggleApiKey(api) {
            const input = event.target.closest('.input-group').querySelector('input');
            input.type = input.type === 'password' ? 'text' : 'password';
        },

        // 复制API密钥
        async copyApiKey(api) {
            try {
                await navigator.clipboard.writeText(api.key);
                alert('API密钥已复制到剪贴板');
            } catch (error) {
                console.error('复制失败:', error);
            }
        },

        // 切换选择所有模型
        toggleSelectAll(ip) {
            const group = this.groupedModelDbList[ip] || [];
            const allSelected = group.every(item => this.selectedModelIds.has(item.model_id));
            
            if (allSelected) {
                group.forEach(item => this.selectedModelIds.delete(item.model_id));
            } else {
                group.forEach(item => this.selectedModelIds.add(item.model_id));
            }
        },

        // 切换选择单个模型
        toggleSelectOne(item) {
            if (this.selectedModelIds.has(item.model_id)) {
                this.selectedModelIds.delete(item.model_id);
            } else {
                this.selectedModelIds.add(item.model_id);
            }
        },

        // 退出登录
        async logout() {
            try {
                localStorage.removeItem('authToken');
                document.cookie = 'taf_sso_token=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;';
                this.auth.isAuthenticated = false;
                this.auth.username = '';
                this.auth.token = '';
                this.isAdmin = false;
                window.location.href = '/api/yz/logout';
            } catch (error) {
                console.error('退出登录失败:', error);
                window.location.href = '/';
            }
        },
        
        // Ping服务器状态
        async pingServers() {
            const pingPromises = this.servers.map(async (server) => {
                try {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), 2000);
                    
                    const res = await fetch(`http://${server.ip}:${server.port}/api/ping`, {
                        signal: controller.signal
                    });
                    
                    clearTimeout(timeoutId);
                    const data = await res.json();
                    server.online = data.status === 'ok';
                } catch (e) {
                    server.online = false;
                }
            });
            await Promise.all(pingPromises);
        },

        // 加载数据
        async loadData() {
            try {
                const [users, navigation, servers] = await Promise.all([
                    this.fetchUsers(),
                    this.fetchNavigation(),
                    this.fetchServers()
                ]);
            } catch (error) {
                console.error('加载数据失败:', error);
            }
        },

        // 认证请求封装
        async makeAuthenticatedRequest(url, method, body = null) {
            const headers = {
                'Authorization': `Bearer ${this.auth.token}`,
                'Content-Type': 'application/json'
            };
            
            const config = {
                method: method,
                headers: headers
            };
            
            if (body) {
                config.body = JSON.stringify(body);
            }
            
            const response = await fetch(url, config);
            
            if (!response.ok) {
                const error = new Error(`请求失败: ${response.statusText}`);
                error.status = response.status;
                throw error;
            }
            
            return response.json();
        },

        // 打开添加用户模态框
        openAddUserModal() {
            this.isEditingUser = false;
            this.userForm = {
                id: '',
                username: '',
                password: '',
                role: 'user'
            };
            this.showUserModal = true;
        },

        closeUserModal() {
            this.showUserModal = false;
            this.userForm = {
                id: '',
                username: '',
                password: '',
                role: 'user'
            };
        },

        // 打开编辑用户模态框
        openEditUserModal(user) {
            this.isEditingUser = true;
            this.userForm = { ...user, password: '' };
            this.showUserModal = true;
        },

        // 提交用户表单
        async submitUserForm() {
            if (this.isEditingUser) {
                await this.updateUser();
            } else {
                await this.addUser();
            }
        },

        // 添加用户
        async addUser() {
            try {
                await this.makeAuthenticatedRequest('/api/admin/users', 'POST', {
                    username: this.userForm.username,
                    password: this.userForm.password,
                    role: this.userForm.role
                });
                
                this.showUserModal = false;
                await this.fetchUsers();
            } catch (error) {
                console.error('添加用户失败:', error);
                alert(`添加用户失败: ${error.message || '未知错误'}`);
            }
        },

        // 更新用户
        async updateUser() {
            try {
                const updateData = {
                    username: this.userForm.username,
                    role: this.userForm.role
                };
                
                if (this.userForm.password) {
                    updateData.password = this.userForm.password;
                }
                
                await this.makeAuthenticatedRequest(`/api/admin/users/${this.userForm.id}`, 'PUT', updateData);
                this.showUserModal = false;
                await this.fetchUsers();
            } catch (error) {
                console.error('更新用户失败:', error);
                alert(`更新用户失败: ${error.message || '未知错误'}`);
            }
        },

        // 确认删除用户
        async confirmDeleteUser(user) {
            if (confirm(`确定要删除用户 ${user.username} 吗？`)) {
                try {
                    await this.makeAuthenticatedRequest(`/api/admin/users/${user.id}`, 'DELETE');
                    await this.fetchUsers();
                } catch (error) {
                    console.error('删除用户失败:', error);
                    alert(`删除用户失败: ${error.message || '未知错误'}`);
                }
            }
        },

        // 打开添加导航模态框
        openAddNavModal() {
            this.isEditingNav = false;
            this.navForm = {
                name: '',
                url: '',
                description: '',
                icon: '',
                tags: '',
                sort_order: this.navigation.length
            };
            this.showNavModal = true;
        },

        // 打开编辑导航模态框
        openEditNavModal(nav) {
            this.isEditingNav = true;
            this.navForm = {
                ...nav,
                tags: Array.isArray(nav.tags) ? nav.tags.join(', ') : nav.tags
            };
            this.showNavModal = true;
        },

        // 提交导航表单
        async submitNavForm() {
            if (this.isEditingNav) {
                await this.updateNav();
            } else {
                await this.addNav();
            }
        },

        // 添加导航
        async addNav() {
            try {
                const tags = typeof this.navForm.tags === 'string' ? 
                    this.navForm.tags.split(',').map(tag => tag.trim()).filter(tag => tag) : 
                    this.navForm.tags;
                
                await this.makeAuthenticatedRequest('/api/admin/navigation', 'POST', {
                    name: this.navForm.name,
                    url: this.navForm.url,
                    description: this.navForm.description,
                    icon: this.navForm.icon,
                    tags: tags,
                    sort_order: this.navForm.sort_order
                });
                
                this.showNavModal = false;
                await this.fetchNavigation();
            } catch (error) {
                console.error('添加导航失败:', error);
                alert(`添加导航失败: ${error.message || '未知错误'}`);
            }
        },

        // 更新导航
        async updateNav() {
            try {
                const tags = typeof this.navForm.tags === 'string' ? 
                    this.navForm.tags.split(',').map(tag => tag.trim()).filter(tag => tag) : 
                    this.navForm.tags;
                
                await this.makeAuthenticatedRequest(`/api/admin/navigation/${this.navForm.id}`, 'PUT', {
                    name: this.navForm.name,
                    url: this.navForm.url,
                    description: this.navForm.description,
                    icon: this.navForm.icon,
                    tags: tags,
                    sort_order: this.navForm.sort_order
                });
                
                this.showNavModal = false;
                await this.fetchNavigation();
            } catch (error) {
                console.error('更新导航失败:', error);
                alert(`更新导航失败: ${error.message || '未知错误'}`);
            }
        },

        // 确认删除导航
        async confirmDeleteNav(nav) {
            if (confirm(`确定要删除导航 ${nav.name} 吗？`)) {
                try {
                    await this.makeAuthenticatedRequest(`/api/admin/navigation/${nav.id}`, 'DELETE');
                    await this.fetchNavigation();
                } catch (error) {
                    console.error('删除导航失败:', error);
                    alert(`删除导航失败: ${error.message || '未知错误'}`);
                }
            }
        },

        // 导航排序
        async moveNav(nav, direction) {
            const index = this.navigation.findIndex(n => n.id === nav.id);
            let newOrder = nav.sort_order;
            
            if (direction === 'up' && index > 0) {
                newOrder = this.navigation[index - 1].sort_order;
                this.navigation[index - 1].sort_order = nav.sort_order;
            } else if (direction === 'down' && index < this.navigation.length - 1) {
                newOrder = this.navigation[index + 1].sort_order;
                this.navigation[index + 1].sort_order = nav.sort_order;
            } else {
                return;
            }
            
            try {
                await this.makeAuthenticatedRequest(`/api/admin/navigation/${nav.id}/sort`, 'PUT', {
                    sort_order: newOrder
                });
                await this.fetchNavigation();
            } catch (error) {
                console.error('更新排序失败:', error);
                alert(`更新排序失败: ${error.message || '未知错误'}`);
            }
        },

        // 打开添加服务器模态框
        openAddServerModal() {
            this.isEditingServer = false;
            this.serverForm = { id: '', name: '', ip: '', port: '', status: 'online' };
            this.showServerModal = true;
        },

        // 打开编辑服务器模态框
        openEditServerModal(server) {
            this.isEditingServer = true;
            this.serverForm = { ...server };
            this.showServerModal = true;
        },

        // 提交服务器表单
        async submitServerForm() {
            if (this.isEditingServer) {
                await this.updateServer();
            } else {
                await this.addServer();
            }
        },

        // 添加服务器
        async addServer() {
            try {
                await this.makeAuthenticatedRequest('/api/admin/servers', 'POST', {
                    name: this.serverForm.name,
                    ip: this.serverForm.ip,
                    port: this.serverForm.port,
                    status: this.serverForm.status
                });
                
                this.showServerModal = false;
                await this.fetchServers();
            } catch (error) {
                console.error('添加服务器失败:', error);
                alert(`添加服务器失败: ${error.message || '未知错误'}`);
            }
        },

        // 更新服务器
        async updateServer() {
            try {
                await this.makeAuthenticatedRequest(`/api/admin/servers/${this.serverForm.id}`, 'PUT', {
                    name: this.serverForm.name,
                    ip: this.serverForm.ip,
                    port: this.serverForm.port,
                    status: this.serverForm.status
                });
                
                this.showServerModal = false;
                await this.fetchServers();
            } catch (error) {
                console.error('更新服务器失败:', error);
                alert(`更新服务器失败: ${error.message || '未知错误'}`);
            }
        },

        // 确认删除服务器
        async confirmDeleteServer(server) {
            if (confirm(`确定要删除服务器 ${server.name} 吗？`)) {
                try {
                    await this.makeAuthenticatedRequest(`/api/admin/servers/${server.id}`, 'DELETE');
                    await this.fetchServers();
                } catch (error) {
                    console.error('删除服务器失败:', error);
                    alert(`删除服务器失败: ${error.message || '未知错误'}`);
                }
            }
        },

        // 获取服务器状态类名
        getServerStatusClass(status) {
            return {
                'online': 'server-status-online',
                'offline': 'server-status-offline',
                'maintenance': 'server-status-maintenance'
            }[status] || 'server-status-unknown';
        },

        // 加载模型数据库列表
        async loadModelDbList() {
            try {
                const response = await fetch('/api/admin/model-db-list', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    this.modelDbList = await response.json();
                }
            } catch (error) {
                console.error('加载模型数据库失败:', error);
                this.modelDbList = [];
            }
        },

        // 打开编辑模型数据库模态框
        openEditModelDbModal(item) {
            this.editModelDbForm = { ...item };
            this.showEditModelDbModal = true;
        },

        // 保存模型数据库编辑
        async saveEditModelDb() {
            try {
                await this.makeAuthenticatedRequest('/api/admin/model-db-update', 'PUT', this.editModelDbForm);
                this.showEditModelDbModal = false;
                await this.loadModelDbList();
            } catch (error) {
                console.error('更新模型数据库失败:', error);
                alert(`更新模型数据库失败: ${error.message || '未知错误'}`);
            }
        },

        // 确认删除模型数据库
        async confirmDeleteModelDb(item) {
            if (confirm('确定要删除该模型记录吗？')) {
                try {
                    // 将 DELETE 改为 POST
                    await this.makeAuthenticatedRequest(
                        '/api/admin/model-db-delete', 
                        'POST',  // 修改为 POST
                        { model_id: item.model_id }
                    );
                    await this.loadModelDbList();
                } catch (error) {
                    console.error('删除模型数据库失败:', error);
                    alert(`删除模型数据库失败: ${error.message || '未知错误'}`);
                }
            }
        },

        // 批量删除相同IP的模型
        async batchDeleteByIp(ip) {
            const group = this.groupedModelDbList[ip] || [];
            const toDelete = group.filter(item => this.selectedModelIds.has(item.model_id));
            
            if (toDelete.length === 0) {
                alert('请至少选择一个模型');
                return;
            }
            
            if (!confirm(`确定要删除IP为 ${ip} 的 ${toDelete.length} 个模型记录吗？`)) {
                return;
            }
            
            try {
                const deletePromises = toDelete.map(item => 
                    this.makeAuthenticatedRequest('/api/admin/model-db-delete', 'DELETE', { model_id: item.model_id })
                );
                
                await Promise.all(deletePromises);
                await this.loadModelDbList();
                this.selectedModelIds.clear();
            } catch (error) {
                console.error('批量删除模型失败:', error);
                alert(`批量删除模型失败: ${error.message || '未知错误'}`);
            }
        },

        // 保存网站设置
        async saveWebsiteSettings() {
            try {
                await this.makeAuthenticatedRequest('/api/admin/website-settings', 'PUT', this.websiteSettings);
                alert('网站设置已保存');
            } catch (error) {
                console.error('保存网站设置失败:', error);
                alert(`保存网站设置失败: ${error.message || '未知错误'}`);
            }
        },

        // 处理Logo上传
        async handleLogoUpload(event) {
            const file = event.target.files[0];
            if (!file) return;
            
            try {
                const formData = new FormData();
                formData.append('logo', file);
                
                const response = await fetch('/api/admin/upload-logo', {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    },
                    body: formData
                });
                
                if (!response.ok) {
                    throw new Error('上传失败');
                }
                
                const result = await response.json();
                alert('Logo上传成功');
                this.websiteSettings.logo_url = result.url;
            } catch (error) {
                console.error('上传Logo失败:', error);
                alert(`上传Logo失败: ${error.message || '未知错误'}`);
            }
        },

        // 获取日志样式类
        getLogClass(type) {
            if (!type) return 'log-info';
            
            const classes = {
                'info': 'log-info',
                'success': 'log-success',
                'warning': 'log-warning',
                'error': 'log-error'
            };
            return classes[type.toLowerCase()] || 'log-info';
        },

        // 添加日志
        addLog(message, type = 'info') {
            logger.addLog(message, type, this);
        },

        // 导出日志
        async exportLogs() {
            try {
                const csvContent = logger.exportLogs(this.systemLogs);
                const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                const link = document.createElement('a');
                link.href = URL.createObjectURL(blob);
                link.download = `system_logs_${new Date().toISOString().slice(0,10)}.csv`;
                link.click();
            } catch (error) {
                console.error('导出日志失败:', error);
                this.addLog('导出日志失败: ' + error.message, 'error');
            }
        },

        // 刷新日志
        async refreshLogs() {
            try {
                const response = await fetch('/api/admin/logs', {
                    headers: {
                        'Authorization': `Bearer ${this.auth.token}`
                    }
                });
                if (response.ok) {
                    const logs = await response.json();
                    // 转换日志格式
                    this.systemLogs = logs.map(log => ({
                        timestamp: new Date(log.created_at).toLocaleString(),
                        type: log.level.toLowerCase(),
                        message: log.message
                    }));
                }
            } catch (error) {
                console.error('刷新日志失败:', error);
                this.addLog('刷新日志失败: ' + error.message, 'error');
            }
        }
    }
});

// 初始化版本历史组件
initVersionHistory(app);

// 挂载应用
app.mount('#app');
