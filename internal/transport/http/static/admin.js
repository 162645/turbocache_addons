document.addEventListener('DOMContentLoaded', () => {
    // 初始化图表
    const ctx = document.getElementById('metricsChart').getContext('2d');
    const metricsChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'QPS',
                data: [],
                borderColor: '#38bdf8',
                backgroundColor: 'rgba(56, 189, 248, 0.1)',
                tension: 0.4,
                fill: true
            }, {
                label: '命中率 (%)',
                data: [],
                borderColor: '#10b981',
                backgroundColor: 'rgba(16, 185, 129, 0.1)',
                tension: 0.4,
                fill: true,
                yAxisID: 'percentage'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: { color: '#94a3b8' }
                },
                percentage: {
                    position: 'right',
                    beginAtZero: true,
                    max: 100,
                    grid: { display: false },
                    ticks: { color: '#94a3b8' }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#94a3b8' }
                }
            },
            plugins: {
                legend: {
                    labels: { color: '#f8fafc' }
                }
            }
        }
    });

    // 数据轮询函数
    async function updateDashboard() {
        try {
            const statsResponse = await fetch('/stats');
            const stats = await statsResponse.json();

            // 更新卡片数据
            document.getElementById('total-requests').textContent = stats.total_requests || 0;
            document.getElementById('hit-rate').textContent = ((stats.overall_hit_rate || 0) * 100).toFixed(1) + '%';

            const l1Size = (stats.l1_stats?.total_size || 0) / (1024 * 1024);
            const l2Size = (stats.l2_stats?.total_size || 0) / (1024 * 1024);
            document.getElementById('l1-size').textContent = l1Size.toFixed(2) + ' MB';
            document.getElementById('l2-size').textContent = l2Size.toFixed(2) + ' MB';

            // 更新图表
            const now = new Date().toLocaleTimeString();
            if (metricsChart.data.labels.length > 20) {
                metricsChart.data.labels.shift();
                metricsChart.data.datasets[0].data.shift();
                metricsChart.data.datasets[1].data.shift();
            }

            metricsChart.data.labels.push(now);
            // 简单的 QPS 计算模拟（增量统计）
            const qps = stats.total_requests - (window.lastTotalRequests || stats.total_requests);
            window.lastTotalRequests = stats.total_requests;

            metricsChart.data.datasets[0].data.push(qps);
            metricsChart.data.datasets[1].data.push((stats.overall_hit_rate || 0) * 100);
            metricsChart.update('none');

        } catch (error) {
            console.error('Failed to fetch stats:', error);
        }
    }

    // 轮询节点列表
    async function updateNodes() {
        try {
            // 注意：暂时假设 API 返回 health 状态
            const healthResponse = await fetch('/health');
            const health = await healthResponse.json();

            const nodeContainer = document.getElementById('node-list');
            nodeContainer.innerHTML = ''; // 清空

            // 模拟当前节点显示
            const li = document.createElement('li');
            li.className = 'node-item';
            li.innerHTML = `
                <div class="node-info">
                    <span class="name">Node 1 (Current)</span>
                    <span class="addr">${window.location.host}</span>
                </div>
                <div class="node-status-dot"></div>
            `;
            nodeContainer.appendChild(li);

        } catch (error) {
            console.error('Failed to fetch health/nodes:', error);
        }
    }

    // 初始调用
    updateDashboard();
    updateNodes();

    // 定时轮询
    setInterval(updateDashboard, 2000);
    setInterval(updateNodes, 10000);
});
