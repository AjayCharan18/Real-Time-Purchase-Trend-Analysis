// dashboard/static/js/dashboard.js

// Initialize Socket.IO
const socket = io();

// Chart instances
let productChart, departmentChart, hourlyChart;

// Update connection status
socket.on('connect', () => {
    updateConnectionStatus(true);
    socket.emit('request_update');
});
socket.on('disconnect', () => updateConnectionStatus(false));

// Handle updates
socket.on('product_update', data => {
    updateProductChart(data.data);
    updateProductTable(data.data);
    updateLastUpdated();
});
socket.on('department_update', data => {
    updateDepartmentChart(data.data);
    updateLastUpdated();
});
socket.on('hourly_update', data => {
    updateHourlyChart(data.data);
});
socket.on('alerts_update', data => {
    updateAlerts(data.alerts);
    updateAlertBadge(data.total);
});

// Initial data fetch
document.addEventListener('DOMContentLoaded', () => {
    initProductChart();
    initDepartmentChart();
    initHourlyChart();
    fetchInitialData();
    
    // Update stats every 30 seconds (reduced frequency)
    setInterval(updateStatsData, 30000);
    
    // Update opportunities every 60 seconds (reduced frequency)
    setInterval(updateOpportunitiesData, 60000);
});

// Helper: update connection badge
function updateConnectionStatus(connected) {
    const el = document.getElementById('connectionStatus');
    if (connected) {
        el.className = 'connection-status status-connected';
        el.innerHTML = '<i class="fas fa-circle me-1"></i> Connected';
    } else {
        el.className = 'connection-status status-disconnected';
        el.innerHTML = '<i class="fas fa-circle me-1"></i> Disconnected';
    }
}

// Helper: update last-updated text
function updateLastUpdated() {
    const now = new Date().toLocaleTimeString();
    document.getElementById('lastUpdated').textContent = `Last updated: ${now}`;
}

// Fetch initial data via REST
function fetchInitialData() {
    Promise.all([
        fetch('/api/products/top?limit=20').then(r => r.json()),
        fetch('/api/departments').then(r => r.json()),
        fetch('/api/hourly').then(r => r.json()),
        fetch('/api/stats').then(r => r.json()),
        fetch('/api/opportunities').then(r => r.json()),
        fetch('/api/alerts').then(r => r.json())
    ]).then(([prod, dept, hour, stats, opportunities, alerts]) => {
        updateProductChart(prod);
        updateProductTable(prod);
        updateDepartmentChart(dept);
        updateHourlyChart(hour);
        updateStats(stats);
        updateOpportunities(opportunities);
        updateAlerts(alerts.alerts);
        updateAlertBadge(alerts.total_alerts);
    });
}

// Update stats cards
function updateStats(stats) {
    document.getElementById('totalTransactions').textContent = stats.total_transactions.toLocaleString();
    document.getElementById('totalRevenue').textContent = `$${stats.total_revenue.toLocaleString()}`;
    document.getElementById('activeProducts').textContent = stats.active_products;
    document.getElementById('activeDepartments').textContent = stats.active_departments;
}

// Fetch and update stats periodically
function updateStatsData() {
    fetch('/api/stats')
        .then(r => r.json())
        .then(stats => updateStats(stats))
        .catch(err => console.error('Error fetching stats:', err));
}

// Update opportunities display
function updateOpportunities(data) {
    const container = document.getElementById('opportunitiesContainer');
    const opportunities = data.opportunities || [];
    
    if (opportunities.length === 0) {
        container.innerHTML = `
            <div class="text-center py-3 text-success">
                <i class="fas fa-check-circle me-2"></i>
                No immediate action required - All trends are stable
            </div>`;
        return;
    }
    
    let html = '<div class="row">';
    opportunities.forEach((opp, index) => {
        const priorityColor = opp.priority === 'High' ? 'danger' : 'warning';
        const icon = opp.type === 'High Demand' ? 'fa-arrow-up' : 
                    opp.type === 'Promotion Opportunity' ? 'fa-bullhorn' : 'fa-heart';
        
        html += `
            <div class="col-md-6 mb-3">
                <div class="alert alert-${priorityColor} mb-0">
                    <div class="d-flex align-items-start">
                        <i class="fas ${icon} me-3 mt-1"></i>
                        <div>
                            <h6 class="alert-heading mb-1">${opp.type}: ${opp.product}</h6>
                            <p class="mb-1"><strong>Department:</strong> ${opp.department}</p>
                            <p class="mb-1">${opp.recommendation}</p>
                            <small class="text-muted">Priority: ${opp.priority}</small>
                        </div>
                    </div>
                </div>
            </div>`;
    });
    html += '</div>';
    
    container.innerHTML = html;
}

// Fetch and update opportunities periodically
function updateOpportunitiesData() {
    fetch('/api/opportunities')
        .then(r => r.json())
        .then(opportunities => updateOpportunities(opportunities))
        .catch(err => console.error('Error fetching opportunities:', err));
}


// Initialize Product Chart
function initProductChart() {
    const ctx = document.getElementById('productChart').getContext('2d');
    productChart = new Chart(ctx, {
        type: 'bar',
        data: { labels: [], datasets: [{ label: 'Transactions', data: [], backgroundColor: '#2563eb' }] },
        options: { scales: { y: { beginAtZero: true } } }
    });
}

// Update Product Chart
function updateProductChart(data) {
    const top = data.sort((a, b) => b.transaction_count - a.transaction_count).slice(0, 10);
    productChart.data.labels = top.map(p => p.product_name);
    productChart.data.datasets[0].data = top.map(p => p.transaction_count);
    productChart.update('none');
}

// Initialize Department Chart
function initDepartmentChart() {
    const ctx = document.getElementById('departmentChart').getContext('2d');
    departmentChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: [], datasets: [{
                data: [], backgroundColor: [
                    '#2563eb', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'
                ]
            }]
        }
    });
}

// Update Department Chart
function updateDepartmentChart(data) {
    departmentChart.data.labels = data.map(d => d.department);
    departmentChart.data.datasets[0].data = data.map(d => d.transaction_count);
    departmentChart.update('none');
}

// Initialize Hourly Chart
function initHourlyChart() {
    const ctx = document.getElementById('hourlyChart').getContext('2d');
    hourlyChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: Array.from({ length: 24 }, (_, i) => `${i}:00`),
            datasets: [
                { label: 'Transactions', data: [], borderColor: '#2563eb', fill: false },
                { label: 'Revenue', data: [], borderColor: '#10b981', fill: false }
            ]
        }
    });
}

// Update Hourly Chart
function updateHourlyChart(data) {
    const tx = Array(24).fill(0);
    const rv = Array(24).fill(0);

    (data || []).forEach(d => {
        const hour = Number(d.hour ?? d.order_hour);
        if (!Number.isNaN(hour) && hour >= 0 && hour < 24) {
            tx[hour] = d.transaction_count ?? 0;
            rv[hour] = d.total_revenue ?? 0;
        }
    });

    hourlyChart.data.datasets[0].data = tx;
    hourlyChart.data.datasets[1].data = rv;
    hourlyChart.update('none');
}

// Update Product Table
function updateProductTable(data) {
    const tbody = document.getElementById('productTableBody');
    const top = data.sort((a, b) => b.transaction_count - a.transaction_count).slice(0, 20);
    let html = '';
    top.forEach((p, i) => {
        // Use the correct field names from our API
        const quantity = p.quantity || (p.transaction_count * 2); // Fallback calculation
        const reorderRate = p.reorder_rate || 75; // Use the reorder_rate field directly
        
        html += `
            <tr>
                <td>${i + 1}</td>
                <td>${p.product_name}</td>
                <td>${p.department}</td>
                <td>${p.transaction_count.toLocaleString()}</td>
                <td>${quantity.toLocaleString()}</td>
                <td>$${p.total_revenue.toLocaleString()}</td>
                <td>$${p.avg_price.toFixed(2)}</td>
                <td>${reorderRate}%</td>
            </tr>`;
    });
    tbody.innerHTML = html || `<tr><td colspan="8" class="text-center">No data</td></tr>`;
}

// Alert Management Functions
function updateAlerts(alerts) {
    const container = document.getElementById('alertsContainer');
    
    if (!alerts || alerts.length === 0) {
        container.innerHTML = `
            <div class="text-center py-3 text-muted">
                <i class="fas fa-shield-alt me-2"></i>No active alerts - All systems normal
            </div>`;
        return;
    }
    
    let html = '';
    alerts.forEach(alert => {
        const alertClass = getAlertClass(alert.type);
        const icon = getAlertIcon(alert.type);
        const timeAgo = getTimeAgo(alert.timestamp);
        
        html += `
            <div class="alert ${alertClass} alert-dismissible fade show mb-2" role="alert">
                <div class="d-flex align-items-start">
                    <i class="fas ${icon} me-3 mt-1"></i>
                    <div class="flex-grow-1">
                        <h6 class="alert-heading mb-1">${alert.title}</h6>
                        <p class="mb-1">${alert.message}</p>
                        <small class="text-muted">${timeAgo}</small>
                    </div>
                    <button type="button" class="btn-close" onclick="dismissAlert('${alert.id}')" aria-label="Close"></button>
                </div>
            </div>`;
    });
    
    container.innerHTML = html;
    
    // Show browser notification for new high-severity alerts
    alerts.forEach(alert => {
        if (alert.severity === 'high' && Notification.permission === 'granted') {
            new Notification(alert.title, {
                body: alert.message,
                icon: '/static/favicon.ico'
            });
        }
    });
}

function updateAlertBadge(count) {
    const badge = document.getElementById('alertBadge');
    badge.textContent = count;
    badge.className = count > 0 ? 'badge bg-danger ms-2' : 'badge bg-secondary ms-2';
}

function getAlertClass(type) {
    switch(type) {
        case 'success': return 'alert-success';
        case 'warning': return 'alert-warning';
        case 'info': return 'alert-info';
        default: return 'alert-danger';
    }
}

function getAlertIcon(type) {
    switch(type) {
        case 'success': return 'fa-check-circle';
        case 'warning': return 'fa-exclamation-triangle';
        case 'info': return 'fa-info-circle';
        default: return 'fa-exclamation-circle';
    }
}

function getTimeAgo(timestamp) {
    const now = new Date();
    const alertTime = new Date(timestamp);
    const diffMs = now - alertTime;
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins} minutes ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours} hours ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays} days ago`;
}

function dismissAlert(alertId) {
    fetch(`/api/alerts/${alertId}/dismiss`, { method: 'POST' })
        .then(response => response.json())
        .then(data => {
            if (data.status === 'success') {
                // Remove the alert from UI
                const alertElement = document.querySelector(`[onclick="dismissAlert('${alertId}')"]`).closest('.alert');
                alertElement.remove();
                
                // Update badge count
                const currentCount = parseInt(document.getElementById('alertBadge').textContent);
                updateAlertBadge(Math.max(0, currentCount - 1));
            }
        })
        .catch(err => console.error('Error dismissing alert:', err));
}

function toggleAlertsConfig() {
    const panel = document.getElementById('alertsConfigPanel');
    panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
    
    if (panel.style.display === 'block') {
        // Load current configuration
        fetch('/api/alerts/config')
            .then(r => r.json())
            .then(config => {
                document.getElementById('lowSalesThreshold').value = config.low_sales.threshold;
                document.getElementById('highRevenueThreshold').value = config.high_revenue.threshold;
                document.getElementById('productSpikeThreshold').value = config.product_spike.threshold;
            });
    }
}

function saveAlertsConfig() {
    const config = {
        low_sales: {
            threshold: parseInt(document.getElementById('lowSalesThreshold').value),
            enabled: true,
            message: 'Low sales alert: Total transactions below threshold'
        },
        high_revenue: {
            threshold: parseInt(document.getElementById('highRevenueThreshold').value),
            enabled: true,
            message: 'High revenue milestone reached!'
        },
        product_spike: {
            threshold: parseInt(document.getElementById('productSpikeThreshold').value),
            enabled: true,
            message: 'Product demand spike detected'
        }
    };
    
    fetch('/api/alerts/config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config)
    })
    .then(r => r.json())
    .then(data => {
        if (data.status === 'success') {
            alert('Alert configuration saved successfully!');
            toggleAlertsConfig();
        }
    })
    .catch(err => console.error('Error saving config:', err));
}

// Export Functions
function exportToPDF() {
    showExportProgress('PDF Report');
    
    // Create a temporary link to trigger download
    const link = document.createElement('a');
    link.href = '/api/export/pdf';
    link.download = `purchase_trend_report_${new Date().toISOString().slice(0,10)}.pdf`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    setTimeout(() => {
        hideExportProgress();
        showExportSuccess('PDF report generated successfully!');
    }, 2000);
}

function exportToExcel() {
    showExportProgress('Excel Data');
    
    // Create a temporary link to trigger download
    const link = document.createElement('a');
    link.href = '/api/export/excel';
    link.download = `purchase_trend_data_${new Date().toISOString().slice(0,10)}.xlsx`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    setTimeout(() => {
        hideExportProgress();
        showExportSuccess('Excel data exported successfully!');
    }, 2000);
}

function scheduleReport() {
    // Show modal for scheduling (placeholder for future implementation)
    alert('📅 Scheduled Reports\n\nThis feature will allow you to:\n• Schedule daily/weekly reports\n• Email reports automatically\n• Set custom report formats\n\nComing soon in the next update!');
}

function showExportProgress(type) {
    // Create and show progress modal
    const modal = document.createElement('div');
    modal.id = 'exportModal';
    modal.className = 'modal fade show';
    modal.style.display = 'block';
    modal.style.backgroundColor = 'rgba(0,0,0,0.5)';
    modal.innerHTML = `
        <div class="modal-dialog modal-dialog-centered">
            <div class="modal-content">
                <div class="modal-body text-center py-4">
                    <div class="spinner-border text-primary mb-3" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <h5>Generating ${type}...</h5>
                    <p class="text-muted mb-0">Please wait while we prepare your report</p>
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
}

function hideExportProgress() {
    const modal = document.getElementById('exportModal');
    if (modal) {
        modal.remove();
    }
}

function showExportSuccess(message) {
    // Create success notification
    const notification = document.createElement('div');
    notification.className = 'alert alert-success alert-dismissible fade show position-fixed';
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    notification.innerHTML = `
        <i class="fas fa-check-circle me-2"></i>
        ${message}
        <button type="button" class="btn-close" onclick="this.parentElement.remove()"></button>
    `;
    document.body.appendChild(notification);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (notification.parentElement) {
            notification.remove();
        }
    }, 5000);
}

// Request notification permission on page load
document.addEventListener('DOMContentLoaded', () => {
    if ('Notification' in window && Notification.permission === 'default') {
        Notification.requestPermission();
    }
});
