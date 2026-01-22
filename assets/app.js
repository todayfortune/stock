window.watchlistData = [];
window.backtestData = {}; 

document.addEventListener('DOMContentLoaded', function() {
    initDashboard();
});

function initDashboard() {
    loadData();
    setInterval(loadData, 60000);
}

window.switchTab = function(tabName) {
    ['dashboard', 'backtest', 'manual'].forEach(t => {
        const el = document.getElementById('tab-' + t);
        if (el) el.style.display = 'none';
        const btn = document.getElementById('nav-' + t);
        if (btn) btn.classList.remove('active');
    });

    // 모든 백테스트 버튼 비활성화
    ['recent', 'covid', 'box', 'early', 'early_covid', 'early_box'].forEach(t => {
        const btn = document.getElementById('nav-bt-' + t);
        if (btn) btn.classList.remove('active');
    });

    const selectedTab = document.getElementById('tab-' + tabName);
    if (selectedTab) selectedTab.style.display = 'block';

    const activeBtn = document.getElementById('nav-' + tabName);
    if (activeBtn) activeBtn.classList.add('active');

    closeSidebar();
    window.scrollTo(0, 0);
}

window.switchBacktest = function(periodKey) {
    switchTab('backtest');
    document.getElementById('nav-backtest')?.classList.remove('active');

    // 선택된 버튼만 활성화
    ['recent', 'covid', 'box', 'early', 'early_covid', 'early_box'].forEach(t => {
        const btn = document.getElementById('nav-bt-' + t);
        if (btn) {
            if (t === periodKey) btn.classList.add('active');
            else btn.classList.remove('active');
        }
    });

    if (window.backtestData && window.backtestData[periodKey]) {
        renderBacktest(window.backtestData[periodKey], periodKey);
    } else {
        document.getElementById('bt-title').textContent = "데이터 없음 / 로딩 중";
    }
}

function closeSidebar() {
    const sidebar = document.getElementById('sidebar');
    if (sidebar) {
        const bsOffcanvas = bootstrap.Offcanvas.getInstance(sidebar);
        if (bsOffcanvas) bsOffcanvas.hide();
    }
}

function loadData() {
    const timestamp = new Date().getTime();
    fetch(`data/meta.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => {
            document.getElementById('update-time').textContent = data.asOf;
            updateMarketBadge(data.market);
        });

    fetch(`data/sector_leaders.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => renderSectors(data.items));

    fetch(`data/watchlist.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => {
            window.watchlistData = data.items;
            renderWatchlist(data.items);
        });

    fetch(`data/backtest.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => {
            window.backtestData = data;
            if(document.getElementById('tab-backtest').style.display !== 'block') {
            } else {
               const activeBtn = document.querySelector('[id^="nav-bt-"].active');
               const key = activeBtn ? activeBtn.id.replace('nav-bt-', '') : 'recent';
               renderBacktest(data[key], key);
            }
        });
}

function updateMarketBadge(market) {
    const badge = document.getElementById('market-badge');
    if(!badge) return;
    if (market && market.state === 'RISK_ON') {
        badge.className = 'badge bg-success me-2';
        badge.textContent = `ON: ${market.reason}`;
    } else {
        badge.className = 'badge bg-danger me-2';
        badge.textContent = `OFF: ${market.reason || '리스크 관리'}`;
    }
}

function renderSectors(items) {
    const container = document.getElementById('sector-area');
    container.innerHTML = '';
    if (!items || items.length === 0) return;

    items.slice(0, 3).forEach(item => {
        let scoreColor = item.score >= 80 ? 'text-danger fw-bold' : (item.score >= 50 ? 'text-primary fw-bold' : 'text-muted');
        const card = `<div class="col-12 col-md-4"><div class="card border-0 shadow-sm h-100"><div class="card-body p-3"><div class="d-flex justify-content-between align-items-start mb-2"><h6 class="fw-bold mb-0 text-secondary" style="font-size: 0.8rem;">${item.sector}</h6><span class="badge bg-light text-dark border">${(item.turnover / 100000000).toFixed(0)}억</span></div><h5 class="fw-bold mb-2">${item.topTickers[0]}</h5><div class="d-flex align-items-center justify-content-between"><span class="small ${scoreColor}">Score ${item.score}</span><small class="text-muted" style="font-size: 0.75rem;">${item.topTickers.slice(1).join(', ')}</small></div></div></div></div>`;
        container.innerHTML += card;
    });
}

function renderWatchlist(items) {
    const desktopBody = document.getElementById('desktop-table-body');
    const mobileList = document.getElementById('mobile-card-list');
    desktopBody.innerHTML = '';
    mobileList.innerHTML = '';

    if (!items || items.length === 0) {
        mobileList.innerHTML = '<div class="text-center p-4 text-muted">표시할 종목이 없습니다.</div>';
        return;
    }

    items.forEach(item => {
        const priceColor = item.change > 0 ? 'text-up' : (item.change < 0 ? 'text-down' : 'text-dark');
        const badgeClass = `badge-${item.grade}`;
        const actionClass = `action-${item.action}`;
        const reasons = item.why && item.why.length > 0 ? item.why.join('<br>') : '-';
        const tr = `<tr onclick="showDetail('${item.ticker}')" style="cursor: pointer;"><td class="ps-4"><div class="fw-bold">${item.name}</div><div class="small text-muted">${item.ticker}</div></td><td class="fw-bold">${item.close.toLocaleString()}</td><td class="${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</td><td><span class="badge ${badgeClass}">${item.grade}</span></td><td><span class="badge ${actionClass}">${item.action}</span></td><td class="small text-muted">${reasons}</td><td class="small text-primary fw-bold">Click View</td></tr>`;
        desktopBody.innerHTML += tr;
        const card = `<div class="mobile-card" onclick="showDetail('${item.ticker}')"><div class="d-flex justify-content-between mb-2"><div><span class="fw-bold fs-5 me-2">${item.name}</span><span class="small text-muted">${item.sector}</span></div><span class="badge ${badgeClass}">${item.grade}</span></div><div class="d-flex justify-content-between align-items-end mb-3"><div><div class="fs-4 fw-bold">${item.close.toLocaleString()}</div><div class="small ${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</div></div><span class="badge ${actionClass} px-3 py-2 rounded-pill">${item.action}</span></div></div>`;
        mobileList.innerHTML += card;
    });
}

function renderBacktest(data, key) {
    if (!data) return;
    
    // 타이틀 매핑
    const titles = {
        'recent': 'Standard: 최근 3년 (Trend)',
        'covid': 'Standard: 2020~2023 (Volatility)',
        'box': 'Standard: 2015~2019 (Box)',
        
        'early': 'SDI Mode: 최근 3년',
        'early_covid': 'SDI Mode: 2020~2023 (Crisis)',
        'early_box': 'SDI Mode: 2015~2019 (Boring)'
    };
    
    document.getElementById('bt-title').textContent = "📊 " + (titles[key] || '전략 검증');
    document.getElementById('bt-desc').textContent = key.includes('early')
        ? "Logic: 역배열 말기 + 바닥 구조 + 0.5배수 진입 (SDI Strategy)" 
        : "Logic: 정배열 추세 + 구조 돌파 (Standard Strategy)";

    document.getElementById('bt-return').textContent = (data.summary.total_return > 0 ? '+' : '') + data.summary.total_return + '%';
    document.getElementById('bt-final').textContent = (data.summary.final_balance / 10000).toFixed(0) + '만';
    document.getElementById('bt-mdd').textContent = data.summary.mdd + '%';
    document.getElementById('bt-win').textContent = data.summary.win_rate + '%';
    document.getElementById('bt-return').className = 'stat-value ' + (data.summary.total_return >= 0 ? 'text-danger' : 'text-primary');

    const ctx = document.getElementById('equityChart').getContext('2d');
    if (window.myEquityChart) window.myEquityChart.destroy();
    
    // 컬러 매핑
    const colorMap = {
        'recent': '#0d6efd', // Blue
        'covid': '#dc3545',  // Red
        'box': '#198754',    // Green
        
        'early': '#6f42c1',       // Purple
        'early_covid': '#fd7e14', // Orange
        'early_box': '#20c997'    // Teal
    };
    const color = colorMap[key] || '#0d6efd';

    window.myEquityChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.equity_curve.map(d => d.date),
            datasets: [{
                label: '누적 자산',
                data: data.equity_curve.map(d => d.equity),
                borderColor: color,
                backgroundColor: color + '10',
                borderWidth: 2,
                fill: true,
                pointRadius: 0,
                tension: 0.1
            }]
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { grid: { borderDash: [2, 4] } } } }
    });
}

window.showDetail = function(ticker) {
    const item = window.watchlistData.find(i => i.ticker === ticker);
    if (!item) return;

    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');
    modalTitle.innerHTML = `${item.name} <span class="text-muted small">(${item.ticker})</span>`;
    
    const stopPrice = item.stop.price > 0 ? item.stop.price.toLocaleString() : '-';
    const targetPrice = item.target.price > 0 ? item.target.price.toLocaleString() : '-';
    const risk = item.stop.price > 0 ? item.close - item.stop.price : 0;
    const reward = item.target.price > 0 ? item.target.price - item.close : 0;
    
    let rrRatio = 'N/A';
    if(risk > 0 && reward > 0) rrRatio = '1 : ' + (reward / risk).toFixed(1);

    modalBody.innerHTML = `<div class="row g-3"><div class="col-6"><div class="p-3 bg-light rounded text-center"><div class="small text-muted mb-1">진입가</div><div class="fw-bold fs-5">${item.close.toLocaleString()}</div></div></div><div class="col-6"><div class="p-3 bg-light rounded text-center"><div class="small text-muted mb-1">손익비</div><div class="fw-bold fs-5 text-primary">${rrRatio}</div></div></div><div class="col-12"><div class="d-flex justify-content-between align-items-center border-bottom pb-2 mb-2"><span class="text-danger fw-bold"><i class="fas fa-stop-circle me-1"></i> 손절가</span><span class="fw-bold text-danger">${stopPrice}</span></div><div class="d-flex justify-content-between align-items-center"><span class="text-success fw-bold"><i class="fas fa-bullseye me-1"></i> 목표가</span><span class="fw-bold text-success">${targetPrice}</span></div></div><div class="col-12"><div class="alert alert-secondary mb-0 small"><strong>💡 분석 요약:</strong><br>${item.why.join('<br>')}</div></div></div>`;
    new bootstrap.Modal(document.getElementById('detailModal')).show();
}
