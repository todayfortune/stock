document.addEventListener('DOMContentLoaded', function() {
    initDashboard();
});

function initDashboard() {
    loadData();
    // 60초마다 데이터 자동 갱신
    setInterval(loadData, 60000);
}

// 탭 전환 기능 (설명서 포함)
window.switchTab = function(tabName) {
    // 1. 모든 탭 숨기기
    const tabs = ['dashboard', 'backtest', 'manual'];
    tabs.forEach(t => {
        const el = document.getElementById('tab-' + t);
        if (el) el.style.display = 'none';
        
        // 데스크탑 메뉴 활성화 상태 해제
        const btn = document.getElementById('nav-' + t);
        if (btn) btn.classList.remove('active');
    });

    // 2. 선택한 탭 보이기
    const selectedTab = document.getElementById('tab-' + tabName);
    if (selectedTab) selectedTab.style.display = 'block';

    // 3. 버튼 활성화
    const activeBtn = document.getElementById('nav-' + tabName);
    if (activeBtn) activeBtn.classList.add('active');

    // 4. 모바일 사이드바 닫기
    const sidebar = document.getElementById('sidebar');
    if (sidebar) {
        const bsOffcanvas = bootstrap.Offcanvas.getInstance(sidebar);
        if (bsOffcanvas) bsOffcanvas.hide();
    }
    
    // 5. 스크롤 맨 위로
    window.scrollTo(0, 0);
}

function loadData() {
    const timestamp = new Date().getTime();
    
    // 메타 데이터 로드
    fetch(`data/meta.json?t=${timestamp}`)
        .then(response => response.json())
        .then(data => {
            document.getElementById('update-time').textContent = data.asOf;
            updateMarketBadge(data.market);
        })
        .catch(err => console.log('Meta Load Error:', err));

    // 섹터 데이터 로드
    fetch(`data/sector_leaders.json?t=${timestamp}`)
        .then(response => response.json())
        .then(data => renderSectors(data.items))
        .catch(err => console.log('Sector Load Error:', err));

    // 관심종목 데이터 로드
    fetch(`data/watchlist.json?t=${timestamp}`)
        .then(response => response.json())
        .then(data => renderWatchlist(data.items))
        .catch(err => console.log('Watchlist Load Error:', err));

    // 백테스트 데이터 로드
    fetch(`data/backtest.json?t=${timestamp}`)
        .then(response => response.json())
        .then(data => renderBacktest(data))
        .catch(err => console.log('Backtest Load Error:', err));
}

function updateMarketBadge(market) {
    const badge = document.getElementById('market-badge');
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
    
    if (!items || items.length === 0) {
        container.innerHTML = '<div class="col-12 text-center text-muted">데이터 수집 중...</div>';
        return;
    }

    // 상위 3개 섹터만 표시
    items.slice(0, 3).forEach(item => {
        // 점수 100점 만점 기준 색상
        let scoreColor = 'text-muted';
        if(item.score >= 80) scoreColor = 'text-danger fw-bold';
        else if(item.score >= 50) scoreColor = 'text-primary fw-bold';

        const card = `
            <div class="col-12 col-md-4">
                <div class="card border-0 shadow-sm h-100">
                    <div class="card-body p-3">
                        <div class="d-flex justify-content-between align-items-start mb-2">
                            <h6 class="fw-bold mb-0 text-secondary" style="font-size: 0.8rem;">${item.sector}</h6>
                            <span class="badge bg-light text-dark border">${(item.turnover / 100000000).toFixed(0)}억</span>
                        </div>
                        <h5 class="fw-bold mb-2">${item.topTickers[0]}</h5>
                        <div class="d-flex align-items-center justify-content-between">
                            <span class="small ${scoreColor}">Score ${item.score}</span>
                            <small class="text-muted" style="font-size: 0.75rem;">${item.topTickers.slice(1).join(', ')}</small>
                        </div>
                    </div>
                </div>
            </div>
        `;
        container.innerHTML += card;
    });
}

function renderWatchlist(items) {
    const desktopBody = document.getElementById('desktop-table-body');
    const mobileList = document.getElementById('mobile-card-list');
    
    desktopBody.innerHTML = '';
    mobileList.innerHTML = '';

    if (!items || items.length === 0) {
        mobileList.innerHTML = '<div class="text-center p-4 text-muted">표시할 종목이 없습니다. (장 마감 후 업데이트됨)</div>';
        return;
    }

    items.forEach(item => {
        // 색상 클래스
        const priceColor = item.change > 0 ? 'text-up' : (item.change < 0 ? 'text-down' : 'text-dark');
        const badgeClass = `badge-${item.grade}`;
        const actionClass = `action-${item.action}`;
        
        // 상세 정보 (Why)
        const reasons = item.why && item.why.length > 0 ? item.why.join('<br>') : '-';
        
        // Entry/Stop 가격 표시 (0이면 - 표시)
        const entryPrice = item.entry.price > 0 ? item.entry.price.toLocaleString() : '-';
        const stopPrice = item.stop.price > 0 ? item.stop.price.toLocaleString() : '-';

        // 데스크탑 테이블 행
        const tr = `
            <tr onclick="showDetail('${item.ticker}')" style="cursor: pointer;">
                <td class="ps-4">
                    <div class="fw-bold">${item.name}</div>
                    <div class="small text-muted">${item.ticker}</div>
                </td>
                <td class="fw-bold">${item.close.toLocaleString()}</td>
                <td class="${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</td>
                <td><span class="badge ${badgeClass}">${item.grade}</span></td>
                <td><span class="badge ${actionClass}">${item.action}</span></td>
                <td class="small text-muted">${reasons}</td>
                <td class="small">
                    <div>Entry: <strong>${entryPrice}</strong></div>
                    <div class="text-muted">Stop: ${stopPrice}</div>
                </td>
            </tr>
        `;
        desktopBody.innerHTML += tr;

        // 모바일 카드
        const card = `
            <div class="mobile-card" onclick="showDetail('${item.ticker}')">
                <div class="d-flex justify-content-between mb-2">
                    <div>
                        <span class="fw-bold fs-5 me-2">${item.name}</span>
                        <span class="small text-muted">${item.sector}</span>
                    </div>
                    <span class="badge ${badgeClass}">${item.grade}</span>
                </div>
                <div class="d-flex justify-content-between align-items-end mb-3">
                    <div>
                        <div class="fs-4 fw-bold">${item.close.toLocaleString()}</div>
                        <div class="small ${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</div>
                    </div>
                    <span class="badge ${actionClass} px-3 py-2 rounded-pill">${item.action}</span>
                </div>
                <div class="bg-light p-2 rounded small text-secondary">
                    ${reasons}
                </div>
            </div>
        `;
        mobileList.innerHTML += card;
    });
}

// 백테스트 렌더링
function renderBacktest(data) {
    if (!data) return;
    
    // 요약 카드
    document.getElementById('bt-return').textContent = (data.summary.total_return > 0 ? '+' : '') + data.summary.total_return + '%';
    document.getElementById('bt-final').textContent = (data.summary.final_balance / 10000).toFixed(0) + '만';
    document.getElementById('bt-mdd').textContent = data.summary.mdd + '%';
    document.getElementById('bt-win').textContent = data.summary.win_rate + '%';

    // 색상 처리
    document.getElementById('bt-return').className = 'stat-value ' + (data.summary.total_return >= 0 ? 'text-danger' : 'text-primary');

    // 차트 그리기
    const ctx = document.getElementById('equityChart').getContext('2d');
    if (window.myEquityChart) window.myEquityChart.destroy();

    const labels = data.equity_curve.map(d => d.date);
    const values = data.equity_curve.map(d => d.equity);

    window.myEquityChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: '누적 자산',
                data: values,
                borderColor: '#0d6efd',
                backgroundColor: 'rgba(13, 110, 253, 0.1)',
                borderWidth: 2,
                fill: true,
                pointRadius: 0,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { display: false },
                y: { grid: { borderDash: [2, 4] } }
            }
        }
    });
}

// 종목 상세 팝업 (가짜 기능 - 필요시 구현)
window.showDetail = function(ticker) {
    // alert(ticker + " 상세 분석 팝업 준비 중");
}
