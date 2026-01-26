window.watchlistData = [];
window.backtestData = {};    // Standard 전략
window.sdiData = {};         // SDI 전략
window.wallstreetData = {};  // Wall St. 전략
window.quantData = {};       // [NEW] 퀀트 데이터
window.telegramNews = { global: [], specific: {} }; 

document.addEventListener('DOMContentLoaded', function() {
    initDashboard();
});

function initDashboard() {
    loadData();
    setInterval(loadData, 60000);
}

// 탭 전환 로직 (퀀트 탭 추가)
window.switchTab = function(tabName) {
    // 1. 모든 탭 숨기기 (quant 추가됨)
    ['dashboard', 'backtest', 'manual', 'telegram', 'quant'].forEach(t => {
        const el = document.getElementById('tab-' + t);
        if (el) el.style.display = 'none';
        const btn = document.getElementById('nav-' + t);
        if (btn) btn.classList.remove('active');
    });

    // 2. 서브 메뉴 하이라이트 초기화
    document.querySelectorAll('[id^="nav-bt-"]').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('[id^="nav-ws-"]').forEach(el => el.classList.remove('active'));

    // 3. 선택한 탭 보이기
    const selectedTab = document.getElementById('tab-' + tabName);
    if (selectedTab) selectedTab.style.display = 'block';

    const activeBtn = document.getElementById('nav-' + tabName);
    if (activeBtn) activeBtn.classList.add('active');
    
    // 탭별 특수 동작
    if (tabName === 'telegram') renderTelegramDashboard();
    if (tabName === 'quant') initQuantSelect(); // 퀀트 탭 열 때 리스트 초기화

    closeSidebar();
    window.scrollTo(0, 0);
}

// 백테스트 전환
window.switchBacktest = function(periodKey) {
    switchTab('backtest');
    document.getElementById('nav-backtest')?.classList.remove('active');
    
    document.querySelectorAll('[id^="nav-ws-"]').forEach(el => el.classList.remove('active'));
    ['recent', 'covid', 'box', 'early', 'early_covid', 'early_box'].forEach(t => {
        document.getElementById('nav-bt-' + t)?.classList.remove('active');
    });
    
    const targetBtn = document.getElementById('nav-bt-' + periodKey);
    if(targetBtn) targetBtn.classList.add('active');

    let targetData = null;
    if (periodKey.includes('early')) {
        targetData = window.sdiData[periodKey];
    } else {
        targetData = window.backtestData[periodKey];
    }

    if (targetData) renderBacktest(targetData, periodKey);
    else {
        document.getElementById('bt-title').textContent = "데이터 로딩 중...";
        document.getElementById('bt-return').textContent = "-";
    }
}

// 월가 전략 전환
window.switchWallStreet = function(periodKey) {
    switchTab('backtest');
    document.getElementById('nav-backtest')?.classList.remove('active');
    document.querySelectorAll('[id^="nav-bt-"]').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('[id^="nav-ws-"]').forEach(el => el.classList.remove('active'));

    const targetBtn = document.getElementById('nav-' + periodKey.replace('_', '-')); 
    if(targetBtn) targetBtn.classList.add('active');
    
    if (window.wallstreetData && window.wallstreetData[periodKey]) {
        renderBacktest(window.wallstreetData[periodKey], periodKey);
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
    
    // 메타 & 마켓 상태
    fetch(`data/meta.json?t=${timestamp}`).then(r=>r.json()).then(d=>{
        document.getElementById('update-time').textContent = d.asOf;
        updateMarketBadge(d.market);
    });

    // 섹터 & 관심종목
    fetch(`data/sector_leaders.json?t=${timestamp}`).then(r=>r.json()).then(d=>renderSectors(d.items));
    fetch(`data/watchlist.json?t=${timestamp}`).then(r=>r.json()).then(d=>{
        window.watchlistData = d.items;
        renderWatchlist(d.items);
    });

    // 백테스트 데이터들
    fetch(`data/backtest.json?t=${timestamp}`).then(r=>r.json()).then(d=>{ window.backtestData = d; });
    
    fetch(`data/backtest_sdi.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => { window.sdiData = data; })
        .catch(() => {});

    fetch(`data/backtest_wallstreet.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => { window.wallstreetData = data; })
        .catch(() => {});
        
    // [NEW] 퀀트 데이터 로드
    fetch(`data/quant_stats.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => {
            window.quantData = data;
            // 이미 퀀트 탭이 열려있다면 바로 렌더링
            if(document.getElementById('tab-quant').style.display === 'block') {
                initQuantSelect();
            }
        })
        .catch(() => console.log('Quant data pending...'));

    // 텔레그램
    fetch(`data/telegram_news.json?t=${timestamp}`)
        .then(res => res.json())
        .then(data => {
            window.telegramNews = Array.isArray(data) ? { global: [], specific: data } : data;
        })
        .catch(() => {});
}

// ---------------------------------------------------------
// [NEW] 퀀트 실험실 기능 (PBR-ROE)
// ---------------------------------------------------------
function initQuantSelect() {
    const select = document.getElementById('quant-sector-select');
    if (!select || !window.quantData) return;
    
    // 이미 채워져 있으면 중복 실행 방지 (단, 데이터 갱신 시에는 다시 그림)
    if (select.options.length > 1 && select.value !== "") return;

    const currentVal = select.value;
    select.innerHTML = '<option value="">섹터 선택...</option>';
    
    Object.keys(window.quantData).sort().forEach(sector => {
        select.innerHTML += `<option value="${sector}">${sector}</option>`;
    });
    
    if (currentVal && window.quantData[currentVal]) {
        select.value = currentVal;
    }
}

window.renderQuantSector = function() {
    const sector = document.getElementById('quant-sector-select').value;
    const container = document.getElementById('quant-ranking-list');
    if (!sector || !window.quantData[sector]) return;

    const data = window.quantData[sector];
    const items = data.items;
    
    // 1. 리스트 렌더링
    container.innerHTML = '';
    items.slice(0, 15).forEach((item, idx) => {
        const isVeryCheap = idx < 3; 
        const badge = isVeryCheap ? '<span class="badge bg-danger ms-1">Cheap</span>' : '';
        
        container.innerHTML += `
            <div class="list-group-item d-flex justify-content-between align-items-center" onclick="showDetail('${item.code}')" style="cursor:pointer;">
                <div>
                    <div class="fw-bold">${item.name} ${badge}</div>
                    <div class="small text-muted">PBR ${item.pbr} / ROE ${item.roe}%</div>
                </div>
                <div class="text-end">
                    <span class="text-success fw-bold small">${item.residual.toFixed(2)}</span>
                </div>
            </div>
        `;
    });

    // 2. 차트 렌더링
    renderQuantChart(data);
}

function renderQuantChart(data) {
    const ctx = document.getElementById('quantChart').getContext('2d');
    if (window.quantChartInstance) window.quantChartInstance.destroy();

    const scatterData = data.items.map(item => ({
        x: item.roe,
        y: item.pbr,
        name: item.name,
        code: item.code
    }));

    // 회귀선 그리기
    const roeValues = data.items.map(d => d.roe);
    const minRoe = Math.min(...roeValues);
    const maxRoe = Math.max(...roeValues);
    
    // 단순 선형 회귀: y = ax + b
    const regressionLine = [
        { x: minRoe, y: data.slope * minRoe + data.intercept },
        { x: maxRoe, y: data.slope * maxRoe + data.intercept }
    ];

    window.quantChartInstance = new Chart(ctx, {
        type: 'scatter',
        data: {
            datasets: [
                {
                    label: '종목',
                    data: scatterData,
                    backgroundColor: '#0d6efd',
                    pointRadius: 6,
                    pointHoverRadius: 8
                },
                {
                    type: 'line',
                    label: '적정 가치 (회귀선)',
                    data: regressionLine,
                    borderColor: '#dc3545',
                    borderWidth: 2,
                    borderDash: [5, 5],
                    pointRadius: 0,
                    fill: false,
                    order: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(ctx) {
                            const p = ctx.raw;
                            return `${p.name}: ROE ${p.x}%, PBR ${p.y}`;
                        }
                    }
                },
                legend: { position: 'bottom' }
            },
            scales: {
                x: { 
                    title: { display: true, text: 'ROE (%)' },
                    grid: { borderDash: [2, 2] }
                },
                y: { 
                    title: { display: true, text: 'PBR (배)' },
                    grid: { borderDash: [2, 2] }
                }
            },
            onClick: (e, activeEls) => {
                if (activeEls.length > 0) {
                    const idx = activeEls[0].index;
                    // 첫 번째 데이터셋(점)인 경우만 클릭 이벤트 처리
                    if (activeEls[0].datasetIndex === 0) {
                        const code = scatterData[idx].code;
                        showDetail(code);
                    }
                }
            }
        }
    });
}

function renderBacktest(data, key) {
    if (!data) return;
    const titles = {
        'recent': 'Standard: 최근 3년', 'covid': 'Standard: 20~23', 'box': 'Standard: 15~20',
        'early': 'SDI Mode: 최근 3년', 'early_covid': 'SDI Mode: 20~23', 'early_box': 'SDI Mode: 15~20',
        'ws_recent': 'Wall St. Logic: 최근 3년', 'ws_covid': 'Wall St. Logic: 20~23', 'ws_box': 'Wall St. Logic: 15~20'
    };

    document.getElementById('bt-title').textContent = "📊 " + (titles[key] || '전략 검증');
    
    const descEl = document.getElementById('bt-desc');
    if (key.includes('ws_')) {
        descEl.textContent = "Logic: 시장필터(200일) + ATR 변동성 조절 + 1% 룰";
        descEl.className = "badge bg-warning text-dark border mt-1";
    } else if (key.includes('early')) {
        descEl.textContent = "Logic: 20일선 Gate + Break10 + RS강도 (Separate Engine)";
        descEl.className = "badge bg-info text-dark border mt-1";
    } else {
        descEl.textContent = "Logic: 정배열 추세 + 구조 돌파 (Standard Strategy)";
        descEl.className = "badge bg-light text-dark border mt-1";
    }

    document.getElementById('bt-return').textContent = (data.summary.total_return > 0 ? '+' : '') + data.summary.total_return + '%';
    document.getElementById('bt-final').textContent = (data.summary.final_balance / 10000).toFixed(0) + '만';
    document.getElementById('bt-mdd').textContent = data.summary.mdd + '%';
    document.getElementById('bt-win').textContent = data.summary.win_rate + '%';
    document.getElementById('bt-return').className = 'stat-value ' + (data.summary.total_return >= 0 ? 'text-danger' : 'text-primary');
    
    const ctx = document.getElementById('equityChart').getContext('2d');
    if (window.myEquityChart) window.myEquityChart.destroy();
    
    const colorMap = { 
        'recent': '#0d6efd', 'covid': '#dc3545', 'box': '#198754', 
        'early': '#0dcaf0', 'early_covid': '#0dcaf0', 'early_box': '#0dcaf0',
        'ws_recent': '#ffc107', 'ws_covid': '#fd7e14', 'ws_box': '#ffc107'
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
        options: { 
            responsive: true, maintainAspectRatio: false, 
            plugins: { legend: { display: false } }, 
            scales: { x: { display: false }, y: { grid: { borderDash: [2, 4] } } } 
        } 
    });
}

function updateMarketBadge(market) { const badge = document.getElementById('market-badge'); if(!badge) return; if (market && market.state === 'RISK_ON') { badge.className = 'badge bg-success me-2'; badge.textContent = `ON: ${market.reason}`; } else { badge.className = 'badge bg-danger me-2'; badge.textContent = `OFF: ${market.reason || '리스크 관리'}`; } } 
function renderSectors(items) { const container = document.getElementById('sector-area'); container.innerHTML = ''; if (!items || items.length === 0) return; items.slice(0, 3).forEach(item => { let scoreColor = item.score >= 80 ? 'text-danger fw-bold' : (item.score >= 50 ? 'text-primary fw-bold' : 'text-muted'); container.innerHTML += `<div class="col-12 col-md-4"><div class="card border-0 shadow-sm h-100"><div class="card-body p-3"><div class="d-flex justify-content-between align-items-start mb-2"><h6 class="fw-bold mb-0 text-secondary" style="font-size: 0.8rem;">${item.sector}</h6><span class="badge bg-light text-dark border">${(item.turnover / 100000000).toFixed(0)}억</span></div><h5 class="fw-bold mb-2">${item.topTickers[0]}</h5><div class="d-flex align-items-center justify-content-between"><span class="small ${scoreColor}">Score ${item.score}</span><small class="text-muted" style="font-size: 0.75rem;">${item.topTickers.slice(1).join(', ')}</small></div></div></div></div>`; }); } 
function renderWatchlist(items) { const desktopBody = document.getElementById('desktop-table-body'); const mobileList = document.getElementById('mobile-card-list'); desktopBody.innerHTML = ''; mobileList.innerHTML = ''; if (!items || items.length === 0) { mobileList.innerHTML = '<div class="text-center p-4 text-muted">표시할 종목이 없습니다.</div>'; return; } items.forEach(item => { const priceColor = item.change > 0 ? 'text-up' : (item.change < 0 ? 'text-down' : 'text-dark'); const badgeClass = `badge-${item.grade}`; const actionClass = `action-${item.action}`; const reasons = item.why && item.why.length > 0 ? item.why.join('<br>') : '-'; desktopBody.innerHTML += `<tr onclick="showDetail('${item.ticker}')" style="cursor: pointer;"><td class="ps-4"><div class="fw-bold">${item.name}</div><div class="small text-muted">${item.ticker}</div></td><td class="fw-bold">${item.close.toLocaleString()}</td><td class="${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</td><td><span class="badge ${badgeClass}">${item.grade}</span></td><td><span class="badge ${actionClass}">${item.action}</span></td><td class="small text-muted">${reasons}</td><td class="small text-primary fw-bold">Click View</td></tr>`; mobileList.innerHTML += `<div class="mobile-card" onclick="showDetail('${item.ticker}')"><div class="d-flex justify-content-between mb-2"><div><span class="fw-bold fs-5 me-2">${item.name}</span><span class="small text-muted">${item.sector}</span></div><span class="badge ${badgeClass}">${item.grade}</span></div><div class="d-flex justify-content-between align-items-end mb-3"><div><div class="fs-4 fw-bold">${item.close.toLocaleString()}</div><div class="small ${priceColor}">${item.change > 0 ? '+' : ''}${item.change}%</div></div><span class="badge ${actionClass} px-3 py-2 rounded-pill">${item.action}</span></div></div>`; }); } 
function renderTelegramDashboard() { const container = document.getElementById('telegram-feed-area'); if(!container) return; const allNews = window.telegramNews.global || []; allNews.sort((a, b) => new Date(b.date) - new Date(a.date)); if (allNews.length === 0) { container.innerHTML = '<div class="col-12 text-center py-5 text-muted">수집된 키워드 뉴스가 없습니다.<br><small>"상향", "서프라이즈" 등의 키워드를 찾습니다.</small></div>'; return; } container.innerHTML = ''; allNews.forEach(news => { let keywordBadges = ''; if (news.keywords && news.keywords.length > 0) { news.keywords.forEach(k => { keywordBadges += `<span class="badge bg-warning text-dark me-1 border">${k}</span>`; }); } else { keywordBadges = `<span class="badge bg-secondary">News</span>`; } container.innerHTML += `<div class="col-12 col-md-6 col-lg-4"><div class="card border-0 shadow-sm h-100"><div class="card-body"><div class="d-flex justify-content-between mb-2"><div>${keywordBadges}</div><small class="text-muted">${news.date.substring(5)}</small></div><h6 class="card-title fw-bold text-dark" style="font-size: 0.95rem;"><a href="${news.link}" target="_blank" class="text-decoration-none text-dark">${news.text}</a></h6><div class="d-flex justify-content-between align-items-center mt-3"><span class="small text-secondary"><i class="fab fa-telegram-plane me-1"></i>${news.source}</span><a href="${news.link}" target="_blank" class="btn btn-sm btn-outline-primary rounded-pill px-3">보기</a></div></div></div></div>`; }); } 
window.showDetail = function(ticker) { const item = window.watchlistData.find(i => i.ticker === ticker); if (!item) return; const modalTitle = document.getElementById('modal-title'); const modalBody = document.getElementById('modal-body'); modalTitle.innerHTML = `${item.name} <span class="text-muted small">(${item.ticker})</span>`; const stopPrice = item.stop.price > 0 ? item.stop.price.toLocaleString() : '-'; const targetPrice = item.target.price > 0 ? item.target.price.toLocaleString() : '-'; const risk = item.stop.price > 0 ? item.close - item.stop.price : 0; const reward = item.target.price > 0 ? item.target.price - item.close : 0; let rrRatio = (risk > 0 && reward > 0) ? '1 : ' + (reward / risk).toFixed(1) : 'N/A'; let newsHtml = ''; const specificNews = window.telegramNews.specific || {}; const newsList = specificNews[ticker] || []; if (newsList && newsList.length > 0) { newsHtml = `<div class="col-12 mt-3"><h6 class="fw-bold small text-muted border-bottom pb-2"><i class="fab fa-telegram-plane text-info me-1"></i> ${item.name} 관련 언급</h6><div class="list-group list-group-flush">`; newsList.slice(0, 3).forEach(news => { newsHtml += `<a href="${news.link}" target="_blank" class="list-group-item list-group-item-action px-0 py-2 border-0"><div class="d-flex justify-content-between align-items-center mb-1"><span class="badge bg-light text-dark border" style="font-size: 0.7rem;">${news.source}</span><span class="text-muted small" style="font-size: 0.7rem;">${news.date.substring(5)}</span></div><div class="text-dark small text-truncate" style="max-width: 100%;">${news.text}</div></a>`; }); newsHtml += `</div></div>`; } else { newsHtml = `<div class="col-12 mt-3"><div class="p-3 bg-light rounded text-center text-muted small"><i class="fas fa-comment-slash mb-1"></i><br>최근 언급된 내용이 없습니다.</div></div>`; } modalBody.innerHTML = `<div class="row g-3"><div class="col-6"><div class="p-3 bg-light rounded text-center"><div class="small text-muted mb-1">진입가</div><div class="fw-bold fs-5">${item.close.toLocaleString()}</div></div></div><div class="col-6"><div class="p-3 bg-light rounded text-center"><div class="small text-muted mb-1">손익비</div><div class="fw-bold fs-5 text-primary">${rrRatio}</div></div></div><div class="col-12"><div class="d-flex justify-content-between align-items-center border-bottom pb-2 mb-2"><span class="text-danger fw-bold"><i class="fas fa-stop-circle me-1"></i> 손절가</span><span class="fw-bold text-danger">${stopPrice}</span></div><div class="d-flex justify-content-between align-items-center"><span class="text-success fw-bold"><i class="fas fa-bullseye me-1"></i> 목표가</span><span class="fw-bold text-success">${targetPrice}</span></div></div><div class="col-12"><div class="alert alert-secondary mb-0 small"><strong>💡 분석 요약:</strong><br>${item.why.join('<br>')}</div></div>${newsHtml}</div>`; new bootstrap.Modal(document.getElementById('detailModal')).show(); }
