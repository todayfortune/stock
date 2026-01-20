// assets/app.js v4.0

const state = { meta: null, sectors: [], watchlist: [], backtest: null };

function getBasePath() {
    let path = window.location.pathname;
    if (path.endsWith('.html')) path = path.substring(0, path.lastIndexOf('/'));
    return path.endsWith('/') ? path : path + '/';
}

// assets/app.js (switchTab 함수 부분만 수정하거나, 전체 덮어쓰기)


// ... (뒷부분 renderDashboard 등은 그대로 유지) ...

async function initDashboard() {
    const BASE = getBasePath();
    try {
        const [metaRes, sectorsRes, watchRes, btRes] = await Promise.all([
            fetch(`${BASE}data/meta.json`),
            fetch(`${BASE}data/sector_leaders.json`),
            fetch(`${BASE}data/watchlist.json`),
            fetch(`${BASE}data/backtest.json`) // [NEW] 백테스트 데이터 로드
        ]);

        state.meta = await metaRes.json();
        const sData = await sectorsRes.json();
        const wData = await watchRes.json();
        state.sectors = sData.items || [];
        state.watchlist = wData.items || [];
        
        // 백테스트 데이터 처리
        if (btRes.ok) {
            state.backtest = await btRes.json();
            renderBacktest();
        }

        // 정렬
        const gw = { 'S': 3, 'A': 2, 'B': 1, 'C': 0 };
        const aw = { 'READY': 2, 'WAIT': 1, 'NO_TRADE': 0 };
        state.watchlist.sort((a, b) => {
            const ad = aw[b.action] - aw[a.action];
            if (ad !== 0) return ad;
            const gd = gw[b.grade] - gw[a.grade];
            if (gd !== 0) return gd;
            return b.volume - a.volume;
        });

        renderDashboard();

    } catch (err) {
        console.error(err);
    }
}

// ... (앞부분 initDashboard 등은 그대로 유지) ...

// [수정] 탭 전환 기능에 'manual' 추가
window.switchTab = function(tabName) {
    // 1. 모든 탭 숨기기
    document.getElementById('tab-dashboard').style.display = 'none';
    document.getElementById('tab-backtest').style.display = 'none';
    document.getElementById('tab-manual').style.display = 'none';
    
    // 2. 선택한 탭만 보이기
    const selectedTab = document.getElementById('tab-' + tabName);
    if (selectedTab) selectedTab.style.display = 'block';
    
    // 3. 사이드바 버튼 활성화 상태 변경 (데스크탑)
    document.querySelectorAll('.list-group-item').forEach(btn => btn.classList.remove('active'));
    const activeBtn = document.getElementById('nav-' + tabName);
    if (activeBtn) activeBtn.classList.add('active');

    // 4. 모바일 사이드바 닫기
    const sidebar = document.getElementById('sidebar');
    if (sidebar) {
        const bsOffcanvas = bootstrap.Offcanvas.getInstance(sidebar);
        if (bsOffcanvas) bsOffcanvas.hide();
    }
    
    // 5. 화면 맨 위로 스크롤
    window.scrollTo(0, 0);
}
function renderDashboard() {
    const mkt = state.meta.market;
    const badge = document.getElementById('market-badge');
    if (mkt && mkt.state === 'RISK_ON') {
        badge.className = 'badge bg-success';
        badge.innerHTML = '<i class="fas fa-check-circle me-1"></i>ON';
    } else {
        badge.className = 'badge bg-danger';
        badge.innerHTML = '<i class="fas fa-ban me-1"></i>OFF';
    }
    document.getElementById('update-time').innerText = state.meta.asOf;

    // 섹터 및 워치리스트 렌더링 (기존 동일)
    const secArea = document.getElementById('sector-area');
    secArea.innerHTML = '';
    state.sectors.slice(0, 3).forEach(sec => {
        secArea.innerHTML += `
            <div class="col-md-4 col-12">
                <div class="card border-0 shadow-sm h-100" style="background: linear-gradient(135deg, #ffffff 0%, #f1f3f5 100%);">
                    <div class="card-body p-3">
                        <small class="text-muted fw-bold text-uppercase" style="font-size:0.75rem">${sec.sector}</small>
                        <h5 class="fw-bold mt-1 mb-2 text-dark">${sec.topTickers[0]}</h5>
                        <div class="d-flex justify-content-between align-items-end">
                            <span class="badge bg-white text-dark border">Score ${sec.score}</span>
                            <small class="text-muted">${(sec.turnover/1e8).toFixed(0)}억</small>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    const pcTable = document.getElementById('desktop-table-body');
    const mobList = document.getElementById('mobile-card-list');
    pcTable.innerHTML = ''; mobList.innerHTML = '';

    state.watchlist.forEach((item, idx) => {
        const isUp = item.change > 0;
        const colorClass = isUp ? 'text-up' : 'text-down';
        const sign = isUp ? '+' : '';
        const vol = Math.round(item.volume / 1e8).toLocaleString();

        pcTable.innerHTML += `
            <tr style="cursor:pointer" onclick="showDetail(${idx})">
                <td class="ps-4"><div class="fw-bold text-dark">${item.name}</div><small class="text-muted">${item.ticker}</small></td>
                <td class="fw-bold">${item.close.toLocaleString()}</td>
                <td class="${colorClass}">${sign}${item.change}%</td>
                <td><span class="badge badge-${item.grade}">${item.grade}</span></td>
                <td><span class="badge action-${item.action}">${item.action}</span></td>
                <td><small class="text-muted text-truncate d-block" style="max-width:150px">${item.why[0] || '-'}</small></td>
                <td><small>Entry <b>${item.entry.price.toLocaleString()}</b></small></td>
            </tr>
        `;

        mobList.innerHTML += `
            <div class="mobile-card" onclick="showDetail(${idx})" style="border-left: 5px solid ${item.action === 'READY' ? '#198754' : '#dee2e6'}">
                <div class="d-flex justify-content-between mb-2">
                    <div><span class="fw-bold text-dark" style="font-size:1.1rem">${item.name}</span><small class="text-muted ms-1">${item.sector}</small></div>
                    <div><span class="badge badge-${item.grade} me-1">${item.grade}</span><span class="badge action-${item.action}">${item.action}</span></div>
                </div>
                <div class="d-flex justify-content-between align-items-end">
                    <div><div class="fw-bold fs-4 text-dark" style="line-height:1">${item.close.toLocaleString()}</div><small class="${colorClass} fw-bold">${sign}${item.change}%</small></div>
                    <div class="text-end"><small class="text-muted d-block" style="font-size:0.8rem">거래대금 ${vol}억</small><small class="text-primary fw-bold" style="font-size:0.8rem">전략 보기 <i class="fas fa-arrow-right"></i></small></div>
                </div>
            </div>
        `;
    });
}

function renderBacktest() {
    if (!state.backtest) return;
    const summary = state.backtest.summary;
    const curve = state.backtest.equity_curve;

    // 요약 카드 채우기
    document.getElementById('bt-return').innerText = `+${summary.total_return}%`;
    document.getElementById('bt-final').innerText = `${(summary.final_balance/10000).toLocaleString()}만`;
    document.getElementById('bt-mdd').innerText = `${summary.mdd}%`;
    document.getElementById('bt-win').innerText = `${summary.win_rate}%`;

    // 차트 그리기 (Chart.js)
    const ctx = document.getElementById('equityChart').getContext('2d');
    const labels = curve.map(d => d.date);
    const data = curve.map(d => d.equity);

    new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: '자산 추이',
                data: data,
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                borderWidth: 2,
                fill: true,
                pointRadius: 0, // 점 숨김
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                x: { display: false }, // X축 날짜 너무 많아서 숨김
                y: { grid: { borderDash: [2, 4] } }
            }
        }
    });
}

window.showDetail = function(idx) {
    const item = state.watchlist[idx];
    const isUp = item.change > 0;
    const colorClass = isUp ? 'text-up' : 'text-down';
    document.getElementById('modal-title').innerText = item.name;
    const html = `
        <div class="mb-3"><span class="badge badge-${item.grade} me-1">${item.grade}-Tier</span><span class="badge bg-light text-dark border">${item.sector}</span></div>
        <div class="card bg-light border-0 mb-3"><div class="card-body py-2"><div class="d-flex justify-content-between mb-1"><span class="text-muted">현재가</span><strong class="${colorClass}">${item.close.toLocaleString()} (${item.change}%)</strong></div><div class="d-flex justify-content-between"><span class="text-muted">상태</span><strong class="${item.action === 'READY' ? 'text-success' : 'text-dark'}">${item.action}</strong></div></div></div>
        <h6 class="fw-bold small text-muted border-bottom pb-1 mb-2">ANALYSIS (WHY)</h6><ul class="small text-secondary ps-3 mb-4">${item.why.map(w => `<li>${w}</li>`).join('')}</ul>
        <h6 class="fw-bold small text-muted border-bottom pb-1 mb-2">TRADING PLAN</h6>
        <div class="row g-2 text-center"><div class="col-4"><div class="border rounded p-2 bg-white h-100"><small class="d-block text-muted" style="font-size:0.7rem">ENTRY</small><strong class="text-primary">${item.entry.price.toLocaleString()}</strong></div></div><div class="col-4"><div class="border rounded p-2 bg-white h-100"><small class="d-block text-muted" style="font-size:0.7rem">STOP</small><strong class="text-danger">${item.stop.price.toLocaleString()}</strong></div></div><div class="col-4"><div class="border rounded p-2 bg-white h-100"><small class="d-block text-muted" style="font-size:0.7rem">TARGET</small><strong class="text-success">${item.target.price.toLocaleString()}</strong></div></div></div>
    `;
    document.getElementById('modal-body').innerHTML = html;
    new bootstrap.Modal(document.getElementById('detailModal')).show();
};

document.addEventListener('DOMContentLoaded', initDashboard);
