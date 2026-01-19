// assets/app.js

const state = { meta: null, sectors: [], watchlist: [] };

function getBasePath() {
    const path = window.location.pathname;
    return path === '/' ? '' : path.replace(/\/$/, '') + '/';
}

async function initDashboard() {
    const BASE = getBasePath();
    try {
        const [metaRes, sectorsRes, watchRes] = await Promise.all([
            fetch(`${BASE}data/meta.json`),
            fetch(`${BASE}data/sector_leaders.json`),
            fetch(`${BASE}data/watchlist.json`)
        ]);

        if (!metaRes.ok) throw new Error("Data Load Error");

        state.meta = await metaRes.json();
        const sData = await sectorsRes.json();
        state.sectors = sData.items || [];
        const wData = await watchRes.json();
        state.watchlist = wData.items || [];

        // 정렬: Action > Grade > Volume
        const gradeWeight = { 'S': 3, 'A': 2, 'B': 1, 'C': 0 };
        const actionWeight = { 'READY': 2, 'WAIT': 1, 'NO_TRADE': 0 };
        state.watchlist.sort((a, b) => {
            const ad = actionWeight[b.action] - actionWeight[a.action];
            if (ad !== 0) return ad;
            const gd = gradeWeight[b.grade] - gradeWeight[a.grade];
            if (gd !== 0) return gd;
            return b.volume - a.volume;
        });

        renderDashboard();

    } catch (err) {
        console.error(err);
        document.getElementById('mobile-card-list').innerHTML = `<div class="alert alert-danger">데이터 로딩 실패</div>`;
    }
}

function renderDashboard() {
    // 1. 시장 상태 뱃지
    const mkt = state.meta.market;
    const badge = document.getElementById('market-badge');
    if (mkt && mkt.state === 'RISK_ON') {
        badge.className = 'badge bg-success';
        badge.innerText = 'RISK ON';
    } else {
        badge.className = 'badge bg-danger';
        badge.innerText = 'RISK OFF';
    }
    document.getElementById('update-time').innerText = new Date(state.meta.asOf).toLocaleString('ko-KR');

    // 2. 섹터 (Top 3)
    const secArea = document.getElementById('sector-area');
    secArea.innerHTML = '';
    state.sectors.slice(0, 3).forEach(sec => {
        secArea.innerHTML += `
            <div class="col-md-4 col-12">
                <div class="card border-0 shadow-sm h-100" style="background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);">
                    <div class="card-body">
                        <small class="text-muted fw-bold text-uppercase">${sec.sector}</small>
                        <h5 class="fw-bold mt-1 mb-0">${sec.topTickers[0]}</h5>
                        <div class="mt-2 d-flex justify-content-between">
                            <span class="badge bg-light text-dark border">Score: ${sec.score}</span>
                            <small class="text-muted">${(sec.turnover/1e8).toFixed(0)}억</small>
                        </div>
                    </div>
                </div>
            </div>
        `;
    });

    // 3. 워치리스트 렌더링 (PC용 테이블 + 모바일용 카드)
    const pcTable = document.getElementById('desktop-table-body');
    const mobileList = document.getElementById('mobile-card-list');
    
    pcTable.innerHTML = '';
    mobileList.innerHTML = '';

    state.watchlist.forEach((item, idx) => {
        const isUp = item.change > 0;
        const colorClass = isUp ? 'text-up' : 'text-down'; // 빨강/파랑
        const sign = isUp ? '+' : '';
        const vol = Math.round(item.volume / 1e8).toLocaleString();
        
        // --- A. 데스크톱 테이블 행 (TR) ---
        const tr = `
            <tr style="cursor:pointer" onclick="showDetail(${idx})">
                <td class="ps-4">
                    <div class="fw-bold">${item.name}</div>
                    <small class="text-muted">${item.ticker}</small>
                </td>
                <td class="fw-bold">${item.close.toLocaleString()}</td>
                <td class="${colorClass}">${sign}${item.change}%</td>
                <td><span class="badge badge-${item.grade}">${item.grade}</span></td>
                <td><span class="badge action-${item.action}">${item.action}</span></td>
                <td><small class="text-muted">${item.why[0] || '-'}</small></td>
                <td>
                    <small>Entry: <b>${item.entry.price.toLocaleString()}</b></small>
                </td>
            </tr>
        `;
        pcTable.innerHTML += tr;

        // --- B. 모바일 카드 (DIV) ---
        // 모바일에서는 중요한 정보만 크게 보여줌
        const card = `
            <div class="mobile-card" onclick="showDetail(${idx})" style="border-left: 5px solid ${item.action === 'READY' ? '#198754' : '#ccc'}">
                <div class="d-flex justify-content-between align-items-start mb-2">
                    <div>
                        <h5 class="fw-bold mb-0">${item.name}</h5>
                        <small class="text-muted">${item.ticker} | ${item.sector}</small>
                    </div>
                    <div class="text-end">
                        <span class="badge badge-${item.grade} mb-1">${item.grade}</span><br>
                        <span class="badge action-${item.action}">${item.action}</span>
                    </div>
                </div>
                
                <div class="d-flex justify-content-between align-items-end">
                    <div>
                        <div class="fs-4 fw-bold text-dark">${item.close.toLocaleString()}</div>
                        <div class="small ${colorClass}">${sign}${item.change}%</div>
                    </div>
                    <div class="text-end">
                        <small class="text-muted d-block">거래대금 ${vol}억</small>
                        <small class="text-primary fw-bold" style="font-size:0.8rem;">터치하여 전략 보기 <i class="fas fa-chevron-right"></i></small>
                    </div>
                </div>
            </div>
        `;
        mobileList.innerHTML += card;
    });
}

// 상세 모달 열기
window.showDetail = function(idx) {
    const item = state.watchlist[idx];
    const isUp = item.change > 0;
    const colorClass = isUp ? 'text-up' : 'text-down';

    document.getElementById('modal-title').innerText = item.name;
    
    const html = `
        <div class="mb-3">
            <span class="badge badge-${item.grade} me-1">${item.grade}-Tier</span>
            <span class="badge bg-light text-dark border">${item.sector}</span>
        </div>

        <div class="card bg-light border-0 mb-3">
            <div class="card-body py-2">
                <div class="d-flex justify-content-between mb-1">
                    <span class="text-muted">현재가</span>
                    <strong class="${colorClass}">${item.close.toLocaleString()} (${item.change}%)</strong>
                </div>
                <div class="d-flex justify-content-between">
                    <span class="text-muted">상태</span>
                    <strong class="${item.action === 'READY' ? 'text-success' : 'text-warning'}">${item.action}</strong>
                </div>
            </div>
        </div>

        <h6 class="fw-bold small text-muted border-bottom pb-1">ANALYSIS (WHY)</h6>
        <ul class="small text-muted ps-3 mb-4">
            ${item.why.map(w => `<li>${w}</li>`).join('')}
        </ul>

        <h6 class="fw-bold small text-muted border-bottom pb-1">TRADING PLAN</h6>
        <div class="row g-2 text-center mt-1">
            <div class="col-4">
                <div class="border rounded p-2 bg-white">
                    <small class="d-block text-muted" style="font-size:0.7rem">ENTRY</small>
                    <strong class="text-primary">${item.entry.price.toLocaleString()}</strong>
                </div>
            </div>
            <div class="col-4">
                <div class="border rounded p-2 bg-white">
                    <small class="d-block text-muted" style="font-size:0.7rem">STOP</small>
                    <strong class="text-danger">${item.stop.price.toLocaleString()}</strong>
                </div>
            </div>
            <div class="col-4">
                <div class="border rounded p-2 bg-white">
                    <small class="d-block text-muted" style="font-size:0.7rem">TARGET</small>
                    <strong class="text-success">${item.target.price.toLocaleString()}</strong>
                </div>
            </div>
        </div>
        <div class="text-center mt-2">
            <small class="text-muted">손익비 (R:R) 1 : ${item.target.rr}</small>
        </div>
    `;
    
    document.getElementById('modal-body').innerHTML = html;
    new bootstrap.Modal(document.getElementById('detailModal')).show();
};

document.addEventListener('DOMContentLoaded', initDashboard);
