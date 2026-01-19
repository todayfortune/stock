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

        if (!metaRes.ok) throw new Error("Data Load Failed");

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

        renderAll();

    } catch (err) {
        console.error(err);
        document.getElementById('watchlist-body').innerHTML = `<tr><td colspan="6" class="text-center text-danger py-4">데이터 로딩 실패 (GitHub Actions 확인)</td></tr>`;
    }
}

function renderAll() {
    // 1. 헤더 (시장 상태)
    const mkt = state.meta.market;
    const badge = document.getElementById('market-regime-badge');
    if (mkt && mkt.state === 'RISK_ON') {
        badge.className = 'badge bg-success me-2';
        badge.innerHTML = `<i class="fas fa-check-circle me-1"></i>RISK ON`;
    } else {
        badge.className = 'badge bg-danger me-2';
        badge.innerHTML = `<i class="fas fa-ban me-1"></i>RISK OFF`;
    }
    document.getElementById('last-updated').innerText = new Date(state.meta.asOf).toLocaleString('ko-KR');

    // 2. 섹터 카드 (Top 3)
    const secRow = document.getElementById('sector-row');
    secRow.innerHTML = '';
    state.sectors.slice(0, 3).forEach(sec => {
        const html = `
            <div class="col-md-4">
                <div class="card h-100 border-start border-4 border-primary">
                    <div class="card-body">
                        <div class="d-flex justify-content-between align-items-center mb-2">
                            <h6 class="card-title mb-0 fw-bold text-uppercase text-muted">${sec.sector}</h6>
                            <span class="badge bg-light text-primary rounded-pill">Score: ${sec.score}</span>
                        </div>
                        <h4 class="mb-0 fw-bold">${sec.topTickers[0]}</h4>
                        <small class="text-muted">Volume: ${(sec.turnover/100000000).toFixed(0)}억</small>
                    </div>
                </div>
            </div>
        `;
        secRow.innerHTML += html;
    });

    // 3. 워치리스트 테이블
    const tbody = document.getElementById('watchlist-body');
    tbody.innerHTML = '';
    
    state.watchlist.forEach((item, idx) => {
        const isUp = item.change > 0;
        const colorClass = isUp ? 'text-up' : 'text-down';
        const sign = isUp ? '+' : '';
        const vol = Math.round(item.volume / 100000000).toLocaleString();

        let actionClass = 'bg-secondary';
        if (item.action === 'READY') actionClass = 'badge-action-READY';
        if (item.action === 'WAIT') actionClass = 'badge-action-WAIT';

        const row = `
            <tr style="cursor:pointer;" onclick="showDetail(${idx})">
                <td>
                    <div class="fw-bold">${item.name}</div>
                    <div class="small text-muted">${item.ticker}</div>
                </td>
                <td class="fw-bold">${item.close.toLocaleString()}</td>
                <td class="${colorClass}">${sign}${item.change}%</td>
                <td><span class="badge badge-grade-${item.grade}">${item.grade}</span></td>
                <td><span class="badge ${actionClass}">${item.action}</span></td>
                <td><small class="text-truncate d-block" style="max-width: 150px;">${item.why[0] || '-'}</small></td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// 상세 정보 보기 (우측 패널 or 모달)
window.showDetail = function(idx) {
    const item = state.watchlist[idx];
    const html = `
        <h4 class="fw-bold mb-1">${item.name} <span class="fs-6 text-muted">${item.ticker}</span></h4>
        <span class="badge bg-light text-dark mb-3 border">${item.sector}</span>
        
        <div class="alert ${item.action === 'READY' ? 'alert-success' : 'alert-secondary'} mb-3">
            <div class="d-flex justify-content-between">
                <strong>Signal:</strong> <span>${item.action}</span>
            </div>
            <div class="d-flex justify-content-between">
                <strong>Grade:</strong> <span>${item.grade}-Tier</span>
            </div>
        </div>

        <h6 class="fw-bold border-bottom pb-2"><i class="fas fa-clipboard-check me-2"></i>Logic (Why)</h6>
        <ul class="small text-muted mb-4 ps-3">
            ${item.why.map(w => `<li>${w}</li>`).join('')}
        </ul>

        <h6 class="fw-bold border-bottom pb-2"><i class="fas fa-crosshairs me-2"></i>Trading Plan</h6>
        <div class="row g-2 text-center small mb-3">
            <div class="col-4"><div class="p-2 border rounded bg-light"><strong>Entry</strong><br>${item.entry.price.toLocaleString()}</div></div>
            <div class="col-4"><div class="p-2 border rounded bg-light text-danger"><strong>Stop</strong><br>${item.stop.price.toLocaleString()}</div></div>
            <div class="col-4"><div class="p-2 border rounded bg-light text-success"><strong>Target</strong><br>${item.target.price.toLocaleString()}</div></div>
        </div>
        <div class="text-center small text-muted">R:R Ratio 1 : ${item.target.rr}</div>
    `;

    // 데스크탑이면 우측 패널에, 모바일이면 모달에 표시
    if (window.innerWidth >= 992) {
        document.getElementById('detail-panel').innerHTML = html;
    } else {
        document.getElementById('mobile-detail-body').innerHTML = html;
        new bootstrap.Modal(document.getElementById('mobileModal')).show();
    }
};

document.addEventListener('DOMContentLoaded', initDashboard);
