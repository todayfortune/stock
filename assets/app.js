// assets/app.js v3.1

const state = { meta: null, sectors: [], watchlist: [] };

// [중요] 경로 자동 인식 함수 (index.html 제거 로직 추가)
function getBasePath() {
    let path = window.location.pathname;
    // 파일명(.html)으로 끝나는 경우 제거
    if (path.endsWith('.html')) {
        path = path.substring(0, path.lastIndexOf('/'));
    }
    // 끝에 슬래시 보장
    return path.endsWith('/') ? path : path + '/';
}

async function initDashboard() {
    const BASE = getBasePath();
    console.log("API Base Path:", BASE); // 디버깅용

    try {
        const [metaRes, sectorsRes, watchRes] = await Promise.all([
            fetch(`${BASE}data/meta.json`),
            fetch(`${BASE}data/sector_leaders.json`),
            fetch(`${BASE}data/watchlist.json`)
        ]);

        if (!metaRes.ok || !sectorsRes.ok || !watchRes.ok) {
            throw new Error(`Data Load Failed (Path: ${BASE})`);
        }

        state.meta = await metaRes.json();
        const sData = await sectorsRes.json();
        state.sectors = sData.items || [];
        const wData = await watchRes.json();
        state.watchlist = wData.items || [];

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
        const msg = `<div class="alert alert-danger m-3">⚠️ 데이터 로딩 실패<br><small>${err.message}</small></div>`;
        const mobList = document.getElementById('mobile-card-list');
        const pcTable = document.getElementById('desktop-table-body');
        
        if (mobList) mobList.innerHTML = msg;
        if (pcTable) pcTable.innerHTML = `<tr><td colspan="7">${msg}</td></tr>`;
        
        document.getElementById('market-badge').innerText = "Error";
        document.getElementById('market-badge').className = "badge bg-danger";
    }
}

function renderDashboard() {
    // 1. 시장 상태
    const mkt = state.meta.market;
    const badge = document.getElementById('market-badge');
    if (mkt && mkt.state === 'RISK_ON') {
        badge.className = 'badge bg-success';
        badge.innerHTML = '<i class="fas fa-check-circle me-1"></i>RISK ON';
    } else {
        badge.className = 'badge bg-danger';
        badge.innerHTML = '<i class="fas fa-ban me-1"></i>RISK OFF';
    }
    
    // 시간
    const dateObj = new Date(state.meta.asOf);
    document.getElementById('update-time').innerText = dateObj.toLocaleString('ko-KR');

    // 2. 섹터
    const secArea = document.getElementById('sector-area');
    if (secArea) {
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
    }

    // 3. 리스트 (PC & Mobile)
    const pcTable = document.getElementById('desktop-table-body');
    const mobList = document.getElementById('mobile-card-list');
    
    if (pcTable) pcTable.innerHTML = '';
    if (mobList) mobList.innerHTML = '';

    if (state.watchlist.length === 0) {
        const emptyMsg = '<div class="text-center p-4 text-muted">감지된 종목이 없습니다.</div>';
        if (mobList) mobList.innerHTML = emptyMsg;
        if (pcTable) pcTable.innerHTML = `<tr><td colspan="7">${emptyMsg}</td></tr>`;
        return;
    }

    state.watchlist.forEach((item, idx) => {
        const isUp = item.change > 0;
        const colorClass = isUp ? 'text-up' : 'text-down';
        const sign = isUp ? '+' : '';
        const vol = Math.round(item.volume / 1e8).toLocaleString();

        // PC Row
        if (pcTable) {
            pcTable.innerHTML += `
                <tr style="cursor:pointer" onclick="showDetail(${idx})">
                    <td class="ps-4">
                        <div class="fw-bold text-dark">${item.name}</div>
                        <small class="text-muted">${item.ticker}</small>
                    </td>
                    <td class="fw-bold">${item.close.toLocaleString()}</td>
                    <td class="${colorClass}">${sign}${item.change}%</td>
                    <td><span class="badge badge-${item.grade}">${item.grade}</span></td>
                    <td><span class="badge action-${item.action}">${item.action}</span></td>
                    <td><small class="text-muted text-truncate d-block" style="max-width:150px">${item.why[0] || '-'}</small></td>
                    <td><small>Entry <b>${item.entry.price.toLocaleString()}</b></small></td>
                </tr>
            `;
        }

        // Mobile Card
        if (mobList) {
            mobList.innerHTML += `
                <div class="mobile-card" onclick="showDetail(${idx})" style="border-left: 5px solid ${item.action === 'READY' ? '#198754' : '#dee2e6'}">
                    <div class="d-flex justify-content-between mb-2">
                        <div>
                            <span class="fw-bold text-dark" style="font-size:1.1rem">${item.name}</span>
                            <small class="text-muted ms-1">${item.sector}</small>
                        </div>
                        <div>
                            <span class="badge badge-${item.grade} me-1">${item.grade}</span>
                            <span class="badge action-${item.action}">${item.action}</span>
                        </div>
                    </div>
                    <div class="d-flex justify-content-between align-items-end">
                        <div>
                            <div class="fw-bold fs-4 text-dark" style="line-height:1">${item.close.toLocaleString()}</div>
                            <small class="${colorClass} fw-bold">${sign}${item.change}%</small>
                        </div>
                        <div class="text-end">
                            <small class="text-muted d-block" style="font-size:0.8rem">거래대금 ${vol}억</small>
                            <small class="text-primary fw-bold" style="font-size:0.8rem">전략 보기 <i class="fas fa-arrow-right"></i></small>
                        </div>
                    </div>
                </div>
            `;
        }
    });
}

// 상세 모달
window.showDetail = function(idx) {
    const item = state.watchlist[idx];
    const isUp = item.change > 0;
    const colorClass = isUp ? 'text-up' : 'text-down';

    document.getElementById('modal-title').innerText = item.name;
    
    const html = `
        <div class="d-flex align-items-center mb-3">
            <span class="badge badge-${item.grade} me-2">${item.grade}-Tier</span>
            <span class="badge bg-light text-dark border me-auto">${item.sector}</span>
            <span class="small text-muted">${item.ticker}</span>
        </div>

        <div class="card bg-light border-0 mb-3">
            <div class="card-body py-2">
                <div class="d-flex justify-content-between mb-1">
                    <span class="text-muted">현재가</span>
                    <strong class="${colorClass}">${item.close.toLocaleString()} (${item.change}%)</strong>
                </div>
                <div class="d-flex justify-content-between">
                    <span class="text-muted">상태</span>
                    <strong class="${item.action === 'READY' ? 'text-success' : 'text-dark'}">${item.action}</strong>
                </div>
            </div>
        </div>

        <h6 class="fw-bold small text-muted border-bottom pb-1 mb-2">ANALYSIS (WHY)</h6>
        <ul class="small text-secondary ps-3 mb-4">
            ${item.why.map(w => `<li>${w}</li>`).join('')}
        </ul>

        <h6 class="fw-bold small text-muted border-bottom pb-1 mb-2">TRADING PLAN</h6>
        <div class="row g-2 text-center">
            <div class="col-4">
                <div class="border rounded p-2 bg-white h-100">
                    <small class="d-block text-muted" style="font-size:0.7rem">ENTRY</small>
                    <strong class="text-primary">${item.entry.price.toLocaleString()}</strong>
                </div>
            </div>
            <div class="col-4">
                <div class="border rounded p-2 bg-white h-100">
                    <small class="d-block text-muted" style="font-size:0.7rem">STOP</small>
                    <strong class="text-danger">${item.stop.price.toLocaleString()}</strong>
                </div>
            </div>
            <div class="col-4">
                <div class="border rounded p-2 bg-white h-100">
                    <small class="d-block text-muted" style="font-size:0.7rem">TARGET</small>
                    <strong class="text-success">${item.target.price.toLocaleString()}</strong>
                </div>
            </div>
        </div>
        <div class="text-center mt-2">
            <small class="text-muted">Risk Reward Ratio 1 : ${item.target.rr}</small>
        </div>
    `;
    
    document.getElementById('modal-body').innerHTML = html;
    new bootstrap.Modal(document.getElementById('detailModal')).show();
};

document.addEventListener('DOMContentLoaded', initDashboard);
