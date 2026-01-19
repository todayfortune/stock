// assets/app.js

// 상태 관리
const state = {
    loading: true,
    error: null,
    meta: null,
    sectors: [],
    watchlist: []
};

// [핵심 1] GitHub Pages 경로 문제 해결 (Base Path 감지)
function getBasePath() {
    const path = window.location.pathname;
    // 로컬이나 루트면 빈 문자열, 아니면 /repo-name/ 반환
    return path === '/' ? '' : path.replace(/\/$/, '') + '/';
}

// 1. 초기화
async function initDashboard() {
    renderLoading(true);
    const BASE = getBasePath();
    console.log("Current Base Path:", BASE); // 디버깅용

    try {
        // [핵심 1] 절대 경로 대신 상대 경로 + Base Path 조합 사용
        const [metaRes, sectorsRes, watchRes] = await Promise.all([
            fetch(`${BASE}data/meta.json`),
            fetch(`${BASE}data/sector_leaders.json`),
            fetch(`${BASE}data/watchlist.json`)
        ]);

        if (!metaRes.ok || !sectorsRes.ok || !watchRes.ok) {
            throw new Error(`데이터를 불러올 수 없습니다. (Path: ${BASE}data/...)`);
        }

        state.meta = await metaRes.json();
        const sectorData = await sectorsRes.json();
        state.sectors = sectorData.items || [];
        const watchData = await watchRes.json();
        state.watchlist = watchData.items || [];

        // [핵심 3] 정렬 로직 개선: Grade(S>A>B) -> Action(READY>WAIT) -> Volume
        const gradeWeight = { 'S': 3, 'A': 2, 'B': 1, 'C': 0 };
        const actionWeight = { 'READY': 2, 'WAIT': 1, 'NO_TRADE': 0 };

        state.watchlist.sort((a, b) => {
            const gradeDiff = gradeWeight[b.grade] - gradeWeight[a.grade];
            if (gradeDiff !== 0) return gradeDiff;
            
            const actionDiff = actionWeight[b.action] - actionWeight[a.action];
            if (actionDiff !== 0) return actionDiff;

            return b.volume - a.volume;
        });

        renderDashboard();

    } catch (err) {
        console.error(err);
        renderError(err.message);
    } finally {
        renderLoading(false);
    }
}

// 2. 화면 그리기
function renderDashboard() {
    if(state.meta) {
        const updateTime = new Date(state.meta.asOf).toLocaleString('ko-KR');
        document.getElementById('last-updated').innerText = `Last Updated: ${updateTime}`;
        document.getElementById('market-status').innerText = `Ver: ${state.meta.version}`;
    }

    // 섹터 리스트
    const sectorContainer = document.getElementById('sector-list');
    sectorContainer.innerHTML = '';
    
    state.sectors.slice(0, 5).forEach((sec, idx) => {
        const html = `
            <div class="sector-card">
                <span class="sector-rank">#${idx + 1}</span>
                <span class="sector-name">${sec.sector}</span>
                <div class="sector-score">
                    Score: ${sec.score}<br>
                    <small>${sec.topTickers[0] || '-'}</small>
                </div>
            </div>
        `;
        sectorContainer.innerHTML += html;
    });

    // 워치리스트
    const watchContainer = document.getElementById('candidate-list');
    watchContainer.innerHTML = '';

    state.watchlist.forEach((item, index) => {
        const price = item.close.toLocaleString();
        const vol = Math.round(item.volume / 100000000).toLocaleString();
        const changeClass = item.change > 0 ? 'text-red' : 'text-blue';
        
        // [핵심 2] Grade & Action 시각화
        let gradeBadge = `<span class="badge secondary">${item.grade}</span>`;
        if (item.grade === 'S') gradeBadge = `<span class="badge" style="background:#ef4444;">S-Tier</span>`;
        else if (item.grade === 'A') gradeBadge = `<span class="badge" style="background:#f59e0b;">A-Tier</span>`;

        let actionBadge = `<span class="badge secondary">${item.action}</span>`;
        if (item.action === 'READY') actionBadge = `<span class="badge" style="background:#10b981; font-weight:bold;">⚡ READY</span>`;
        
        const whyBadges = item.why.slice(0, 2).map(w => `<span class="badge secondary" style="font-size:0.6rem;">${w}</span>`).join(' ');

        const html = `
            <article class="stock-card" onclick="openModal(${index})" style="cursor:pointer; border-left: 5px solid ${item.action === 'READY' ? '#10b981' : 'transparent'};">
                <div class="card-header">
                    <div>
                        <span class="stock-name">${item.name}</span>
                        <span class="stock-code">${item.ticker}</span>
                    </div>
                    <div>
                        ${gradeBadge}
                        ${actionBadge}
                    </div>
                </div>
                <div class="card-body">
                    <div class="price-box">
                        <span class="current-price">${price}</span>
                        <span class="price-change ${changeClass}">${item.change > 0 ? '+' : ''}${item.change}%</span>
                    </div>
                    <div style="text-align:right;">
                        <span style="font-size:0.8rem; color:#666;">거래대금 ${vol}억</span>
                    </div>
                </div>
                <div style="margin-top: 10px;">
                    ${whyBadges}
                </div>
            </article>
        `;
        watchContainer.innerHTML += html;
    });
}

// 3. 모달 기능 (기존과 동일, 데이터 바인딩만 확실하게)
function openModal(index) {
    const item = state.watchlist[index];
    if (!item) return;

    document.getElementById('modal-title').innerText = item.name;
    document.getElementById('modal-subtitle').innerText = `${item.ticker} | ${item.sector}`;

    const whyList = document.getElementById('modal-why-list');
    whyList.innerHTML = '';
    item.why.forEach(reason => {
        const li = document.createElement('li');
        li.innerText = reason;
        whyList.appendChild(li);
    });

    document.getElementById('modal-entry').innerText = item.entry.price.toLocaleString();
    document.getElementById('modal-stop').innerText = item.stop.price.toLocaleString();
    document.getElementById('modal-target').innerText = item.target.price.toLocaleString();
    document.getElementById('modal-rr').innerText = `1 : ${item.target.rr}`;

    const modal = document.getElementById('modal-detail');
    modal.setAttribute('open', true);
}

function closeModal() {
    document.getElementById('modal-detail').removeAttribute('open');
}

// 유틸리티
function renderLoading(isLoading) {
    const loader = document.getElementById('loader');
    const content = document.getElementById('main-content');
    if (isLoading) {
        loader.style.display = 'block';
        content.style.display = 'none';
    } else {
        loader.style.display = 'none';
        content.style.display = 'block';
    }
}

function renderError(msg) {
    const container = document.getElementById('error-container');
    container.style.display = 'block';
    container.innerHTML = `
        <strong>⚠️ 시스템 알림</strong><br>
        ${msg}<br>
        <button onclick="location.reload()" style="margin-top:10px; padding:5px 10px;">새로고침</button>
    `;
}

document.addEventListener('DOMContentLoaded', initDashboard);
