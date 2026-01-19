// assets/app.js

// 상태 관리
const state = {
    loading: true,
    error: null,
    meta: null,
    sectors: [],
    watchlist: []
};

// 1. 초기화
async function initDashboard() {
    renderLoading(true);
    
    try {
        // 데이터 가져오기
        const [metaRes, sectorsRes, watchRes] = await Promise.all([
            fetch('data/meta.json'),
            fetch('data/sector_leaders.json'),
            fetch('data/watchlist.json')
        ]);

        if (!metaRes.ok || !sectorsRes.ok || !watchRes.ok) {
            throw new Error("데이터 파일을 찾을 수 없습니다.");
        }

        state.meta = await metaRes.json();
        const sectorData = await sectorsRes.json();
        state.sectors = sectorData.items || [];
        const watchData = await watchRes.json();
        state.watchlist = watchData.items || [];

        renderDashboard();

    } catch (err) {
        renderError(err.message);
    } finally {
        renderLoading(false);
    }
}

// 2. 화면 그리기
function renderDashboard() {
    // 업데이트 시간
    if(state.meta) {
        const updateTime = new Date(state.meta.asOf).toLocaleString('ko-KR');
        document.getElementById('last-updated').innerText = `Last Updated: ${updateTime}`;
        document.getElementById('market-status').innerText = `Status: ${state.meta.status}`;
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

    // 워치리스트 (클릭 이벤트 추가됨!)
    const watchContainer = document.getElementById('candidate-list');
    watchContainer.innerHTML = '';

    state.watchlist.forEach((item, index) => {
        const price = item.close.toLocaleString();
        const vol = Math.round(item.volume / 100000000).toLocaleString();
        const changeClass = item.change > 0 ? 'text-red' : 'text-blue';
        const changeSign = item.change > 0 ? '+' : '';
        const whyBadges = item.why.slice(0, 2).map(w => `<span class="badge secondary">${w}</span>`).join(' ');

        // onclick="openModal(${index})" 추가됨
        const html = `
            <article class="stock-card" onclick="openModal(${index})" style="cursor:pointer;">
                <div class="card-header">
                    <div>
                        <span class="stock-name">${item.name}</span>
                        <span class="stock-code">${item.ticker}</span>
                    </div>
                    <span class="badge primary">${item.state}</span>
                </div>
                <div class="card-body">
                    <div class="price-box">
                        <span class="current-price">${price}</span>
                        <span class="price-change ${changeClass}">${changeSign}${item.change}%</span>
                    </div>
                    <div style="text-align:right;">
                        <span style="font-size:0.8rem; color:#666;">거래대금 ${vol}억</span>
                    </div>
                </div>
                <div style="margin-top: 10px;">
                    ${whyBadges} <span style="font-size:0.8rem; color:#aaa;">+더보기</span>
                </div>
            </article>
        `;
        watchContainer.innerHTML += html;
    });
}

// 3. 모달(팝업) 기능
function openModal(index) {
    const item = state.watchlist[index];
    if (!item) return;

    // 제목 설정
    document.getElementById('modal-title').innerText = item.name;
    document.getElementById('modal-subtitle').innerText = `${item.ticker} | ${item.sector}`;

    // Why 리스트 채우기
    const whyList = document.getElementById('modal-why-list');
    whyList.innerHTML = '';
    item.why.forEach(reason => {
        const li = document.createElement('li');
        li.innerText = reason;
        whyList.appendChild(li);
    });

    // Plan 채우기
    document.getElementById('modal-entry').innerText = item.entry.price.toLocaleString();
    document.getElementById('modal-stop').innerText = item.stop.price.toLocaleString();
    document.getElementById('modal-target').innerText = item.target.price.toLocaleString();
    document.getElementById('modal-rr').innerText = `1 : ${item.target.rr}`;

    // 모달 열기
    const modal = document.getElementById('modal-detail');
    modal.setAttribute('open', true);
}

function closeModal() {
    const modal = document.getElementById('modal-detail');
    modal.removeAttribute('open');
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
    container.innerText = `⚠️ ${msg}`;
}

// 초기화 실행
document.addEventListener('DOMContentLoaded', initDashboard);
