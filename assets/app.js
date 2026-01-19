// assets/app.js

// 상태 관리
const state = {
    loading: true,
    error: null,
    meta: null,
    sectors: [],
    watchlist: []
};

// 초기화
async function initDashboard() {
    renderLoading(true);
    
    try {
        // 병렬 요청으로 속도 향상
        const [metaRes, sectorsRes, watchRes] = await Promise.all([
            fetch('data/meta.json'),
            fetch('data/sector_leaders.json'),
            fetch('data/watchlist.json')
        ]);

        if (!metaRes.ok || !sectorsRes.ok || !watchRes.ok) {
            throw new Error("데이터 파일을 찾을 수 없습니다. (GitHub Actions 확인 필요)");
        }

        state.meta = await metaRes.json();
        const sectorData = await sectorsRes.json();
        state.sectors = sectorData.items || [];
        const watchData = await watchRes.json();
        state.watchlist = watchData.items || [];
        
        // 데이터가 비어있을 경우 처리
        if (state.watchlist.length === 0) {
            throw new Error("수집된 데이터가 없습니다. (장 마감 후 업데이트 대기)");
        }

        renderDashboard();

    } catch (err) {
        state.error = err.message;
        renderError(err.message);
    } finally {
        state.loading = false;
        renderLoading(false);
    }
}

// 렌더링: 로딩 상태
function renderLoading(isLoading) {
    const loader = document.getElementById('loader');
    const content = document.getElementById('main-content');
    
    if (isLoading) {
        loader.style.display = 'block';
        content.style.display = 'none';
    } else {
        loader.style.display = 'none';
        // 에러가 없을 때만 콘텐츠 표시
        if (!state.error) content.style.display = 'block';
    }
}

// 렌더링: 에러 상태
function renderError(msg) {
    const container = document.getElementById('error-container');
    container.style.display = 'block';
    container.innerHTML = `
        <article style="background-color: #fee2e2; color: #991b1b; border: 1px solid #f87171; padding: 20px; border-radius: 8px;">
            <strong>⚠️ 시스템 알림</strong><br>
            ${msg}<br>
            <small>잠시 후 다시 접속하거나, GitHub Actions 로그를 확인해주세요.</small>
        </article>
    `;
}

// 렌더링: 메인 대시보드
function renderDashboard() {
    // 1. 메타 정보
    const updateTime = new Date(state.meta.asOf).toLocaleString('ko-KR');
    document.getElementById('last-updated').innerText = `Last Updated: ${updateTime}`;
    document.getElementById('market-status').innerText = `Status: ${state.meta.status}`;

    // 2. 섹터 리스트
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

    // 3. 워치리스트
    const watchContainer = document.getElementById('candidate-list');
    watchContainer.innerHTML = '';

    state.watchlist.forEach(item => {
        const price = item.close.toLocaleString();
        const vol = Math.round(item.volume / 100000000).toLocaleString();
        const changeClass = item.change > 0 ? 'text-red' : 'text-blue';
        const changeSign = item.change > 0 ? '+' : '';
        
        // Why 뱃지 생성
        const whyBadges = item.why.map(w => `<span class="badge secondary">${w}</span>`).join(' ');

        // Plan 정보 (Entry/Stop/Target)
        const planHtml = `
            <div style="font-size: 0.75rem; color: #666; margin-top: 5px;">
                <strong>Plan:</strong> 
                Entry ${item.entry.price.toLocaleString()} | 
                Stop <span style="color:red">${item.stop.price.toLocaleString()}</span> | 
                Target <span style="color:green">${item.target.price.toLocaleString()}</span> 
                (R:R ${item.target.rr})
            </div>
        `;

        const html = `
            <article class="stock-card">
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
                ${planHtml}
                <div style="margin-top: 10px;">
                    ${whyBadges}
                </div>
            </article>
        `;
        watchContainer.innerHTML += html;
    });
}

// 실행
document.addEventListener('DOMContentLoaded', initDashboard);
