// assets/app.js

async function loadDashboard() {
    try {
        // 1. 요약 정보
        const summaryRes = await fetch('data/summary.json?v=' + new Date().getTime()); // 캐시 방지
        const summary = await summaryRes.json();
        document.getElementById('update-time').innerText = `Updated: ${summary.updated_at} (${summary.market_status})`;

        // 2. 섹터 정보
        const sectorsRes = await fetch('data/sectors.json?v=' + new Date().getTime());
        const sectors = await sectorsRes.json();
        renderSectors(sectors);

        // 3. 후보 종목 정보
        const candidatesRes = await fetch('data/candidates.json?v=' + new Date().getTime());
        const candidates = await candidatesRes.json();
        renderCandidates(candidates);

    } catch (error) {
        console.error("Data Load Error:", error);
        document.getElementById('sector-list').innerHTML = `<article class="sector-card">⚠️ 데이터 로딩 실패. GitHub Actions 로그를 확인하세요.</article>`;
    }
}

function renderSectors(sectors) {
    const container = document.getElementById('sector-list');
    container.innerHTML = '';

    // 상위 3개 섹터 카드 생성
    sectors.slice(0, 3).forEach(sector => {
        const html = `
            <article class="sector-card">
                <header>
                    <strong>${sector.name}</strong> 
                    <span style="float:right; color:#26a69a; font-weight:bold;">Total: ${sector.msi_score}</span>
                </header>
                <div class="score-box">
                    <div class="score-item">
                        <span class="small-meta">💰 Flow</span>
                        <strong>${sector.flow_score}</strong>
                    </div>
                    <div class="score-item">
                        <span class="small-meta">📈 Trend</span>
                        <strong>${sector.trend_score}</strong>
                    </div>
                    <div class="score-item">
                        <span class="small-meta">🌊 Breadth</span>
                        <strong>${sector.breadth_score}%</strong>
                    </div>
                </div>
                <footer>
                    <small>👑 대장: ${sector.leader_name}</small>
                </footer>
            </article>
        `;
        container.innerHTML += html;
    });
}

function renderCandidates(candidates) {
    const tbody = document.getElementById('candidate-list');
    tbody.innerHTML = '';

    candidates.forEach(stock => {
        // 등락률 색상
        const colorClass = stock.change_rate > 0 ? 'up-text' : 'down-text';
        const sign = stock.change_rate > 0 ? '+' : '';
        
        // Plan 표시 (ENTRY일 때만)
        const planDisplay = stock.msi_action === 'ENTRY' 
            ? `<small class="up-text">${stock.plan}</small>` 
            : `<small class="small-meta">-</small>`;

        const row = `
            <tr>
                <td>
                    <strong>${stock.name}</strong> <small class="small-meta">${stock.code}</small>
                    <small style="color:#aaa">${stock.sector}</small>
                </td>
                <td>
                    <span class="badge ${stock.msi_action}">${stock.msi_action}</span>
                </td>
                <td>
                    <small>📍 ${stock.location}</small><br>
                    <small>⏱️ ${stock.timing}</small>
                </td>
                <td>
                    <div class="${colorClass}">
                        ${Number(stock.close).toLocaleString()}
                        <br>
                        <small>(${sign}${stock.change_rate}%)</small>
                    </div>
                    <small class="small-meta">${Number(stock.volume_money / 100000000).toFixed(0)}억</small>
                </td>
                <td>
                    ${planDisplay}
                </td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// 실행
loadDashboard();
