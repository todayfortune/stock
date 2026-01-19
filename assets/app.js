// assets/app.js

async function loadDashboard() {
    try {
        // 1. 요약 정보 가져오기
        const summaryRes = await fetch('data/summary.json');
        const summary = await summaryRes.json();
        document.getElementById('update-time').innerText = `Updated: ${summary.updated_at}`;

        // 2. 섹터 정보 가져오기 & 그리기
        const sectorsRes = await fetch('data/sectors.json');
        const sectors = await sectorsRes.json();
        renderSectors(sectors);

        // 3. 종목 정보 가져오기 & 그리기
        const candidatesRes = await fetch('data/candidates.json');
        const candidates = await candidatesRes.json();
        renderCandidates(candidates);

    } catch (error) {
        console.error("데이터 로딩 실패:", error);
        document.getElementById('sector-list').innerHTML = `<article>⚠️ 데이터를 불러오지 못했습니다. (GitHub Actions가 아직 안 돌았거나 경로 문제)</article>`;
    }
}

function renderSectors(sectors) {
    const container = document.getElementById('sector-list');
    container.innerHTML = ''; // 로딩 문구 제거

    // 상위 3개만 표시
    sectors.slice(0, 3).forEach(sector => {
        const html = `
            <article class="sector-card">
                <header>
                    <strong>${sector.name}</strong> 
                    <span style="float:right">Score: ${sector.msi_score}</span>
                </header>
                <small>자금강도: ${Number(sector.flow_won / 100000000).toFixed(0)}억</small><br>
                <small>상승확산: ${sector.breadth}%</small>
                <footer>
                    <span class="badge">대장: ${sector.leader_name}</span>
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
        const row = `
            <tr>
                <td><strong>${stock.name}</strong> <small>(${stock.code})</small></td>
                <td>${stock.sector}</td>
                <td>${Number(stock.close).toLocaleString()}원</td>
                <td class="up-trend">+${stock.change_rate}%</td>
                <td>${Number(stock.volume_money / 100000000).toFixed(0)}억</td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// 실행
loadDashboard();
