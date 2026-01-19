// assets/app.js

async function loadDashboard() {
    try {
        // 1. 데이터 가져오기 (캐시 방지 적용)
        const v = new Date().getTime();
        const [summaryRes, sectorsRes, candidatesRes] = await Promise.all([
            fetch(`data/summary.json?v=${v}`),
            fetch(`data/sectors.json?v=${v}`),
            fetch(`data/candidates.json?v=${v}`)
        ]);

        const summary = await summaryRes.json();
        const sectors = await sectorsRes.json();
        const candidates = await candidatesRes.json();

        // 2. 상단 업데이트 시간 표시
        document.getElementById('update-time').innerHTML = 
            `<i class="fa-regular fa-clock"></i> Updated: ${summary.updated_at}`;

        // 3. 화면 그리기
        renderSectors(sectors);
        renderCandidates(candidates);

    } catch (error) {
        console.error("Load Error:", error);
        document.getElementById('sector-list').innerHTML = 
            `<div style="color:red">⚠️ 데이터 로딩 실패. (GitHub Actions 확인 필요)</div>`;
    }
}

function renderSectors(sectors) {
    const container = document.getElementById('sector-list');
    container.innerHTML = '';

    // 상위 3개만 표시 (테마 색상 순환)
    sectors.slice(0, 3).forEach((sector, index) => {
        // 인덱스에 따라 테마 클래스 적용 (theme-1, theme-2, theme-3)
        const themeClass = `theme-${(index % 3) + 1}`;
        
        // 점수 계산 (예: 100점 만점 환산 등 시각적 처리)
        const score = Math.min(sector.msi_score, 100).toFixed(0);

        const html = `
            <div class="card ${themeClass}">
                <h3>Rank ${index + 1} Sector</h3>
                <div class="value">${sector.name}</div>
                <div style="display:flex; justify-content:space-between; align-items:end;">
                    <div class="sub-info">
                        <i class="fa-solid fa-crown"></i> ${sector.leader_name}
                    </div>
                    <div style="text-align:right;">
                        <div style="font-size:0.8rem; opacity:0.8;">MSI Score</div>
                        <div style="font-size:1.2rem; font-weight:bold;">${score}</div>
                    </div>
                </div>
            </div>
        `;
        container.innerHTML += html;
    });
}

function renderCandidates(candidates) {
    const tbody = document.getElementById('candidate-list');
    tbody.innerHTML = '';

    candidates.forEach(stock => {
        // 등락률 색상 처리
        const isUp = stock.change_rate > 0;
        const colorClass = isUp ? 'price-up' : 'price-down';
        const sign = isUp ? '+' : '';
        const iconInitial = stock.name.charAt(0); // 종목명 첫 글자 아이콘

        // 거래대금 억 단위 변환
        const vol = (stock.volume_money / 100000000).toFixed(0);

        const row = `
            <tr>
                <td>
                    <div class="stock-info">
                        <div class="stock-icon">${iconInitial}</div>
                        <div>
                            <div style="font-weight:bold;">${stock.name}</div>
                            <div style="font-size:0.8rem; color:#888;">${stock.code}</div>
                        </div>
                    </div>
                </td>
                <td>
                    <span style="background:#f1f2f6; padding:4px 8px; border-radius:6px; font-size:0.8rem; color:#555;">
                        ${stock.sector}
                    </span>
                </td>
                <td style="font-weight:600;">${Number(stock.close).toLocaleString()}</td>
                <td class="${colorClass}">${sign}${stock.change_rate}%</td>
                <td>
                    <span class="status-badge status-${stock.msi_action}">
                        ${stock.msi_action}
                    </span>
                </td>
                <td>
                    <div style="font-size:0.8rem; font-weight:bold; color:#6c5ce7;">${vol}억</div>
                </td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

// 실행
loadDashboard();
