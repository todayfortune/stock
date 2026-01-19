// assets/app.js

async function loadDashboard() {
    try {
        const v = new Date().getTime();
        const [summaryRes, sectorsRes, candidatesRes] = await Promise.all([
            fetch(`data/summary.json?v=${v}`),
            fetch(`data/sectors.json?v=${v}`),
            fetch(`data/candidates.json?v=${v}`)
        ]);

        const summary = await summaryRes.json();
        const sectors = await sectorsRes.json();
        const candidates = await candidatesRes.json();

        // 시간 표시
        document.getElementById('update-time').innerHTML = 
            `<i class="fa-regular fa-clock"></i> Updated: ${summary.updated_at}`;

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

    sectors.slice(0, 3).forEach((sector, index) => {
        const themeClass = `theme-${(index % 3) + 1}`;
        const score = Math.min(sector.msi_score, 100).toFixed(0);
        
        // [번역]
        // Flow: 자금력 (억 단위 환산은 이미 Python에서 됨, 여기선 점수만)
        // Trend: 평균등락
        // Breadth: 상승비중

        const html = `
            <div class="card ${themeClass}">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <h3 style="font-size:0.9rem; opacity:0.9;">Rank ${index + 1}</h3>
                    <span style="background:rgba(255,255,255,0.2); padding:2px 8px; border-radius:10px; font-size:0.8rem;">
                        Score ${score}
                    </span>
                </div>
                <div class="value" style="margin:15px 0;">${sector.name}</div>
                
                <div style="font-size:0.85rem; background:rgba(0,0,0,0.1); padding:10px; border-radius:10px;">
                    <div style="display:flex; justify-content:space-between; margin-bottom:5px;">
                        <span>💰 자금력</span> <strong>${sector.flow_score}점</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between; margin-bottom:5px;">
                        <span>📈 평균등락</span> <strong>${sector.trend_score}%</strong>
                    </div>
                    <div style="display:flex; justify-content:space-between;">
                        <span>🌊 상승비중</span> <strong>${sector.breadth_score}%</strong>
                    </div>
                </div>
                
                <div style="margin-top:10px; font-size:0.8rem; text-align:right;">
                    👑 대장: ${sector.leader_name}
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
        const isUp = stock.change_rate > 0;
        const colorClass = isUp ? 'price-up' : 'price-down';
        const sign = isUp ? '+' : '';
        const iconInitial = stock.name.charAt(0);
        const vol = (stock.volume_money / 100000000).toFixed(0);

        // [번역 로직] 영어 상태값 -> 한국어 설명
        let timingKr = stock.timing;
        if (stock.timing.includes("Wait MSS")) timingKr = "⏱️ 눌림목 대기 (Wait MSS)";
        else if (stock.timing.includes("Strong Momentum")) timingKr = "🚀 강한 시세 (급등)";
        else if (stock.timing.includes("MSS Confirmed")) timingKr = "✅ 타점 확인 (진입 가능)";

        let locationKr = stock.location;
        if (stock.location.includes("In Zone")) locationKr = "📍 수급 존 내부";
        else if (stock.location.includes("Approaching")) locationKr = "📍 존 접근 중";

        const row = `
            <tr>
                <td>
                    <div class="stock-info">
                        <div class="stock-icon">${iconInitial}</div>
                        <div>
                            <div style="font-weight:bold;">${stock.name}</div>
                            <div style="font-size:0.8rem; color:#888;">${stock.code} | ${stock.sector}</div>
                        </div>
                    </div>
                </td>
                <td>
                    <span class="status-badge status-${stock.msi_action}">
                        ${stock.msi_action}
                    </span>
                </td>
                <td>
                    <div style="font-size:0.85rem; color:#555;">${locationKr}</div>
                    <div style="font-size:0.8rem; color:#888;">${timingKr}</div>
                </td>
                <td class="${colorClass}">
                    ${Number(stock.close).toLocaleString()}원
                    <br>
                    <small>(${sign}${stock.change_rate}%)</small>
                </td>
                <td>
                    <div style="font-weight:bold; color:#6c5ce7;">${vol}억</div>
                </td>
            </tr>
        `;
        tbody.innerHTML += row;
    });
}

loadDashboard();
