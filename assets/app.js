// assets/app.js
async function loadDashboard() {
    try {
        const summaryRes = await fetch('data/summary.json');
        const summary = await summaryRes.json();
        document.getElementById('update-time').innerText = `Updated: ${summary.updated_at}`;

        const sectorsRes = await fetch('data/sectors.json');
        const sectors = await sectorsRes.json();
        renderSectors(sectors);

        const candidatesRes = await fetch('data/candidates.json');
        const candidates = await candidatesRes.json();
        renderCandidates(candidates);

    } catch (error) {
        console.error("Error:", error);
    }
}

function renderSectors(sectors) {
    const container = document.getElementById('sector-list');
    container.innerHTML = '';
    
    sectors.slice(0, 5).forEach((sector, index) => {
        const html = `
            <div class="sector-card">
                <span class="sector-rank">Rank ${index + 1}</span>
                <span class="sector-name">${sector.name}</span>
                <div class="sector-score">
                    Score: ${sector.msi_score}<br>
                    <small>대장: ${sector.leader_name}</small>
                </div>
            </div>
        `;
        container.innerHTML += html;
    });
}

// assets/app.js (renderCandidates 함수 수정)

function renderCandidates(candidates) {
    const container = document.getElementById('candidate-list');
    container.innerHTML = '';

    candidates.forEach(stock => {
        // ... (이전 코드 동일) ...
        const volume = Math.round(stock.volume_money / 100000000).toLocaleString();

        // 1H Zone 상태에 따른 뱃지 색상
        let locBadge = `<span style="color:#6b7280; font-size:0.75rem;">${stock.location}</span>`;
        if (stock.location && stock.location.includes("IN_ZONE")) {
            locBadge = `<span style="color:#10b981; font-weight:bold; font-size:0.8rem;">⚡ ${stock.location}</span>`;
        }

        const html = `
            <article class="stock-card">
                <div class="card-header">
                    <div>
                        <span class="stock-name">${stock.name}</span>
                        <span class="stock-code">${stock.code}</span>
                    </div>
                    <span class="badge">${stock.sector}</span>
                </div>
                
                <div class="card-body">
                    <div class="price-box">
                        <span class="current-price">${Number(stock.close).toLocaleString()}</span>
                        <span class="price-change ${stock.change_rate >= 0 ? 'price-up' : 'price-down'}">
                            ${stock.change_rate > 0 ? '+' : ''}${stock.change_rate}%
                        </span>
                    </div>
                    <div class="action-box">
                        <span class="action-btn btn-watch">${stock.msi_action}</span>
                    </div>
                </div>

                <div class="volume-info">
                    <span>${volume}억</span>
                    ${locBadge} </div>
            </article>
        `;
        container.innerHTML += html;
    });
}

loadDashboard();
