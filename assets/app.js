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

function renderCandidates(candidates) {
    const container = document.getElementById('candidate-list');
    container.innerHTML = '';

    candidates.forEach(stock => {
        // 숫자 포맷팅
        const price = Number(stock.close).toLocaleString();
        const change = stock.change_rate > 0 ? `+${stock.change_rate}%` : `${stock.change_rate}%`;
        const colorClass = stock.change_rate >= 0 ? 'price-up' : 'price-down';
        const volume = Math.round(stock.volume_money / 100000000).toLocaleString(); // 억 단위
        
        // Action 버튼 스타일
        let btnClass = 'btn-watch';
        let btnText = 'WATCH';
        
        // 현재 로직상 100% WATCH지만, 나중에 ENTRY 부활 시 사용
        if (stock.msi_action === 'ENTRY') {
            btnClass = 'btn-entry';
            btnText = 'ENTRY';
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
                        <span class="current-price">${price}</span>
                        <span class="price-change ${colorClass}">${change}</span>
                    </div>
                    <div class="action-box">
                        <span class="action-btn ${btnClass}">${btnText}</span>
                    </div>
                </div>

                <div class="volume-info">
                    <span>거래대금 ${volume}억</span>
                    <span>${stock.location || 'Setup Check'}</span>
                </div>
            </article>
        `;
        container.innerHTML += html;
    });
}

loadDashboard();
