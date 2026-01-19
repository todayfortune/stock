import os
import json
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime

# ---------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def load_theme_map():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 데이터 수집 (전 종목 스캔)
# ---------------------------------------------------------
def fetch_market_data():
    print("📡 KRX 전 종목 스캔 중 (FDR)...")
    # 코스피, 코스닥 전체 로딩
    df = fdr.StockListing('KRX')
    
    # 컬럼 정리
    df.rename(columns={
        'Code': 'Code', 'Name': 'Name', 'Close': '종가',
        'ChagesRatio': '등락률', 'Amount': '거래대금', 
        'Marcap': '시가총액', 'Sector': 'KRX_Sector'
    }, inplace=True)
    
    df.set_index('Code', inplace=True)
    
    # 숫자형 변환 및 결측치 제거
    cols = ['종가', '거래대금', '등락률']
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
    return df

# ---------------------------------------------------------
# 3. 전략 분석 (개별 종목)
# ---------------------------------------------------------
def analyze_strategy(row):
    price = row['종가']
    change = row['등락률']
    volume_money = row['거래대금']
    
    action = "PASS"
    location = "OUT"
    timing = "-"
    plan = "-"
    
    # [조건 1] 거래대금이 300억 이상 터지면서 상승 중인가? (수급 유입)
    if volume_money >= 30_000_000_000 and change > 0:
        action = "WATCH"
        location = "In Zone (Daily)"
        timing = "Wait MSS"
        
        # [조건 2] 10% 이상 급등하거나, 거래대금이 1000억 이상이면 강력 신호
        if change >= 10.0 or volume_money >= 100_000_000_000:
            action = "ENTRY" # (실제론 승인 대기)
            timing = "Strong Momentum"
            # 가상 플랜 수립
            stop = int(price * 0.97)
            target = int(price * 1.09)
            plan = f"Stop: {stop:,} / Target: {target:,}"
            
    elif volume_money >= 10_000_000_000 and change > 0:
        action = "WATCH"
        location = "Approaching"
        
    return action, location, timing, plan

# ---------------------------------------------------------
# 4. 데이터 가공 (수동 맵 + 자동 발굴)
# ---------------------------------------------------------
def process_data(df, theme_map):
    print("⚙️ 데이터 필터링 및 자동 발굴 중...")
    
    # 섹터 초기화
    df['sector'] = 'Unclassified'
    
    # [Track A] 내 관심 종목 (theme_map) 매핑
    for code, sector_name in theme_map.items():
        if code in df.index:
            df.loc[code, 'sector'] = sector_name

    # [Track B] 자동 발굴 (Auto-Discovery)
    # 조건: 1) 테마맵에 없는데 2) 거래대금 500억 이상 3) 3% 이상 상승 4) 동전주 아님
    mask_auto = (
        (df['sector'] == 'Unclassified') & 
        (df['거래대금'] >= 50_000_000_000) & 
        (df['등락률'] >= 3.0) &
        (df['종가'] > 1000)
    )
    
    # 발굴된 종목에 '🔥 Market Leader' 섹터 부여
    df.loc[mask_auto, 'sector'] = '🔥 Market_Leader (Auto)'
    
    # ------------------------------------------------
    # 공통: 유효한 데이터만 남기기 (관심종목 OR 발굴종목)
    # ------------------------------------------------
    mask_valid = (df['sector'] != 'Unclassified')
    df_clean = df[mask_valid].copy()
    
    # 1. 섹터 통계 계산
    sector_stats = []
    for sector, group in df_clean.groupby('sector'):
        if len(group) < 1: continue
        
        avg_flow = group['거래대금'].mean()
        avg_change = group['등락률'].mean()
        up_count = len(group[group['등락률'] > 0])
        total = len(group)
        breadth = (up_count / total) * 100
        
        # 점수 계산
        flow_score = min(avg_flow / 10_000_000_000, 50)
        msi_score = flow_score + (breadth * 0.3) + (avg_change * 2)
        
        # 대장주
        leader = group.sort_values(by='거래대금', ascending=False).iloc[0]
        
        sector_stats.append({
            "name": sector,
            "msi_score": round(msi_score, 2),
            "flow_score": round(flow_score, 1),
            "trend_score": round(avg_change, 2),
            "breadth_score": round(breadth, 1),
            "leader_name": leader['Name'],
            "leader_code": leader.name,
            "stock_count": total
        })
        
    sector_stats.sort(key=lambda x: x['msi_score'], reverse=True)
    
    # 2. 후보 종목 리스트 (Candidates)
    candidates = []
    for code, row in df_clean.iterrows():
        action, loc, time, plan = analyze_strategy(row)
        
        # PASS가 아니면 리스트에 추가
        if action != "PASS":
            candidates.append({
                "code": code,
                "name": row['Name'],
                "sector": row['sector'], # Auto인 경우 '🔥 Market_Leader'로 뜸
                "close": int(row['종가']),
                "change_rate": round(row['등락률'], 2),
                "volume_money": int(row['거래대금']),
                "msi_action": action,
                "location": loc,
                "timing": time,
                "plan": plan
            })
            
    candidates.sort(key=lambda x: x['volume_money'], reverse=True)
    
    return sector_stats, candidates, len(df_clean)

# ---------------------------------------------------------
# 5. 저장
# ---------------------------------------------------------
def save_results(sectors, candidates, total_count):
    print("💾 JSON 저장...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary = {
        "updated_at": now_str,
        "market_status": "CLOSE",
        "top_sectors": [s['name'] for s in sectors[:3]],
        "total_analyzed": total_count
    }
    
    with open(os.path.join(DATA_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sectors.json'), 'w', encoding='utf-8') as f:
        json.dump(sectors, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'candidates.json'), 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 완료: 섹터 {len(sectors)}개 / 후보 {len(candidates)}개 (자동발굴 포함)")

if __name__ == "__main__":
    theme_map = load_theme_map()
    df = fetch_market_data()
    sectors, candidates, total = process_data(df, theme_map)
    save_results(sectors, candidates, total)
