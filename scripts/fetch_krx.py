import os
import json
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime

# ---------------------------------------------------------
# 1. 설정 및 초기화
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
# 2. 데이터 수집 (FDR)
# ---------------------------------------------------------
def fetch_market_data():
    print("📡 KRX 전 종목 시세 수집 중 (FDR)...")
    df = fdr.StockListing('KRX')
    
    # 컬럼 표준화
    df.rename(columns={
        'Code': 'Code', 'Name': 'Name', 'Close': '종가',
        'ChagesRatio': '등락률', 'Amount': '거래대금', 
        'Marcap': '시가총액', 'Sector': 'KRX_Sector'
    }, inplace=True)
    
    df.set_index('Code', inplace=True)
    
    # 데이터 타입 정리 (NaN 처리)
    df['종가'] = pd.to_numeric(df['종가'], errors='coerce').fillna(0)
    df['거래대금'] = pd.to_numeric(df['거래대금'], errors='coerce').fillna(0)
    df['등락률'] = pd.to_numeric(df['등락률'], errors='coerce').fillna(0)
    
    return df

# ---------------------------------------------------------
# 3. 블루프린트 로직: Selection (Why?)
# ---------------------------------------------------------
def calculate_sector_metrics(group):
    """
    섹터별 'Why' 지표 산출
    - Flow: 평균 거래대금 강도
    - Trend: 상승 종목 비중 (MA20 대체용 약식)
    - Breadth: 상승 종목 수 비율
    """
    total_count = len(group)
    if total_count == 0: return None
    
    # 1. Flow (자금)
    avg_flow = group['거래대금'].mean()
    
    # 2. Breadth (확산)
    up_count = len(group[group['등락률'] > 0])
    breadth_score = (up_count / total_count) * 100
    
    # 3. Trend (추세 강도) - 등락률 평균으로 약식 계산
    avg_change = group['등락률'].mean()
    
    # MSI Score (자금 + 확산 + 추세)
    # 거래대금은 로그스케일 개념 적용하여 점수화 (약식)
    flow_score = min(avg_flow / 10_000_000_000, 50) # 100억 평균이면 1점, 최대 50점
    msi_score = flow_score + (breadth_score * 0.3) + (avg_change * 2)
    
    # 대장주 선정
    leader = group.sort_values(by='거래대금', ascending=False).iloc[0]
    
    return {
        "msi_score": round(msi_score, 2),
        "flow_score": round(flow_score, 1), # 자금 점수
        "trend_score": round(avg_change, 2), # 추세 점수
        "breadth_score": round(breadth_score, 1), # 확산 점수
        "leader_name": leader['Name'],
        "leader_code": leader.name
    }

# ---------------------------------------------------------
# 4. 블루프린트 로직: Strategy (Location/Timing/Plan)
# ---------------------------------------------------------
def analyze_strategy(row):
    """
    개별 종목의 전략 상태(Action) 판별
    *일봉 데이터 기반의 시뮬레이션 (v1.5)*
    """
    price = row['종가']
    change = row['등락률']
    
    # [가정] 일봉상 전일 종가 부근을 Zone으로 인식한다고 가정 (약식)
    # 실전에서는 과거 캔들 분석이 필요하지만, 여기서는 '상승 추세' 여부로 판단
    
    action = "PASS"
    location = "OUT_ZONE"
    timing = "WAIT"
    plan = "-"
    
    # 로직: 거래대금이 터지면서 양봉이면 WATCH
    if row['거래대금'] > 30_000_000_000 and change > 0: # 300억 이상 양봉
        action = "WATCH"
        location = "IN_ZONE (Daily)"
        timing = "Wait MSS"
        
        # ENTRY 시나리오 (가상)
        if change > 3.0: # 3% 이상 강한 상승이면 진입 가능으로 간주
            action = "ENTRY"
            timing = "MSS Confirmed"
            stop_loss = int(price * 0.97) # -3%
            target = int(price * 1.09)    # +9% (1:3 RR)
            plan = f"Stop: {stop_loss:,} / Target: {target:,}"
            
    elif row['거래대금'] > 10_000_000_000 and change > 0:
        action = "WATCH"
        location = "Approaching"
        
    return action, location, timing, plan

# ---------------------------------------------------------
# 5. 메인 처리
# ---------------------------------------------------------
def process_data(df, theme_map):
    print("⚙️ 블루프린트 데이터 가공 중...")
    
    df['sector'] = 'Unclassified'
    for code, sector_name in theme_map.items():
        if code in df.index:
            df.loc[code, 'sector'] = sector_name
            
    # 유효 데이터 필터링
    mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df_clean = df[mask].copy()
    
    # --- A. 섹터 분석 ---
    sector_stats = []
    for sector, group in df_clean.groupby('sector'):
        if sector == 'Unclassified': continue
        if len(group) < 2: continue
        
        metrics = calculate_sector_metrics(group)
        if metrics:
            metrics['name'] = sector
            metrics['stock_count'] = len(group)
            sector_stats.append(metrics)
            
    sector_stats.sort(key=lambda x: x['msi_score'], reverse=True)
    
    # --- B. 종목 전략 분석 (Candidates) ---
    candidates = []
    filtered = df_clean[df_clean['sector'] != 'Unclassified']
    
    for code, row in filtered.iterrows():
        # 전략 분석 실행
        action, loc, time, plan = analyze_strategy(row)
        
        if action != "PASS": # 의미 있는 종목만 리스트업
            candidates.append({
                "code": code,
                "name": row['Name'],
                "sector": row['sector'],
                "close": int(row['종가']),
                "change_rate": round(row['등락률'], 2),
                "volume_money": int(row['거래대금']),
                
                # [중요] 블루프린트 검증용 필드 추가
                "msi_action": action,
                "location": loc,
                "timing": time,
                "plan": plan
            })
            
    candidates.sort(key=lambda x: x['volume_money'], reverse=True)
    
    return sector_stats, candidates, len(df_clean)

# ---------------------------------------------------------
# 6. 결과 저장
# ---------------------------------------------------------
def save_results(sectors, candidates, total_count):
    print("💾 JSON 데이터 저장 중...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary = {
        "updated_at": now_str,
        "market_status": "OPEN" if datetime.now().hour < 16 else "CLOSED",
        "top_sectors": [s['name'] for s in sectors[:3]],
        "data_source": "FinanceDataReader (OK)"
    }
    
    with open(os.path.join(DATA_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
        
    with open(os.path.join(DATA_DIR, 'sectors.json'), 'w', encoding='utf-8') as f:
        json.dump(sectors, f, ensure_ascii=False, indent=2)
        
    with open(os.path.join(DATA_DIR, 'candidates.json'), 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"✅ 완료! (섹터 {len(sectors)}개, 후보 {len(candidates)}개)")

if __name__ == "__main__":
    theme_map = load_theme_map()
    df = fetch_market_data()
    sectors, candidates, total = process_data(df, theme_map)
    save_results(sectors, candidates, total)
