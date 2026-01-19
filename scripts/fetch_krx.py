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
# 2. 데이터 수집 (FinanceDataReader 사용)
# ---------------------------------------------------------
def fetch_market_data():
    print("📡 KRX 전 종목 시세 수집 중 (FDR)...")
    
    # KRX 전 종목 리스팅 (가격, 등락률, 거래대금 포함)
    # 로봇이 돌려도 차단되지 않고 아주 빠릅니다.
    df = fdr.StockListing('KRX')
    
    # 컬럼 이름을 우리 로직에 맞게 변경
    # FDR 컬럼: Code, Name, Close, ChagesRatio, Amount(거래대금), Marcap(시총) 등
    df.rename(columns={
        'Code': 'Code',
        'Name': 'Name',
        'Close': '종가',
        'ChagesRatio': '등락률',
        'Amount': '거래대금',
        'Marcap': '시가총액',
        'Sector': 'KRX_Sector' # 기본 업종
    }, inplace=True)
    
    # 인덱스를 종목코드로 설정
    df.set_index('Code', inplace=True)
    
    return df

# ---------------------------------------------------------
# 3. 데이터 가공 및 점수 산출 (로직 동일)
# ---------------------------------------------------------
def process_data(df, theme_map):
    print("⚙️ 데이터 가공 및 섹터 점수 계산 중...")
    
    df['sector'] = 'Unclassified'
    
    # 커스텀 테마 맵 적용
    for code, sector_name in theme_map.items():
        if code in df.index:
            df.loc[code, 'sector'] = sector_name
            
    # 필터링: 동전주 제외, 거래대금 10억 이상
    # FDR 데이터엔 NaN이 있을 수 있으므로 처리
    df['종가'] = pd.to_numeric(df['종가'], errors='coerce').fillna(0)
    df['거래대금'] = pd.to_numeric(df['거래대금'], errors='coerce').fillna(0)
    
    mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df_clean = df[mask].copy()
    
    sector_stats = []
    
    for sector, group in df_clean.groupby('sector'):
        if sector == 'Unclassified': continue
        if len(group) < 2: continue
        
        avg_flow = group['거래대금'].mean()
        avg_change = group['등락률'].mean()
        up_count = len(group[group['등락률'] > 0])
        total_count = len(group)
        breadth = (up_count / total_count) * 100
        
        # MSI Score 계산
        score = avg_change + (breadth / 5)
        
        # 대장주 선정
        leader = group.sort_values(by='거래대금', ascending=False).iloc[0]
        
        sector_stats.append({
            "name": sector,
            "msi_score": round(score, 2),
            "flow_won": int(avg_flow),
            "avg_change": round(avg_change, 2),
            "breadth": round(breadth, 1),
            "leader_code": leader.name, # 인덱스가 코드
            "leader_name": leader['Name'],
            "stock_count": total_count
        })
        
    sector_stats.sort(key=lambda x: x['msi_score'], reverse=True)
    
    return sector_stats, df_clean

# ---------------------------------------------------------
# 4. 결과 저장
# ---------------------------------------------------------
def save_results(sectors, df_clean):
    print("💾 결과 JSON 저장 중...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary = {
        "updated_at": now_str,
        "market_status": "Neutral",
        "top_sectors": [s['name'] for s in sectors[:3]],
        "total_analyzed": len(df_clean)
    }
    with open(os.path.join(DATA_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
        
    with open(os.path.join(DATA_DIR, 'sectors.json'), 'w', encoding='utf-8') as f:
        json.dump(sectors, f, ensure_ascii=False, indent=2)
        
    candidates = []
    filtered = df_clean[df_clean['sector'] != 'Unclassified']
    
    for code, row in filtered.iterrows():
        if row['등락률'] > 0:
            candidates.append({
                "code": code,
                "name": row['Name'],
                "sector": row['sector'],
                "close": int(row['종가']),
                "change_rate": round(row['등락률'], 2),
                "volume_money": int(row['거래대금']),
                "msi_action": "WATCH"
            })
            
    candidates.sort(key=lambda x: x['volume_money'], reverse=True)
    
    with open(os.path.join(DATA_DIR, 'candidates.json'), 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"✅ 완료! (섹터 {len(sectors)}개, 후보 {len(candidates)}개)")

if __name__ == "__main__":
    theme_map = load_theme_map()
    df = fetch_market_data()
    sectors, df_clean = process_data(df, theme_map)
    save_results(sectors, df_clean)
