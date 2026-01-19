import os
import json
import time
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime

# 1. 설정
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

# 2. 데이터 수집
def process_market_data(theme_map):
    print("📡 Market Data Fetching...")
    df = fdr.StockListing('KRX')
    df.rename(columns={'Code':'Code','Name':'Name','Close':'종가','ChagesRatio':'등락률','Amount':'거래대금','Marcap':'시가총액','Sector':'KRX_Sector'}, inplace=True)
    df.set_index('Code', inplace=True)
    
    cols = ['종가','거래대금','등락률']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector
    
    # 필터: 30억 이상 (조건 완화, Grade로 거를 예정)
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 3_000_000_000)
    return df[valid_mask].copy()

# 3. 신호 산출 (Grade & Action Logic Added)
def calculate_signals(row):
    price = int(row['종가'])
    change = float(row['등락률'])
    vol = int(row['거래대금'])
    
    signal = {
        "ticker": row.name,
        "name": row['Name'],
        "sector": row['CustomSector'],
        "state": "NO_TRADE",
        "grade": "C",   # Default
        "action": "WAIT", # Default
        "close": price,
        "change": round(change, 2),
        "volume": vol,
        "entry": {"type": "-", "price": 0},
        "stop": {"price": 0},
        "target": {"price": 0, "rr": 0},
        "why": []
    }
    
    # --- [Logic 1] Grade 산출 (체급 나누기) ---
    if vol >= 100_000_000_000 or (vol >= 50_000_000_000 and change >= 15.0):
        signal["grade"] = "S"
        signal["why"].append("S급: 압도적 거래대금/폭등")
    elif vol >= 30_000_000_000:
        signal["grade"] = "A"
        signal["why"].append("A급: 메이저 수급 (300억↑)")
    elif vol >= 10_000_000_000:
        signal["grade"] = "B"
        signal["why"].append("B급: 일반 수급")
    else:
        signal["grade"] = "C"

    # --- [Logic 2] Action 산출 (매매 타이밍) ---
    # Grade B 이상이면서 양봉인 경우만 분석
    if signal["grade"] in ["S", "A", "B"] and change > 0:
        signal["state"] = "WATCH"
        
        # [Entry Strategy]
        # 1. 시나리오: 강한 상승 후 눌림목 예상 지점 (피보나치 0.382 되돌림 가정 등)
        # MVP에서는 '오늘 시가' 또는 '3일선' 부근을 타점으로 잡는 로직 예시
        # 여기서는 단순화를 위해 '현재가'를 기준으로 잡되, 
        # 만약 15% 이상 급등했으면 -5% 아래를 타점으로, 아니면 현재가를 타점으로 잡음.
        
        if change > 15.0:
            target_entry = int(price * 0.95) # 너무 올랐으니 눌림 기다림
            signal["entry"]["price"] = target_entry
            signal["why"].append("급등 피로감 → 눌림목 대기")
        else:
            target_entry = price # 지금도 진입 가능 영역
            signal["entry"]["price"] = target_entry
            signal["why"].append("추세 지속형 → 즉시 진입 검토")

        # Action 판단: 현재가가 Entry 가격의 ±2% 이내인가?
        dist = abs(price - target_entry) / price
        if dist <= 0.02:
            signal["action"] = "READY"
        else:
            signal["action"] = "WAIT"

        # Plan 수립
        stop_price = int(target_entry * 0.97) # -3%
        target_price = int(target_entry * 1.09) # +9%
        signal["stop"] = {"price": stop_price}
        signal["target"] = {"price": target_price, "rr": 3.0}

    else:
        signal["state"] = "NO_TRADE"
    
    return signal

# 4. 메인 파이프라인
def run_pipeline():
    theme_map = load_theme_map()
    df = process_market_data(theme_map)
    
    # A. Sector Leaders
    sector_leaders = []
    for sector, group in df.groupby('CustomSector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        score = int((group['거래대금'].mean() / 100_000_000) + (group['등락률'].mean() * 10))
        top_ticker_names = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sector_leaders.append({
            "sector": sector, "score": score,
            "turnover": int(group['거래대금'].sum()),
            "topTickers": top_ticker_names
        })
    sector_leaders.sort(key=lambda x: x['score'], reverse=True)

    # B. Watchlist
    watchlist_items = []
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(30) # 유니버스 확대
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
    for code, row in target_pool.iterrows():
        sig = calculate_signals(row)
        if sig["state"] != "NO_TRADE":
            watchlist_items.append(sig)
            
    # C. Export
    now_str = datetime.now().isoformat()
    meta = {
        "asOf": now_str,
        "source": ["KRX", "FDR"],
        "version": "v2.0.0 (Grade/Action Added)",
        "status": "ok"
    }
    
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": sector_leaders}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": watchlist_items}, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline v2 Completed: Watchlist({len(watchlist_items)})")

if __name__ == "__main__":
    run_pipeline()
