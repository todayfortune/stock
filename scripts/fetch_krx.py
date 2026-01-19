# scripts/fetch_krx.py
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

# 2. 데이터 수집 & 가공
def process_market_data(theme_map):
    print("📡 Market Data Fetching...")
    
    # KOSPI/KOSDAQ 전체
    df = fdr.StockListing('KRX')
    df.rename(columns={'Code':'Code','Name':'Name','Close':'종가','ChagesRatio':'등락률','Amount':'거래대금','Marcap':'시가총액','Sector':'KRX_Sector'}, inplace=True)
    df.set_index('Code', inplace=True)
    
    # 데이터 타입 변환 (NaN 처리)
    cols = ['종가','거래대금','등락률']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    # 섹터 매핑
    df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector
    
    # 유효 종목 필터 (동전주 제외, 거래대금 50억 이상)
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 5_000_000_000)
    df_clean = df[valid_mask].copy()

    return df_clean

# 3. 로직: 신호 산출 (Signal Engine MVP)
def calculate_signals(row):
    """
    기현 님의 정의: state, entry, stop, target, why 산출
    """
    price = row['종가']
    change = row['등락률']
    vol = row['거래대금']
    
    # 기본값
    signal = {
        "ticker": row.name,
        "name": row['Name'],
        "sector": row['CustomSector'],
        "state": "NO_TRADE",
        "close": int(price),
        "change": round(change, 2),
        "volume": int(vol),
        "entry": {"type": "-", "price": 0},
        "stop": {"price": 0},
        "target": {"price": 0, "rr": 0},
        "why": []
    }
    
    # [Logic] 주도주 조건 (거래대금 300억 이상 + 양봉)
    if vol >= 30_000_000_000 and change > 0:
        signal["state"] = "WATCH"
        signal["why"].append("메이저 수급 유입 (300억↑)")
        
        # 가상 시나리오 (일봉상 눌림목 가정)
        # 실전에서는 1H/15M 데이터를 봐야 하지만, MVP에서는 일봉 기준으로 가이드만 제공
        signal["entry"] = {"type": "stop_limit", "price": int(price)}
        stop_price = int(price * 0.97) # -3% 손절
        target_price = int(price * 1.09) # +9% 익절
        
        signal["stop"] = {"price": stop_price}
        signal["target"] = {"price": target_price, "rr": 3.0}
        
        if change > 5.0:
            signal["why"].append("강한 모멘텀 발생 (+5%↑)")
            
    elif vol >= 10_000_000_000 and change > 0:
        signal["state"] = "WATCH"
        signal["why"].append("섹터 수급 유입 (100억↑)")
    
    return signal

# 4. 메인 실행 및 JSON 저장
def run_pipeline():
    theme_map = load_theme_map()
    df = process_market_data(theme_map)
    
    # --- A. Sector Leaders ---
    sector_leaders = []
    for sector, group in df.groupby('CustomSector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        
        score = int((group['거래대금'].mean() / 100_000_000) + (group['등락률'].mean() * 10))
        top_ticker_names = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        
        sector_leaders.append({
            "sector": sector,
            "score": score,
            "turnover": int(group['거래대금'].sum()),
            "topTickers": top_ticker_names
        })
    sector_leaders.sort(key=lambda x: x['score'], reverse=True)

    # --- B. Watchlist ---
    watchlist_items = []
    # 타겟: 커스텀 섹터 + 전체 거래대금 상위 20위
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(20)
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
    for code, row in target_pool.iterrows():
        sig = calculate_signals(row)
        if sig["state"] != "NO_TRADE":
            watchlist_items.append(sig)
            
    watchlist_items.sort(key=lambda x: x['volume'], reverse=True)

    # --- C. JSON Export (표준 스키마 준수) ---
    now_str = datetime.now().isoformat()
    
    # 1. meta.json
    meta = {
        "asOf": now_str,
        "source": ["KRX", "FDR"],
        "universeSize": len(df),
        "version": "v1.5.0",
        "status": "ok",
        "errors": []
    }
    
    # 2. sector_leaders.json
    sectors_data = {
        "asOf": now_str,
        "items": sector_leaders
    }
    
    # 3. watchlist.json
    watchlist_data = {
        "asOf": now_str,
        "items": watchlist_items
    }
    
    # 파일 쓰기
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f:
        json.dump(sectors_data, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f:
        json.dump(watchlist_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline Completed: Sectors({len(sector_leaders)}), Watchlist({len(watchlist_items)})")

if __name__ == "__main__":
    run_pipeline()
