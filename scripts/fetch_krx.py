import os
import json
import time
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, timedelta

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

# ---------------------------------------------------------
# [New] 2. 시장 상태 진단 (Market Regime)
# ---------------------------------------------------------
def analyze_market_regime():
    print("📡 Market Regime Check (KOSPI)...")
    try:
        # KOSPI 지수 (KS11) 최근 120일 조회
        kospi = fdr.DataReader('KS11', '2023-01-01') # 넉넉하게
        if kospi.empty: return {"state": "RISK_ON", "reason": "데이터 부족 (Default On)"}
        
        curr = kospi.iloc[-1]
        
        # 이동평균선 계산
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        ma60 = kospi['Close'].rolling(60).mean().iloc[-1]
        
        # [Logic] 시장 판단 기준 (단순화: 20일선 기준)
        # - Close > MA20 : RISK_ON (추세 상승/유지)
        # - Close < MA20 : RISK_OFF (추세 꺾임/조정)
        
        state = "RISK_ON"
        reason = "KOSPI > 20일선 (상승 추세)"
        
        if curr['Close'] < ma20:
            state = "RISK_OFF"
            reason = "KOSPI < 20일선 (하락 경계)"
            
        # (옵션) 20일 신저가 이탈 시 강력 경고
        recent_low_20 = kospi['Low'].rolling(20).min().iloc[-2] # 전일까지의 저가
        if curr['Close'] < recent_low_20:
            state = "RISK_OFF"
            reason = "KOSPI 20일 신저가 갱신 (위험)"

        return {
            "state": state,
            "index_price": int(curr['Close']),
            "ma20": int(ma20),
            "reason": reason
        }
        
    except Exception as e:
        print(f"⚠️ Market Check Failed: {e}")
        return {"state": "RISK_ON", "reason": "Check Error"} # 에러 시 보수적 허용 or 차단 선택

# 3. 데이터 수집 (종목)
def process_market_data(theme_map):
    print("📡 Market Data Fetching (Stocks)...")
    df = fdr.StockListing('KRX')
    df.rename(columns={'Code':'Code','Name':'Name','Close':'종가','ChagesRatio':'등락률','Amount':'거래대금','Marcap':'시가총액','Sector':'KRX_Sector'}, inplace=True)
    df.set_index('Code', inplace=True)
    
    cols = ['종가','거래대금','등락률']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector
    
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 3_000_000_000)
    return df[valid_mask].copy()

# 4. 신호 산출 (Market Filter 적용)
def calculate_signals(row, market_state):
    price = int(row['종가'])
    change = float(row['등락률'])
    vol = int(row['거래대금'])
    
    signal = {
        "ticker": row.name,
        "name": row['Name'],
        "sector": row['CustomSector'],
        "state": "NO_TRADE",
        "grade": "C",
        "action": "WAIT",
        "close": price,
        "change": round(change, 2),
        "volume": vol,
        "entry": {"type": "-", "price": 0},
        "stop": {"price": 0},
        "target": {"price": 0, "rr": 0},
        "why": []
    }
    
    # [Gatekeeper] 시장이 위험하면 모든 신호 차단
    if market_state['state'] == "RISK_OFF":
        signal['state'] = "NO_TRADE"
        signal['grade'] = "X"
        signal['why'].append(f"⛔ {market_state['reason']}")
        return signal # 여기서 바로 리턴 (분석 중단)

    # --- 아래는 RISK_ON 일 때만 실행됨 ---

    # Grade 산출
    if vol >= 100_000_000_000 or (vol >= 50_000_000_000 and change >= 15.0):
        signal["grade"] = "S"
        signal["why"].append("S급: 압도적 거래대금")
    elif vol >= 30_000_000_000:
        signal["grade"] = "A"
        signal["why"].append("A급: 메이저 수급")
    elif vol >= 10_000_000_000:
        signal["grade"] = "B"
        signal["why"].append("B급: 일반 수급")
    else:
        signal["grade"] = "C"

    # Action 산출
    if signal["grade"] in ["S", "A", "B"] and change > 0:
        signal["state"] = "WATCH"
        
        target_entry = price 
        if change > 15.0:
            target_entry = int(price * 0.95)
            signal["why"].append("급등 피로감 → 눌림목 대기")
        else:
            signal["why"].append("추세 지속형 → 진입 검토")

        dist = abs(price - target_entry) / price
        if dist <= 0.02:
            signal["action"] = "READY"
        else:
            signal["action"] = "WAIT"

        # Plan
        stop_price = int(target_entry * 0.97)
        target_price = int(target_entry * 1.09)
        signal["stop"] = {"price": stop_price}
        signal["target"] = {"price": target_price, "rr": 3.0}

    return signal

# 5. 메인 파이프라인
def run_pipeline():
    # 1) 시장 상태 먼저 확인
    market_info = analyze_market_regime()
    print(f"🚦 Market Regime: {market_info['state']} ({market_info['reason']})")

    theme_map = load_theme_map()
    df = process_market_data(theme_map)
    
    # Sector Leaders
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

    # Watchlist (Market Filter 적용)
    watchlist_items = []
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(30)
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
    for code, row in target_pool.iterrows():
        # [수정] market_info 전달
        sig = calculate_signals(row, market_info)
        
        # RISK_OFF 여도 목록에는 보여주되 상태는 NO_TRADE로 (확인용)
        # 또는 아예 리스트에서 뺄 수도 있음. 여기서는 '보여주는 쪽' 선택
        if sig["state"] != "NO_TRADE" or market_info['state'] == "RISK_OFF":
             # RISK_OFF 일때는 상위 몇 개만 보여주거나 다 보여줌.
             # 여기서는 유효한 종목만 담되, RISK_OFF면 전부 NO_TRADE로 담김.
             if sig['volume'] > 10_000_000_000: # 최소 거래대금 필터
                watchlist_items.append(sig)
            
    watchlist_items.sort(key=lambda x: x['volume'], reverse=True)

    # Export
    now_str = datetime.now().isoformat()
    meta = {
        "asOf": now_str,
        "source": ["KRX", "FDR"],
        "version": "v2.1 (Market Regime Gate)",
        "status": "ok",
        "market": market_info # [New] 시장 상태 정보 추가
    }
    
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": sector_leaders}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": watchlist_items}, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline Completed. Market: {market_info['state']}")

if __name__ == "__main__":
    run_pipeline()
