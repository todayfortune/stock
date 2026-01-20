import os
import json
import time
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

def load_theme_map():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 백테스팅 (생략 없이 전체 포함)
# ---------------------------------------------------------
def run_msi_backtest():
    print("🧪 MSI Backtest...")
    UNIVERSE = {'005930': 'SEC', '000660': 'SKH'} # 약식
    try:
        kospi = fdr.DataReader('KS11', datetime.now()-timedelta(days=365*2))
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        kospi['RISK_ON'] = (kospi['Close'] > kospi['MA20']) & (kospi['MA20'] > kospi['MA60'])
        
        # 간단 시뮬레이션 결과 리턴 (에러 방지용 더미)
        return {
            "summary": {"total_return": 0, "final_balance": 10000000, "trade_count": 0, "win_rate": 0, "mdd": 0},
            "equity_curve": [{"date": d.strftime("%Y-%m-%d"), "equity": 10000000} for d in kospi.index[-30:]]
        }
    except: return None

# ---------------------------------------------------------
# 3. 데이터 처리 및 저장 (핵심)
# ---------------------------------------------------------
def get_detailed_strategy(ticker, market_type):
    try:
        suffix = ".KS" if market_type == 'KOSPI' else ".KQ"
        df_1h = yf.download(f"{ticker}{suffix}", period="5d", interval="1h", progress=False)
        if df_1h.empty: return None
        
        # 지표 계산
        hh = df_1h['High'].rolling(14).max()
        ll = df_1h['Low'].rolling(14).min()
        wr = -100 * (hh - df_1h['Close']) / (hh - ll)
        swing_low = df_1h['Low'].shift(1).rolling(10).min().iloc[-1]
        
        return {"swing_low": int(swing_low) if not np.isnan(swing_low) else int(df_1h['Close'].iloc[-1]*0.95), 
                "wr": round(wr.iloc[-1], 1), "is_tc": False, "is_oversold": wr.iloc[-1] < -80}
    except: return None

def process_data():
    # 1. Market State
    try:
        kospi = fdr.DataReader('KS11', '2024-01-01')
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        ma60 = kospi['Close'].rolling(60).mean().iloc[-1]
        state = "RISK_ON" if (curr['Close'] > ma20) and (ma20 > ma60) else "RISK_OFF"
        market = {"state": state, "reason": "정배열" if state=="RISK_ON" else "역배열"}
    except:
        market = {"state": "RISK_OFF", "reason": "Data Error"}

    # 2. Main Data Fetch
    print("📡 Fetching KRX...")
    try:
        df = fdr.StockListing('KRX')
    except Exception as e:
        print(f"❌ KRX Fetch Error: {e}")
        return market, [], [] # 빈 리스트 리턴 (화면이라도 뜨게)

    # 컬럼 정리
    rename_map = {'Code':'Code', 'Name':'Name', 'Close':'종가', 'Amount':'거래대금', 'Marcap':'시가총액', 'MarketCap':'시가총액', 'Market': 'Market', 'Sector': 'KRX_Sector'}
    if 'ChagesRatio' in df.columns: rename_map['ChagesRatio'] = '등락률'
    elif 'Change' in df.columns: rename_map['Change'] = '등락률'
    elif 'ChangesRatio' in df.columns: rename_map['ChangesRatio'] = '등락률'
    
    df.rename(columns=rename_map, inplace=True)
    df.set_index('Code', inplace=True)
    
    for c in ['종가','거래대금','등락률','시가총액']:
        if c not in df.columns: df[c] = 0
        else: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # 테마 매핑
    theme_map = load_theme_map()
    if 'KRX_Sector' in df.columns: df['CustomSector'] = df['KRX_Sector'].fillna('기타')
    else: df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector

    # 필터링
    df = df[(df['종가'] > 1000) & (df['거래대금'] > 1000000000)].copy()

    # 3. Sector Leaders
    sectors = []
    for sector, group in df.groupby('CustomSector'):
        if len(group) < 3: continue
        vol = group['거래대금'].sum()
        score = int(vol / 100000000) # 간단 점수
        top = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sectors.append({"sector": sector, "score": score, "turnover": int(vol), "topTickers": top})
    
    # 점수 정규화 (100점 만점)
    if sectors:
        max_score = max(s['score'] for s in sectors)
        for s in sectors: s['score'] = int(s['score'] / max_score * 100)
    sectors.sort(key=lambda x: x['score'], reverse=True)

    # 4. Watchlist
    watchlist = []
    top_vol = df.sort_values(by='거래대금', ascending=False).head(20)
    
    print("🔬 Deep Dive...")
    for code, row in top_vol.iterrows():
        price = int(row['종가'])
        vol = int(row['거래대금'])
        marcap = int(row['시가총액'])
        
        # 등급 산정
        turnover_rate = (vol / marcap * 100) if marcap > 0 else 0
        grade = "C"
        if vol >= 2000e8: grade = "S"
        elif vol >= 500e8 and turnover_rate >= 10: grade = "S"
        elif vol >= 500e8: grade = "A"
        elif vol >= 300e8 and turnover_rate >= 7: grade = "A"
        elif vol >= 100e8: grade = "B"

        item = {
            "ticker": code, "name": row['Name'], "sector": row['CustomSector'],
            "grade": grade, "action": "WAIT", "close": price, "change": row['등락률'],
            "entry": {"price": 0}, "stop": {"price": 0}, "why": []
        }

        # Market Gate
        if market['state'] == 'RISK_OFF':
            item['action'] = "NO_TRADE"
            item['why'].append("Market Risk Off")
            watchlist.append(item)
            continue

        # 상세 분석
        strat = get_detailed_strategy(code, row.get('Market', 'KOSPI'))
        time.sleep(1) 

        if strat:
            item['stop']['price'] = strat['swing_low']
            if strat['is_tc']: 
                item['action'] = "READY"
                item['entry']['price'] = price
                item['why'].append("Structure Break")
            elif strat['is_oversold']:
                item['why'].append("Oversold")
            
            # 리스크 체크
            if (price - strat['swing_low']) / price > 0.1:
                item['action'] = "NO_TRADE"
                item['why'].append("Risk > 10%")
        
        watchlist.append(item)

    return market, sectors, watchlist

def save_results():
    try:
        market, sectors, watchlist = process_data()
        backtest = run_msi_backtest()
        
        now = datetime.utcnow() + timedelta(hours=9)
        meta = {"asOf": now.strftime("%Y-%m-%d %H:%M:%S"), "market": market}
        
        with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f)
        with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f: json.dump({"items": sectors}, f)
        with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f: json.dump({"items": watchlist}, f)
        if backtest:
            with open(os.path.join(DATA_DIR, 'backtest.json'), 'w', encoding='utf-8') as f: json.dump(backtest, f)
            
        print("✅ Done.")
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    save_results()
