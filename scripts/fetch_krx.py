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
# 2. 기술적 지표 및 로직 함수
# ---------------------------------------------------------
def calc_williams_r(df, period=14):
    highest_high = df['High'].rolling(window=period).max()
    lowest_low = df['Low'].rolling(window=period).min()
    wr = -100 * (highest_high - df['Close']) / (highest_high - lowest_low)
    return wr.fillna(-50)

def find_swing_low(df, window=5):
    recent = df.iloc[-window:]
    swing_low = recent['Low'].min()
    return swing_low

def detect_trend_change(df_15m):
    if len(df_15m) < 20: return False
    recent_highs = df_15m['High'].iloc[-15:-5].max()
    current_close = df_15m['Close'].iloc[-1]
    return current_close > recent_highs

def get_detailed_strategy(ticker, daily_price):
    try:
        symbol = f"{ticker}.KS"
        df_1h = yf.download(symbol, period="5d", interval="1h", progress=False)
        if df_1h.empty:
            symbol = f"{ticker}.KQ"
            df_1h = yf.download(symbol, period="5d", interval="1h", progress=False)
        
        if df_1h.empty: return None

        df_15m = yf.download(symbol, period="2d", interval="15m", progress=False)

        if isinstance(df_1h.columns, pd.MultiIndex): df_1h.columns = df_1h.columns.get_level_values(0)
        if isinstance(df_15m.columns, pd.MultiIndex): df_15m.columns = df_15m.columns.get_level_values(0)

        df_1h['WR'] = calc_williams_r(df_1h)
        current_wr = df_1h['WR'].iloc[-1]
        swing_low = find_swing_low(df_1h, window=10)
        
        is_tc = detect_trend_change(df_15m) if not df_15m.empty else False
        is_oversold = current_wr < -80
        
        return {
            "swing_low": int(swing_low),
            "wr": round(current_wr, 1),
            "is_tc": is_tc,
            "is_oversold": is_oversold
        }
    except:
        return None

# ---------------------------------------------------------
# 3. 시장 레짐 & 데이터 처리
# ---------------------------------------------------------
def analyze_market_regime():
    print("📡 Market Regime Check (KOSPI)...")
    try:
        kospi = fdr.DataReader('KS11', '2023-01-01')
        if kospi.empty: return {"state": "RISK_ON", "reason": "Data Missing"}
        
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        
        state = "RISK_ON"
        reason = "KOSPI > 20MA (상승)"
        if curr['Close'] < ma20:
            state = "RISK_OFF"
            reason = "KOSPI < 20MA (하락 경계)"
        return {"state": state, "reason": reason}
    except:
        return {"state": "RISK_ON", "reason": "Error (Default ON)"}

def process_data():
    market = analyze_market_regime()
    print(f"🚦 Market: {market['state']}")
    
    theme_map = load_theme_map()
    df = fdr.StockListing('KRX')
    df.rename(columns={'Code':'Code','Name':'Name','Close':'종가','ChagesRatio':'등락률','Amount':'거래대금','Sector':'KRX_Sector'}, inplace=True)
    df.set_index('Code', inplace=True)
    
    cols = ['종가','거래대금','등락률']
    for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    
    df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector
        
    # 필터 완화: 거래대금 10억 이상 (장 초반 고려)
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df = df[valid_mask].copy()
    
    # 섹터 리더
    sector_leaders = []
    for sector, group in df.groupby('CustomSector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        score = int((group['거래대금'].mean()/1e8) + (group['등락률'].mean()*10))
        top_names = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sector_leaders.append({
            "sector": sector, "score": score,
            "turnover": int(group['거래대금'].sum()), "topTickers": top_names
        })
    sector_leaders.sort(key=lambda x: x['score'], reverse=True)
    
    # Watchlist
    watchlist = []
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(30)
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
    print(f"🔬 Analyzing {len(target_pool)} tickers...")
    
    count = 0
    for code, row in target_pool.iterrows():
        if count >= 20: break 
        
        price = int(row['종가'])
        vol = int(row['거래대금'])
        change = float(row['등락률'])
        
        item = {
            "ticker": code, "name": row['Name'], "sector": row['CustomSector'],
            "state": "NO_TRADE", "grade": "C", "action": "WAIT",
            "close": price, "change": round(change, 2), "volume": vol,
            "entry": {"price": 0}, "stop": {"price": 0}, "target": {"price": 0, "rr": 0},
            "why": []
        }
        
        if market['state'] == 'RISK_OFF':
            item['why'].append(f"⛔ {market['reason']}")
            watchlist.append(item)
            continue

        # [수정된 부분] Grade 산출 (장 초반 고려하여 C급도 허용)
        if vol >= 1000e8 or (vol >= 500e8 and change >= 15): 
            item['grade'] = "S"
            item['why'].append("S급 수급")
        elif vol >= 300e8:
            item['grade'] = "A"
            item['why'].append("A급 수급")
        elif vol >= 100e8:
            item['grade'] = "B"
        else:
            item['grade'] = "C" # C급이라도 리스트에 넣음 (거래대금 상위니까)

        if change < 0: continue

        # Deep Dive
        strat = get_detailed_strategy(code, price)
        count += 1
        time.sleep(1.0)
        
        if strat:
            swing_low = strat['swing_low']
            if price > 0 and (price - swing_low)/price > 0.1:
                item['stop']['price'] = int(price * 0.97)
                item['why'].append("Stop: 3% (Low 멈)")
            else:
                item['stop']['price'] = swing_low
                item['why'].append("Stop: 1H Low")

            if strat['is_tc']:
                item['action'] = "READY"
                item['entry']['price'] = price
                item['why'].append("15M 구조전환")
            elif strat['is_oversold']:
                item['action'] = "WAIT"
                item['why'].append("%R 과매도")
            else:
                item['action'] = "WAIT" 
                item['entry']['price'] = int(price * 0.98)
            
            risk = item['entry']['price'] - item['stop']['price']
            if risk <= 0: risk = price * 0.03
            item['target']['price'] = int(item['entry']['price'] + (risk * 3))
            item['target']['rr'] = 3.0
            item['state'] = "WATCH"

        watchlist.append(item)
    
    gw = {'S':3, 'A':2, 'B':1, 'C':0}
    aw = {'READY':2, 'WAIT':1, 'NO_TRADE':0}
    watchlist.sort(key=lambda x: (aw.get(x['action'],0), gw.get(x['grade'],0), x['volume']), reverse=True)
    
    return market, sector_leaders, watchlist

def save_results():
    market, sectors, watchlist = process_data()
    # [수정] 한국 시간(KST)으로 저장 (UTC+9)
    kst_now = datetime.utcnow() + timedelta(hours=9)
    now_str = kst_now.strftime("%Y-%m-%d %H:%M:%S") # 보기 편한 포맷
    
    meta = {
        "asOf": now_str,
        "source": ["KRX", "FDR", "YFinance"],
        "version": "v3.1 (Morning Fix)",
        "status": "ok",
        "market": market
    }
    
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": sectors}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f:
        json.dump({"asOf": now_str, "items": watchlist}, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline v3.1 Completed. Watchlist: {len(watchlist)}")

if __name__ == "__main__":
    save_results()
