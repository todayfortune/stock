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
# 2. [NEW] 백테스팅 엔진 (최근 3년 시뮬레이션)
# ---------------------------------------------------------
def run_simple_backtest():
    print("🧪 백테스팅 시뮬레이션 가동 중...")
    
    # 1. 유니버스 (대표 주도주 10개)
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '006400': '삼성SDI', '005380': '현대차', '005490': 'POSCO홀딩스',
        '035420': 'NAVER', '068270': '셀트리온', '010120': 'LS ELECTRIC',
        '042700': '한미반도체'
    }
    
    # 최근 3년
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*3)
    
    # 데이터 수집
    kospi = fdr.DataReader('KS11', start_date, end_date)
    kospi['MA20'] = kospi['Close'].rolling(20).mean()
    kospi['RISK_ON'] = kospi['Close'] > kospi['MA20']
    
    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            df['Amount'] = df['Close'] * df['Volume']
            df['MA5'] = df['Close'].rolling(5).mean()
            df['MA20'] = df['Close'].rolling(20).mean()
            
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            stock_db[code] = df
        except: pass

    # 시뮬레이션
    balance = 10_000_000
    initial_balance = balance
    holding_code = None
    shares = 0
    equity_curve = [] # 일자별 자산
    trade_count = 0
    wins = 0
    
    dates = kospi.index
    entry_price = 0
    
    for i in range(20, len(dates)):
        today = dates[i]
        if today not in kospi.index: continue
        
        # 자산 평가
        curr_eq = balance
        if holding_code:
            if today in stock_db[holding_code].index:
                curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        
        equity_curve.append({
            "date": today.strftime("%Y-%m-%d"),
            "equity": int(curr_eq)
        })
        
        # 매도 로직
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            # 익절 7% / 손절 5% / 시장 하락 시 청산
            is_win = False
            is_sell = False
            sell_price = 0
            
            if row['High'] >= entry_price * 1.07:
                sell_price = entry_price * 1.07
                is_win = True; is_sell = True
            elif row['Low'] <= entry_price * 0.95:
                sell_price = entry_price * 0.95
                is_win = False; is_sell = True
            elif not kospi.loc[today]['RISK_ON']:
                sell_price = row['Close']
                is_win = sell_price > entry_price
                is_sell = True
                
            if is_sell:
                balance += shares * sell_price
                holding_code = None
                shares = 0
                trade_count += 1
                if is_win: wins += 1
                continue

        # 매수 로직 (눌림목)
        if holding_code is None and kospi.loc[today]['RISK_ON']:
            candidates = []
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                if curr['MA5'] > curr['MA20'] and curr['Close'] <= curr['MA5']*1.01 and curr['RSI'] < 70:
                    candidates.append({'code': code, 'rsi': curr['RSI']})
            
            if candidates:
                best = sorted(candidates, key=lambda x: x['rsi'])[0]
                code = best['code']
                price = stock_db[code].loc[today]['Close']
                shares = int(balance / price)
                if shares > 0:
                    balance -= shares * price
                    entry_price = price
                    holding_code = code

    # 결과 요약
    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    # MDD 계산
    eq_series = pd.Series([e['equity'] for e in equity_curve])
    peak = eq_series.cummax()
    drawdown = (eq_series - peak) / peak
    mdd = drawdown.min() * 100

    return {
        "summary": {
            "total_return": round(total_return, 2),
            "final_balance": int(final_eq),
            "trade_count": trade_count,
            "win_rate": round(win_rate, 1),
            "mdd": round(mdd, 2)
        },
        "equity_curve": equity_curve # 차트용 데이터
    }

# ---------------------------------------------------------
# 3. 기존 수집 로직 (v3.2 유지)
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
        
        return {"swing_low": int(swing_low), "wr": round(current_wr, 1), "is_tc": is_tc, "is_oversold": is_oversold}
    except: return None

def analyze_market_regime():
    try:
        kospi = fdr.DataReader('KS11', '2023-01-01')
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        state = "RISK_ON" if curr['Close'] > ma20 else "RISK_OFF"
        reason = "KOSPI > 20MA" if state == "RISK_ON" else "KOSPI < 20MA"
        return {"state": state, "reason": reason}
    except: return {"state": "RISK_ON", "reason": "Error"}

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
        
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df = df[valid_mask].copy()
    
    sector_leaders = []
    for sector, group in df.groupby('CustomSector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        score = int((group['거래대금'].mean()/1e8) + (group['등락률'].mean()*10))
        top_names = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sector_leaders.append({"sector": sector, "score": score, "turnover": int(group['거래대금'].sum()), "topTickers": top_names})
    sector_leaders.sort(key=lambda x: x['score'], reverse=True)
    
    watchlist = []
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(30)
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
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

        if vol >= 1000e8 or (vol >= 500e8 and change >= 15): item['grade'] = "S"
        elif vol >= 300e8: item['grade'] = "A"
        elif vol >= 100e8: item['grade'] = "B"
        else: item['grade'] = "C"

        if change < 0: continue

        strat = get_detailed_strategy(code, price)
        count += 1
        time.sleep(1.0)
        
        if strat:
            swing_low = strat['swing_low']
            if price > 0 and (price - swing_low)/price > 0.1: item['stop']['price'] = int(price * 0.97)
            else: item['stop']['price'] = swing_low

            if strat['is_tc']: item['action'] = "READY"; item['entry']['price'] = price; item['why'].append("15M 구조전환")
            elif strat['is_oversold']: item['action'] = "WAIT"; item['why'].append("%R 과매도")
            else: item['action'] = "WAIT"; item['entry']['price'] = int(price * 0.98)
            
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
    
    # [NEW] 백테스트 실행 및 저장
    backtest_data = run_simple_backtest()
    
    kst_now = datetime.utcnow() + timedelta(hours=9)
    now_str = kst_now.strftime("%Y-%m-%d %H:%M:%S (KST)")
    
    meta = {"asOf": now_str, "source": ["KRX", "FDR", "YFinance"], "version": "v4.0 (Backtest)", "status": "ok", "market": market}
    
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f: json.dump({"asOf": now_str, "items": sectors}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f: json.dump({"asOf": now_str, "items": watchlist}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'backtest.json'), 'w', encoding='utf-8') as f: json.dump(backtest_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline v4.0 Completed (with Backtest).")

if __name__ == "__main__":
    save_results()
