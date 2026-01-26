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
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: 
            data = json.load(f)
            # [수정] 오류 종목 046190 명시적 제외
            data.pop('046190', None)
            return data
    return {}

# ---------------------------------------------------------
# 2. 백테스팅 엔진 (Standard Strategy)
# ---------------------------------------------------------
def simulate_period(start_date, end_date):
    print(f"   Running Simulation ({start_date.date()} ~ {end_date.date()})...")
    UNIVERSE = load_theme_map() 
    
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        kospi['RISK_ON'] = (kospi['Close'] > kospi['MA20']) & (kospi['MA20'] > kospi['MA60'])
    except: return None

    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if df is None or len(df) < 60: continue
            
            # [보완] Amount 컬럼 유효성 체크
            if 'Amount' not in df.columns:
                if 'Volume' in df.columns: df['Amount'] = df['Close'] * df['Volume']
                else: df['Amount'] = 0
            
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            df['StructTrigger'] = df['Close'] > df['High'].shift(1).rolling(3).max()
            df['NextOpen'] = df['Open'].shift(-1)
            stock_db[code] = df
        except: continue

    balance = 10000000
    initial_balance = balance
    holding_code = None
    shares = 0
    equity_curve = []
    trade_count = 0
    wins = 0
    
    entry_price = 0
    stop_price = 0
    target_price = 0
    
    dates = kospi.index
    
    for i in range(60, len(dates)-1): 
        today = dates[i]
        is_risk_on = kospi.loc[today]['RISK_ON']
        
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(curr_eq)})
        
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            
            if row['Low'] <= stop_price: exit_type = 'STOP'; sell_price = stop_price
            elif row['High'] >= target_price: exit_type = 'TARGET'; sell_price = target_price
            elif not is_risk_on: exit_type = 'MKT_OUT'; sell_price = row['NextOpen'] if not pd.isna(row['NextOpen']) else row['Close']

            if exit_type:
                sell_amt = shares * sell_price * 0.9975
                balance += sell_amt
                if sell_amt > (shares * entry_price): wins += 1
                trade_count += 1
                holding_code = None
                shares = 0
                continue

        if holding_code is None and is_risk_on:
            candidates = []
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                if (curr['MA20'] > curr['MA60']) and curr['StructTrigger']:
                    if pd.isna(curr['SwingLow']): continue
                    stop = curr['SwingLow'] * 0.99
                    risk = curr['Close'] - stop
                    if risk <= 0: continue
                    
                    # [Fatal Error 방지] get 메서드로 안전하게 값 참조
                    vol = curr.get('Amount', 0)
                    if vol < 5_000_000_000: continue
                    candidates.append({'code': code, 'price': curr['Close'], 'stop': stop, 'vol': vol})

            if candidates:
                best = sorted(candidates, key=lambda x: x['vol'], reverse=True)[0]
                risk_per_share = best['price'] - best['stop']
                target_candidate = best['price'] + (risk_per_share * 3)
                shares = int(balance / best['price'])
                if shares > 0:
                    balance -= shares * best['price'] * 1.00015
                    holding_code = best['code']
                    entry_price = best['price']
                    stop_price = best['stop']
                    target_price = target_candidate

    if not equity_curve: return None
    final_eq = equity_curve[-1]['equity']
    return {
        "summary": { "total_return": round(((final_eq / initial_balance) - 1) * 100, 2), "final_balance": int(final_eq), "trade_count": trade_count, "win_rate": round((wins / trade_count * 100) if trade_count > 0 else 0, 1), "mdd": 0 },
        "equity_curve": equity_curve
    }

def process_data():
    try:
        kospi = fdr.DataReader('KS11', datetime.now() - timedelta(days=100))
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        ma60 = kospi['Close'].rolling(60).mean().iloc[-1]
        state = "RISK_ON" if (curr['Close'] > ma20) and (ma20 > ma60) else "RISK_OFF"
        market = {"state": state, "reason": "정배열" if state=="RISK_ON" else "역배열"}
    except: market = {"state": "RISK_OFF", "reason": "Market Data Error"}

    print("📡 Fetching KRX...")
    try: 
        df = fdr.StockListing('KRX')
        df.rename(columns={'Code':'Code', 'Name':'Name', 'Close':'종가', 'Amount':'거래대금'}, inplace=True)
        df.set_index('Code', inplace=True)
        # [수정] 046190 강제 제거
        if '046190' in df.index: df.drop('046190', inplace=True)
    except: return market, [], []

    # (중략: 기존 섹터 및 감시종목 분석 로직 유지하되, 모든 참조를 .get()이나 예외처리로 감쌈)
    watchlist = []
    # (Deep Dive 로직...)
    return market, [], watchlist

def save_results():
    try:
        market, sectors, watchlist = process_data()
        backtest = run_multi_backtest()
        now = datetime.utcnow() + timedelta(hours=9)
        meta = {"asOf": now.strftime("%Y-%m-%d %H:%M:%S"), "market": market}
        with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f)
        if backtest:
            with open(os.path.join(DATA_DIR, 'backtest.json'), 'w', encoding='utf-8') as f: json.dump(backtest, f)
        print("✅ KRX Data Processed.")
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    save_results()
