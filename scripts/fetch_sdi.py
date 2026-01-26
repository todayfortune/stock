import os
import json
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화
# ---------------------------------------------------------
HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(HERE)
DATA_DIR = os.path.join(BASE_DIR, "data")
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')

def load_universe():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: 
            data = json.load(f)
            # [수정] 오류 종목 제외
            if '046190' in data: del data['046190']
            return data
    return {'006400': '삼성SDI', '010060': 'OCI홀딩스'}

def simulate_sdi_period(start_date, end_date):
    UNIVERSE = load_universe()
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 40: return None
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        # [완화] 시장이 너무 하락세만 아니면 기회 탐색
        kospi['EARLY_GATE'] = kospi['Close'] > (kospi['MA60'] * 0.95)
    except: return None

    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if df is None or len(df) < 40: continue
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
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
    dates = kospi.index
    
    for i in range(20, len(dates)-1): 
        today = dates[i]
        is_gate_open = kospi.loc[today]['EARLY_GATE']
        
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(curr_eq)})
        
        if holding_code:
            df = stock_db[holding_code]
            row = df.loc[today]
            # [현실화] 손절 및 익절 로직 감도 조정
            stop_price = row['SwingLow'] * 0.97 if not pd.isna(row['SwingLow']) else entry_price * 0.92
            if row['Low'] <= stop_price:
                balance += shares * stop_price * 0.9975
                if stop_price > entry_price: wins += 1
                trade_count += 1
                holding_code = None
                shares = 0
            elif row['High'] >= entry_price * 1.10: 
                balance += shares * (entry_price * 1.10) * 0.9975
                wins += 1
                trade_count += 1
                holding_code = None
                shares = 0

        # [매수 로직: MSI EARLY]
        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                # 하락 후 바닥권에서 20일선 회복 시 진입
                if curr['Close'] < curr['MA60'] and curr['Close'] > curr['MA20']:
                    shares = int((balance * 0.7) / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015
                        holding_code = code
                        entry_price = curr['Close']
                        print(f"   🚀 MSI EARLY Buy {code} on {today.date()}")
                        break

    return {"summary": {"total_return": round(((equity_curve[-1]['equity'] / initial_balance) - 1) * 100, 2), "trade_count": trade_count}, "equity_curve": equity_curve}

# (run_sdi_backtest 로직 기존 유지)
