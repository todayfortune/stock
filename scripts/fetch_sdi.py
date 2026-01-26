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
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    return {'006400': '삼성SDI', '010060': 'OCI홀딩스'}

# ---------------------------------------------------------
# 2. MSI EARLY 전략 (진입 조건 최적화)
# ---------------------------------------------------------
def simulate_sdi_period(start_date, end_date):
    UNIVERSE = load_universe()
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        # [완화] 60일선 근처(-2%)만 가도 시장 참여 허용
        kospi['EARLY_GATE'] = kospi['Close'] > (kospi['MA60'] * 0.98)
    except: return None

    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if df is None or len(df) < 40: continue # 최소 데이터 기준 완화
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            df['MA20_Slope'] = df['MA20'].diff(3) # 기울기 감지 기간 단축
            
            # RS 상대강도 계산 개선
            kospi_matched = kospi['Close'].reindex(df.index).ffill()
            df['RS_Ratio'] = df['Close'] / kospi_matched
            df['RS_MA20'] = df['RS_Ratio'].rolling(20).mean()
            
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            df['Break20'] = df['Close'] > df['High'].shift(1).rolling(10).max() # 돌파 기준 완화
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
            if today not in df.index: continue
            row = df.loc[today]
            stop_price = row['SwingLow'] * 0.97 if not pd.isna(row['SwingLow']) else entry_price * 0.93
            
            if row['Low'] <= stop_price:
                balance += shares * stop_price * 0.9975
                if stop_price > entry_price: wins += 1
                trade_count += 1
                holding_code = None
                shares = 0
            elif row['High'] >= entry_price * 1.15: # 15% 익절
                balance += shares * (entry_price * 1.15) * 0.9975
                wins += 1
                trade_count += 1
                holding_code = None
                shares = 0

        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # [MSI EARLY 진입 조건]
                cond_downtrend = curr['Close'] < curr['MA60'] # 아직 저평가
                cond_recovery = curr['Close'] > curr['MA20']  # 단기 추세 회복
                cond_rs = curr['RS_Ratio'] > curr['RS_MA20'] if not pd.isna(curr['RS_MA20']) else True
                
                if cond_downtrend and cond_recovery and cond_rs:
                    shares = int((balance * 0.5) / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015
                        holding_code = code
                        entry_price = curr['Close']
                        print(f"   🚀 Buy {code} on {today.date()}")
                        break

    return {"summary": {"total_return": round(((equity_curve[-1]['equity']/initial_balance)-1)*100, 2), "trade_count": trade_count}, "equity_curve": equity_curve}

def run_sdi_backtest():
    print("🚀 Running MSI EARLY Strategy Backtest...")
    res = simulate_sdi_period(datetime.now()-timedelta(days=365), datetime.now())
    if res:
        with open(os.path.join(DATA_DIR, 'backtest_sdi.json'), 'w', encoding='utf-8') as f:
            json.dump({"recent": res}, f, ensure_ascii=False, indent=2)
        print("✅ Results saved.")

if __name__ == "__main__":
    run_sdi_backtest()
