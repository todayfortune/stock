import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화 (경로 자동 탐색)
# ---------------------------------------------------------
def find_repo_root(start_path: str) -> str:
    p = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(p, "data")):
            return p
        parent = os.path.dirname(p)
        if parent == p:
            return os.path.dirname(os.path.abspath(start_path))
        p = parent

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = find_repo_root(HERE)
DATA_DIR = os.path.join(BASE_DIR, "data")
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json') # 테마맵 경로

os.makedirs(DATA_DIR, exist_ok=True)

# [핵심] 테마 종목 불러오기 (유니버스 확장)
def load_universe():
    # 1. 테마 맵 파일이 있으면 로드
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            theme_map = json.load(f)
            print(f"📂 Loaded {len(theme_map)} stocks from theme_map.json")
            return theme_map
    
    # 2. 없으면 기본 대형주 9개 사용 (Fallback)
    print("⚠️ theme_map.json not found. Using default 9 stocks.")
    return {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '035420': 'NAVER',
        '068270': '셀트리온', '042700': '한미반도체', '006400': '삼성SDI'
    }

# ---------------------------------------------------------
# 2. SDI 전용 시뮬레이터
# ---------------------------------------------------------
def simulate_sdi_period(start_date, end_date):
    UNIVERSE = load_universe()
    
    # [1] 시장 데이터
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        # Gate: 20일선 위 (반등장)
        kospi['EARLY_GATE'] = kospi['Close'] > kospi['MA20']
    except: return None

    # [2] 종목 데이터 가공
    stock_db = {}
    print(f"📊 Processing {len(UNIVERSE)} stocks data...")
    
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if len(df) < 60: continue # 데이터 부족하면 패스

            df['MA20'] = df['Close'].rolling(20).mean()
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            df['NextOpen'] = df['Open'].shift(-1)
            
            # SDI 지표
            kospi_matched = kospi['Close'].reindex(df.index).fillna(method='ffill')
            df['RS'] = df['Close'] / kospi_matched
            df['RS_MA20'] = df['RS'].rolling(20).mean().fillna(df['RS']) 
            
            df['MA20_Slope'] = df['MA20'].diff(3).fillna(0)
            
            df['Low10'] = df['Low'].shift(1).rolling(10).min()
            df['Prev_Low10'] = df['Low10'].shift(10)
            df['Break10'] = df['Close'] > df['High'].shift(1).rolling(10).max()

            stock_db[code] = df
        except: pass

    # [3] 시뮬레이션
    balance = 10000000
    initial_balance = balance
    holding_code = None
    shares = 0
    equity_curve = []
    trade_count = 0
    wins = 0
    
    dates = kospi.index
    
    for i in range(60, len(dates)-1): 
        today = dates[i]
        if today not in kospi.index: continue
        
        is_gate_open = kospi.loc[today]['EARLY_GATE']
        
        # 자산 평가
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(curr_eq)})
        
        # 매도 로직
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            stop_price = row['SwingLow'] * 0.98 if not pd.isna(row['SwingLow']) else row['Close'] * 0.95
            
            if row['Low'] <= stop_price: exit_type = 'STOP'; sell_price = stop_price
            elif (not is_gate_open) or (row['Close'] < row['MA20']):
                exit_type = 'MKT_OUT'; sell_price = row['NextOpen']

            if exit_type:
                final_sell = sell_price if sell_price > 0 else row['Close']
                sell_amt = shares * final_sell * 0.9975
                balance += sell_amt
                if sell_amt > (shares * entry_price): wins += 1
                trade_count += 1
                holding_code = None
                shares = 0
                continue

        # 매수 로직
        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # 진입 조건 (v7.1 + 완화)
                # Slope > -0.05 (완전 하락만 아니면 OK)
                c1_trend = (curr['Close'] > curr['MA20']) and (curr['MA20_Slope'] > -0.05)
                # RS > 0.98 * MA (살짝 낮아도 허용)
                c2_rs = curr['RS'] > (curr['RS_MA20'] * 0.98)
                # 구조
                c3_struct = (curr['Low10'] > curr['Prev_Low10']) or curr['Break10']
                
                if c1_trend and c2_rs and c3_struct:
                    shares = int(balance / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015
                        holding_code = code
                        entry_price = curr['Close']
                        break # 하루에 한 종목만 매수

    # 결과 요약
    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    eq_series = pd.Series([e['equity'] for e in equity_curve])
    peak = eq_series.cummax()
    mdd = ((eq_series - peak) / peak).min() * 100

    # [DEBUG] 결과 출력
    print(f"   [Result] Trades: {trade_count}, Return: {total_return:.2f}%")

    return {
        "summary": {
            "total_return": round(total_return, 2),
            "final_balance": int(final_eq),
            "trade_count": trade_count,
            "win_rate": round(win_rate, 1),
            "mdd": round(mdd, 2)
        },
        "equity_curve": equity_curve
    }

def run_sdi_backtest():
    print("🚀 Running SDI Strategy Backtest (Expanded)...")
    
    recent_start = datetime.now() - timedelta(days=365*3)
    recent_end = datetime.now()
    
    periods = {
        "early": (recent_start, recent_end),
        "early_covid": (datetime(2020,1,1), datetime(2023,12,31)),
        "early_box": (datetime(2015,1,1), datetime(2019,12,31))
    }
    
    results = {}
    for key, (start, end) in periods.items():
        print(f"   Running {key}...")
        res = simulate_sdi_period(start, end)
        if res: results[key] = res
        
    output_path = os.path.join(DATA_DIR, 'backtest_sdi.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✅ SDI Strategy Saved to '{output_path}'")

if __name__ == "__main__":
    run_sdi_backtest()
