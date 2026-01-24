import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화 (기현 님의 스마트 경로 탐색 적용)
# ---------------------------------------------------------
def find_repo_root(start_path: str) -> str:
    """스크립트 위치와 상관없이 프로젝트 루트(data 폴더 있는 곳)를 찾음"""
    p = os.path.abspath(start_path)
    while True:
        # 현재 위치에 'data' 폴더가 있으면 거기가 루트
        if os.path.isdir(os.path.join(p, "data")):
            return p
        # 상위 폴더로 이동
        parent = os.path.dirname(p)
        if parent == p: # 더 이상 올라갈 곳이 없으면
            # (차선책) 처음에 시작했던 스크립트 위치의 부모를 반환 (기존 방식)
            return os.path.dirname(os.path.abspath(start_path))
        p = parent

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = find_repo_root(HERE)
DATA_DIR = os.path.join(BASE_DIR, "data")

# 데이터 폴더가 없으면 생성 (안전장치)
os.makedirs(DATA_DIR, exist_ok=True)

print(f"📂 Data Directory: {DATA_DIR}") # 로그로 경로 확인

# ---------------------------------------------------------
# 2. SDI 전용 시뮬레이터 (v7.1 Logic: NaN Fix + Break10)
# ---------------------------------------------------------
def simulate_sdi_period(start_date, end_date):
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '035420': 'NAVER',
        '068270': '셀트리온', '042700': '한미반도체', '006400': '삼성SDI'
    }
    
    # [1] 시장 데이터 (Gate용)
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        
        # Gate: 20일선 위에 있으면 진입 허용 (하락장 속 반등장)
        kospi['EARLY_GATE'] = kospi['Close'] > kospi['MA20']
    except: return None

    # [2] 종목 데이터 가공
    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            # 기본 지표
            df['MA20'] = df['Close'].rolling(20).mean()
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min() # 직전 저점 (손절용)
            df['NextOpen'] = df['Open'].shift(-1) # 다음날 시가 (청산용)
            
            # [SDI 핵심 지표 - ★여기가 중요합니다!]
            
            # 1. RS (상대강도) - NaN 처리 포함 (v7.1 Fix)
            kospi_matched = kospi['Close'].reindex(df.index).fillna(method='ffill')
            df['RS'] = df['Close'] / kospi_matched
            # ★ 값이 없으면(NaN) 현재 RS 값으로 채워서 에러 방지
            df['RS_MA20'] = df['RS'].rolling(20).mean().fillna(df['RS']) 
            
            # 2. 추세 강도 (NaN 처리)
            df['MA20_Slope'] = df['MA20'].diff(3).fillna(0)
            
            # 3. 구조적 반등 (Break10 & HigherLow)
            df['Low10'] = df['Low'].shift(1).rolling(10).min()
            df['Prev_Low10'] = df['Low10'].shift(10)
            # ★ 20일 신고가(Break20)는 너무 빡빡해서 10일(Break10)로 완화
            df['Break10'] = df['Close'] > df['High'].shift(1).rolling(10).max()

            stock_db[code] = df
        except: pass

    # [3] 시뮬레이션 루프
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
        
        # --- 매도 로직 ---
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            
            stop_price = row['SwingLow'] * 0.98 if not pd.isna(row['SwingLow']) else row['Close'] * 0.95
            
            # 손절/익절 조건 (간소화)
            if row['Low'] <= stop_price: exit_type = 'STOP'; sell_price = stop_price
            
            # 시장 퇴출: 코스피가 20일선 깨지거나, 종목이 20일선 깨지면
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

        # --- 매수 로직 ---
        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # 진입 조건 (v7.1 Relaxed)
                # 1. 단기 상승세
                c1_trend = (curr['Close'] > curr['MA20']) and (curr['MA20_Slope'] > 0)
                # 2. RS 강도 (NaN 처리됨)
                c2_rs = curr['RS'] > curr['RS_MA20']
                # 3. 구조적 반등 (Break10 OR HigherLow)
                c3_struct = (curr['Low10'] > curr['Prev_Low10']) or curr['Break10']
                
                if c1_trend and c2_rs and c3_struct:
                    # 손절가 설정
                    stop_lvl = curr['SwingLow']
                    if pd.isna(stop_lvl) or stop_lvl > curr['Close']:
                        stop_lvl = curr['MA20'] * 0.98

                    stop = stop_lvl * 0.98
                    risk = curr['Close'] - stop
                    if risk <= 0: continue

                    # 진입 (비중 100%)
                    shares = int(balance / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015
                        holding_code = code
                        entry_price = curr['Close']
                        break

    # 결과 정리
    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    eq_series = pd.Series([e['equity'] for e in equity_curve])
    peak = eq_series.cummax()
    mdd = ((eq_series - peak) / peak).min() * 100

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
    print("🚀 Running SDI Strategy Backtest...")
    
    recent_start = datetime.now() - timedelta(days=365*3)
    recent_end = datetime.now()
    
    # SDI 메뉴에서 쓸 키값들 (early, early_covid, early_box)
    periods = {
        "early": (recent_start, recent_end),
        "early_covid": ("2020-01-01", "2023-12-31"),
        "early_box": ("2015-01-01", "2019-12-31")
    }
    
    results = {}
    for key, (start, end) in periods.items():
        print(f"   Running {key}...")
        res = simulate_sdi_period(start, end)
        if res: results[key] = res
        
    # 결과 저장 (경로는 위에서 자동으로 찾은 DATA_DIR 사용)
    output_path = os.path.join(DATA_DIR, 'backtest_sdi.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✅ SDI Strategy Saved to '{output_path}'")

if __name__ == "__main__":
    run_sdi_backtest()
