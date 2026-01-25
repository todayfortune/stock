import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화
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
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')

os.makedirs(DATA_DIR, exist_ok=True)

def load_universe():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '006400': '삼성SDI'
    }

# ---------------------------------------------------------
# 2. MSI EARLY 전략 엔진 (저점 턴어라운드 포착)
# ---------------------------------------------------------
def simulate_sdi_period(start_date, end_date):
    UNIVERSE = load_universe()
    
    # [1] 시장 데이터 (Early Gate)
    # 조건: 시장이 "완전 붕괴(MA60 아래)"만 아니면 기회를 엿봄
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        # Gate: 60일선 위에 있으면 "최소한의 바닥은 다졌다"고 판단
        kospi['EARLY_GATE'] = kospi['Close'] > kospi['MA60']
    except: return None

    # [2] 종목 데이터 가공 (MSI Logic 적용)
    stock_db = {}
    print(f"📊 Processing {len(UNIVERSE)} stocks for MSI EARLY...")
    
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if len(df) < 60: continue

            # 이평선
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            
            # 1. 단기 회복 시그널 (MA20 기울기)
            # 5일 전 MA20과 비교하여 상승 중인지 확인
            df['MA20_Slope'] = df['MA20'] - df['MA20'].shift(5)
            
            # 2. RS (상대강도)
            # 종목 등락 / 시장 등락 비율 (간소화된 RS)
            kospi_matched = kospi['Close'].reindex(df.index).fillna(method='ffill')
            df['RS_Ratio'] = df['Close'] / kospi_matched
            df['RS_MA20'] = df['RS_Ratio'].rolling(20).mean().fillna(df['RS_Ratio'])
            
            # 3. 바닥 구조 (Higher Low)
            # 최근 10일 저점 vs 그 이전 10일 저점 비교
            df['Low10'] = df['Low'].shift(1).rolling(10).min()      # 어제까지 10일 최저
            df['Prev_Low10'] = df['Low'].shift(11).rolling(10).min() # 그 전 10일 최저
            
            # 4. 트리거 (Break20)
            # 20일 고가 돌파 (가짜 반등 필터링)
            df['Break20'] = df['Close'] > df['High'].shift(1).rolling(20).max()
            
            # 손절/청산용 데이터
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            df['NextOpen'] = df['Open'].shift(-1)

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
    
    entry_price = 0
    
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
        
        # --- [매도 로직] ---
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            
            # 손절: 스윙 저점 이탈
            stop_price = row['SwingLow'] * 0.98 if not pd.isna(row['SwingLow']) else row['Close'] * 0.90
            # 익절: RR 1:3 (진입가 + 리스크*3)
            risk = entry_price - stop_price
            target_price = entry_price + (risk * 3) if risk > 0 else entry_price * 1.15

            if row['Low'] <= stop_price: 
                exit_type = 'STOP'; sell_price = stop_price
            elif row['High'] >= target_price:
                exit_type = 'TARGET'; sell_price = target_price
            # 시장 퇴출 (Gate 닫히면)
            elif not is_gate_open:
                exit_type = 'MKT_OUT'
                sell_price = row['NextOpen'] if not pd.isna(row['NextOpen']) else row['Close']

            if exit_type:
                final_sell = sell_price
                sell_amt = shares * final_sell * 0.9975
                balance += sell_amt
                
                if final_sell > entry_price: wins += 1
                trade_count += 1
                holding_code = None
                shares = 0
                continue

        # --- [매수 로직: MSI EARLY] ---
        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # -----------------------------------------
                # 🔥 MSI EARLY 핵심 진입 조건
                # -----------------------------------------
                
                # 1. 아직 역배열인가? (장기 하락 중 반등 노림)
                cond_downtrend = curr['Close'] < curr['MA60']
                
                # 2. 단기 회복세인가? (20일선 위 + 기울기 상승)
                cond_recovery = (curr['Close'] > curr['MA20']) and (curr['MA20_Slope'] > 0)
                
                # 3. 바닥을 높였는가? (Higher Low)
                cond_structure = curr['Low10'] > curr['Prev_Low10']
                
                # 4. 시장보다 강한가? (RS 개선)
                cond_rs = curr['RS_Ratio'] > curr['RS_MA20']
                
                # 5. 매물대 돌파했는가? (Trigger)
                cond_trigger = curr['Break20']
                
                # [최종 진입]
                if cond_downtrend and cond_recovery and cond_structure and cond_rs and cond_trigger:
                    
                    # 리스크 관리: 스윙 저점 없으면 패스
                    if pd.isna(curr['SwingLow']): continue
                    
                    # 포지션 사이징: "작게 진입" (자본의 50%만 투입)
                    invest_amount = balance * 0.5 
                    
                    shares = int(invest_amount / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015
                        holding_code = code
                        entry_price = curr['Close']
                        print(f"   🚀 Buy {code} on {today.date()} (MSI Early Signal)")
                        break

    # 결과 요약
    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    trade_count = trade_count if trade_count > 0 else 0
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    if trade_count == 0:
        print("⚠️ Warning: No trades executed. Strategy might be too strict for this period.")

    return {
        "summary": {
            "total_return": round(total_return, 2),
            "final_balance": int(final_eq),
            "trade_count": trade_count,
            "win_rate": round(win_rate, 1),
            "mdd": 0 # (약식)
        },
        "equity_curve": equity_curve
    }

def run_sdi_backtest():
    print("🚀 Running MSI EARLY Strategy Backtest...")
    
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
    print(f"✅ Results saved to {output_path}")

if __name__ == "__main__":
    run_sdi_backtest()
