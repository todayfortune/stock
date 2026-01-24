import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

# ---------------------------------------------------------
# 2. SDI 전용 전략: Dynamic Trailing Stop (DTS)
#    - 예측(Entry)은 유지하되, 대응(Exit)을 시스템화
#    - 핵심: "예측이 틀리면 짧게 자르고(Time-Cut), 맞으면 끝까지 먹는다(Trailing Stop)"
# ---------------------------------------------------------
def simulate_sdi_period(start_date, end_date):
    # SDI 전략을 테스트할 유니버스 (삼성SDI 포함 주요 대형주)
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '035420': 'NAVER',
        '068270': '셀트리온', '042700': '한미반도체', '006400': '삼성SDI'
    }
    
    # [1] 시장 데이터 (Market Gate)
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        
        # Gate: 20일선 위에 있으면 진입 허용 (최소한의 시장 필터)
        kospi['EARLY_GATE'] = kospi['Close'] > kospi['MA20']
    except: return None

    # [2] 종목 데이터 가공
    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            # --- 기본 지표 ---
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean() # 추세 생명선 (손절 기준)
            
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min() # 직전 저점
            df['NextOpen'] = df['Open'].shift(-1) # 다음날 시가 (실제 매매가)
            
            # --- Entry용 분석 지표 (기존 예측 로직 유지) ---
            # 1. RS (상대강도)
            kospi_matched = kospi['Close'].reindex(df.index).fillna(method='ffill')
            df['RS'] = df['Close'] / kospi_matched
            df['RS_MA20'] = df['RS'].rolling(20).mean().fillna(df['RS'])
            
            # 2. 추세 강도
            df['MA20_Slope'] = df['MA20'].diff(3).fillna(0)
            
            # 3. 구조적 반등 (Break10 OR HigherLow)
            df['Low10'] = df['Low'].shift(1).rolling(10).min()
            df['Prev_Low10'] = df['Low10'].shift(10)
            df['Break10'] = df['Close'] > df['High'].shift(1).rolling(10).max()

            stock_db[code] = df
        except: pass

    # [3] 시뮬레이션 루프
    balance = 10000000
    initial_balance = balance
    
    # 포지션 상태 변수
    holding_code = None
    shares = 0
    entry_price = 0
    highest_price = 0 # 보유 중 최고가 (DTS 핵심 변수)
    
    equity_curve = []
    trade_count = 0
    wins = 0
    
    dates = kospi.index
    
    for i in range(60, len(dates)-1): 
        today = dates[i]
        if today not in kospi.index: continue
        
        is_gate_open = kospi.loc[today]['EARLY_GATE']
        
        # 자산 평가 (일별 마킹)
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(curr_eq)})
        
        # =========================================================
        # [전략 핵심] 매도 로직 (Dynamic Trailing Stop & Time-Cut)
        # =========================================================
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            # 1. 최고가 갱신 (Trailing 기준점 업데이트)
            if row['High'] > highest_price:
                highest_price = row['High']
            
            # 2. 핵심 변수 계산
            current_price = row['Close']
            profit_rate = (current_price - entry_price) / entry_price # 수익률
            
            # 고점 대비 하락률 (Drop Rate)
            drop_rate = 0
            if highest_price > 0:
                drop_rate = (highest_price - current_price) / highest_price
            
            sell_signal = False
            sell_reason = ""
            
            # --- [Rule 1: 기계적 손절 (사조대림 방지)] ---
            # A. -7% 도달 시 즉시 손절 (묻지도 따지지도 않음)
            if profit_rate <= -0.07:
                sell_signal = True
                sell_reason = "LOSS_CUT_7%"
            # B. 60일선 이탈 시 추세 붕괴로 판단
            elif row['Close'] < row['MA60']:
                sell_signal = True
                sell_reason = "MA60_BREAK"
                
            # --- [Rule 2: 수익 보전 및 극대화 (효성중공업 방지)] ---
            elif profit_rate > 0:
                # A. 수익 초기 (0~10%): 타이트하게 방어 (3% 반납 시 매도)
                if profit_rate < 0.10:
                    if drop_rate >= 0.03:
                        sell_signal = True
                        sell_reason = "TS_TIGHT (3% Drop)"
                # B. 추세 형성 (10~30%): 숨통 트기 (5% 반납 시 매도)
                elif 0.10 <= profit_rate < 0.30:
                    if drop_rate >= 0.05:
                        sell_signal = True
                        sell_reason = "TS_NORMAL (5% Drop)"
                # C. 대세 상승 (30%~): 길게 먹기 (10% 반납 시 매도 - 효성중공업 Case)
                else:
                    if drop_rate >= 0.10:
                        sell_signal = True
                        sell_reason = "TS_LOOSE (10% Drop)"

            # 매도 실행
            if sell_signal:
                # 다음날 시가 매도 (보수적 접근)
                sell_price = row['NextOpen'] if not np.isnan(row['NextOpen']) else row['Close']
                sell_amt = shares * sell_price * 0.9975 # 수수료/세금 반영
                balance += sell_amt
                
                if sell_amt > (shares * entry_price): wins += 1
                trade_count += 1
                
                # 포지션 초기화
                holding_code = None
                shares = 0
                entry_price = 0
                highest_price = 0
                continue

        # =========================================================
        # 매수 로직 (기존 High Logic 유지)
        # =========================================================
        if holding_code is None and is_gate_open:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # 진입 조건 (Trend + RS + Structure)
                c1_trend = (curr['Close'] > curr['MA20']) and (curr['MA20_Slope'] > 0)
                c2_rs = curr['RS'] > curr['RS_MA20']
                c3_struct = (curr['Low10'] > curr['Prev_Low10']) or curr['Break10']
                
                if c1_trend and c2_rs and c3_struct:
                    # 진입 (비중 100%)
                    shares = int(balance / curr['Close'])
                    if shares > 0:
                        balance -= shares * curr['Close'] * 1.00015 # 수수료 반영
                        holding_code = code
                        entry_price = curr['Close']
                        highest_price = curr['Close'] # 매수 직후 최고가는 매수가
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
    print("🚀 Running SDI Strategy (DTS & Time-Cut) Backtest...")
    
    recent_start = datetime.now() - timedelta(days=365*3)
    recent_end = datetime.now()
    
    # 테스트 기간 설정
    periods = {
        "early": (recent_start, recent_end),             # 최근 3년
        "early_covid": ("2020-01-01", "2023-12-31"),     # 코로나 유동성 장세
        "early_box": ("2015-01-01", "2019-12-31")        # 박스권 장세
    }
    
    results = {}
    for key, (start, end) in periods.items():
        print(f"   Running {key}...")
        res = simulate_sdi_period(start, end)
        if res: results[key] = res
        
    output_path = os.path.join(DATA_DIR, 'backtest_sdi.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"✅ SDI Strategy (DTS) Saved to '{output_path}'")

if __name__ == "__main__":
    run_sdi_backtest()
