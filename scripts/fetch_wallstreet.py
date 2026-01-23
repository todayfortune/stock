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
# 2. 보조지표 계산 함수 (ATR, EMA, RS)
# ---------------------------------------------------------
def calculate_indicators(df, kospi_df):
    # EMA (지수이동평균)
    df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
    
    # ATR (변동성 지표)
    df['TR'] = np.maximum(
        df['High'] - df['Low'],
        np.maximum(abs(df['High'] - df['Close'].shift(1)), abs(df['Low'] - df['Close'].shift(1)))
    )
    df['ATR'] = df['TR'].rolling(window=14).mean()
    
    # Swing Low (최근 10일 저점)
    df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
    
    # RS (상대강도): (종목60일상승률) - (시장60일상승률)
    stock_ret = df['Close'].pct_change(60)
    market_ret = kospi_df['Close'].pct_change(60).reindex(df.index).fillna(0)
    df['RS_Score'] = stock_ret - market_ret
    
    return df

# ---------------------------------------------------------
# 3. 월가 전략 백테스팅 엔진
# ---------------------------------------------------------
def simulate_wallstreet(start_date, end_date):
    # 유니버스 (우량주 위주)
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '035420': 'NAVER',
        '068270': '셀트리온', '042700': '한미반도체', '006400': '삼성SDI'
    }

    # [1] 시장 데이터 (Market Regime)
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        kospi['MA50'] = kospi['Close'].rolling(50).mean()
        kospi['MA200'] = kospi['Close'].rolling(200).mean()
        # 시장 필터: 50일 > 200일 AND 현재가 > 200일 (완전 정배열)
        kospi['Bull_Market'] = (kospi['MA50'] > kospi['MA200']) & (kospi['Close'] > kospi['MA200'])
    except:
        return None

    # [2] 종목 데이터 준비
    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if len(df) < 200: continue
            df = calculate_indicators(df, kospi)
            stock_db[code] = df
        except: pass

    # [3] 시뮬레이션 루프
    balance = 10000000
    initial_balance = balance
    equity_curve = []
    
    # 포지션 관리 변수
    positions = {} # { 'code': { 'shares': 0, 'stop_price': 0, 'entry_price': 0 } }
    
    dates = kospi.index
    trade_count = 0
    wins = 0
    
    for i in range(200, len(dates)):
        today = dates[i]
        if today not in kospi.index: continue
        
        # 1. 시장 필터 확인
        is_bull_market = kospi.loc[today]['Bull_Market']
        
        # 2. 보유 종목 관리 (매도/트레일링스탑)
        active_codes = list(positions.keys())
        for code in active_codes:
            pos = positions[code]
            df = stock_db[code]
            if today not in df.index: continue
            
            row = df.loc[today]
            current_price = row['Close']
            high_price = row['High']
            low_price = row['Low']
            atr = row['ATR']
            
            # [익절/손절 로직]
            # 트레일링 스탑 업데이트: (최고가 - 2*ATR) 따라가기
            new_stop = high_price - (atr * 2)
            if new_stop > pos['stop_price']:
                pos['stop_price'] = new_stop
            
            # 매도 조건: 저가가 스탑 가격 건드리면 청산
            if low_price <= pos['stop_price']:
                sell_price = pos['stop_price'] # 실전 슬리피지 고려 안함 (보수적)
                pnl = (sell_price - pos['entry_price']) * pos['shares']
                balance += (sell_price * pos['shares']) * 0.9975 # 수수료
                
                if pnl > 0: wins += 1
                trade_count += 1
                del positions[code]
                continue
                
            # 시장 필터가 꺼지면 강제 청산 (현금화)
            if not is_bull_market:
                balance += (row['Open'] * pos['shares']) * 0.9975
                trade_count += 1
                del positions[code]
                continue

        # 3. 신규 진입 (현금 있을 때만)
        # 시장이 좋을 때 + RS 상위 종목 스캔
        if is_bull_market and len(positions) == 0: # 1종목 집중 투자 (예시)
            candidates = []
            for code, df in stock_db.items():
                if today not in df.index: continue
                row = df.loc[today]
                
                # 진입 조건: 20EMA 위 + 구조적 눌림(SwingLow 지지) + RS 양수
                if (row['Close'] > row['EMA20']) and \
                   (row['RS_Score'] > 0) and \
                   (row['Low'] > row['SwingLow']): # Higher Low
                    candidates.append((code, row['RS_Score']))
            
            # RS 점수 높은 순 정렬
            candidates.sort(key=lambda x: x[1], reverse=True)
            
            if candidates:
                target_code = candidates[0][0] # 1등주 선택
                df = stock_db[target_code]
                row = df.loc[today]
                
                # 포지션 사이징 (1% 룰)
                # 리스크 = 진입가 - (SwingLow - 0.5ATR)
                risk_per_share = row['Close'] - (row['SwingLow'] - (row['ATR'] * 0.5))
                if risk_per_share > 0:
                    risk_amount = balance * 0.01 # 계좌의 1%만 리스크 허용
                    shares_to_buy = int(risk_amount / risk_per_share)
                    
                    cost = shares_to_buy * row['Close']
                    if cost < balance and shares_to_buy > 0:
                        balance -= cost * 1.00015
                        positions[target_code] = {
                            'shares': shares_to_buy,
                            'entry_price': row['Close'],
                            'stop_price': row['SwingLow'] - (row['ATR'] * 0.5)
                        }

        # 자산 가치 기록
        current_equity = balance
        for code, pos in positions.items():
            if today in stock_db[code].index:
                current_equity += pos['shares'] * stock_db[code].loc[today]['Close']
        
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(current_equity)})

    # 결과 정리
    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    
    # MDD 계산
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

def run_wallstreet_backtest():
    print("🎩 Wall Street Strategy Backtesting...")
    
    recent_start = datetime.now() - timedelta(days=365*3)
    recent_end = datetime.now()
    
    periods = {
        "ws_recent": (recent_start, recent_end),
        "ws_covid": ("2020-01-01", "2023-12-31"),
        "ws_box": ("2015-01-01", "2019-12-31")
    }
    
    results = {}
    for key, (start, end) in periods.items():
        print(f"   Running {key}...")
        res = simulate_wallstreet(start, end)
        if res: results[key] = res
        
    # 결과 저장 (별도 파일)
    output_path = os.path.join(DATA_DIR, 'backtest_wallstreet.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("✅ Wall Street Strategy Saved.")

if __name__ == "__main__":
    run_wallstreet_backtest()
