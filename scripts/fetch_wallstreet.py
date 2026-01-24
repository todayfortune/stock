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

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

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
    except Exception:
        return None

    # [2] 종목 데이터 준비
    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            if len(df) < 200:
                continue
            df = calculate_indicators(df, kospi)
            stock_db[code] = df
        except Exception:
            pass

    # [3] 시뮬레이션 루프
    balance = 10000000
    initial_balance = balance
    equity_curve = []

    # 포지션 관리 변수
    positions = {}  # { 'code': { ... } }

    # ------------------------------
    # [추가] Re-rating 스코어 계산 (대화 기반)
    # ------------------------------
    def _safe(v, default=0.0):
        try:
            if np.isnan(v):
                return default
            return float(v)
        except Exception:
            return default

    def compute_score(code, today):
        """Re-rating Score (proxy 버전)
        - ΔE_proxy: 단기모멘텀 + 거래대금 모멘텀 + 고점근접
        - S: RS_Score (60일 상대강도)
        - V_gap 대용: 고점근접 + 과열(변동성) 패널티
        - T: 추세초입(120MA 상향돌파 초기)
        반환 None이면 진입/유지 대상 제외.
        """
        df = stock_db.get(code)
        if df is None or today not in df.index:
            return None

        row = df.loc[today]
        close = _safe(row.get('Close', np.nan))
        atr = _safe(row.get('ATR', np.nan))
        ema20 = _safe(row.get('EMA20', np.nan))
        rs = _safe(row.get('RS_Score', 0.0))

        # 기본 필터: EMA20 위 + ATR 유효
        if close <= 0 or atr <= 0 or close <= ema20:
            return None

        # 거래대금 모멘텀 (Close*Volume proxy): 20일 / 60일
        if 'Volume' in df.columns:
            dv = (df['Close'] * df['Volume']).replace([np.inf, -np.inf], np.nan)
            v20 = _safe(dv.rolling(20).mean().loc[today], 0.0)
            v60 = _safe(dv.rolling(60).mean().loc[today], 0.0)
            vol_mom = (v20 / v60 - 1.0) if v60 > 0 else 0.0
        else:
            vol_mom = 0.0

        # 단기 모멘텀: 20일 수익률
        mom20 = _safe(df['Close'].pct_change(20).loc[today], 0.0)

        # 고점 근접: 60일 고점 대비 (0에 가까울수록 좋음)
        hi60 = _safe(df['High'].rolling(60).max().loc[today], 0.0)
        near_high = (close / hi60 - 1.0) if hi60 > 0 else 0.0
        near_high_score = -abs(near_high)

        # 과열 패널티: ATR/Close 과도 시 감점
        vol_ratio = atr / close
        vol_penalty = -max(0.0, vol_ratio - 0.035)

        # 추세초입 T: 120MA 상향 + 최근 35일 내 상향돌파 존재(근사)
        ma120 = _safe(df['Close'].rolling(120).mean().loc[today], 0.0)
        ma120_prev = _safe(df['Close'].rolling(120).mean().shift(5).loc[today], ma120)
        ma120_up = 1.0 if ma120 > ma120_prev else 0.0
        above120 = 1.0 if close > ma120 and ma120 > 0 else 0.0

        t_window = df.loc[:today].tail(35)
        if len(t_window) >= 10:
            ma120_w = t_window['Close'].rolling(120).mean()
            cross_up = ((t_window['Close'] > ma120_w) & (t_window['Close'].shift(1) <= ma120_w.shift(1))).any()
        else:
            cross_up = False
        t_score = 1.0 if (above120 and ma120_up and cross_up) else 0.0

        # ΔE_proxy (리비전/수주 기대 선반영): 가격 + 거래대금 + 고점근접
        deltaE_proxy = (0.45 * mom20) + (0.35 * vol_mom) + (0.20 * near_high_score)

        # 최종 스코어(대화 가중치 근사)
        score = (0.35 * deltaE_proxy) + (0.25 * rs) + (0.20 * (near_high_score + vol_penalty)) + (0.20 * t_score)
        return float(score)

    dates = kospi.index
    trade_count = 0
    wins = 0

    # 200일 워밍업 이후부터
    for i in range(200, len(dates)):
        today = dates[i]
        if today not in kospi.index:
            continue

        # 1) 시장 필터 확인
        is_bull_market = bool(kospi.loc[today]['Bull_Market'])

        # 2) 보유 종목 관리 (A/B/C 매도 엔진)
        active_codes = list(positions.keys())
        for code in active_codes:
            pos = positions[code]
            df = stock_db[code]
            if today not in df.index:
                continue

            row = df.loc[today]
            current_price = float(row['Close'])
            high_price = float(row['High'])
            low_price = float(row['Low'])
            atr = float(row['ATR'])

            swing_low = None
            if 'SwingLow' in row and not np.isnan(row['SwingLow']):
                swing_low = float(row['SwingLow'])

            # (A) 구조 붕괴 스탑: SwingLow - 0.5*ATR (상향만 허용)
            if swing_low is not None and atr > 0:
                hard_stop = swing_low - (0.5 * atr)
                pos['hard_stop'] = max(pos.get('hard_stop', hard_stop), hard_stop)

            # (B) ATR 트레일링: 변동성에 따라 k 자동 조정 (2.2~3.6)
            if atr > 0 and current_price > 0:
                vol_ratio = atr / current_price
                k = 2.2 + min(1.4, max(0.0, (vol_ratio - 0.02) * 50.0))
            else:
                k = 2.8

            pos['peak_price'] = max(pos.get('peak_price', pos['entry_price']), high_price)
            trail_stop = pos['peak_price'] - (k * atr) if atr > 0 else pos.get('stop_price', 0.0)
            pos['trail_stop'] = max(pos.get('trail_stop', trail_stop), trail_stop)

            # (C) 리레이팅 종료: 스코어가 장기간 악화(3주 근사: 15거래일)하면 청산
            today_score = compute_score(code, today)
            if today_score is not None:
                prev_score = pos.get('prev_score', today_score)
                if today_score < prev_score:
                    pos['score_down_streak'] = pos.get('score_down_streak', 0) + 1
                else:
                    pos['score_down_streak'] = 0
                pos['prev_score'] = today_score

            rerating_exit = (pos.get('score_down_streak', 0) >= 15)

            exit_reason = None
            exit_price = None

            # A 우선
            if low_price <= pos.get('hard_stop', -1e18):
                exit_reason = "A_hard_stop"
                exit_price = float(pos.get('hard_stop', low_price))
            # B
            elif low_price <= pos.get('trail_stop', -1e18):
                exit_reason = "B_trail_stop"
                exit_price = float(pos.get('trail_stop', low_price))
            # C
            elif rerating_exit:
                exit_reason = "C_rerating_end"
                exit_price = float(row['Open'])
            # 시장 OFF
            elif not is_bull_market:
                exit_reason = "M_market_off"
                exit_price = float(row['Open'])

            if exit_reason is not None and exit_price is not None:
                pnl = (exit_price - pos['entry_price']) * pos['shares']
                balance += (exit_price * pos['shares']) * 0.9975  # 수수료 반영(매도)

                if pnl > 0:
                    wins += 1
                trade_count += 1
                del positions[code]
                continue

        # 3) 신규 진입 (시장 ON + 포지션 비어있을 때)
        #    Re-rating Score 상위 종목 스캔 (1종목 집중 투자 예시)
        if is_bull_market and len(positions) == 0:
            candidates = []
            for code, df in stock_db.items():
                if today not in df.index:
                    continue

                sc = compute_score(code, today)
                if sc is None:
                    continue

                row = df.loc[today]

                # 과열 방지(뉴스갭/장대양봉): (Close-Open)/ATR > 2.5 제외
                if row['ATR'] > 0 and ((row['Close'] - row['Open']) / row['ATR']) > 2.5:
                    continue

                # 눌림/지지 성격: Low > SwingLow 유지
                if not (row['Low'] > row['SwingLow']):
                    continue

                candidates.append((code, sc))

            candidates.sort(key=lambda x: x[1], reverse=True)

            if candidates:
                target_code, target_score = candidates[0]
                df = stock_db[target_code]
                row = df.loc[today]

                entry = float(row['Close'])
                atr = float(row['ATR'])
                swing_low = float(row['SwingLow']) if not np.isnan(row['SwingLow']) else entry
                hard_stop = swing_low - (atr * 0.5)

                # 포지션 사이징 (1% 룰)
                risk_per_share = entry - hard_stop
                if risk_per_share > 0:
                    risk_amount = balance * 0.01
                    shares_to_buy = int(risk_amount / risk_per_share)

                    cost = shares_to_buy * entry
                    if cost < balance and shares_to_buy > 0:
                        balance -= cost * 1.00015  # 수수료 반영(매수)
                        positions[target_code] = {
                            'shares': shares_to_buy,
                            'entry_price': entry,
                            'stop_price': hard_stop,   # 기존 호환
                            'hard_stop': hard_stop,
                            'peak_price': entry,
                            'trail_stop': hard_stop,
                            'prev_score': target_score,
                            'score_down_streak': 0
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
        if res:
            results[key] = res

    # 결과 저장 (별도 파일)
    output_path = os.path.join(DATA_DIR, 'backtest_wallstreet.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("✅ Wall Street Strategy Saved.")

if __name__ == "__main__":
    run_wallstreet_backtest()
