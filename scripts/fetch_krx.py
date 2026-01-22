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
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 백테스팅 엔진 (v7.0 Debug & Relaxed)
# ---------------------------------------------------------
def simulate_period(start_date, end_date, strategy_mode='standard'):
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '005380': '현대차', '005490': 'POSCO홀딩스', '035420': 'NAVER',
        '068270': '셀트리온', '042700': '한미반도체', '006400': '삼성SDI'
    }
    
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        if len(kospi) < 60: return None
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        
        # [Gate 1] MAIN: 정배열
        kospi['RISK_ON'] = (kospi['Close'] > kospi['MA20']) & (kospi['MA20'] > kospi['MA60'])
        # [Gate 2] EARLY: 20일선 위 (단기 반등장)
        kospi['EARLY_GATE'] = kospi['Close'] > kospi['MA20']
    except: return None

    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            # 공통 지표
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            df['StructTrigger'] = df['Close'] > df['High'].shift(1).rolling(3).max()
            df['NextOpen'] = df['Open'].shift(-1)
            
            # [MSI_EARLY 지표 계산]
            if strategy_mode == 'early':
                # 1. RS (NaN 처리 포함)
                kospi_matched = kospi['Close'].reindex(df.index).fillna(method='ffill')
                df['RS'] = df['Close'] / kospi_matched
                # MA20이 없으면 RS값 자체를 넣어서 비교 가능하게 처리
                df['RS_MA20'] = df['RS'].rolling(20).mean().fillna(df['RS'])
                
                # 2. MA20 기울기
                df['MA20_Slope'] = df['MA20'].diff(3)
                
                # 3. Higher Low (shift 적용하여 확정된 저점 비교)
                df['Low10'] = df['Low'].shift(1).rolling(10).min()
                df['Prev_Low10'] = df['Low10'].shift(10)
                
                # 4. Break10 (20일->10일로 완화: 초입 포착)
                df['Break10'] = df['Close'] > df['High'].shift(1).rolling(10).max()

            stock_db[code] = df
        except: pass

    # -----------------------------------------------------
    # 🔍 [DEBUG] SDI('006400') 진입 조건 카운트 출력
    # -----------------------------------------------------
    if strategy_mode == 'early':
        sdi_df = stock_db.get('006400')
        if sdi_df is not None:
            tmp = sdi_df.copy()
            # 조건 시뮬레이션
            tmp['is_uptrend_short'] = (tmp['Close'] > tmp['MA20']) & (tmp['MA20_Slope'] > 0)
            tmp['is_rs_good'] = (tmp['RS'] > tmp['RS_MA20'])
            tmp['is_structure_good'] = (tmp['Low10'] > tmp['Prev_Low10']) | (tmp['Break10'])
            
            # 카운트
            cnt_trend = tmp['is_uptrend_short'].sum()
            cnt_rs = tmp['is_rs_good'].sum()
            cnt_struct = tmp['is_structure_good'].sum()
            cnt_all = (tmp['is_uptrend_short'] & tmp['is_rs_good'] & tmp['is_structure_good']).sum()
            
            print(f"🔍 [DEBUG-SDI] Trend: {cnt_trend}, RS: {cnt_rs}, Struct: {cnt_struct} => ALL_MATCH: {cnt_all}")

    # -----------------------------------------------------
    # 시뮬레이션 시작
    # -----------------------------------------------------
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
        if today not in kospi.index: continue
        
        is_risk_on = kospi.loc[today]['RISK_ON']
        is_early_gate = kospi.loc[today]['EARLY_GATE']
        
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        equity_curve.append({"date": today.strftime("%Y-%m-%d"), "equity": int(curr_eq)})
        
        # 1. 매도
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            
            if row['Low'] <= stop_price: exit_type = 'STOP'; sell_price = stop_price
            elif row['High'] >= target_price: exit_type = 'TARGET'; sell_price = target_price
            
            # 시장 퇴출
            elif strategy_mode == 'standard' and not is_risk_on:
                exit_type = 'MKT_OUT'; sell_price = row['NextOpen']
            elif strategy_mode == 'early':
                # 20일선 이탈 시 탈출
                if (not is_early_gate) or (row['Close'] < row['MA20']):
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

        # 2. 매수
        if holding_code is None:
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # [A] MSI_MAIN
                if strategy_mode == 'standard':
                    if is_risk_on and (curr['MA20'] > curr['MA60']) and curr['StructTrigger']:
                        if pd.isna(curr['SwingLow']): continue
                        stop = curr['SwingLow'] * 0.99
                        risk = curr['Close'] - stop
                        if risk <= 0: continue
                        
                        shares = int(balance / curr['Close'])
                        if shares > 0:
                            balance -= shares * curr['Close'] * 1.00015
                            holding_code = code
                            entry_price = curr['Close']
                            stop_price = stop
                            target_price = curr['Close'] + (risk * 3)
                            break 

                # [B] MSI_EARLY (Fix & Relaxed)
                elif strategy_mode == 'early' and is_early_gate:
                    # 1. 단기 상승 (20일선 위 + 기울기 상승)
                    is_uptrend_short = (curr['Close'] > curr['MA20']) and (curr['MA20_Slope'] > 0)
                    
                    # 2. RS 강도 (RS > RS_MA20)
                    is_rs_good = curr['RS'] > curr['RS_MA20']
                    
                    # 3. 구조 (쌍바닥 OR 10일 신고가) -> Break10으로 완화
                    is_structure_good = (curr['Low10'] > curr['Prev_Low10']) or curr['Break10']
                    
                    if is_uptrend_short and is_rs_good and is_structure_good:
                        stop_lvl = curr['SwingLow']
                        if pd.isna(stop_lvl) or stop_lvl > curr['Close']:
                            stop_lvl = curr['MA20'] * 0.98

                        stop = stop_lvl * 0.98
                        risk = curr['Close'] - stop
                        if risk <= 0: continue

                        # 비중 100%
                        invest_amt = balance * 1.0 
                        shares = int(invest_amt / curr['Close'])
                        if shares > 0:
                            balance -= shares * curr['Close'] * 1.00015
                            holding_code = code
                            entry_price = curr['Close']
                            stop_price = stop
                            target_price = curr['Close'] + (risk * 3)
                            break

    final_eq = equity_curve[-1]['equity']
    total_return = ((final_eq / initial_balance) - 1) * 100
    win_rate = (wins / trade_count * 100) if trade_count > 0 else 0
    eq_series = pd.Series([e['equity'] for e in equity_curve])
    peak = eq_series.cummax()
    mdd = ((eq_series - peak) / peak).min() * 100

    return {
        "summary": { "total_return": round(total_return, 2), "final_balance": int(final_eq), "trade_count": trade_count, "win_rate": round(win_rate, 1), "mdd": round(mdd, 2) },
        "equity_curve": equity_curve
    }

def run_multi_backtest():
    print("🧪 Running Multi-Period Backtest (6 Scenarios)...")
    
    recent_start = datetime.now() - timedelta(days=365*3)
    recent_end = datetime.now()
    
    periods = {
        "recent": (recent_start, recent_end, 'standard'),
        "covid": ("2020-01-01", "2023-12-31", 'standard'),
        "box": ("2015-01-01", "2019-12-31", 'standard'),
        "early": (recent_start, recent_end, 'early'),
        "early_covid": ("2020-01-01", "2023-12-31", 'early'),
        "early_box": ("2015-01-01", "2019-12-31", 'early')
    }
    
    results = {}
    for key, (start, end, mode) in periods.items():
        print(f"   Running {key} ({mode})...")
        res = simulate_period(start, end, strategy_mode=mode)
        if res: results[key] = res
        
    return results

# ... (이하 기존 코드 그대로 유지 - calc_williams_r 등) ...
# 반드시 아래 기존 함수들도 포함해서 저장해야 합니다!
def calc_williams_r(df, period=14):
    hh = df['High'].rolling(period).max()
    ll = df['Low'].rolling(period).min()
    wr = -100 * (hh - df['Close']) / (hh - ll)
    return wr.fillna(-50)

def get_detailed_strategy(ticker, market_type):
    try:
        suffix = ".KS" if market_type == 'KOSPI' else ".KQ"
        df_1h = yf.download(f"{ticker}{suffix}", period="5d", interval="1h", progress=False)
        if df_1h.empty: return None
        if isinstance(df_1h.columns, pd.MultiIndex): df_1h.columns = df_1h.columns.get_level_values(0)
        
        df_1h['WR'] = calc_williams_r(df_1h)
        swing_low = df_1h['Low'].shift(1).rolling(10).min().iloc[-1]
        is_tc = df_1h['Close'].iloc[-1] > df_1h['High'].iloc[-5:].max()
        
        return {
            "swing_low": int(swing_low) if not np.isnan(swing_low) else int(df_1h['Close'].iloc[-1]*0.95), 
            "wr": round(df_1h['WR'].iloc[-1], 1), 
            "is_tc": is_tc, 
            "is_oversold": df_1h['WR'].iloc[-1] < -80
        }
    except: return None

def process_data():
    try:
        kospi = fdr.DataReader('KS11', '2024-01-01')
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        ma60 = kospi['Close'].rolling(60).mean().iloc[-1]
        state = "RISK_ON" if (curr['Close'] > ma20) and (ma20 > ma60) else "RISK_OFF"
        market = {"state": state, "reason": "정배열" if state=="RISK_ON" else "역배열"}
    except: market = {"state": "RISK_OFF", "reason": "Data Error"}

    print("📡 Fetching KRX...")
    try: df = fdr.StockListing('KRX')
    except: return market, [], []

    rename_map = {'Code':'Code', 'Name':'Name', 'Close':'종가', 'Amount':'거래대금', 'Marcap':'시가총액', 'MarketCap':'시가총액', 'Market': 'Market', 'Sector': 'KRX_Sector'}
    if 'ChagesRatio' in df.columns: rename_map['ChagesRatio'] = '등락률'
    elif 'Change' in df.columns: rename_map['Change'] = '등락률'
    elif 'ChangesRatio' in df.columns: rename_map['ChangesRatio'] = '등락률'
    df.rename(columns=rename_map, inplace=True)
    df.set_index('Code', inplace=True)
    
    for c in ['종가','거래대금','등락률','시가총액']:
        if c not in df.columns: df[c] = 0
        else: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    theme_map = load_theme_map()
    if 'KRX_Sector' in df.columns: df['CustomSector'] = df['KRX_Sector'].fillna('기타')
    else: df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector

    df = df[(df['종가'] > 1000) & (df['거래대금'] > 1000000000)].copy()

    sectors = []
    for sector, group in df.groupby('CustomSector'):
        if len(group) < 3: continue
        if sector in ['Unclassified', '기타', 'KOSPI', 'KOSDAQ']: continue 
        vol = group['거래대금'].sum()
        score = int(vol / 100000000)
        top = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sectors.append({"sector": sector, "score": score, "turnover": int(vol), "topTickers": top})
    
    if sectors:
        max_score = max(s['score'] for s in sectors)
        for s in sectors: s['score'] = int(s['score'] / max_score * 100)
    sectors.sort(key=lambda x: x['score'], reverse=True)

    watchlist = []
    top_vol = df.sort_values(by='거래대금', ascending=False).head(20)
    
    print("🔬 Deep Dive...")
    for code, row in top_vol.iterrows():
        price = int(row['종가'])
        vol = int(row['거래대금'])
        marcap = int(row['시가총액'])
        turnover_rate = (vol / marcap * 100) if marcap > 0 else 0
        grade = "C"
        if vol >= 2000e8: grade = "S"
        elif vol >= 500e8 and turnover_rate >= 10: grade = "S"
        elif vol >= 500e8: grade = "A"
        elif vol >= 300e8 and turnover_rate >= 7: grade = "A"
        elif vol >= 100e8: grade = "B"

        item = {
            "ticker": code, "name": row['Name'], "sector": row['CustomSector'],
            "grade": grade, "action": "WAIT", "close": price, "change": row['등락률'],
            "entry": {"price": 0}, "stop": {"price": 0}, "target": {"price": 0, "rr": 0},
            "why": []
        }
        if market['state'] == 'RISK_OFF':
            item['action'] = "NO_TRADE"
            item['why'].append("Market Risk Off")
            watchlist.append(item)
            continue

        strat = get_detailed_strategy(code, row.get('Market', 'KOSPI'))
        time.sleep(1) 
        if strat:
            item['stop']['price'] = strat['swing_low']
            risk = price - strat['swing_low']
            if risk > 0 and (risk / price) <= 0.1:
                item['entry']['price'] = price
                item['target']['price'] = int(price + (risk * 3))
                item['target']['rr'] = 3.0
                if strat['is_tc']: item['action'] = "READY"; item['why'].append("Structure Break")
                elif strat['is_oversold']: item['why'].append("Oversold")
            else: item['why'].append("Risk > 10% (Skip)")
        watchlist.append(item)

    return market, sectors, watchlist

def save_results():
    try:
        market, sectors, watchlist = process_data()
        backtest = run_multi_backtest() # 멀티 백테스트
        
        now = datetime.utcnow() + timedelta(hours=9)
        meta = {"asOf": now.strftime("%Y-%m-%d %H:%M:%S"), "market": market}
        
        with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f)
        with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f: json.dump({"items": sectors}, f)
        with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f: json.dump({"items": watchlist}, f)
        if backtest:
            with open(os.path.join(DATA_DIR, 'backtest.json'), 'w', encoding='utf-8') as f: json.dump(backtest, f)
            
        print("✅ Done.")
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    save_results()
