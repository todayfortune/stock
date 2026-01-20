# scripts/fetch_krx.py (v4.1 Final Polish)
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

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def load_theme_map():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. [FIX] 백테스팅 엔진 (MSI v1 일봉 근사 모델 - 청산 로직 강화)
# ---------------------------------------------------------
def run_msi_backtest():
    print("🧪 MSI Blueprint 백테스팅 (v1 일봉 근사) 가동...")
    
    # 유니버스 (대표 주도주 10개)
    UNIVERSE = {
        '005930': '삼성전자', '000660': 'SK하이닉스', '086520': '에코프로',
        '006400': '삼성SDI', '005380': '현대차', '005490': 'POSCO홀딩스',
        '035420': 'NAVER', '068270': '셀트리온', '010120': 'LS ELECTRIC',
        '042700': '한미반도체'
    }
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*3) # 최근 3년
    
    # 1. 데이터 수집
    try:
        kospi = fdr.DataReader('KS11', start_date, end_date)
        kospi['MA20'] = kospi['Close'].rolling(20).mean()
        kospi['MA60'] = kospi['Close'].rolling(60).mean()
        # [Rule] Market Gate: 정배열(20>60) & 가격>20일선
        kospi['RISK_ON'] = (kospi['Close'] > kospi['MA20']) & (kospi['MA20'] > kospi['MA60'])
    except: return None

    stock_db = {}
    for code in UNIVERSE.keys():
        try:
            df = fdr.DataReader(code, start_date, end_date)
            df['MA20'] = df['Close'].rolling(20).mean()
            df['MA60'] = df['Close'].rolling(60).mean()
            
            # Williams %R
            hh = df['High'].rolling(14).max()
            ll = df['Low'].rolling(14).min()
            df['WR'] = -100 * (hh - df['Close']) / (hh - ll)
            
            # Swing Low (Stop Loss 기준, 전일 제외)
            df['SwingLow'] = df['Low'].shift(1).rolling(10).min()
            
            # 구조 트리거 (전일 포함 최근 3일 고가 돌파 여부)
            prev_high = df['High'].shift(1).rolling(3).max()
            df['StructTrigger'] = df['Close'] > prev_high
            
            # Next Open (다음날 시가 - 청산용)
            df['NextOpen'] = df['Open'].shift(-1)
            
            stock_db[code] = df
        except: pass

    # 2. 시뮬레이션
    balance = 10_000_000
    initial_balance = balance
    holding_code = None
    shares = 0
    equity_curve = []
    trade_count = 0
    wins = 0
    
    # 진입/청산 변수
    entry_price = 0
    stop_price = 0
    target_price = 0
    
    dates = kospi.index
    
    # 지표 계산 기간 고려하여 시작점 설정
    for i in range(60, len(dates)-1): 
        today = dates[i]
        if today not in kospi.index: continue
        
        is_risk_on = kospi.loc[today]['RISK_ON']
        
        # A. 자산 평가 (Mark to Market)
        curr_eq = balance
        if holding_code and today in stock_db[holding_code].index:
            curr_eq = balance + (shares * stock_db[holding_code].loc[today]['Close'])
        
        equity_curve.append({
            "date": today.strftime("%Y-%m-%d"),
            "equity": int(curr_eq)
        })
        
        # B. 매도 로직 (보유 시)
        if holding_code:
            df = stock_db[holding_code]
            if today not in df.index: continue
            row = df.loc[today]
            
            exit_type = None
            sell_price = 0
            
            # 1. Stop Loss
            if row['Low'] <= stop_price:
                exit_type = 'STOP'
                sell_price = stop_price
            # 2. Target Hit
            elif row['High'] >= target_price:
                exit_type = 'TARGET'
                sell_price = target_price
            # 3. Market Risk Off ([P0-4] 다음날 시가 청산)
            elif not is_risk_on:
                exit_type = 'MKT_OUT'
                # 다음날 데이터가 있으면 시가 청산, 없으면 당일 종가
                sell_price = row['NextOpen'] if not pd.isna(row['NextOpen']) else row['Close']
            
            if exit_type:
                # 슬리피지/수수료 반영 (0.25%)
                sell_amt = shares * sell_price * 0.9975
                balance += sell_amt
                
                is_win = sell_amt > (shares * entry_price)
                if is_win: wins += 1
                trade_count += 1
                
                holding_code = None
                shares = 0
                continue

        # C. 매수 로직 (미보유 & Risk On)
        if holding_code is None and is_risk_on:
            candidates = []
            for code, df in stock_db.items():
                if today not in df.index: continue
                curr = df.loc[today]
                
                # [MSI 필터]
                if not (curr['MA20'] > curr['MA60']): continue # 정배열
                if not curr['StructTrigger']: continue # 구조 돌파
                
                # [Risk Setup]
                if pd.isna(curr['SwingLow']): continue
                stop_candidate = curr['SwingLow'] * 0.998
                
                risk = curr['Close'] - stop_candidate
                if risk <= 0: continue
                
                # 점수화 (거래량 우선)
                score = curr['Volume'] 
                candidates.append({
                    'code': code, 'price': curr['Close'], 
                    'stop': stop_candidate, 'score': score
                })
            
            if candidates:
                # 주도주 1개 선정
                best = sorted(candidates, key=lambda x: x['score'], reverse=True)[0]
                
                # RR 1:3 타겟 설정
                risk_per_share = best['price'] - best['stop']
                target_candidate = best['price'] + (risk_per_share * 3)
                
                # 매수 실행
                shares = int(balance / best['price'])
                if shares > 0:
                    balance -= shares * best['price'] * 1.00015
                    holding_code = best['code']
                    entry_price = best['price']
                    stop_price = best['stop']
                    target_price = target_candidate

    # 결과 요약
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

# ---------------------------------------------------------
# 3. 유틸리티 및 데이터 수집
# ---------------------------------------------------------
def calc_williams_r(df, period=14):
    hh = df['High'].rolling(period).max()
    ll = df['Low'].rolling(period).min()
    wr = -100 * (hh - df['Close']) / (hh - ll)
    return wr.fillna(-50)

def find_swing_low(df, window=5):
    recent = df.iloc[-window:]
    return recent['Low'].min()

def detect_trend_change(df_15m):
    if len(df_15m) < 20: return False
    recent_highs = df_15m['High'].iloc[-15:-5].max()
    current_close = df_15m['Close'].iloc[-1]
    return current_close > recent_highs

def get_detailed_strategy(ticker, market_type):
    """ [P1-2] 심볼 최적화: 시장 타입(KS/KQ)을 받아 한 번에 호출 """
    try:
        suffix = ".KS" if market_type == 'KOSPI' else ".KQ"
        symbol = f"{ticker}{suffix}"
        
        df_1h = yf.download(symbol, period="5d", interval="1h", progress=False)
        if df_1h.empty: return None

        df_15m = yf.download(symbol, period="2d", interval="15m", progress=False)

        if isinstance(df_1h.columns, pd.MultiIndex): df_1h.columns = df_1h.columns.get_level_values(0)
        if isinstance(df_15m.columns, pd.MultiIndex): df_15m.columns = df_15m.columns.get_level_values(0)

        df_1h['WR'] = calc_williams_r(df_1h)
        current_wr = df_1h['WR'].iloc[-1]
        swing_low = find_swing_low(df_1h, window=10)
        
        is_tc = detect_trend_change(df_15m) if not df_15m.empty else False
        is_oversold = current_wr < -80
        
        return {"swing_low": int(swing_low), "wr": round(current_wr, 1), "is_tc": is_tc, "is_oversold": is_oversold}
    except: return None

def analyze_market_regime():
    """ Market Gate: MA20 > MA60 (정배열) & 가격 > MA20 """
    try:
        kospi = fdr.DataReader('KS11', '2023-01-01')
        curr = kospi.iloc[-1]
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        ma60 = kospi['Close'].rolling(60).mean().iloc[-1]
        
        state = "RISK_ON"
        reason = "KOSPI 정배열 (상승)"
        
        if (curr['Close'] < ma20) or (ma20 < ma60):
            state = "RISK_OFF"
            reason = "KOSPI 추세 이탈"
            
        return {"state": state, "reason": reason}
    except: return {"state": "RISK_ON", "reason": "Data Error"}

def process_data():
    market = analyze_market_regime()
    print(f"🚦 Market: {market['state']} ({market['reason']})")
    
    theme_map = load_theme_map()
    df = fdr.StockListing('KRX')
    
    rename_map = {
        'Code':'Code', 'Name':'Name', 'Close':'종가', 'Amount':'거래대금', 
        'Marcap':'시가총액', 'Market': 'Market' # [P1-2] 시장 구분 컬럼 확보
    }
    if 'ChagesRatio' in df.columns: rename_map['ChagesRatio'] = '등락률'
    elif 'Change' in df.columns: rename_map['Change'] = '등락률'
    elif 'ChangesRatio' in df.columns: rename_map['ChangesRatio'] = '등락률'
    
    df.rename(columns=rename_map, inplace=True)
    df.set_index('Code', inplace=True)
    
    cols = ['종가','거래대금','등락률']
    for c in cols: 
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    
    df['CustomSector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector
        
    valid_mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df = df[valid_mask].copy()
    
    # 섹터 리더
    sector_leaders = []
    for sector, group in df.groupby('CustomSector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        score = int((group['거래대금'].mean()/1e8) + (group['등락률'].mean()*10))
        top_names = group.sort_values(by='거래대금', ascending=False).head(3)['Name'].tolist()
        sector_leaders.append({"sector": sector, "score": score, "turnover": int(group['거래대금'].sum()), "topTickers": top_names})
    sector_leaders.sort(key=lambda x: x['score'], reverse=True)
    
    # Watchlist
    watchlist = []
    target_pool = df[df['CustomSector'] != 'Unclassified'].copy()
    top_vol = df.sort_values(by='거래대금', ascending=False).head(30)
    target_pool = pd.concat([target_pool, top_vol])
    target_pool = target_pool[~target_pool.index.duplicated()]
    
    print(f"🔬 Analyzing Top Candidates...")
    
    count = 0
    for code, row in target_pool.iterrows():
        # [P0-1] 카운트 로직: Deep Dive 성공 여부와 관계없이 시도 횟수로 제한하거나, 성공 횟수로 제한
        # 여기서는 '시도 횟수'를 제한하여 전체 실행 시간 보장 (최대 12개)
        if count >= 12: break
        
        price = int(row['종가'])
        vol = int(row['거래대금'])
        change = float(row['등락률'])
        market_type = row.get('Market', 'KOSPI') # KOSPI or KOSDAQ
        
        item = {
            "ticker": code, "name": row['Name'], "sector": row['CustomSector'],
            "state": "NO_TRADE", "grade": "C", "action": "WAIT",
            "close": price, "change": round(change, 2), "volume": vol,
            "entry": {"price": 0}, "stop": {"price": 0}, "target": {"price": 0, "rr": 0},
            "why": []
        }
        
        # [P0-3] Market Gate: RISK_OFF면 action 강제 조정
        if market['state'] == 'RISK_OFF':
            item['why'].append(f"⛔ {market['reason']}")
            item['action'] = "NO_TRADE" # [Fix] 정렬 꼬임 방지
            watchlist.append(item)
            continue 

        if vol >= 1000e8 or (vol >= 500e8 and change >= 15): item['grade'] = "S"
        elif vol >= 300e8: item['grade'] = "A"
        elif vol >= 100e8: item['grade'] = "B"
        else: item['grade'] = "C"

        if change < 0: continue

        # Deep Dive
        # [P0-1] 여기서 카운트 증가
        count += 1
        strat = get_detailed_strategy(code, market_type)
        time.sleep(1.5) 
        
        if strat:
            swing_low = strat['swing_low']
            if price > 0 and (price - swing_low)/price > 0.1: 
                item['stop']['price'] = int(price * 0.97)
                item['why'].append("Stop: 3% (Low 너무 멈)")
            else: 
                item['stop']['price'] = swing_low
                item['why'].append("Stop: 1H Swing Low")

            # Entry & Action
            if strat['is_tc']: 
                item['action'] = "READY"
                item['entry']['price'] = price
                item['why'].append("15M 구조전환(TC)")
            elif strat['is_oversold']: 
                item['action'] = "WAIT"
                item['why'].append("%R 과매도")
                # [P0-2] Oversold일 때도 Entry 가격 할당 (Target 계산용)
                item['entry']['price'] = price 
            else: 
                item['action'] = "WAIT"
                item['entry']['price'] = int(price * 0.98)
            
            # Target (RR 1:3)
            risk = item['entry']['price'] - item['stop']['price']
            if risk <= 0: risk = price * 0.03
            item['target']['price'] = int(item['entry']['price'] + (risk * 3))
            item['target']['rr'] = 3.0
            item['state'] = "WATCH"
        else:
            item['why'].append("상세 데이터 로드 실패")
            item['state'] = "NO_TRADE"

        watchlist.append(item)
    
    # 정렬: Action > Grade > Volume
    # [P0-3] NO_TRADE는 맨 아래로 가도록 점수 배정 (WAIT:1, NO_TRADE:0)
    gw = {'S':3, 'A':2, 'B':1, 'C':0}
    aw = {'READY':2, 'WAIT':1, 'NO_TRADE':0}
    watchlist.sort(key=lambda x: (aw.get(x['action'],0), gw.get(x['grade'],0), x['volume']), reverse=True)
    return market, sector_leaders, watchlist

def save_results():
    market, sectors, watchlist = process_data()
    backtest_data = run_msi_backtest()
    
    kst_now = datetime.utcnow() + timedelta(hours=9)
    now_str = kst_now.strftime("%Y-%m-%d %H:%M:%S (KST)")
    
    meta = {"asOf": now_str, "source": ["KRX", "FDR", "YFinance"], "version": "v4.1 (Final Polish)", "status": "ok", "market": market}
    
    with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f: json.dump({"asOf": now_str, "items": sectors}, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f: json.dump({"asOf": now_str, "items": watchlist}, f, ensure_ascii=False, indent=2)
    if backtest_data:
        with open(os.path.join(DATA_DIR, 'backtest.json'), 'w', encoding='utf-8') as f: json.dump(backtest_data, f, ensure_ascii=False, indent=2)

    print(f"✅ Pipeline v4.1 Completed. Watchlist: {len(watchlist)}")

if __name__ == "__main__":
    save_results()
