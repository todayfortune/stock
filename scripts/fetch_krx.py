import os
import json
import time
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from pykrx import stock
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
# 2. 데이터 수집 (pykrx 엔진 도입)
# ---------------------------------------------------------
def get_latest_market_data():
    """오늘(장중) 또는 가장 최근 영업일의 시세 데이터를 가져옴"""
    now = datetime.now()
    
    # 최근 5일 중 데이터가 있는 날짜 찾기 (휴일/주말 패스)
    for i in range(5):
        target_date = now - timedelta(days=i)
        date_str = target_date.strftime("%Y%m%d")
        try:
            print(f"   Trying to fetch market data for {date_str}...")
            # pykrx로 전종목 시세 로드
            df = stock.get_market_ohlcv_by_ticker(date_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Data found for {date_str}")
                return df
        except:
            continue
    return pd.DataFrame()

def process_data():
    print("📡 Fetching Real-time Price (pykrx)...")
    
    # 1. 최신 시세 데이터 가져오기
    df_price = get_latest_market_data()
    if df_price.empty:
        print("❌ Failed to fetch market data.")
        return {"state": "ERROR"}, [], []

    # pykrx는 티커가 인덱스임. 컬럼 정리
    df_price = df_price.reset_index().rename(columns={'티커': 'Code', '종가': 'Close', '등락률': 'ChagesRatio', '거래대금': 'Amount', '시가총액': 'Marcap'})
    
    # 2. 종목명 및 섹터 정보 가져오기 (FDR 보조)
    try:
        # KOSPI/KOSDAQ 목록 합치기
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        df_info = pd.concat([k, q])
        
        # 컬럼 표준화
        col_map = {'Symbol': 'Code', 'Name': 'Name', 'Sector': 'Sector', 'Industry': 'Sector', '업종명': 'Sector'}
        df_info = df_info.rename(columns=col_map)
        
        # 필요한 컬럼만
        if 'Sector' not in df_info.columns: df_info['Sector'] = 'Unclassified'
        df_info = df_info[['Code', 'Name', 'Sector']]
        
    except:
        # FDR 실패 시 pykrx로 이름만이라도 가져옴
        df_info = pd.DataFrame({'Code': df_price['Code'], 'Name': [stock.get_market_ticker_name(c) for c in df_price['Code']], 'Sector': 'Unclassified'})

    # 3. 데이터 병합
    df = pd.merge(df_price, df_info, on='Code', how='left')
    df['Name'] = df['Name'].fillna(df['Code'])
    df['Sector'] = df['Sector'].fillna('기타')
    
    # 인덱스 설정
    df.set_index('Code', inplace=True)

    # 4. 테마 맵핑 및 필터링
    theme_map = load_theme_map()
    df['CustomSector'] = df['Sector']
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'CustomSector'] = sector

    # 유효 종목 필터 (동전주 제외)
    df = df[(df['Close'] > 500) & (df['Amount'] > 0)].copy()

    # ---------------------------------------------------------
    # 5. 시장 상태 판단 (KOSPI)
    # ---------------------------------------------------------
    try:
        kospi = fdr.DataReader('KS11', (datetime.now()-timedelta(days=100)).strftime("%Y-%m-%d"))
        curr = kospi.iloc[-1]['Close']
        ma20 = kospi['Close'].rolling(20).mean().iloc[-1]
        state = "RISK_ON" if curr > ma20 else "RISK_OFF"
        market = {"state": state, "reason": "20일선 위" if state=="RISK_ON" else "20일선 아래"}
    except:
        market = {"state": "RISK_ON", "reason": "Market Check Skip"}

    # ---------------------------------------------------------
    # 6. 섹터 리더 추출
    # ---------------------------------------------------------
    sectors = []
    for sector, group in df.groupby('CustomSector'):
        if len(group) < 3 or sector in ['기타', 'Unclassified']: continue
        vol = group['Amount'].sum()
        score = int(vol / 100000000) # 억 단위
        top = group.sort_values(by='Amount', ascending=False).head(3)['Name'].tolist()
        sectors.append({"sector": sector, "score": score, "turnover": int(vol), "topTickers": top})
    
    sectors.sort(key=lambda x: x['score'], reverse=True)
    if sectors:
        max_score = sectors[0]['score']
        for s in sectors: s['score'] = int(s['score'] / max_score * 100) if max_score > 0 else 0

    # ---------------------------------------------------------
    # 7. 관심종목 선정 (거래대금 상위)
    # ---------------------------------------------------------
    watchlist = []
    top_vol = df.sort_values(by='Amount', ascending=False).head(20)
    
    print("🔬 Analyzing Top 20 Stocks...")
    for code, row in top_vol.iterrows():
        grade = "C"
        vol = row['Amount']
        marcap = row['Marcap']
        
        # 등급 산정
        if vol >= 200000000000: grade = "S" # 2000억
        elif vol >= 50000000000: grade = "A" # 500억
        elif vol >= 20000000000: grade = "B" # 200억

        item = {
            "ticker": code, "name": row['Name'], "sector": row['CustomSector'],
            "grade": grade, "action": "WAIT", 
            "close": int(row['Close']), 
            "change": round(row['ChagesRatio'], 2),
            "entry": {"price": 0}, "stop": {"price": 0}, "target": {"price": 0},
            "why": []
        }
        
        # 전략적 판단 (윌리엄스R 등)
        try:
            strat = get_detailed_strategy(code, 'KOSPI') # 마켓 구분 생략
            if strat:
                item['stop']['price'] = strat['swing_low']
                risk = item['close'] - strat['swing_low']
                if risk > 0 and (risk / item['close']) <= 0.15:
                    item['entry']['price'] = item['close']
                    item['target']['price'] = int(item['close'] + (risk * 3))
                    if strat['is_tc']: item['action'] = "READY"; item['why'].append("Structure Break")
        except: pass
        
        if market['state'] == 'RISK_OFF': 
            item['action'] = 'WAIT'
            item['why'].append("Market Risk Off")

        watchlist.append(item)

    return market, sectors, watchlist

# ... (기존 calc_williams_r, get_detailed_strategy, 백테스팅 관련 함수들 유지) ...
# 아래는 기존 파일의 함수들을 그대로 붙여넣어야 합니다. (너무 길어서 핵심만 수정함)
# 하지만 전체 덮어쓰기를 위해 최소한의 필요한 함수는 포함합니다.

def calc_williams_r(df, period=14):
    hh = df['High'].rolling(period).max()
    ll = df['Low'].rolling(period).min()
    wr = -100 * (hh - df['Close']) / (hh - ll)
    return wr.fillna(-50)

def get_detailed_strategy(ticker, market_type):
    try:
        # yfinance로 최근 데이터 조회
        ticker_yf = f"{ticker}.KS"
        df = yf.download(ticker_yf, period="5d", interval="1h", progress=False)
        if df.empty: 
            df = yf.download(f"{ticker}.KQ", period="5d", interval="1h", progress=False)
            
        if df.empty: return None
        if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        
        df['WR'] = calc_williams_r(df)
        swing_low = df['Low'].shift(1).rolling(10).min().iloc[-1]
        is_tc = df['Close'].iloc[-1] > df['High'].iloc[-5:].max()
        
        return {"swing_low": int(swing_low) if not np.isnan(swing_low) else int(df['Close'].iloc[-1]*0.95), "is_tc": is_tc}
    except: return None

# simulate_period, run_multi_backtest 함수는 기존 파일에 있던 것 그대로 사용하시면 됩니다.
# 여기서는 파일 구조상 포함하지 않았으나, 기존 코드를 유지해주세요.
# 만약 백테스트도 같이 돌리려면 기존 fetch_krx.py의 백테스트 부분을 여기 아래에 붙여넣어주세요.
# 일단 대시보드 갱신이 급하므로 process_data 위주로 작성했습니다.

def simulate_period(start_date, end_date):
    # (백테스팅 코드는 기존 유지 - 생략 가능하거나 기존 코드 복붙)
    return None 
def run_multi_backtest(): return {}

def save_results():
    try:
        market, sectors, watchlist = process_data()
        
        now = datetime.utcnow() + timedelta(hours=9)
        meta = {"asOf": now.strftime("%Y-%m-%d %H:%M:%S"), "market": market}
        
        with open(os.path.join(DATA_DIR, 'meta.json'), 'w', encoding='utf-8') as f: json.dump(meta, f)
        with open(os.path.join(DATA_DIR, 'sector_leaders.json'), 'w', encoding='utf-8') as f: json.dump({"items": sectors}, f)
        with open(os.path.join(DATA_DIR, 'watchlist.json'), 'w', encoding='utf-8') as f: json.dump({"items": watchlist}, f)
        
        # 백테스트는 별도 파일(backtest_standard.json)로 분리하거나 기존 로직 유지
        # 여기서는 생략
            
        print("✅ KRX Update Done.")
    except Exception as e:
        print(f"❌ Fatal Error: {e}")

if __name__ == "__main__":
    save_results()
