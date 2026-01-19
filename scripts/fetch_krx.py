import os
import json
import time
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
import yfinance as yf
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정
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
# 2. 1차 수집: Daily 전체 스캔 (FDR)
# ---------------------------------------------------------
def fetch_market_data():
    print("📡 1단계: KRX 전 종목 일봉 스캔 (Selection)...")
    df = fdr.StockListing('KRX')
    df.rename(columns={'Code':'Code','Name':'Name','Close':'종가','ChagesRatio':'등락률','Amount':'거래대금','Marcap':'시가총액','Sector':'KRX_Sector'}, inplace=True)
    df.set_index('Code', inplace=True)
    
    # 숫자형 변환
    cols = ['종가','거래대금','등락률']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
        
    return df

# ---------------------------------------------------------
# 3. 2차 수집: 분봉 정밀 분석 (YFinance) - Top N만 실행
# ---------------------------------------------------------
def check_1h_logic(code):
    """
    야후 파이낸스에서 60분봉을 가져와서
    1. 최근 Displacement(거래량 실린 장대양봉) 찾기 (1H Zone)
    2. 현재 위치 판별 (In Zone / Above / Below)
    """
    try:
        # 야후 파이낸스 코드는 뒤에 .KS(코스피) or .KQ(코스닥) 필요
        # FDR 정보로는 구분이 어려우니 둘 다 시도하거나, 에러나면 패스
        ticker = f"{code}.KS" 
        
        # 최근 5일치 60분봉 (1h)
        df_1h = yf.download(ticker, period="5d", interval="1h", progress=False)
        
        if df_1h.empty:
            ticker = f"{code}.KQ" # 코스닥 시도
            df_1h = yf.download(ticker, period="5d", interval="1h", progress=False)
            
        if df_1h.empty: return "No Data", "-"

        # 데이터 정리 (MultiIndex 컬럼 문제 해결)
        if isinstance(df_1h.columns, pd.MultiIndex):
            df_1h.columns = df_1h.columns.get_level_values(0)
            
        # --- 로직: 1H Displacement (간이 OB) 찾기 ---
        # 조건: 양봉이면서 + 몸통이 평균보다 크고 + 거래량이 평균의 2배 이상
        df_1h['Body'] = df_1h['Close'] - df_1h['Open']
        df_1h['Vol_MA'] = df_1h['Volume'].rolling(10).mean()
        
        # 최근 캔들부터 역순으로 탐색
        ob_low = 0
        ob_high = 0
        found = False
        
        for i in range(len(df_1h)-2, 0, -1): # 마지막 봉은 진행 중일 수 있으니 제외
            row = df_1h.iloc[i]
            if row['Body'] > 0 and row['Volume'] > (row['Vol_MA'] * 1.5): # 조건 완화 (1.5배)
                # 발견! 양봉의 시가~저가 부근을 Zone으로 설정 (Bullish OB 약식)
                ob_high = row['Open']
                ob_low = row['Low']
                found = True
                break
        
        if not found:
            return "No Zone", "-"
            
        # 현재가 위치 확인
        curr_price = df_1h.iloc[-1]['Close']
        
        if ob_low <= curr_price <= (ob_high * 1.02): # Zone 내부 (약간 위까지 허용)
            return "IN_ZONE (Buy)", f"{int(ob_low)}~{int(ob_high)}"
        elif curr_price < ob_low:
            return "Broken (Zone 이탈)", f"{int(ob_low)}"
        else:
            dist = round((curr_price - ob_high) / ob_high * 100, 1)
            return "Above Zone", f"+{dist}% 위"

    except Exception as e:
        return "Error", str(e)

# ---------------------------------------------------------
# 4. 메인 처리
# ---------------------------------------------------------
def process_and_save(df, theme_map):
    print("⚙️ 데이터 가공 및 1H 정밀 분석 중...")
    
    # 1. 섹터 매핑 및 필터링
    df['sector'] = 'Unclassified'
    for code, sector in theme_map.items():
        if code in df.index: df.loc[code, 'sector'] = sector
            
    # 필터: 동전주 제외, 거래대금 30억 이상 (조건 완화)
    mask = (df['종가'] > 1000) & (df['거래대금'] > 3_000_000_000)
    df_clean = df[mask].copy()
    
    # 2. 섹터 통계 (기존 로직)
    sector_stats = []
    for sector, group in df_clean.groupby('sector'):
        if sector == 'Unclassified' or len(group) < 2: continue
        
        score = (group['거래대금'].mean() / 100_000_000) + (group['등락률'].mean() * 10)
        leader = group.sort_values(by='거래대금', ascending=False).iloc[0]
        
        sector_stats.append({
            "name": sector,
            "msi_score": round(score, 1),
            "leader_name": leader['Name']
        })
    sector_stats.sort(key=lambda x: x['msi_score'], reverse=True)

    # 3. 후보군 선정 (Selection)
    # 조건: 커스텀 섹터이거나, 거래대금이 300억 이상인 종목
    candidates = []
    # 타겟: 커스텀 섹터 종목 + 전체 시장에서 거래대금 상위 10개
    target_pool = df_clean[df_clean['sector'] != 'Unclassified'].copy()
    top_volume = df_clean.sort_values(by='거래대금', ascending=False).head(10)
    target_pool = pd.concat([target_pool, top_volume])
    target_pool = target_pool[~target_pool.index.duplicated()] # 중복 제거
    
    # 4. [Deep Dive] Top 종목들에 대해 1H 분석 실행
    print(f"🔬 {len(target_pool)}개 종목 정밀 분석(Deep Dive) 시작...")
    
    analyzed_count = 0
    for code, row in target_pool.iterrows():
        # 너무 많이 하면 타임아웃 되므로 상위 15개만 분석
        if analyzed_count >= 15: break 
        if row['등락률'] < 0: continue # 하락 종목은 굳이 분석 안 함 (WATCH 대상 아님)

        # 1H 로직 체크
        zone_status, zone_price = check_1h_logic(code)
        
        # Action 결정
        action = "WATCH" # 기본
        if "IN_ZONE" in zone_status:
            action = "READY (Zone)" # 1H 존 도달!
        elif "No Zone" in zone_status:
            action = "Wait Setup"
        elif "Broken" in zone_status:
            action = "PASS"

        candidates.append({
            "code": code,
            "name": row['Name'],
            "sector": row['sector'],
            "close": int(row['종가']),
            "change_rate": round(row['등락률'], 2),
            "volume_money": int(row['거래대금']),
            "msi_action": action,
            "location": zone_status, # 1H 분석 결과
            "zone_price": zone_price
        })
        
        analyzed_count += 1
        time.sleep(0.5) # API 매너 호출

    # 결과 저장
    candidates.sort(key=lambda x: x['volume_money'], reverse=True)
    
    summary = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "top_sectors": [s['name'] for s in sector_stats[:3]]
    }
    
    with open(os.path.join(DATA_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'sectors.json'), 'w', encoding='utf-8') as f:
        json.dump(sector_stats, f, ensure_ascii=False, indent=2)
    with open(os.path.join(DATA_DIR, 'candidates.json'), 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 완료! 후보 {len(candidates)}개 저장됨.")

if __name__ == "__main__":
    theme_map = load_theme_map()
    df = fetch_market_data()
    process_and_save(df, theme_map)
