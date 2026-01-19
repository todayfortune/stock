import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pykrx import stock

# ---------------------------------------------------------
# 1. 설정 및 초기화
# ---------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')

# 데이터 저장 폴더 없으면 생성
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def load_theme_map():
    """커스텀 테마 맵 로드"""
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 데이터 수집 (KRX)
# ---------------------------------------------------------
def fetch_market_data():
    print("📡 KRX 데이터 수집 시작...")
    
    # 오늘 날짜 (장 마감 후 가정)
    today = datetime.now().strftime("%Y%m%d")
    # 주말이면 금요일로, 장중이면 어제로... (안전하게 가장 최근 영업일 조회)
    # pykrx는 미래 날짜를 넣으면 자동으로 가장 최근 영업일 데이터를 줍니다.
    
    # 1. 전 종목 시세 (코스피 + 코스닥)
    df_kospi = stock.get_market_ohlcv(today, market="KOSPI")
    df_kosdaq = stock.get_market_ohlcv(today, market="KOSDAQ")
    df = pd.concat([df_kospi, df_kosdaq])
    
    # 2. 펀더멘털 (시가총액 등) - 거래대금/시총 필터링용
    df_cap_kospi = stock.get_market_cap(today, market="KOSPI")
    df_cap_kosdaq = stock.get_market_cap(today, market="KOSDAQ")
    df_cap = pd.concat([df_cap_kospi, df_cap_kosdaq])
    
    # 데이터 병합
    df = df.join(df_cap[['시가총액', '상장주식수']], how='left')
    
    # 3. 이동평균선 계산 (Trend 파악용)
    # 전 종목의 과거 데이터를 다 가져오면 느리므로, 
    # 여기서는 '오늘 종가' 기준으로 약식 계산하거나, 
    # 정확도를 위해 주요 종목만 Loop를 돌려야 하는데,
    # v1에서는 "등락률"과 "거래대금" 위주로 가볍게 갑니다.
    # (GitHub Actions 시간 제한 고려: MA20은 생략하거나 필요한 경우 개별 조회)
    
    return df

# ---------------------------------------------------------
# 3. 데이터 가공 및 점수 산출
# ---------------------------------------------------------
def process_data(df, theme_map):
    print("⚙️ 데이터 가공 및 섹터 점수 계산 중...")
    
    # 1. 섹터 매핑
    # 기본적으로 pykrx에서 업종 분류를 가져올 수도 있지만,
    # 여기서는 theme_map에 있는 건 우선 적용, 나머지는 '기타'로 처리 (v1 단순화)
    # *실전 팁: KRX 업종분류 API를 호출해서 병합해도 됨.
    
    df['sector'] = 'Unclassified'
    
    # 커스텀 맵 적용
    for code, sector_name in theme_map.items():
        if code in df.index:
            df.loc[code, 'sector'] = sector_name
            
    # 2. 필터링 (동전주, 거래대금 10억 미만 제외)
    mask = (df['종가'] > 1000) & (df['거래대금'] > 1_000_000_000)
    df_clean = df[mask].copy()
    
    # 3. 섹터별 통계 계산
    # Flow(자금): 평균 거래대금
    # Trend(추세): 평균 등락률
    # Breadth(확산): 상승 종목 비율
    
    sector_stats = []
    
    for sector, group in df_clean.groupby('sector'):
        if sector == 'Unclassified': continue
        if len(group) < 2: continue # 종목 수 너무 적으면 패스
        
        # 지표 계산
        avg_flow = group['거래대금'].mean()
        avg_change = group['등락률'].mean()
        up_count = len(group[group['등락률'] > 0])
        total_count = len(group)
        breadth = (up_count / total_count) * 100
        
        # MSI Score (가중치: 자금 40%, 추세 30%, 확산 30%)
        # 정규화가 필요하지만 v1은 단순 합산 점수로 랭킹
        # (거래대금은 단위가 크므로 로그 스케일 적용 등 보정 필요. 여기선 단순화)
        
        score = avg_change + (breadth / 5) # 임시 스코어링 로직
        
        # 대장주 찾기 (거래대금 1등)
        leader = group.sort_values(by='거래대금', ascending=False).iloc[0]
        
        sector_stats.append({
            "name": sector,
            "msi_score": round(score, 2),
            "flow_won": int(avg_flow),
            "avg_change": round(avg_change, 2),
            "breadth": round(breadth, 1),
            "leader_code": leader.name,
            "leader_name": stock.get_market_ticker_name(leader.name),
            "stock_count": total_count
        })
        
    # 랭킹 정렬 (점수 높은 순)
    sector_stats.sort(key=lambda x: x['msi_score'], reverse=True)
    
    return sector_stats, df_clean

# ---------------------------------------------------------
# 4. 결과 저장 (JSON)
# ---------------------------------------------------------
def save_results(sectors, df_clean):
    print("💾 결과 JSON 저장 중...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 1. Summary
    summary = {
        "updated_at": now_str,
        "market_status": "Neutral", # 나중에 코스피 지수 로직 추가
        "top_sectors": [s['name'] for s in sectors[:3]],
        "total_analyzed": len(df_clean)
    }
    with open(os.path.join(DATA_DIR, 'summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
        
    # 2. Sectors
    with open(os.path.join(DATA_DIR, 'sectors.json'), 'w', encoding='utf-8') as f:
        json.dump(sectors, f, ensure_ascii=False, indent=2)
        
    # 3. Candidates (MSI 조건 만족하는 개별 종목)
    # 로직: 커스텀 섹터에 포함되어 있고 + 양봉(상승)인 종목들
    candidates = []
    
    # 커스텀 섹터 종목만 필터링
    filtered = df_clean[df_clean['sector'] != 'Unclassified']
    
    for code, row in filtered.iterrows():
        # 상승한 종목만 WATCH 리스트에 담음
        if row['등락률'] > 0:
            candidates.append({
                "code": code,
                "name": stock.get_market_ticker_name(code),
                "sector": row['sector'],
                "close": int(row['종가']),
                "change_rate": round(row['등락률'], 2),
                "volume_money": int(row['거래대금']),
                "msi_action": "WATCH"
            })
            
    # 거래대금 순 정렬
    candidates.sort(key=lambda x: x['volume_money'], reverse=True)
    
    with open(os.path.join(DATA_DIR, 'candidates.json'), 'w', encoding='utf-8') as f:
        json.dump(candidates, f, ensure_ascii=False, indent=2)

    print(f"✅ 완료! (섹터 {len(sectors)}개, 후보 {len(candidates)}개)")

# ---------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    theme_map = load_theme_map()
    df = fetch_market_data()
    sectors, df_clean = process_data(df, theme_map)
    save_results(sectors, df_clean)
