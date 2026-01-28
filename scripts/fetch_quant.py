import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from pykrx import stock
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 유틸리티
# ---------------------------------------------------------
def find_repo_root(start_path: str) -> str:
    # [Bug Fix #2] 무한 루프 방지 (최대 10단계만 탐색)
    p = os.path.abspath(start_path)
    for _ in range(10):
        if os.path.isdir(os.path.join(p, "data")): return p
        parent = os.path.dirname(p)
        if parent == p: break
        p = parent
    return os.path.dirname(os.path.abspath(start_path)) # 못 찾으면 현재 위치 반환

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = find_repo_root(HERE)
DATA_DIR = os.path.join(BASE_DIR, "data")
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')
os.makedirs(DATA_DIR, exist_ok=True)

def load_theme_map():
    if os.path.exists(THEME_MAP_FILE):
        try:
            with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except Exception as e:
            print(f"⚠️ Theme Map Load Failed: {e}")
            return {}
    return {}

# ---------------------------------------------------------
# 2. 섹터 정리 함수 (우선순위 로직 강화)
# ---------------------------------------------------------
def clean_sector_name(raw_sector):
    if pd.isna(raw_sector): return "기타"
    s = str(raw_sector).replace(' ', '')

    # [Bug Fix #8] 매핑 오탐 방지 (리스트 순서대로 우선순위 적용)
    # 긴 단어부터 먼저 매칭해야 정확도가 높음 (예: '전기전자' vs '전기')
    keyword_order = [
        (['제약', '의약', '바이오', '의료'], '제약/바이오'),
        (['반도체'], '반도체/장비'),
        (['소프트웨어', '게임', '정보서비스', '인터넷', '디지털'], 'SW/게임/인터넷'),
        (['자동차', '트레일러', '모빌리티'], '자동차/부품'),
        (['2차전지', '배터리', '에너지솔루션'], '2차전지'),
        (['화학', '석유', '고무', '플라스틱'], '화학/정유'),
        (['철강', '금속', '알루미늄', '광물'], '철강/소재'),
        (['기계', '엔진', '장비'], '기계/장비'),
        (['건설', '토목', '건축', '엔지니어링'], '건설/엔지니어링'),
        (['전기', '전자', '통신', '방송', '디스플레이'], 'IT/전기전자'),
        (['금융', '은행', '보험', '증권', '지주', '투자'], '금융/지주'),
        (['식료품', '음료', '음식'], '음식료'),
        (['유통', '도매', '소매', '백화점', '상사'], '유통/상사'),
        (['운송', '항공', '창고', '해운', '물류'], '운송/물류'),
        (['섬유', '의복', '의류', '패션'], '의류/섬유'),
        (['종이', '펄프', '목재'], '제지/목재'),
        (['조선', '중공업'], '조선/중공업'),
        (['서비스'], '서비스업'),
    ]

    for keywords, sector in keyword_order:
        if any(k in s for k in keywords):
            return sector
    
    if '제조' in s: return '기타제조'
    return '기타'

# ---------------------------------------------------------
# 3. 데이터 수집
# ---------------------------------------------------------
def get_fundamental_data():
    date = datetime.now()
    for i in range(7):
        d_str = date.strftime("%Y%m%d")
        try:
            print(f"   Trying fundamentals for {d_str}...")
            df = stock.get_market_fundamental_by_ticker(d_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Found fundamentals for {d_str} ({len(df)} items)")
                return df
        # [Bug Fix #1] 구체적인 에러 출력
        except Exception as e:
            print(f"   ⚠️ Failed for {d_str}: {e}")
            pass
        date -= timedelta(days=1)
    return None

def get_sector_data():
    print("   Fetching Sector info (KRX-DESC)...")
    try:
        df = fdr.StockListing('KRX-DESC')
        print(f"   ✅ Sector info fetched ({len(df)} items)")
        return df
    except Exception as e:
        print(f"   ❌ Sector Fetch Error: {e}")
        return pd.DataFrame()

# ---------------------------------------------------------
# 4. 메인 분석 로직
# ---------------------------------------------------------
def run_quant_analysis():
    print("🧪 Running Quant Analysis (Ultimate v3.0)...")
    
    # 1. 데이터 수집 및 유효성 검사
    df_fund = get_fundamental_data()
    # [Bug Fix #9] None 체크 명확화
    if df_fund is None: 
        print("❌ Critical: No fundamental data found. Aborting.")
        return

    # [Bug Fix #3] 컬럼명 하드코딩 방지 (유연한 처리)
    df_fund = df_fund.reset_index()
    ticker_col = None
    for col in ['티커', 'Code', 'code', 'Symbol', 'symbol']:
        if col in df_fund.columns:
            ticker_col = col
            break
    
    if ticker_col:
        df_fund = df_fund.rename(columns={ticker_col: 'Code'})
    else:
        print(f"❌ Critical: Ticker column not found. Cols: {df_fund.columns}")
        return

    # 종목코드 문자열 통일 ('005930')
    df_fund['Code'] = df_fund['Code'].astype(str).str.zfill(6)

    # 2. 섹터 데이터 준비
    df_master = get_sector_data()
    if df_master.empty:
        print("❌ Critical: No sector data found. Aborting.")
        return

    # 컬럼 표준화
    rename_map = {
        'Symbol': 'Code', 'Code': 'Code', 
        'Name': 'Name', 'Sector': 'RawSector', '업종': 'RawSector'
    }
    df_master = df_master.rename(columns=rename_map)
    
    # 필수 컬럼 확인
    required_cols = ['Code', 'Name', 'RawSector']
    available_cols = [c for c in required_cols if c in df_master.columns]
    
    if 'RawSector' not in df_master.columns:
        print("⚠️ 'RawSector' column missing. Trying to fetch KOSPI/KOSDAQ separately...")
        # 비상 대책: 개별 호출 시도
        try:
            k = fdr.StockListing('KOSPI'); q = fdr.StockListing('KOSDAQ')
            df_master = pd.concat([k, q]).rename(columns=rename_map)
        except: pass

    if 'RawSector' not in df_master.columns:
        print("❌ Sector column absolutely missing. Cannot proceed.")
        return

    df_master['Code'] = df_master['Code'].astype(str).str.zfill(6)

    # 3. 데이터 병합 & 손실 검증
    print("   Merging Data...")
    before_count = len(df_master)
    
    # [Bug Fix #4] 데이터 손실 추적
    df = pd.merge(df_master[['Code', 'Name', 'RawSector']], df_fund, on='Code', how='inner')
    after_count = len(df)
    print(f"   📊 Merge Status: {before_count} -> {after_count} stocks (Dropped: {before_count - after_count})")

    # 4. 섹터 매핑 및 정리
    df['Sector'] = df['RawSector'].apply(clean_sector_name)

    # Theme Map 적용 (사용자 정의 테마)
    theme_map = load_theme_map()
    print(f"   Applying {len(theme_map)} custom themes...")
    
    # [Bug Fix #5] 타입 불일치 해결 (str.zfill(6)로 양쪽 통일 후 비교)
    count_custom = 0
    for code, custom_sector in theme_map.items():
        code_str = str(code).zfill(6)
        mask = df['Code'] == code_str
        if mask.any():
            df.loc[mask, 'Sector'] = custom_sector
            count_custom += 1
    print(f"   👉 Applied {count_custom} custom theme mappings.")

    # 5. 데이터 정제 (PBR/ROE)
    for col in ['PBR', 'PER']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # [Bug Fix #6] ROE 계산 안전성 (PER 0.01 미만 제외로 무한대 방지)
    df = df[(df['PBR'] > 0) & (df['PER'] > 0.01)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거 (차트 왜곡 방지)
    df = df[(df['ROE'] > -20) & (df['ROE'] < 100) & (df['PBR'] < 20)]

    # 6. 섹터별 분석 및 저장
    quant_data = {}
    
    # '기타' 섹터는 분석에서 제외 (선택적)
    filtered_df = df[~df['Sector'].isin(['기타', '기타제조'])]
    
    print(f"   Analyzing {len(filtered_df)} valid stocks...")
    success_count = 0

    for sector, group in filtered_df.groupby('Sector'):
        if len(group) < 5: continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        # [Bug Fix #7] 회귀분석 실패 로그
        try:
            slope, intercept = np.polyfit(x, y, 1)
        except Exception as e:
            print(f"   ⚠️ Regression failed for {sector}: {e}")
            continue
        
        # 잔차 계산
        group = group.copy()
        group['PBR_Expected'] = slope * group['ROE'] + intercept
        group['Residual'] = group['PBR'] - group['PBR_Expected']
        
        items = []
        for _, row in group.iterrows():
            items.append({
                'code': row['Code'], 'name': row['Name'],
                'pbr': round(row['PBR'], 2), 'roe': round(row['ROE'], 2),
                'residual': round(row['Residual'], 3),
                'is_undervalued': bool(row['Residual'] < 0)
            })
        
        items.sort(key=lambda k: k['residual'])
        
        # [Bug Fix #10] JSON 직렬화 에러 해결 (numpy type -> python float)
        quant_data[sector] = {
            'slope': float(slope),
            'intercept': float(intercept),
            'count': int(len(items)),
            'items': items
        }
        success_count += 1

    # 최종 저장
    try:
        with open(os.path.join(DATA_DIR, 'quant_stats.json'), 'w', encoding='utf-8') as f:
            json.dump(quant_data, f, ensure_ascii=False, indent=2)
        print(f"✅ Quant Analysis Completed. Saved {success_count} sectors.")
        print(f"   File path: {os.path.join(DATA_DIR, 'quant_stats.json')}")
    except Exception as e:
        print(f"❌ Final Save Error: {e}")

if __name__ == "__main__":
    run_quant_analysis()
