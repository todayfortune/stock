import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from pykrx import stock
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정
# ---------------------------------------------------------
def find_repo_root(start_path: str) -> str:
    p = os.path.abspath(start_path)
    while True:
        if os.path.isdir(os.path.join(p, "data")): return p
        parent = os.path.dirname(p)
        if parent == p: return os.path.dirname(os.path.abspath(start_path))
        p = parent

HERE = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = find_repo_root(HERE)
DATA_DIR = os.path.join(BASE_DIR, "data")
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json')
os.makedirs(DATA_DIR, exist_ok=True)

def load_theme_map():
    if os.path.exists(THEME_MAP_FILE):
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f: return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 스마트 섹터 분류기 (핵심 로직)
# ---------------------------------------------------------
def clean_sector_name(raw_sector):
    """
    KRX의 지저분한 상세 업종명을 '투자용 메이저 섹터'로 통합
    """
    if pd.isna(raw_sector): return "기타"
    s = str(raw_sector).replace(' ', '') # 공백 제거 후 비교

    # 매핑 키워드 (위에서부터 우선순위 적용)
    keywords = {
        '제약': '제약/바이오', '의약': '제약/바이오', '바이오': '제약/바이오', '의료': '제약/바이오',
        '반도체': '반도체/장비',
        '소프트웨어': 'SW/게임/인터넷', '게임': 'SW/게임/인터넷', '정보서비스': 'SW/게임/인터넷',
        '자동차': '자동차/부품', '트레일러': '자동차/부품',
        '화학': '화학/정유', '석유': '화학/정유', '고무': '화학/정유', '플라스틱': '화학/정유',
        '철강': '철강/금속', '금속': '철강/금속', '알루미늄': '철강/금속',
        '기계': '기계/장비', '엔진': '기계/장비',
        '건설': '건설/엔지니어링', '토목': '건설/엔지니어링', '건축': '건설/엔지니어링',
        '전기': '전기/전자', '전자': '전기/전자', '통신': '전기/전자', '방송': '전기/전자',
        '금융': '금융/지주', '은행': '금융/지주', '보험': '금융/지주', '증권': '금융/지주', '지주': '금융/지주', '투자': '금융/지주',
        '식료품': '음식료', '음료': '음식료',
        '유통': '유통/상사', '도매': '유통/상사', '소매': '유통/상사', '백화점': '유통/상사',
        '운송': '운송/물류', '항공': '운송/물류', '창고': '운송/물류', '해운': '운송/물류',
        '섬유': '의류/섬유', '의복': '의류/섬유',
        '종이': '제지/목재', '펄프': '제지/목재',
    }

    for key, val in keywords.items():
        if key in s:
            return val
    
    # 매핑 안 된 나머지는 원래 이름 사용하되, 너무 길면 '기타 제조' 등으로 퉁침
    if '제조' in s: return '기타제조'
    return '기타'

# ---------------------------------------------------------
# 3. 데이터 수집 및 분석
# ---------------------------------------------------------
def get_fundamental_data():
    date = datetime.now()
    for i in range(7):
        d_str = date.strftime("%Y%m%d")
        try:
            print(f"   Trying fundamentals for {d_str}...")
            df = stock.get_market_fundamental_by_ticker(d_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Found fundamentals.")
                return df
        except: pass
        date -= timedelta(days=1)
    return None

def get_sector_data():
    print("   Fetching Sector info (KRX-DESC)...")
    try:
        # 상세 업종 정보 가져오기
        df = fdr.StockListing('KRX-DESC')
        return df
    except: return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (Sector Cleaning v2.0)...")
    
    # 1. 데이터 수집
    df_fund = get_fundamental_data()
    if df_fund is None: return
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    df_master = get_sector_data()
    
    # 컬럼 표준화
    col_map = {'Symbol': 'Code', 'Code': 'Code', 'Name': 'Name', 'Sector': 'RawSector', '업종': 'RawSector'}
    df_master = df_master.rename(columns=col_map)
    
    if 'RawSector' not in df_master.columns:
        print("⚠️ Sector column missing.")
        return

    # 2. 병합
    print("   Merging Data...")
    df = pd.merge(df_master[['Code', 'Name', 'RawSector']], df_fund, on='Code', how='inner')

    # 3. [핵심] 섹터 정리 프로세스
    # (A) 1차: KRX 상세 업종명을 메이저 섹터로 그룹핑
    df['Sector'] = df['RawSector'].apply(clean_sector_name)

    # (B) 2차: 사용자 정의 테마(Theme Map) 최우선 적용
    theme_map = load_theme_map()
    print(f"   Applying {len(theme_map)} custom themes...")
    for code, custom_sector in theme_map.items():
        if code in df['Code'].values:
            df.loc[df['Code'] == code, 'Sector'] = custom_sector

    # 4. 데이터 정제 (PBR/ROE)
    if 'PBR' in df.columns: df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
    if 'PER' in df.columns: df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    
    # 유효 데이터 필터링
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거
    df = df[(df['ROE'] > -10) & (df['ROE'] < 70) & (df['PBR'] < 15)]

    # 5. 섹터별 분석 및 저장
    quant_data = {}
    
    # '기타'나 '기타제조'는 분석 가치가 떨어지므로 제외하거나 맨 뒤로
    filtered_df = df[~df['Sector'].isin(['기타', '기타제조'])]
    
    print(f"   Analyzing {len(filtered_df)} valid stocks...")

    for sector, group in filtered_df.groupby('Sector'):
        # 종목 수가 5개 미만인 자투리 섹터는 버림 (노이즈 제거)
        if len(group) < 5: continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        try: slope, intercept = np.polyfit(x, y, 1)
        except: continue
        
        group = group.copy() # 경고 방지
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
        quant_data[sector] = { 'slope': slope, 'intercept': intercept, 'items': items }

    # 저장
    with open(os.path.join(DATA_DIR, 'quant_stats.json'), 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Cleaned Sectors: {list(quant_data.keys())}")
    print(f"✅ Quant Analysis Done. Saved to {os.path.join(DATA_DIR, 'quant_stats.json')}")

if __name__ == "__main__":
    run_quant_analysis()
