import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from pykrx import stock
from datetime import datetime, timedelta

# ---------------------------------------------------------
# 1. 설정 및 초기화
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
        with open(THEME_MAP_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

# ---------------------------------------------------------
# 2. 데이터 수집 엔진
# ---------------------------------------------------------
def get_fundamental_data():
    """pykrx로 펀더멘털 데이터 수집"""
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
    """KOSPI/KOSDAQ 개별 호출로 섹터 정보 확보"""
    print("   Fetching Sector info (Separately)...")
    try:
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        df = pd.concat([k, q])
        return df
    except Exception as e:
        print(f"   ⚠️ Sector Fetch Error: {e}")
        return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (Expansion v1.7)...")
    
    # 1. 펀더멘털 데이터
    df_fund = get_fundamental_data()
    if df_fund is None:
        print("❌ Fund data missing.")
        return
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    # 2. 업종 데이터
    df_master = get_sector_data()
    
    # 컬럼명 표준화
    col_map = {
        'Symbol': 'Code', '종목코드': 'Code', 'Name': 'Name', '종목명': 'Name',
        'Sector': 'Sector', 'Industry': 'Sector', 'Wics': 'Sector', '업종': 'Sector', '업종명': 'Sector'
    }
    df_master = df_master.rename(columns=col_map)

    # 3. 데이터 병합
    print("   Merging Data...")
    df = pd.merge(df_master, df_fund, on='Code', how='inner')

    # ---------------------------------------------------------
    # [Fix] 섹터 분류 확장 (한글화 대폭 강화)
    # ---------------------------------------------------------
    if 'Sector' not in df.columns:
        df['Sector'] = '기타'
    df['Sector'] = df['Sector'].fillna('기타')

    # (1) 영어 섹터명 -> 한글 매핑 (누락 없이 대거 추가)
    sector_translate = {
        # KOSPI/KOSDAQ 주요 영어 표기
        'Chemicals': '화학', 
        'Services': '서비스업', 
        'Finance': '금융', 
        'IT': 'IT/전기전자',
        'Pharmaceutical': '의약품', 
        'Distribution': '유통', 
        'Construction': '건설',
        'Food & Beverage': '음식료', 
        'Machinery': '기계', 
        'Metal': '철강/금속',
        'Transport': '운수장비', 
        'Textile & Apparel': '섬유/의복', 
        'Paper & Wood': '종이/목재',
        'Non-Metallic Minerals': '비금속광물', 
        'Telecommunication': '통신',
        'Electricity & Gas': '전기가스', 
        'Medical & Precision': '의료정밀',
        'Other Manufacturing': '기타제조', 
        'Semiconductor': '반도체(공식)', # 기존 테마맵과 구분을 위해
        'Digital Contents': '디지털컨텐츠', 
        'Software': '소프트웨어',
        'Computer Services': '컴퓨터서비스', 
        'Telecommunication Equip': '통신장비',
        'Electronic Components': '전자부품', 
        'Information Equipment': '정보기기',
        'Broadcasting Service': '방송서비스', 
        'Internet': '인터넷',
        'IT H/W': 'IT부품',
        'Manufacturing': '제조업',
        'Wholesale & Retail': '도소매',
    }
    # 부분 일치라도 번역하기 위해 replace 대신 map 사용 고려, 여기선 직접 치환
    df['Sector'] = df['Sector'].replace(sector_translate)

    # (2) Theme Map 오버라이드 (사용자 정의 테마가 최우선)
    theme_map = load_theme_map()
    print(f"   Applying {len(theme_map)} custom themes over official sectors...")
    
    for code, custom_sector in theme_map.items():
        if code in df['Code'].values:
            df.loc[df['Code'] == code, 'Sector'] = custom_sector

    # ---------------------------------------------------------

    # 4. 데이터 정제
    if 'PBR' in df.columns: df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
    if 'PER' in df.columns: df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    
    # 5. PBR-ROE 분석 대상 필터링
    # - PBR, PER 양수 (적자 제외)
    # - 이상치 제거 (ROE > 50, PBR > 10 등은 왜곡 가능성 큼)
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 너무 극단적인 값 제외 (차트 깨짐 방지)
    df = df[(df['ROE'] > -10) & (df['ROE'] < 60) & (df['PBR'] < 12)]

    # 6. 섹터별 분석 및 저장
    quant_data = {}
    print(f"   Analyzing {len(df)} valid stocks...")

    sector_counts = df['Sector'].value_counts()
    valid_sectors = sector_counts[sector_counts >= 5].index # 종목 5개 이상인 섹터만

    for sector in valid_sectors:
        group = df[df['Sector'] == sector]
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        try:
            slope, intercept = np.polyfit(x, y, 1)
        except: continue
        
        group = group.copy()
        group['PBR_Expected'] = slope * group['ROE'] + intercept
        group['Residual'] = group['PBR'] - group['PBR_Expected']
        
        items = []
        for _, row in group.iterrows():
            items.append({
                'code': row['Code'],
                'name': row['Name'],
                'pbr': round(row['PBR'], 2),
                'roe': round(row['ROE'], 2),
                'residual': round(row['Residual'], 3),
                'is_undervalued': bool(row['Residual'] < 0)
            })
            
        items.sort(key=lambda k: k['residual'])
        
        quant_data[sector] = {
            'slope': slope,
            'intercept': intercept,
            'items': items
        }

    # 결과 저장
    output_path = os.path.join(DATA_DIR, 'quant_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Quant Analysis Done. (Generated {len(quant_data)} sectors)")

if __name__ == "__main__":
    run_quant_analysis()
