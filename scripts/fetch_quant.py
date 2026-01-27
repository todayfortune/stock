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
THEME_MAP_FILE = os.path.join(BASE_DIR, 'scripts', 'theme_map.json') # 테마맵 로드 추가

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
    """
    [핵심 수정] KOSPI/KOSDAQ 개별 호출로 섹터 정보 확보
    """
    print("   Fetching Sector info (Separately)...")
    try:
        # 각각 가져와야 Sector 컬럼이 살아있음
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        
        # 구분자 추가
        k['Market_Type'] = 'KOSPI'
        q['Market_Type'] = 'KOSDAQ'
        
        df = pd.concat([k, q])
        return df
    except Exception as e:
        print(f"   ⚠️ Sector Fetch Error: {e}")
        return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (Sector Fix v1.6)...")
    
    # 1. 펀더멘털 데이터
    df_fund = get_fundamental_data()
    if df_fund is None:
        print("❌ Fund data missing.")
        return
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    # 2. 업종 데이터
    df_master = get_sector_data()
    
    # 컬럼명 표준화 (한글/영어 모두 Sector로)
    col_map = {
        'Symbol': 'Code', '종목코드': 'Code', 'Name': 'Name', '종목명': 'Name',
        'Sector': 'Sector', 'Industry': 'Sector', 'Wics': 'Sector', '업종': 'Sector', '업종명': 'Sector'
    }
    df_master = df_master.rename(columns=col_map)

    # 3. 데이터 병합
    print("   Merging Data...")
    df = pd.merge(df_master, df_fund, on='Code', how='inner')

    # ---------------------------------------------------------
    # [Fix] 섹터 분류 로직 강화
    # ---------------------------------------------------------
    # 1. 'Unknown' 처리된 것들 복구 시도
    if 'Sector' not in df.columns:
        df['Sector'] = '기타'
    
    df['Sector'] = df['Sector'].fillna('기타')

    # 2. Theme Map 오버라이드 (우리가 정한 테마가 최우선)
    theme_map = load_theme_map()
    print(f"   Applying {len(theme_map)} custom themes...")
    
    for code, custom_sector in theme_map.items():
        if code in df['Code'].values:
            # 해당 종목의 Sector를 커스텀 테마로 강제 변경
            df.loc[df['Code'] == code, 'Sector'] = custom_sector

    # 3. 주요 영어 섹터명 한글 변환 (보기 좋게)
    sector_translate = {
        'IT': 'IT/전기전자', 'Finance': '금융', 'Health Care': '바이오/헬스케어',
        'Energy': '에너지', 'Materials': '소재/화학', 'Industrials': '산업재/기계',
        'Consumer Discretionary': '경기소비재', 'Consumer Staples': '필수소비재',
        'Utilities': '유틸리티', 'Telecommunication Services': '통신'
    }
    df['Sector'] = df['Sector'].replace(sector_translate)

    # ---------------------------------------------------------

    # 4. 데이터 정제
    if 'PBR' in df.columns: df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
    if 'PER' in df.columns: df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    df = df[(df['ROE'] > 0) & (df['ROE'] < 60) & (df['PBR'] < 15)]
    
    # 5. 분석 및 저장
    quant_data = {}
    print(f"   Analyzing {len(df)} stocks...")

    for sector, group in df.groupby('Sector'):
        # 종목 수 너무 적거나 '기타' 섹터는 제외
        if len(group) < 5 or sector == '기타': continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        try:
            slope, intercept = np.polyfit(x, y, 1)
        except: continue
        
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
        quant_data[sector] = { 'slope': slope, 'intercept': intercept, 'items': items }

    # 저장
    output_path = os.path.join(DATA_DIR, 'quant_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Quant Analysis Done (Saved {len(quant_data)} sectors).")

if __name__ == "__main__":
    run_quant_analysis()
