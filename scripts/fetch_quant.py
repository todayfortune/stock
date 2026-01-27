import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr
from pykrx import stock
from datetime import datetime, timedelta

# 1. 설정
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

# 2. 데이터 수집
def get_fundamental_data():
    """pykrx로 PBR/PER 수집"""
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
    [핵심] KRX-DESC 옵션을 사용하여 '업종(Sector)' 정보를 확실하게 가져옴
    """
    print("   Fetching Sector info (KRX-DESC)...")
    try:
        # KRX-DESC: 종목 상세 정보 (업종 포함)
        df = fdr.StockListing('KRX-DESC')
        return df
    except Exception as e:
        print(f"   ⚠️ Sector Fetch Error: {e}")
        return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (v1.8 KRX-DESC)...")
    
    # 1. 펀더멘털 (PBR/PER)
    df_fund = get_fundamental_data()
    if df_fund is None: return
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    # 2. 업종 (Sector)
    df_master = get_sector_data()
    
    # 컬럼 표준화
    col_map = {'Symbol': 'Code', 'Code': 'Code', 'Name': 'Name', 'Sector': 'Sector', '업종': 'Sector'}
    df_master = df_master.rename(columns=col_map)
    
    if 'Sector' not in df_master.columns:
        print("⚠️ 'Sector' column missing even in KRX-DESC. Check FDR version.")
        return

    # 3. 병합
    print("   Merging Data...")
    df = pd.merge(df_master[['Code', 'Name', 'Sector']], df_fund, on='Code', how='inner')

    # 4. 섹터 정리 (테마 적용 + 한글화)
    df['Sector'] = df['Sector'].fillna('기타')
    
    # 영어 섹터명 한글 변환
    sector_translate = {
        'IT': 'IT/전기전자', 'Finance': '금융', 'Health Care': '바이오/헬스케어',
        'Energy': '에너지', 'Materials': '소재', 'Industrials': '산업재',
        'Consumer Discretionary': '경기소비재', 'Consumer Staples': '필수소비재',
        'Utilities': '유틸리티', 'Telecommunication Services': '통신',
        'Information Technology': 'IT', 'Financials': '금융'
    }
    df['Sector'] = df['Sector'].replace(sector_translate)

    # 사용자 테마 덮어쓰기
    theme_map = load_theme_map()
    for code, custom_sector in theme_map.items():
        if code in df['Code'].values:
            df.loc[df['Code'] == code, 'Sector'] = custom_sector

    # 5. 데이터 정제
    if 'PBR' in df.columns: df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
    if 'PER' in df.columns: df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    df = df[(df['ROE'] > 0) & (df['ROE'] < 60) & (df['PBR'] < 15)]

    # 6. 분석
    quant_data = {}
    print(f"   Analyzing {len(df)} valid stocks...")

    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        try: slope, intercept = np.polyfit(x, y, 1)
        except: continue
        
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
    print(f"✅ Done. Saved {len(quant_data)} sectors.")

if __name__ == "__main__":
    run_quant_analysis()
