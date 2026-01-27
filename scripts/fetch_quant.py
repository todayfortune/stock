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
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------------------------------------------------
# 2. 데이터 수집 엔진 (에러 원천 봉쇄)
# ---------------------------------------------------------
def get_fundamental_data():
    date = datetime.now()
    for _ in range(7):
        d_str = date.strftime("%Y%m%d")
        try:
            print(f"   Trying fundamentals for {d_str}...")
            df = stock.get_market_fundamental_by_ticker(d_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Found fundamentals.")
                return df
        except:
            pass
        date -= timedelta(days=1)
    return None

def get_sector_data():
    print("   Fetching Sector info...")
    try:
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        df = pd.concat([k, q])
        return df
    except Exception as e:
        print(f"   ⚠️ Sector Fetch Warning: {e}")
        return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (v1.5 Robust)...")
    
    # 1. 펀더멘털 데이터
    df_fund = get_fundamental_data()
    if df_fund is None:
        print("❌ Fund data missing.")
        return
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    # 2. 업종 데이터
    df_master = get_sector_data()
    
    # [핵심] 컬럼명 강제 표준화 (어떤 이름이 오든 Sector로 바꿈)
    renames = {
        'Symbol': 'Code', '종목코드': 'Code',
        'Name': 'Name', '종목명': 'Name',
        'Sector': 'Sector', 'Industry': 'Sector', 'Wics': 'Sector', 
        '업종': 'Sector', '업종명': 'Sector', '산업군': 'Sector'
    }
    df_master = df_master.rename(columns=renames)

    # 3. 데이터 병합
    print("   Merging Data...")
    df = pd.merge(df_master, df_fund, on='Code', how='inner')

    # [최후의 방어] Sector 컬럼이 아예 없으면 'Unknown'으로 채워서라도 진행
    if 'Sector' not in df.columns:
        print("⚠️ 'Sector' column missing. Filling with 'Unknown'.")
        df['Sector'] = 'Unknown'
    
    # 4. 데이터 정제 (PBR/PER/ROE)
    cols = ['PBR', 'PER']
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = df.dropna(subset=['PBR', 'PER']) # 숫자 없는거 제거
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거
    df = df[(df['ROE'] > 0) & (df['ROE'] < 60) & (df['PBR'] < 15)]
    
    # 섹터별 분석 시작
    quant_data = {}
    print(f"   Analyzing {len(df)} valid stocks...")

    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue 
        
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
        
        quant_data[sector] = {
            'slope': slope,
            'intercept': intercept,
            'items': items
        }

    # 결과 저장
    output_path = os.path.join(DATA_DIR, 'quant_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Quant Analysis Done. Saved to {output_path}")

if __name__ == "__main__":
    run_quant_analysis()
