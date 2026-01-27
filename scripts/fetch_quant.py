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
# 2. 데이터 수집 엔진 (Robust Version)
# ---------------------------------------------------------
def get_fundamental_data():
    """pykrx로 PBR, PER, ROE 등 펀더멘털 데이터 수집"""
    date = datetime.now()
    for _ in range(7):
        d_str = date.strftime("%Y%m%d")
        try:
            print(f"   Searching fundamentals for {d_str}...")
            df = stock.get_market_fundamental_by_ticker(d_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Found fundamentals for {d_str}")
                return df
        except:
            pass
        date -= timedelta(days=1)
    return None

def get_sector_data():
    """FDR로 KOSPI/KOSDAQ 업종 정보 수집 (Sector 컬럼 강제 확보)"""
    print("   Fetching Sector info (KOSPI+KOSDAQ)...")
    try:
        # KRX 전체 통합 리스트는 컬럼이 누락될 때가 많아 개별 수집 후 병합
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        
        # 데이터프레임에 Sector 컬럼이 있는지 확인하고 표준화
        for df in [k, q]:
            if 'Sector' not in df.columns:
                if 'Industry' in df.columns: 
                    df['Sector'] = df['Industry']
                elif 'Wics' in df.columns:
                    df['Sector'] = df['Wics']
                elif '업종명' in df.columns: # 한글 컬럼 대응
                    df['Sector'] = df['업종명']
        
        # 합치기
        df_master = pd.concat([k, q])
        return df_master
    except Exception as e:
        print(f"   ⚠️ Sector Fetch Error: {e}")
        return pd.DataFrame()

def run_quant_analysis():
    print("🧪 Running Quant Analysis (Final Fix)...")
    
    # 1. 펀더멘털 데이터 (PBR/PER)
    df_fund = get_fundamental_data()
    if df_fund is None:
        print("❌ Fund data missing.")
        return
    # 티커 컬럼 정리
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})

    # 2. 업종 데이터 (Sector)
    df_master = get_sector_data()
    if df_master.empty:
        print("❌ Sector data missing.")
        return

    # 필요한 컬럼만 남기기 (Code, Name, Sector)
    # FDR 버전에 따라 Code가 'Symbol'일 수도 있음
    if 'Code' not in df_master.columns and 'Symbol' in df_master.columns:
        df_master = df_master.rename(columns={'Symbol': 'Code'})
        
    cols_to_keep = ['Code', 'Name', 'Sector']
    # 실제 존재하는 컬럼만 선택
    cols_to_keep = [c for c in cols_to_keep if c in df_master.columns]
    df_master = df_master[cols_to_keep]

    # 3. 데이터 병합
    print("   Merging Data...")
    df = pd.merge(df_master, df_fund, on='Code', how='inner')

    # [핵심] Sector 컬럼이 없는 경우 방어 로직
    if 'Sector' not in df.columns:
        print(f"⚠️ Critical: 'Sector' column still missing. Columns: {list(df.columns)}")
        # 임시 방편: 섹터가 없으면 분석 불가하므로 리턴
        return

    # 4. 데이터 정제
    # 숫자로 변환 (에러 방지)
    if 'PBR' in df.columns: df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
    if 'PER' in df.columns: df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    
    # 유효 데이터 필터링
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    
    # ROE 계산
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거 & 섹터 없는 종목 제거
    df = df[(df['ROE'] > 0) & (df['ROE'] < 50) & (df['PBR'] < 10)]
    df = df.dropna(subset=['Sector'])

    print(f"   Analyzing {len(df)} valid stocks...")

    # 5. 섹터별 회귀분석
    quant_data = {}
    
    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        # 선형 회귀 (y = ax + b)
        try:
            slope, intercept = np.polyfit(x, y, 1)
        except:
            continue # 계산 에러 시 해당 섹터 패스
        
        # 잔차 계산
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
