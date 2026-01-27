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
# 2. 데이터 수집 (pykrx + FDR 하이브리드)
# ---------------------------------------------------------
def get_fundamental_data():
    """pykrx를 이용해 가장 최신 영업일의 PBR/PER 데이터를 가져옵니다."""
    date = datetime.now()
    
    # 오늘 포함 최근 7일 중 데이터가 있는 날을 찾음 (휴일 대비)
    for _ in range(7):
        d_str = date.strftime("%Y%m%d")
        try:
            print(f"   Trying to fetch fundamentals for {d_str}...")
            df = stock.get_market_fundamental_by_ticker(d_str, market="ALL")
            if not df.empty:
                print(f"   ✅ Fetched fundamentals for {d_str}")
                return df
        except:
            pass
        date -= timedelta(days=1)
    return None

def run_quant_analysis():
    print("🧪 Running Quant Analysis (Hybrid Engine)...")
    
    # 1. 펀더멘털 데이터 (PBR, PER) -> pykrx 사용 (정확도 높음)
    df_fund = get_fundamental_data()
    if df_fund is None:
        print("❌ Failed to fetch fundamental data.")
        return

    # pykrx는 티커가 인덱스로 되어있으므로 컬럼으로 변환
    df_fund = df_fund.reset_index().rename(columns={'티커': 'Code'})
    
    # 2. 섹터 정보 -> FDR 사용 (업종 분류가 잘 되어있음)
    try:
        df_master = fdr.StockListing('KRX')
    except:
        # KRX 전체 실패시 코스피/코스닥 합체
        k = fdr.StockListing('KOSPI')
        q = fdr.StockListing('KOSDAQ')
        df_master = pd.concat([k, q])
    
    # 필요한 컬럼만 추출 (Code, Name, Sector)
    cols = ['Code', 'Name']
    if 'Sector' in df_master.columns: cols.append('Sector')
    elif 'KRX_Sector' in df_master.columns: cols.append('KRX_Sector')
    
    df_master = df_master[cols]
    if 'KRX_Sector' in df_master.columns:
        df_master = df_master.rename(columns={'KRX_Sector': 'Sector'})
    
    # 3. 데이터 병합 (Code 기준)
    print("   Merging data...")
    df = pd.merge(df_master, df_fund, on='Code', how='inner')
    
    # 데이터 전처리
    # pykrx 컬럼: BPS, PER, PBR, EPS, DIV, DPS (버전에 따라 다를 수 있음)
    # 안전하게 숫자로 변환
    if 'PBR' in df.columns and 'PER' in df.columns:
        df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
        df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
    else:
        print("⚠️ PBR/PER columns not found in merged data.")
        return

    # ROE 계산 (ROE = PBR / PER * 100)
    # PER가 0이거나 NaN이면 제외
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거 & 섹터 없는 종목 제거
    df = df[(df['ROE'] > 0) & (df['ROE'] < 50) & (df['PBR'] < 10)]
    df = df.dropna(subset=['Sector'])

    quant_data = {}

    # 4. 섹터별 분석
    print(f"   Analyzing {len(df)} stocks...")
    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue 
        
        x = group['ROE'].values
        y = group['PBR'].values
        
        # 선형 회귀
        slope, intercept = np.polyfit(x, y, 1)
        
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
