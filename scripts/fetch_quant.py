import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr

# 1. 경로 설정
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

# 2. 퀀트 분석 엔진
def run_quant_analysis():
    print("🧪 Running Quant Analysis (PBR-ROE)...")
    
    try:
        # KRX 전 종목 데이터 (PBR, PER, EPS 등 포함됨)
        df = fdr.StockListing('KRX')
    except Exception as e:
        print(f"❌ KRX Listing Error: {e}")
        return

    # 데이터 전처리 & 필터링
    # PBR, PER가 존재하는 것만 (적자 기업 일부 제외 효과)
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    
    # ROE 역산 (ROE = PBR / PER * 100)
    # FDR Listing에는 ROE 컬럼이 없어서 이렇게 계산합니다.
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 이상치 제거 (ROE > 50%나 PBR > 10배는 퀀트 분석에서 왜곡을 줌)
    df = df[(df['ROE'] > 0) & (df['ROE'] < 50) & (df['PBR'] < 10)]
    
    # 섹터 정보 (Sector가 없으면 KRX_Sector 확인)
    if 'Sector' not in df.columns and 'KRX_Sector' in df.columns:
        df['Sector'] = df['KRX_Sector']
    
    df = df.dropna(subset=['Sector']) # 섹터 없는 종목 제외

    quant_data = {}

    # 섹터별 회귀분석
    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue # 종목 수 너무 적으면 패스
        
        # X: ROE, Y: PBR
        x = group['ROE'].values
        y = group['PBR'].values
        
        # 선형 회귀 (y = ax + b)
        slope, intercept = np.polyfit(x, y, 1)
        
        # 기대 PBR 및 잔차(저평가 정도) 계산
        group['PBR_Expected'] = slope * group['ROE'] + intercept
        group['Residual'] = group['PBR'] - group['PBR_Expected'] # 실제 - 기대
        
        # 잔차가 마이너스일수록 저평가 (기대보다 싸다)
        
        items = []
        for _, row in group.iterrows():
            items.append({
                'code': row['Code'],
                'name': row['Name'],
                'pbr': round(row['PBR'], 2),
                'roe': round(row['ROE'], 2),
                'residual': round(row['Residual'], 3),
                # 회귀선 아래에 있으면 저평가
                'is_undervalued': bool(row['Residual'] < 0)
            })
            
        # 저평가 순서로 정렬 (잔차가 작은 순)
        items.sort(key=lambda k: k['residual'])
        
        quant_data[sector] = {
            'slope': slope,
            'intercept': intercept,
            'items': items
        }

    # 저장
    output_path = os.path.join(DATA_DIR, 'quant_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Quant Analysis Done. Saved to {output_path}")

if __name__ == "__main__":
    run_quant_analysis()
