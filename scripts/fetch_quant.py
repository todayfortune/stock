import os
import json
import pandas as pd
import numpy as np
import FinanceDataReader as fdr

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
# 2. PBR-ROE 분석 엔진
# ---------------------------------------------------------
def run_quant_analysis():
    print("🧪 Running Quant Analysis (PBR-ROE)...")
    
    # KRX 전 종목 데이터 가져오기 (PBR, PER, BPS, EPS 등 포함)
    try:
        df = fdr.StockListing('KRX')
    except Exception as e:
        print(f"❌ Failed to fetch KRX listing: {e}")
        return

    # 데이터 전처리
    # 1. PBR, PER 등이 없는 우선주/리츠 등 제외
    df = df[df['PBR'] > 0].copy()
    
    # 2. ROE 계산 (ROE = PBR / PER * 100 or EPS / BPS * 100)
    # PER가 0이거나 NaN인 경우(적자) ROE 계산 불가 -> 제외 또는 별도 처리
    # 여기서는 간편하게 PBR/PER 공식을 쓰되, PER>0 인 것만 필터링 (흑자 기업 대상)
    df = df[df['PER'] > 0].copy()
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 3. 이상치 제거 (ROE > 50% or PBR > 10 등은 왜곡 가능성 높음)
    df = df[(df['ROE'] > 0) & (df['ROE'] < 50) & (df['PBR'] < 10)]
    
    # 4. 섹터 분류 (KRX_Sector가 없는 경우 제외)
    if 'Sector' in df.columns:
        df['KRX_Sector'] = df['Sector']
    df = df.dropna(subset=['KRX_Sector'])

    # 결과 저장소
    quant_data = {}

    # 섹터별 루프
    for sector, group in df.groupby('KRX_Sector'):
        if len(group) < 5: continue # 종목 수 너무 적으면 패스
        
        # X: ROE, Y: PBR (ROE가 높을수록 PBR도 높아야 정상)
        x = group['ROE'].values
        y = group['PBR'].values
        
        # 선형 회귀 (Trend Line)
        # y = slope * x + intercept
        slope, intercept = np.polyfit(x, y, 1)
        
        # 기대 PBR 계산
        group['PBR_Expected'] = slope * group['ROE'] + intercept
        
        # 저평가 정도 (잔차): 실제 PBR - 기대 PBR
        # (-)일수록 저평가 (회귀선 아래), (+)일수록 고평가
        group['Residual'] = group['PBR'] - group['PBR_Expected']
        group['Undervalued_Score'] = group['Residual'] / group['PBR'] # 비율로 변환
        
        # 데이터 정리
        items = []
        for _, row in group.iterrows():
            items.append({
                'code': row['Code'],
                'name': row['Name'],
                'pbr': round(row['PBR'], 2),
                'roe': round(row['ROE'], 2),
                'residual': round(row['Residual'], 3),
                'is_undervalued': bool(row['Residual'] < 0) # 회귀선 아래
            })
            
        # 저평가 순(잔차가 가장 작은 순) 정렬
        items.sort(key=lambda k: k['residual'])
        
        quant_data[sector] = {
            'slope': slope,
            'intercept': intercept,
            'items': items
        }

    # JSON 저장
    output_path = os.path.join(DATA_DIR, 'quant_stats.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(quant_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ Quant Analysis Done. Saved to {output_path}")

if __name__ == "__main__":
    run_quant_analysis()
