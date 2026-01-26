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
    
    try:
        # [핵심 수정] KRX 대신 KOSPI, KOSDAQ 각각 호출 후 병합
        # 이렇게 해야 PBR, PER, Sector 정보가 확실하게 들어옵니다.
        print("   Fetching KOSPI & KOSDAQ listings...")
        df_kospi = fdr.StockListing('KOSPI')
        df_kosdaq = fdr.StockListing('KOSDAQ')
        df = pd.concat([df_kospi, df_kosdaq])
        
    except Exception as e:
        print(f"❌ Listing Error: {e}")
        return

    # [데이터 검증] PBR 컬럼이 진짜 있는지 확인
    if 'PBR' not in df.columns:
        print(f"⚠️ Error: 'PBR' column missing. Columns found: {list(df.columns)}")
        return

    # 데이터 전처리
    # 1. PBR, PER 데이터 형변환 (문자열인 경우 대비) 및 0 이하 제거
    for col in ['PBR', 'PER']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. 유효한 데이터만 필터링 (적자 기업 제외 효과)
    df = df[(df['PBR'] > 0) & (df['PER'] > 0)].copy()
    
    # 3. ROE 역산 (ROE = PBR / PER * 100)
    df['ROE'] = (df['PBR'] / df['PER']) * 100
    
    # 4. 이상치 제거 (ROE > 50% or PBR > 10 등은 왜곡 가능성 높음)
    df = df[(df['ROE'] > 0) & (df['ROE'] < 50) & (df['PBR'] < 10)]
    
    # 5. 섹터 분류 (Sector 컬럼 확인)
    # FDR 버전에 따라 'Sector', 'Industry' 등 이름이 다를 수 있음
    if 'Sector' not in df.columns:
        if 'Wics' in df.columns: df['Sector'] = df['Wics'] # 대안 1
        elif 'Industry' in df.columns: df['Sector'] = df['Industry'] # 대안 2
    
    df = df.dropna(subset=['Sector']) # 섹터 없는 종목 제외

    # 결과 저장소
    quant_data = {}

    # 섹터별 루프
    print(f"   Analyzing {len(df)} stocks across sectors...")
    
    for sector, group in df.groupby('Sector'):
        if len(group) < 5: continue # 종목 수 너무 적으면 패스
        
        # X: ROE, Y: PBR
        x = group['ROE'].values
        y = group['PBR'].values
        
        # 선형 회귀 (Trend Line)
        slope, intercept = np.polyfit(x, y, 1)
        
        # 기대 PBR 계산
        group['PBR_Expected'] = slope * group['ROE'] + intercept
        
        # 저평가 정도 (잔차)
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
