import requests
import pandas as pd
import datetime
import os

# 1. 환경변수에서 키 가져오기 (GitHub Secrets에 저장할 예정)
APP_KEY = os.environ.get("KIS_APP_KEY")
APP_SECRET = os.environ.get("KIS_APP_SECRET")
# 모의투자 URL (나중에 실전으로 바꾸세요)
URL_BASE = "https://openapivts.koreainvestment.com:29443" 

def get_token():
    # (앞서 만든 토큰 발급 로직과 동일)
    headers = {"content-type": "application/json"}
    body = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    res = requests.post(f"{URL_BASE}/oauth2/tokenP", headers=headers, json=body)
    return res.json()['access_token']

def get_price(code, token):
    # (현재가 조회 로직 단순화)
    headers = {
        "content-type": "application/json", "authorization": f"Bearer {token}",
        "appkey": APP_KEY, "appsecret": APP_SECRET, "tr_id": "FHKST01010100"
    }
    params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
    res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", headers=headers, params=params)
    if res.status_code == 200:
        return int(res.json()['output']['stck_prpr'])
    return 0

# --- 메인 로직 ---
def run_screening():
    token = get_token()
    
    # [TODO] 나중에는 여기서 전 종목 루프를 돌며 필터링 하겠지만, 지금은 샘플만
    target_stocks = [
        {"code": "005930", "name": "삼성전자", "comment": "반도체 대장"},
        {"code": "000660", "name": "SK하이닉스", "comment": "HBM 주도주"},
        {"code": "042660", "name": "한화비전", "comment": "CCTV/보안"},
    ]
    
    results = []
    for stock in target_stocks:
        price = get_price(stock['code'], token)
        # 여기에 기현님의 로직(이평선, 외국인 수급 등)을 추가하여 통과 여부 결정
        # if logic_pass: 
        results.append({
            "name": stock['name'],
            "code": stock['code'],
            "price": f"{price:,}원",
            "note": stock['comment']
        })
    
    # 데이터프레임 변환
    df = pd.DataFrame(results)
    
    # HTML 생성 (간단한 스타일 적용)
    html_content = f"""
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{ font-family: -apple-system, sans-serif; padding: 20px; }}
            h1 {{ color: #333; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
        </style>
    </head>
    <body>
        <h1>📈 Fortune Lab 주식 스크리너</h1>
        <p>업데이트: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        {df.to_html(index=False, classes='table')}
    </body>
    </html>
    """
    
    # index.html 파일 저장
    with open("index.html", "w", encoding='utf-8') as f:
        f.write(html_content)

if __name__ == "__main__":
    run_screening()
