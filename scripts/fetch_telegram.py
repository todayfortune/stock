import os
import json
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession

# ---------------------------------------------------------
# 1. 감시할 채널 리스트
# ---------------------------------------------------------
TARGET_CHANNELS = [
    '@IDEA_MEMO', '@MASSITRADING', '@JAKE8LEE', '@ONE_GOING', 
    '@BRILLER_RESEARCH', '@MSTARYUN', '@DAISHINSTRATEGY', 
    '@IRNOTE_YSTREET', '@YAZA_STOCK', '@DH_FINANCE', 
    '@SHINHANRESEARCH', '@GLOBALMKTINSIGHT', '@JOORINI34', 
    '@EASOBI', '@TOPTOWNQUANT', '@MERITZ_RESEARCH', 
    '@SKSRESEARCH', '@SURVIVAL_DOPB', '@HEDGECAT0301'
]

# ---------------------------------------------------------
# 2. 발굴용 핵심 키워드 (Trend Keywords)
# ---------------------------------------------------------
TREND_KEYWORDS = [
    "상향", "서프라이즈", "쇼크", "수요", "공급", 
    "이닛", "init", "구조적 성장", "사이클", "업사이드", 
    "OP", "TP", "M/S", "QoQ", "YoY", "밸류체인", "수주"
]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

async def main():
    api_id = os.environ.get('TELEGRAM_API_ID')
    api_hash = os.environ.get('TELEGRAM_API_HASH')
    session_str = os.environ.get('TELEGRAM_SESSION')

    if not api_id or not api_hash or not session_str:
        print("⚠️ 텔레그램 설정이 누락되었습니다.")
        return

    print("📡 텔레그램 접속 시도...")
    client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
    
    try:
        await client.start()
    except Exception as e:
        print(f"❌ 텔레그램 로그인 실패: {e}")
        return

    # 관심종목 불러오기 (개별 종목 매칭용)
    watchlist_path = os.path.join(DATA_DIR, 'watchlist.json')
    watchlist_items = []
    if os.path.exists(watchlist_path):
        with open(watchlist_path, 'r', encoding='utf-8') as f:
            watchlist_items = json.load(f)['items']
    
    # 검색용 매핑: { '삼성전자': '005930', ... }
    stock_keywords = {item['name']: item['ticker'] for item in watchlist_items}
    
    # 데이터 저장소 분리
    final_data = {
        "global": [],      # 키워드로 찾은 뉴스 (발굴용)
        "specific": {}     # 내 종목 관련 뉴스 (관리용)
    }

    print(f"🔍 뉴스 수집 시작 (Target: {len(TREND_KEYWORDS)} Keywords & {len(stock_keywords)} Stocks)...")
    
    for channel in TARGET_CHANNELS:
        try:
            print(f"   👉 스캔: {channel}")
            async for message in client.iter_messages(channel, limit=30):
                if not message.text: continue
                
                msg_text = message.text
                msg_date = message.date + timedelta(hours=9)
                date_str = msg_date.strftime("%Y-%m-%d %H:%M")
                link = f"https://t.me/{channel.replace('@', '')}/{message.id}"
                preview = msg_text[:150].replace('\n', ' ') + "..."

                # 1) [Global] 트렌드 키워드 검색 (새 종목 발굴)
                # 메시지에 키워드가 하나라도 있으면 저장
                matched_keywords = [k for k in TREND_KEYWORDS if k in msg_text]
                if matched_keywords:
                    final_data["global"].append({
                        "source": channel,
                        "date": date_str,
                        "text": preview,
                        "link": link,
                        "keywords": matched_keywords # 어떤 키워드에 걸렸는지 저장
                    })

                # 2) [Specific] 내 관심종목 검색 (기존 기능)
                for name, ticker in stock_keywords.items():
                    if name in msg_text:
                        if ticker not in final_data["specific"]:
                            final_data["specific"][ticker] = []
                        
                        # 중복 저장 방지 (이미 global에 들어갔어도 종목별 정리를 위해 별도 저장)
                        final_data["specific"][ticker].append({
                            "source": channel,
                            "date": date_str,
                            "text": preview,
                            "link": link
                        })

        except Exception as e:
            print(f"   ⚠️ {channel} 에러: {e}")

    await client.disconnect()

    # 결과 저장
    output_path = os.path.join(DATA_DIR, 'telegram_news.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 수집 완료! (키워드 뉴스: {len(final_data['global'])}건)")

if __name__ == '__main__':
    asyncio.run(main())
