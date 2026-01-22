import os
import json
import asyncio
from datetime import datetime, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession

# ---------------------------------------------------------
# 📡 감시할 텔레그램 채널 리스트
# ---------------------------------------------------------
TARGET_CHANNELS = [
    '@IDEA_MEMO', '@MASSITRADING', '@JAKE8LEE', '@ONE_GOING', 
    '@BRILLER_RESEARCH', '@MSTARYUN', '@DAISHINSTRATEGY', 
    '@IRNOTE_YSTREET', '@YAZA_STOCK', '@DH_FINANCE', 
    '@SHINHANRESEARCH', '@GLOBALMKTINSIGHT', '@JOORINI34', 
    '@EASOBI', '@TOPTOWNQUANT', '@MERITZ_RESEARCH', 
    '@SKSRESEARCH', '@SURVIVAL_DOPB', '@HEDGECAT0301'
]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

async def main():
    api_id = os.environ.get('TELEGRAM_API_ID')
    api_hash = os.environ.get('TELEGRAM_API_HASH')
    session_str = os.environ.get('TELEGRAM_SESSION')

    # 👇 [수정] 3개 다 있는지 꼼꼼하게 체크
    if not api_id or not api_hash or not session_str:
        print("⚠️ 텔레그램 설정(API_ID, API_HASH, SESSION)이 누락되었습니다.")
        print("   -> Settings > Secrets and variables > Actions 에서 확인하세요.")
        return

    print("📡 텔레그램 접속 시도...")
    client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
    
    try:
        await client.start()
    except Exception as e:
        print(f"❌ 텔레그램 로그인 실패: {e}")
        return

    watchlist_path = os.path.join(DATA_DIR, 'watchlist.json')
    if not os.path.exists(watchlist_path):
        print(f"❌ '{watchlist_path}' 파일이 없습니다. (주식 분석이 먼저 실행되어야 함)")
        await client.disconnect()
        return
        
    with open(watchlist_path, 'r', encoding='utf-8') as f:
        watchlist = json.load(f)['items']
    
    target_keywords = {item['name']: item['ticker'] for item in watchlist}
    news_data = {}

    print(f"🔍 {len(target_keywords)}개 종목 뉴스 수집 시작...")
    
    for channel in TARGET_CHANNELS:
        try:
            print(f"   👉 스캔: {channel}")
            async for message in client.iter_messages(channel, limit=30):
                if not message.text: continue
                
                msg_text = message.text
                msg_date = message.date + timedelta(hours=9)
                date_str = msg_date.strftime("%Y-%m-%d %H:%M")

                for name, ticker in target_keywords.items():
                    if name in msg_text:
                        if ticker not in news_data: news_data[ticker] = []
                        preview = msg_text[:150].replace('\n', ' ') + "..."
                        link = f"https://t.me/{channel.replace('@', '')}/{message.id}"
                        
                        news_data[ticker].append({
                            "source": channel, "date": date_str, "text": preview, "link": link
                        })
        except Exception as e:
            print(f"   ⚠️ {channel} 에러: {e}")

    await client.disconnect()

    output_path = os.path.join(DATA_DIR, 'telegram_news.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(news_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 수집 완료! (총 {len(news_data)}개 종목 뉴스)")

if __name__ == '__main__':
    asyncio.run(main())
