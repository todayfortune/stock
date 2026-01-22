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
    '@IDEA_MEMO',         # 아이디어 메모
    '@MASSITRADING',      # 매씨 트레이딩
    '@JAKE8LEE',          # 제이크 리
    '@ONE_GOING',         # 한길
    '@BRILLER_RESEARCH',  # 브리이에 리서치
    '@MSTARYUN',          # 엠스타 윤
    '@DAISHINSTRATEGY',   # 대신 전략
    '@IRNOTE_YSTREET',    # 여의도 스토리 (IR노트)
    '@YAZA_STOCK',        # 야자 주식
    '@DH_FINANCE',        # DH 금융
    '@SHINHANRESEARCH',   # 신한 리서치
    '@GLOBALMKTINSIGHT',  # 글로벌 마켓 인사이트
    '@JOORINI34',         # 주린이34
    '@EASOBI',            # 이소비
    '@TOPTOWNQUANT',      # 탑타운 퀀트
    '@MERITZ_RESEARCH',   # 메리츠 리서치
    '@SKSRESEARCH',       # SK증권 리서치
    '@SURVIVAL_DOPB',     # 생존 도피비
    '@HEDGECAT0301'       # 헷지캣
]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

async def main():
    # 1. 시크릿 키 가져오기 (Github Secrets에서 자동 주입됨)
    api_id = os.environ.get('TELEGRAM_API_ID')
    api_hash = os.environ.get('TELEGRAM_API_HASH')
    session_str = os.environ.get('TELEGRAM_SESSION')

    if not api_id or not session_str:
        print("⚠️ 텔레그램 설정이 없습니다. 뉴스 수집을 건너뜁니다.")
        return

    print("📡 텔레그램 접속 중...")
    try:
        client = TelegramClient(StringSession(session_str), int(api_id), api_hash)
        await client.start()
    except Exception as e:
        print(f"❌ 접속 실패: {e}")
        return

    # 2. 관심종목(Watchlist) 불러오기
    watchlist_path = os.path.join(DATA_DIR, 'watchlist.json')
    if not os.path.exists(watchlist_path):
        print("❌ 관심종목 파일이 없습니다.")
        await client.disconnect()
        return
        
    with open(watchlist_path, 'r', encoding='utf-8') as f:
        watchlist = json.load(f)['items']
    
    # { '삼성전자': '005930', ... } 형태로 변환 (검색용)
    target_keywords = {item['name']: item['ticker'] for item in watchlist}
    
    news_data = {} # 결과 담을 통

    print(f"🔍 {len(target_keywords)}개 관심 종목에 대한 뉴스 수색 시작...")
    
    # 3. 채널 순회하며 메시지 긁기 (채널당 최근 50개)
    for channel in TARGET_CHANNELS:
        try:
            print(f"   👉 채널 스캔: {channel}")
            async for message in client.iter_messages(channel, limit=50):
                if not message.text: continue
                
                msg_text = message.text
                msg_date = message.date + timedelta(hours=9) # KST 변환
                date_str = msg_date.strftime("%Y-%m-%d %H:%M")

                # 메시지 안에 우리 종목 이름이 있는지 확인
                for name, ticker in target_keywords.items():
                    if name in msg_text:
                        if ticker not in news_data: news_data[ticker] = []
                        
                        # 중복 방지 및 데이터 정제
                        preview = msg_text[:150].replace('\n', ' ') + "..."
                        link = f"https://t.me/{channel.replace('@', '')}/{message.id}"
                        
                        news_data[ticker].append({
                            "source": channel,
                            "date": date_str,
                            "text": preview,
                            "link": link
                        })
        except Exception as e:
            print(f"   ⚠️ {channel} 스캔 중 에러: {e}")

    await client.disconnect()

    # 4. 결과 저장
    output_path = os.path.join(DATA_DIR, 'telegram_news.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(news_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 텔레그램 뉴스 수집 완료! (총 {len(news_data)}개 종목 관련 뉴스 발견)")

if __name__ == '__main__':
    asyncio.run(main())
