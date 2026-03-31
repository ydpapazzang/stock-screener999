import logic
import os
import pandas as pd
from datetime import datetime

def run_batch():
    # GitHub Secrets에서 정보 가져오기 (환경변수)
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("Error: 환경 변수(Secrets) 설정이 필요합니다.")
        return

    config = logic.load_config()
    schedules = config.get("schedules", [])
    
    if not schedules:
        print("등록된 스케줄이 없습니다.")
        return

    # 오늘 요일 및 날짜 정보
    now = datetime.now()
    is_monday = now.weekday() == 0
    is_first_day = now.day == 1
    
    print(f"--- 배치 스캔 시작 ({now}) ---")

    for sched in schedules:
        freq = sched.get("freq")
        # 실행 조건 체크 (GitHub Actions가 매일 한 번 실행된다고 가정)
        should_run = (freq == "매일") or \
                     (freq == "매주 (월요일)" and is_monday) or \
                     (freq == "매월 (1일)" and is_first_day)
        
        if should_run:
            print(f"실행 중: {sched['strategy']} ({sched['target']})")
            target_key = "KOSPI" if "주식" in sched['target'] else "ETF"
            df_list = logic.get_listing_data(target_key)
            
            results = []
            limit = int(sched.get('limit', 100))
            targets = df_list.iloc[:limit]
            
            # 전략에 따른 주기 설정 (월봉/주봉)
            m_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파"]
            period_key = 'M' if sched['strategy'] in m_strats else 'W'
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(logic.process_stock_multi_worker, row.Symbol, row.Name, [sched['strategy']], period_key): row for row in targets.itertuples()}
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        results.append(res)
            
            if results:
                msg = logic.format_tg_message(results, [sched['strategy']], sched['target'])
                logic.send_telegram_message(token, chat_id, msg)
                print(f"완료: {len(results)}개 종목 전송됨")

if __name__ == "__main__":
    run_batch()
