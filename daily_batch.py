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

    # 한국 시간 기준 현재 시간 구하기 (GitHub Actions는 UTC 기준이므로 +9시간)
    from datetime import datetime, timedelta
    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)
    
    is_monday = now_kst.weekday() == 0
    is_first_day = now_kst.day == 1
    current_hour = now_kst.hour
    current_min = now_kst.minute
    
    print(f"--- 배치 스캔 시작 (KST: {now_kst.strftime('%Y-%m-%d %H:%M')}) ---")

    for sched in schedules:
        freq = sched.get("freq")
        sched_time = sched.get("time", "09:00")
        s_hour, s_min = map(int, sched_time.split(":"))
        
        # 시간 일치 여부 체크 (현재 실행 시점과 스케줄 시간이 맞는지)
        # GitHub Actions가 30분~1시간 단위로 실행된다고 가정할 때, 해당 시간대에 포함되는지 확인
        time_match = (current_hour == s_hour)
        
        should_run = time_match and (
            (freq == "매일") or \
            (freq == "매주 (월요일)" and is_monday) or \
            (freq == "매월 (1일)" and is_first_day)
        )
        
        if should_run:
            print(f"실행 조건 충족: {sched['strategy']} ({sched_time})")
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
