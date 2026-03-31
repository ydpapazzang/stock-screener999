import logic
import os
import pandas as pd
from datetime import datetime, timedelta

def run_batch():
    # GitHub Secrets에서 정보 가져오기
    token = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print(f"Error: 환경 변수(Secrets) 설정이 필요합니다. (TOKEN: {'OK' if token else 'MISSING'}, ID: {'OK' if chat_id else 'MISSING'})")
        return

    config = logic.load_config()
    schedules = config.get("schedules", [])
    
    if not schedules:
        print("등록된 스케줄이 없습니다.")
        return

    # 한국 시간 기준 현재 시간 구하기
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
        try:
            s_hour, s_min = map(int, sched_time.split(":"))
        except:
            s_hour, s_min = 9, 0
        
        # 시간/분 일치 여부 체크 (15분 단위 실행이므로, 해당 구간 내에 있는지 확인)
        # 예: 13:45 스케줄은 13:45 ~ 13:59 사이에 실행되면 발송
        time_match = (current_hour == s_hour) and (current_min >= s_min) and (current_min < s_min + 15)
        
        if time_match:
            print(f"매칭 확인: {sched_time} 스케줄을 현재 시간({current_hour}:{current_min})에 실행합니다.")
        
        should_run = time_match and (
            (freq == "매일") or \
            (freq == "매주 (월요일)" and is_monday) or \
            (freq == "매월 (1일)" and is_first_day)
        )
        
        if should_run:
            print(f"실행 조건 충족: {sched['strategy']} ({sched_time})")
            target_key = "KOSPI" if "주식" in sched['target'] else "ETF"
            df_list = logic.get_listing_data(target_key)
            
            if df_list.empty:
                print("에러: 종목 리스트를 불러오지 못했습니다.")
                continue

            results = []
            limit = int(sched.get('limit', 100))
            targets = df_list.iloc[:limit]
            
            # 전략에 따른 주기 설정
            m_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
            d_strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수"]
            
            if sched['strategy'] in m_strats: period_key = 'M'
            elif sched['strategy'] in d_strats: period_key = 'D'
            else: period_key = 'W'
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(logic.process_stock_multi_worker, row.Symbol, row.Name, [sched['strategy']], period_key): row for row in targets.itertuples()}
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        if res: results.append(res)
                    except Exception as e:
                        print(f"종목 분석 중 에러: {e}")
            
            if results:
                msg = logic.format_tg_message(results, [sched['strategy']], sched['target'])
                logic.send_telegram_message(token, chat_id, msg)
                print(f"성공: {len(results)}개 종목 전송됨")
            else:
                msg = f"🔍 *[{sched['strategy']}]* 스캔 결과\n\n현재 조건에 만족하는 종목이 없습니다. ({sched['target']})"
                logic.send_telegram_message(token, chat_id, msg)
                print("결과 없음: 알림 전송 완료")
        else:
            print(f"건너뜀: {sched['strategy']} ({sched_time}) - 현재 시간과 일치하지 않음")

if __name__ == "__main__":
    run_batch()
