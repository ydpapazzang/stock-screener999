import logic
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import calendar

def run_automated_scan(sched_data):
    """특정 스케줄 데이터에 맞춰 스캔 실행"""
    config = logic.load_config()
    token = config.get("tg_token")
    chat_id = config.get("tg_chat_id")
    
    strategy = sched_data.get("strategy")
    target = sched_data.get("target")
    limit = sched_data.get("limit", 200)
    
    print(f"[{datetime.now()}] 알람 실행 중: {sched_data['freq']} {sched_data['time']} - {strategy} ({target})")
    
    target_key = "KOSPI" if "주식" in target else "ETF"
    df_list = logic.get_listing_data(target_key)
    
    if df_list.empty:
        print(f"[{strategy}] 에러: 종목 리스트 로드 실패")
        return
        
    results = []
    targets = df_list.iloc[:limit]
    
    # 전략에 따른 주기 설정 (월봉/주봉)
    m_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파"]
    period_key = 'M' if strategy in m_strats else 'W'
    
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(logic.process_stock_multi_worker, row.Symbol, row.Name, [strategy], period_key): row for row in targets.itertuples()}
        for future in as_completed(futures):
            res = future.result()
            if res:
                results.append(res)
            
    if results:
        msg = logic.format_tg_message(results, [strategy], target)
        if logic.send_telegram_message(token, chat_id, msg):
            print(f"[{strategy}] 성공: {len(results)}개 종목 발송 완료")
        else:
            print(f"[{strategy}] 실패: 텔레그램 전송 오류")
    else:
        print(f"[{strategy}] 결과 없음: 조건 만족 종목이 없습니다.")

def job_wrapper(sched_data):
    """주기별 날짜 체크 및 실행 위임"""
    freq = sched_data.get("freq")
    now = datetime.now()
    
    # 말일 체크
    if freq == "매월 (말일)":
        last_day = calendar.monthrange(now.year, now.month)[1]
        if now.day != last_day:
            return
            
    run_automated_scan(sched_data)

if __name__ == "__main__":
    scheduler = BlockingScheduler()
    config = logic.load_config()
    
    schedules = config.get("schedules", [])
    
    print(f"--- 다중 알림 스케줄러 시작 (등록된 알람: {len(schedules)}개) ---")
    
    for sched in schedules:
        freq = sched.get("freq")
        time_str = sched.get("time", "09:00")
        hour, minute = map(int, time_str.split(":"))
        
        # 트리거 설정
        if freq == "매일":
            trigger = CronTrigger(hour=hour, minute=minute)
        elif freq == "매주 (월요일)":
            trigger = CronTrigger(day_of_week='mon', hour=hour, minute=minute)
        elif freq == "매월 (1일)":
            trigger = CronTrigger(day=1, hour=hour, minute=minute)
        elif freq == "매월 (말일)":
            trigger = CronTrigger(hour=hour, minute=minute)
        else:
            continue
            
        # 각 스케줄을 개별 작업으로 등록 (args로 데이터 전달)
        scheduler.add_job(job_wrapper, trigger, args=[sched])
        print(f"  > 등록 완료: {freq} {time_str} | 전략: {sched['strategy']}")
        
    if schedules:
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass
    else:
        print("등록된 스케줄이 없습니다. app.py에서 먼저 등록하세요.")
