import logic
import os
import json
from datetime import datetime, timedelta
import calendar

def run_batch():
    # --- [1] 설정 및 Secret 로드 ---
    config = logic.load_config()
    
    # GitHub Actions 환경 변수에서 Secret 로드
    tg_token = os.environ.get("TELEGRAM_TOKEN")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    gh_token = os.environ.get("GH_TOKEN")
    gh_repo = os.environ.get("GH_REPO")
    
    if not tg_token or not tg_chat_id:
        print("Error: Telegram credentials missing in environment.")
        return

    # --- [2] 시간 체크 ---
    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)
    is_monday = now_kst.weekday() == 0
    is_first_day = now_kst.day == 1
    last_day = calendar.monthrange(now_kst.year, now_kst.month)[1]
    is_last_day = now_kst.day == last_day
    
    curr_h, curr_m = now_kst.hour, now_kst.minute
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

    print(f"--- Batch Start (KST: {now_kst.strftime('%Y-%m-%d %H:%M')}) ---")

    executed_any = False
    new_logs = []

    for s in config.get("schedules", []):
        try:
            s_h, s_m = map(int, s['time'].split(':'))
        except: s_h, s_m = 9, 0
        
        # 15분 단위 매칭 체크
        time_match = (curr_h == s_h) and (curr_m >= s_m) and (curr_m < s_m + 15)
        
        should_run = is_manual or (time_match and (
            (s['freq'] == "매일") or 
            (s['freq'] == "매주 (월요일)" and is_monday) or 
            (s['freq'] == "매월 (1일)" and is_first_day) or 
            (s['freq'] == "매월 (말일)" and is_last_day)
        ))

        if should_run:
            print(f"Running: {s['strategy']}")
            df_l = logic.get_listing_data("주식").head(100) # 배치 시 100개 요약
            results = []
            
            # 분석 실행 (병렬)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [s['strategy']], 'W') for r in df_l.itertuples()]
                for f in as_completed(futures):
                    if f.result(): results.append(f.result())
            
            # 알림 발송
            logic.send_telegram_all(tg_token, tg_chat_id, results, [s['strategy']], "주식")
            
            # 로그 생성
            new_logs.append({
                "time": now_kst.strftime("%Y-%m-%d %H:%M"),
                "strategy": s['strategy'],
                "count": len(results),
                "status": "Success"
            })
            executed_any = True

    # --- [3] 결과 저장 및 동기화 ---
    if executed_any:
        config["history"] = (new_logs + config.get("history", []))[:10]
        logic.save_config(config)
        if gh_token and gh_repo:
            logic.update_config_to_github(gh_token, gh_repo, json.dumps(config, indent=4))

if __name__ == "__main__":
    run_batch()
