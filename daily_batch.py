import logic
import os
import json
from datetime import datetime, timedelta
import calendar

def run_batch():
    # --- [1] 설정 및 Secret 로드 ---
    config = logic.load_config()
    
    # GitHub Actions 환경 변수에서 Secret 로드 (없으면 config에서 시도)
    tg_token = os.environ.get("TELEGRAM_TOKEN") or config.get("tg_token")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID") or config.get("tg_chat_id")
    gh_token = os.environ.get("GH_TOKEN")
    gh_repo = os.environ.get("GH_REPO")
    
    if not tg_token or not tg_chat_id:
        print("Error: Telegram credentials missing.")
        return

    # --- [2] 시간 체크 ---
    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)
    today_str = now_kst.strftime("%Y-%m-%d")
    
    is_monday = now_kst.weekday() == 0
    is_first_day = now_kst.day == 1
    last_day = calendar.monthrange(now_kst.year, now_kst.month)[1]
    is_last_day = now_kst.day == last_day
    
    curr_h, curr_m = now_kst.hour, now_kst.minute
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

    print(f"--- Batch Start (KST: {now_kst.strftime('%Y-%m-%d %H:%M')}) ---")
    print(f"Schedules found: {len(config.get('schedules', []))}")

    executed_any = False
    new_logs = []

    # 전략별 권장 주기 매핑
    m_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
    w_strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파"]

    for s in config.get("schedules", []):
        try:
            s_h, s_m = map(int, s['time'].split(':'))
        except: s_h, s_m = 9, 0
        
        # 60분 단위 매칭 체크 (GitHub Action 지연 대비)
        time_match = (curr_h == s_h) and (curr_m >= s_m) and (curr_m < s_m + 60)
        
        # 중복 실행 방지 체크 (오늘 이미 실행되었는지 history 확인)
        already_run = False
        for h in config.get("history", []):
            if h.get("time", "").startswith(today_str) and h.get("strategy") == s['strategy'] and h.get("status") == "Success":
                already_run = True
                break

        should_run = is_manual or (time_match and not already_run and (
            (s['freq'] == "매일") or 
            (s['freq'] == "매주 (월요일)" and is_monday) or 
            (s['freq'] == "매월 (1일)" and is_first_day) or 
            (s['freq'] == "매월 (말일)" and is_last_day)
        ))

        if should_run:
            strat_name = s['strategy']
            target_type = s.get('target', '주식')
            scan_limit = s.get('limit', 200)
            
            # 주기 결정
            if strat_name in m_strats: period = 'M'
            elif strat_name in w_strats: period = 'W'
            else: period = 'D'
            
            print(f"Executing Schedule: {strat_name} ({target_type}) | Period: {period} | Limit: {scan_limit}")
            
            df_l = logic.get_listing_data(target_type)
            if df_l.empty:
                print(f"Error: Could not load listing for {target_type}")
                continue
            
            # 시총/거래대금 필터 (주식인 경우만)
            if target_type == "주식":
                if '시총(억)' in df_l.columns:
                    df_l = df_l.sort_values(by='시총(억)', ascending=False)
            
            targets = df_l.head(scan_limit)
            results = []
            
            # 분석 실행 (병렬)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [strat_name], period) for r in targets.itertuples()]
                for f in as_completed(futures):
                    try:
                        res = f.result()
                        if res: results.append(res)
                    except Exception as e:
                        print(f"Error processing stock: {e}")
            
            # 알림 발송
            sent = logic.send_telegram_all(tg_token, tg_chat_id, results, [strat_name], target_type)
            if sent:
                print(f"Telegram sent: {len(results)} items found.")
            else:
                print(f"No results found for {strat_name} or Telegram failed.")
            
            # 로그 생성
            new_logs.append({
                "time": now_kst.strftime("%Y-%m-%d %H:%M"),
                "strategy": strat_name,
                "target": target_type,
                "count": len(results),
                "status": "Success"
            })
            executed_any = True
        else:
            if time_match and already_run:
                print(f"Skipping {s['strategy']}: Already run today.")
            elif not time_match and not is_manual:
                # Debug: print(f"Time not match for {s['strategy']}: {curr_h}:{curr_m} vs {s_h}:{s_m}")
                pass

    # --- [3] 결과 저장 및 동기화 ---
    if executed_any:
        config["history"] = (new_logs + config.get("history", []))[:20]
        logic.save_config(config)
        if gh_token and gh_repo:
            print("Syncing updated history to GitHub...")
            logic.update_config_to_github(gh_token, gh_repo, json.dumps(config, indent=4))
        print("Batch process completed successfully.")
    else:
        print("No schedules were due for execution in this run.")

if __name__ == "__main__":
    run_batch()
