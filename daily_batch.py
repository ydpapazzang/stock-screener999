import logic
import os
import json
from datetime import datetime, timedelta
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_batch():
    config = logic.load_config()
    
    # GitHub Secrets 우선, 없으면 config.json에서 로드
    tg_token = os.environ.get("TELEGRAM_TOKEN") or config.get("tg_token")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID") or config.get("tg_chat_id")
    gh_token = os.environ.get("GH_TOKEN")
    gh_repo = os.environ.get("GH_REPO")
    
    if not tg_token or not tg_chat_id:
        print("Telegram credentials missing. Batch aborted.")
        return

    # 시간 체크 (KST 기준)
    now_kst = datetime.utcnow() + timedelta(hours=9)
    today_str = now_kst.strftime("%Y-%m-%d")
    curr_h, curr_m = now_kst.hour, now_kst.minute
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

    print(f"--- Batch Job Started (KST: {now_kst.strftime('%Y-%m-%d %H:%M')}) ---")

    executed_any = False
    new_logs = []
    custom_strats = {s['name']: s for s in config.get('custom_strategies', [])}

    for s in config.get("schedules", []):
        try:
            # 알림 시간 설정 읽기 (기본값 06:00)
            s_h, s_m = map(int, s.get('time', '06:00').split(':'))
        except: 
            s_h, s_m = 6, 0
        
        # 60분 윈도우 체크 (GitHub Action 지연 고려)
        time_match = (curr_h == s_h) and (curr_m >= s_m) and (curr_m < s_m + 60)
        
        # 중복 실행 방지
        already_run = False
        for h in config.get("history", []):
            if h.get("time", "").startswith(today_str) and h.get("strategy") == s['strategy'] and h.get("status") == "Success":
                already_run = True
                break

        if is_manual or (time_match and not already_run):
            strat_name = s['strategy']
            target_market = s.get('target', 'KOSPI/KOSDAQ') # 저장된 시장 정보 사용
            
            # 전략 기반 주기(Period) 결정
            s_period = 'D'
            if strat_name in custom_strats:
                tf = custom_strats[strat_name].get('timeframe', '일봉')
                s_period = 'M' if tf == "월봉" else ('W' if tf == "주봉" else 'D')
            
            print(f"Executing: {strat_name} on {target_market} (Period: {s_period})")
            
            # 종목 리스트 로드 (상위 100개)
            df_l = logic.get_listing_data(target_market).head(100)
            if df_l.empty:
                print(f"Listing empty for {target_market}")
                continue
                
            results = []
            # 병렬 분석
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(logic.process_stock_multi_worker, getattr(r,'Symbol',getattr(r,'Index','')), getattr(r,'Name',''), [strat_name], s_period): r for r in df_l.itertuples()}
                for f in as_completed(futures):
                    res = f.result()
                    if res:
                        results.append(res)
            
            # 텍스트 요약 메시지만 발송 (그래프 제외 요청 반영)
            logic.send_telegram_all(tg_token, tg_chat_id, results, [strat_name], target_market)
            print(f"Sent summary for {strat_name} on {target_market}. Total: {len(results)}")

            new_logs.append({
                "time": now_kst.strftime("%Y-%m-%d %H:%M"), 
                "strategy": strat_name, 
                "target": target_market,
                "count": len(results), 
                "status": "Success"
            })
            executed_any = True

    if executed_any:
        # 이력 저장 및 GitHub 동기화
        config["history"] = (new_logs + config.get("history", []))[:20]
        logic.save_config(config)
        if gh_token and gh_repo:
            logic.update_config_to_github(gh_token, gh_repo, json.dumps(config, indent=4))
        print("Batch complete. History updated.")
    else:
        print("No schedules due for execution.")

if __name__ == "__main__":
    run_batch()
