import logic
import os
import json
from datetime import datetime, timedelta
import calendar

def run_batch():
    # --- [1] 설정 및 Secret 로드 ---
    config = logic.load_config()
    
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

    executed_any = False
    new_logs = []

    # 전략 주기 매핑
    m_strats = ["20월선 눌림목 (조정매수)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
    w_strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착"]
    
    custom_strats = {s['name']: s for s in config.get('custom_strategies', [])}

    for s in config.get("schedules", []):
        try:
            s_h, s_m = map(int, s['time'].split(':'))
        except: s_h, s_m = 6, 0
        
        time_match = (curr_h == s_h) and (curr_m >= s_m) and (curr_m < s_m + 60)
        
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
            
            # 주기 결정 로직 (커스텀 전략 우선 확인)
            if strat_name in custom_strats:
                tf = custom_strats[strat_name].get('timeframe', '일봉')
                period = 'M' if tf == "월봉" else ('W' if tf == "주봉" else 'D')
            elif strat_name in m_strats: period = 'M'
            elif strat_name in w_strats: period = 'W'
            else: period = 'D'
            
            print(f"Executing: {strat_name} ({target_type}) | Period: {period}")
            
            df_l = logic.get_listing_data(target_type)
            if df_l.empty: continue
            
            if "주식" in target_type and '시총(억)' in df_l.columns:
                df_l = df_l.sort_values(by='시총(억)', ascending=False)
            
            targets = df_l.head(scan_limit)
            results = []
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [strat_name], period) for r in targets.itertuples()]
                for f in as_completed(futures):
                    try:
                        res = f.result()
                        if res: results.append(res)
                    except: pass
            
            logic.send_telegram_all(tg_token, tg_chat_id, results, [strat_name], target_type)
            
            new_logs.append({
                "time": now_kst.strftime("%Y-%m-%d %H:%M"),
                "strategy": strat_name,
                "target": target_type,
                "count": len(results),
                "status": "Success"
            })
            executed_any = True

    if executed_any:
        config["history"] = (new_logs + config.get("history", []))[:20]
        logic.save_config(config)
        if gh_token and gh_repo:
            logic.update_config_to_github(gh_token, gh_repo, json.dumps(config, indent=4))
        print("Batch complete.")

if __name__ == "__main__":
    run_batch()
