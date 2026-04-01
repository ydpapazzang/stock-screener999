import logic
import os
import json
from datetime import datetime, timedelta
import calendar

def run_batch():
    config = logic.load_config()
    tg_token = os.environ.get("TELEGRAM_TOKEN") or config.get("tg_token")
    tg_chat_id = os.environ.get("TELEGRAM_CHAT_ID") or config.get("tg_chat_id")
    gh_token = os.environ.get("GH_TOKEN")
    gh_repo = os.environ.get("GH_REPO")
    
    if not tg_token or not tg_chat_id: return

    now_kst = datetime.utcnow() + timedelta(hours=9)
    today_str = now_kst.strftime("%Y-%m-%d")
    curr_h, curr_m = now_kst.hour, now_kst.minute
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

    executed_any = False
    new_logs = []
    custom_strats = {s['name']: s for s in config.get('custom_strategies', [])}

    for s in config.get("schedules", []):
        try: s_h, s_m = map(int, s['time'].split(':'))
        except: s_h, s_m = 6, 0
        
        time_match = (curr_h == s_h) and (curr_m >= s_m) and (curr_m < s_m + 60)
        
        already_run = False
        for h in config.get("history", []):
            if h.get("time", "").startswith(today_str) and h.get("strategy") == s['strategy']:
                already_run = True; break

        if is_manual or (time_match and not already_run):
            strat_name = s['strategy']
            target_type = s.get('target', 'KOSPI/KOSDAQ')
            
            if strat_name in custom_strats:
                tf = custom_strats[strat_name].get('timeframe', '일봉')
                period = 'M' if tf == "월봉" else ('W' if tf == "주봉" else 'D')
            else: period = 'D'
            
            df_l = logic.get_listing_data(target_type).head(100)
            results = []
            for r in df_l.itertuples():
                s_code = getattr(r, 'Symbol', getattr(r, 'Index', ''))
                s_name = getattr(r, 'Name', s_code)
                res = logic.process_stock_multi_worker(s_code, s_name, [strat_name], period)
                if res:
                    results.append(res)
                    # 포착 시 차트와 함께 즉시 발송
                    df_data = logic.get_processed_data(s_code, period)
                    if df_data is not None:
                        logic.send_telegram_with_chart(tg_token, tg_chat_id, s_code, s_name, df_data, [strat_name])
            
            if not results:
                logic.send_telegram_all(tg_token, tg_chat_id, [], [strat_name], target_type)

            new_logs.append({"time": now_kst.strftime("%Y-%m-%d %H:%M"), "strategy": strat_name, "count": len(results), "status": "Success"})
            executed_any = True

    if executed_any:
        config["history"] = (new_logs + config.get("history", []))[:20]
        logic.save_config(config)
        if gh_token and gh_repo:
            logic.update_config_to_github(gh_token, gh_repo, json.dumps(config, indent=4))

if __name__ == "__main__":
    run_batch()
