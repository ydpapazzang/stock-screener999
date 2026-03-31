import logic
import os
import pandas as pd
from datetime import datetime, timedelta
import calendar
import json

def run_batch():
    # 1. 설정 로드
    config = logic.load_config()
    schedules = config.get("schedules", [])
    
    token = config.get("tg_token") or os.environ.get("TELEGRAM_TOKEN")
    chat_id = config.get("tg_chat_id") or os.environ.get("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("Error: 텔레그램 설정이 없습니다.")
        return

    # 3. 현재 시간(KST) 정보
    now_utc = datetime.utcnow()
    now_kst = now_utc + timedelta(hours=9)
    is_monday = now_kst.weekday() == 0
    is_first_day = now_kst.day == 1
    last_day = calendar.monthrange(now_kst.year, now_kst.month)[1]
    is_last_day = now_kst.day == last_day
    
    current_hour = now_kst.hour
    current_min = now_kst.minute
    
    print(f"--- 배치 스캔 시작 (KST: {now_kst.strftime('%Y-%m-%d %H:%M')}) ---")

    execution_logs = []

    for sched in schedules:
        freq = sched.get("freq")
        sched_time = sched.get("time", "09:00")
        try:
            s_hour, s_min = map(int, sched_time.split(":"))
        except:
            s_hour, s_min = 9, 0
        
        # 15분 윈도우 체크
        time_match = (current_hour == s_hour) and (current_min >= s_min) and (current_min < s_min + 15)
        
        should_run = time_match and (
            (freq == "매일") or \
            (freq == "매주 (월요일)" and is_monday) or \
            (freq == "매월 (1일)" and is_first_day) or \
            (freq == "매월 (말일)" and is_last_day)
        )
        
        if should_run:
            print(f"실행 조건 충족: {sched['strategy']} ({sched_time})")
            target_key = "KOSPI" if "주식" in sched['target'] else "ETF"
            df_list = logic.get_listing_data(target_key)
            
            results = []
            limit = int(sched.get('limit', 100))
            targets = df_list.iloc[:limit]
            
            m_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
            d_strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수"]
            
            p_key = 'M' if sched['strategy'] in m_strats else ('D' if sched['strategy'] in d_strats else 'W')
            
            from concurrent.futures import ThreadPoolExecutor, as_completed
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(logic.process_stock_multi_worker, row.Symbol, row.Name, [sched['strategy']], p_key): row for row in targets.itertuples()}
                for future in as_completed(futures):
                    try:
                        res = future.result()
                        if res: results.append(res)
                    except: pass
            
            # 메시지 발송
            if results:
                msg = logic.format_tg_message(results, [sched['strategy']], sched['target'])
                logic.send_telegram_message(token, chat_id, msg)
                
                # 상위 3개 종목 차트 발송
                sorted_res = sorted(results, key=lambda x: x.get('승률', 0), reverse=True)
                for item in sorted_res[:3]:
                    df_chart = logic.get_processed_data(item['코드'], p_key)
                    if df_chart is not None:
                        fname = f"{item['코드']}_chart.png"
                        if logic.save_chart_image(df_chart, item['종목명'], fname):
                            logic.send_telegram_photo(token, chat_id, fname, caption=f"📊 *{item['종목명']}* 분석 차트")
                            if os.path.exists(fname): os.remove(fname)
            else:
                msg = f"🔍 *[{sched['strategy']}]* 스캔 결과\n\n현재 조건에 만족하는 종목이 없습니다. ({sched['target']})"
                logic.send_telegram_message(token, chat_id, msg)

            # 로그 기록
            log_entry = {
                "time": now_kst.strftime("%Y-%m-%d %H:%M"),
                "strategy": sched['strategy'],
                "target": sched['target'],
                "count": len(results),
                "status": "Success"
            }
            execution_logs.append(log_entry)

    # 4. 실행 이력을 config.json에 업데이트하여 GitHub로 푸시
    if execution_logs:
        if "history" not in config: config["history"] = []
        config["history"] = (execution_logs + config["history"])[:10] # 최근 10개만 유지
        logic.save_config(config)
        
        # GitHub API 동기화 (PAT 및 REPO 정보는 환경변수에서 가져옴)
        gh_token = os.environ.get("GH_TOKEN")
        gh_repo = os.environ.get("GH_REPO")
        if gh_token and gh_repo:
            logic.update_config_to_github(gh_token, gh_repo, "config.json", "Update execution history", json.dumps(config, ensure_ascii=False, indent=4))

if __name__ == "__main__":
    run_batch()
