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
            targets = df_list.iloc[:int(sched.get('limit', 100))]
            
            for row in targets.itertuples():
                period = 'M' if "월봉" in sched['strategy'] or "전배열" in sched['strategy'] or "눌림목" in sched['strategy'] else 'W'
                df_data = logic.get_processed_data(row.Symbol, period)
                if df_data is not None:
                    signals = logic.check_all_signals(df_data, sched['strategy'])
                    if signals.iloc[-1]:
                        win, ret, cnt = logic.fast_backtest(df_data, sched['strategy'], period)
                        results.append({"코드": row.Symbol, "종목명": row.Name, "승률": win, "평균수익": f"{ret}%"})
            
            if results:
                msg = logic.format_tg_message(results, sched['strategy'], sched['target'])
                logic.send_telegram_message(token, chat_id, msg)
                print(f"완료: {len(results)}개 종목 전송됨")

if __name__ == "__main__":
    run_batch()
