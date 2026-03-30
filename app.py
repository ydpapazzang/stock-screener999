import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [1] 기본 설정 ---
st.set_page_config(page_title="High-Speed Strategic Screener", layout="wide", page_icon="⚡")
config = logic.load_config()

# --- [2] 상단 탭 구성 ---
tab1, tab2 = st.tabs(["🚀 초고속 스캔 & 백테스트", "📅 자동 알림 설정"])

# --- [탭 1: 스캔 및 백테스트] ---
with tab1:
    st.title("⚡ 병렬 스케닝 & 전수 성과 분석")
    
    with st.sidebar:
        st.header("🎯 전략 및 대상")
        category = st.selectbox("분석 단위 (백테스트 기준)", ["월봉 전략 (6개월 보유)", "주봉 전략 (4주 보유)"])
        
        if "월봉" in category:
            strategies = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파"]
            period_key = 'M'
        else:
            strategies = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치"]
            period_key = 'W'
            
        selected_strategy = st.selectbox("사용할 매매 전략", strategies)
        
        # 전략 설명박스
        info_map = {
            "주봉 5/20 골든크로스": "5주 이평선이 20주 이평선을 뚫고 올라올 때 매수하는 중기 추세 전환 전략입니다.",
            "주봉 RSI 과매도 탈출": "RSI가 30 미만에서 30 위로 복귀할 때 매수하는 낙폭과대 반등 전략입니다.",
            "주봉 볼린저 하단 터치": "주가가 밴드 하단에 닿았을 때 기술적 반등을 노리는 전략입니다.",
            "정석 정배열 (추세추종)": "월봉 기준 완벽한 상승 추세를 타는 종목을 선정합니다.",
            "월봉 MA12 돌파": "주가가 1년 평균선인 12월 이평선을 상향 돌파하는 시점을 포착합니다.",
            "20월선 눌림목 (조정매수)": "장기 추세는 우상향이나 현재가가 20월선까지 내려온 지점을 공략합니다.",
            "거래량 폭발 (세력개입)": "월봉 거래량이 급증하며 시세 분출이 시작되는 종목을 찾습니다.",
            "대시세 초입 (20선 돌파)": "역배열을 끝내고 20월선을 돌파하는 대시세의 시작점을 찾습니다."
        }
        st.info(info_map.get(selected_strategy, "선택한 전략의 조건을 분석합니다."))
        
        target_type = st.radio("분석 대상", ["주식 (KOSPI 200)", "ETF"])
        max_scan = 200 if "주식" in target_type else 1000
        scan_limit = st.slider("스캔 종목 수", 10, max_scan, 100)

        st.divider()
        st.header("⚙️ 성능 최적화")
        num_workers = st.slider("병렬 작업자 수 (Workers)", 1, 20, 10, help="수가 높을수록 빠르지만 서버 차단 위험이 있습니다. 10~15를 추천합니다.")

        st.divider()
        st.header("📲 텔레그램 설정")
        tg_token = st.text_input("Bot Token", value=config.get("tg_token", ""), type="password")
        tg_chat_id = st.text_input("Chat ID", value=config.get("tg_chat_id", ""))
        if st.button("💾 설정 저장"):
            config.update({"tg_token": tg_token, "tg_chat_id": tg_chat_id})
            logic.save_config(config)
            st.success("저장 완료")

    # 스캔 로직 (병렬 처리 구현)
    target_key = "KOSPI" if "주식" in target_type else "ETF"
    df_list = logic.get_listing_data(target_key)

    if st.button(f"🚀 '{selected_strategy}' 초고속 병렬 스캔 시작", use_container_width=True):
        if df_list.empty:
            st.error("데이터 로드 실패")
        else:
            results = []
            p_bar = st.progress(0)
            status_text = st.empty()
            targets = df_list.iloc[:scan_limit]
            total_count = len(targets)
            
            start_time = time.time()
            
            # 멀티스레딩 엔진 가동
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                # 작업 제출
                future_to_stock = {
                    executor.submit(logic.process_stock_worker, row.Symbol, row.Name, selected_strategy, period_key): row 
                    for row in targets.itertuples()
                }
                
                completed = 0
                for future in as_completed(future_to_stock):
                    completed += 1
                    try:
                        res = future.result()
                        if res:
                            results.append(res)
                    except Exception as e:
                        pass # 개별 종목 오류는 무시
                    
                    # 진행률 실시간 업데이트
                    p_bar.progress(completed / total_count)
                    status_text.text(f"처리 중: {completed}/{total_count} 종목 완료")
            
            status_text.empty(); p_bar.empty()
            duration = time.time() - start_time
            st.success(f"✅ 분석 완료! 총 {total_count}개 종목 분석에 {duration:.1f}초 소요 (평균 {duration/total_count:.2f}초/종목)")
            
            if results:
                df_res = pd.DataFrame(results).sort_values(by="승률", ascending=False)
                st.subheader(f"🎯 전략 포착 종목 ({len(results)}개)")
                st.dataframe(df_res, use_container_width=True)
                
                # 결과 요약
                st.divider()
                st.subheader("📊 포착 종목 통계")
                c1, c2, c3 = st.columns(3)
                c1.metric("평균 승률", f"{df_res['승률'].mean():.1f}%")
                c2.metric("최고 수익률", df_res['평균수익'].max())
                c3.metric("최다 신호 종목", df_res.loc[df_res['신호수'].idxmax(), '종목명'])

                if st.button("📤 이 분석 결과를 텔레그램으로 전송"):
                    msg = logic.format_tg_message(results, selected_strategy, target_type)
                    if logic.send_telegram_message(tg_token, tg_chat_id, msg):
                        st.success("발송 성공!")
            else:
                st.info("현재 조건을 만족하는 종목이 없습니다.")

# --- [탭 2: 자동 알림 설정] ---
with tab2:
    st.title("📅 자동 알림 스케줄 관리")
    import uuid
    with st.expander("➕ 새 자동 알림 추가", expanded=False):
        c1, c2 = st.columns(2)
        new_freq = c1.selectbox("실행 주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        new_time = c2.time_input("실행 시간", value=datetime.strptime("09:00", "%H:%M").time())
        all_strategies = [
            "정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", 
            "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "주봉 5/20 골든크로스", 
            "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치"
        ]
        new_strat = st.selectbox("적용 전략", all_strategies)
        new_target = st.radio("대상", ["주식 (KOSPI 200)", "ETF"], key="sched_target")
        new_limit = st.number_input("스캔 종목 수", 10, 1000, 100)
        if st.button("🔔 알림 리스트에 추가"):
            new_schedule = {"id": str(uuid.uuid4())[:8], "freq": new_freq, "time": new_time.strftime("%H:%M"), "strategy": new_strat, "target": new_target, "limit": new_limit}
            if 'schedules' not in config: config['schedules'] = []
            config['schedules'].append(new_schedule)
            logic.save_config(config)
            st.success("알림이 등록되었습니다!")
            st.rerun()

    schedules = config.get("schedules", [])
    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1: 
                st.markdown(f"### 📡 {sched['freq']} {sched['time']}")
                st.markdown(f"**전략:** `{sched['strategy']}` | **대상:** `{sched['target']}`")
            
            with col2:
                if st.button("🔔 테스트", key=f"test_{sched['id']}"):
                    test_msg = f"✅ *[알람 테스트]*\n설정: {sched['freq']} {sched['time']}\n전략: {sched['strategy']}\n대상: {sched['target']}\n\n연결이 정상입니다!"
                    if logic.send_telegram_message(config.get("tg_token"), config.get("tg_chat_id"), test_msg):
                        st.toast("테스트 메시지 발송 완료!")
                    else:
                        st.error("발송 실패! 토큰/ID를 확인하세요.")

            with col3:
                if st.button("🗑️ 삭제", key=f"del_{sched['id']}", type="primary"):
                    config['schedules'].pop(idx); logic.save_config(config); st.rerun()
