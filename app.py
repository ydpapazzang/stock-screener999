import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [1] 기본 설정 ---
st.set_page_config(page_title="High-Speed Strategic Screener", layout="wide", page_icon="⚡")
config = logic.load_config()

# 초기 비밀번호 설정 (없을 경우)
if "password" not in config or not config["password"]:
    config["password"] = "1234" # 초기 비번
    logic.save_config(config)

# --- [로그인 시스템] ---
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def check_password():
    if st.session_state["password_input"] == config["password"]:
        st.session_state["authenticated"] = True
        del st.session_state["password_input"] # 보안을 위해 입력값 삭제
    else:
        st.error("❌ 비밀번호가 틀렸습니다.")

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    st.markdown("본 서비스는 인증된 사용자만 이용 가능합니다.")
    st.text_input("접속 비밀번호를 입력하세요", type="password", key="password_input", on_change=check_password)
    st.stop() # 인증 전까지 아래 코드 실행 중단

# --- [2] 메인 UI (인증 성공 시) ---
tab1, tab2, tab3 = st.tabs(["🚀 초고속 스캔 & 백테스트", "📅 자동 알림 설정", "⚙️ 시스템 설정"])

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
        
        target_type = st.radio("분석 대상", ["주식 (KOSPI 200)", "ETF"])
        max_scan = 200 if "주식" in target_type else 1000
        scan_limit = st.slider("스캔 종목 수", 10, max_scan, 100)

        st.divider()
        st.header("📲 텔레그램 알림 설정")
        has_tg = config.get("tg_token") and config.get("tg_chat_id")
        if has_tg:
            st.success("✅ 텔레그램 활성화")
            if st.button("🗑️ 텔레그램 설정 초기화"):
                config.update({"tg_token": "", "tg_chat_id": ""})
                logic.save_config(config); st.rerun()
        else:
            new_token = st.text_input("Bot Token", type="password")
            new_chat_id = st.text_input("Chat ID")
            if st.button("💾 저장"):
                config.update({"tg_token": new_token, "tg_chat_id": new_chat_id})
                logic.save_config(config); st.rerun()

    # 스캔 로직
    target_key = "KOSPI" if "주식" in target_type else "ETF"
    df_list = logic.get_listing_data(target_key)

    if st.button(f"🚀 '{selected_strategy}' 스캔 시작", use_container_width=True):
        if df_list.empty:
            st.error("데이터 로드 실패")
        else:
            results = []
            p_bar = st.progress(0); status_text = st.empty()
            targets = df_list.iloc[:scan_limit]
            
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(logic.process_stock_worker, r.Symbol, r.Name, selected_strategy, period_key): r for r in targets.itertuples()}
                for i, future in enumerate(as_completed(futures)):
                    res = future.result()
                    if res: results.append(res)
                    p_bar.progress((i + 1) / len(targets))
            
            status_text.empty(); p_bar.empty()
            st.session_state['scan_results'] = results
            st.session_state['period_key'] = period_key
            st.success(f"✅ 완료! {len(results)}개 포착 ({time.time()-start_time:.1f}초)")

    if st.session_state.get('scan_results'):
        df_res = pd.DataFrame(st.session_state['scan_results']).sort_values(by="승률", ascending=False)
        st.dataframe(df_res, use_container_width=True)
        st.divider()
        st.subheader("📊 종목 상세 차트 분석")
        selected_name = st.selectbox("차트를 볼 종목을 선택하세요", df_res['종목명'].tolist())
        if selected_name:
            selected_code = df_res[df_res['종목명'] == selected_name]['코드'].values[0]
            with st.spinner("차트 생성 중..."):
                df_chart = logic.get_processed_data(selected_code, st.session_state['period_key'])
                if df_chart is not None:
                    fig = logic.create_interactive_chart(df_chart, selected_name, selected_strategy)
                    st.plotly_chart(fig, use_container_width=True)

# --- [탭 2: 자동 알림 설정] ---
with tab2:
    st.title("📅 자동 알림 스케줄 관리")
    import uuid
    with st.expander("➕ 새 자동 알림 추가"):
        new_freq = st.selectbox("실행 주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        new_time = st.time_input("실행 시간", value=datetime.strptime("09:00", "%H:%M").time())
        all_strat = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치"]
        new_strat = st.selectbox("전략", all_strat)
        new_target = st.radio("대상", ["주식 (KOSPI 200)", "ETF"], key="sched_target")
        if st.button("🔔 알림 등록"):
            new_schedule = {"id": str(uuid.uuid4())[:8], "freq": new_freq, "time": new_time.strftime("%H:%M"), "strategy": new_strat, "target": new_target, "limit": 100}
            if 'schedules' not in config: config['schedules'] = []
            config['schedules'].append(new_schedule)
            logic.save_config(config); st.success("등록됨!"); st.rerun()

    schedules = config.get("schedules", [])
    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            col1, col2 = st.columns([4, 1])
            col1.markdown(f"### 📡 {sched['freq']} {sched['time']} | {sched['strategy']}")
            if col2.button("🗑️ 삭제", key=f"del_{sched['id']}"):
                config['schedules'].pop(idx); logic.save_config(config); st.rerun()

# --- [탭 3: 시스템 설정] ---
with tab3:
    st.title("⚙️ 시스템 설정")
    st.subheader("🔑 비밀번호 변경")
    current_pw = st.text_input("현재 비밀번호", type="password")
    new_pw = st.text_input("새 비밀번호", type="password")
    confirm_pw = st.text_input("새 비밀번호 확인", type="password")
    
    if st.button("비밀번호 변경"):
        if current_pw != config["password"]:
            st.error("현재 비밀번호가 일치하지 않습니다.")
        elif new_pw != confirm_pw:
            st.error("새 비밀번호가 서로 일치하지 않습니다.")
        elif not new_pw:
            st.error("새 비밀번호를 입력해 주세요.")
        else:
            config["password"] = new_pw
            logic.save_config(config)
            st.success("✅ 비밀번호가 변경되었습니다. 다음 접속부터 적용됩니다.")

    st.divider()
    if st.button("🔓 로그아웃"):
        st.session_state["authenticated"] = False
        st.rerun()
