import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [0] 보안 설정 (Secret 로드) ---
# 모든 민감 정보는 logic.get_secret을 통해 Streamlit/GH Secrets에서 가져옴
GH_TOKEN = logic.get_secret("GH_TOKEN", "")
GH_REPO = logic.get_secret("GH_REPO", "ydpapazzang/stock-screener999")
TG_TOKEN = logic.get_secret("TELEGRAM_TOKEN", "")
TG_CHAT_ID = logic.get_secret("TELEGRAM_CHAT_ID", "")
ACCESS_PW = logic.get_secret("ACCESS_PASSWORD", "1234")

# --- [1] 기본 설정 및 로그인 ---
st.set_page_config(page_title="Pro Strategic Screener", layout="wide", page_icon="⚡")
config = logic.load_config()

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    pw_input = st.text_input("비밀번호", type="password")
    if st.button("접속") or pw_input:
        if pw_input == ACCESS_PW:
            st.session_state["authenticated"] = True
            st.rerun()
        else: st.error("비밀번호 불일치")
    st.stop()

# --- [2] 메인 UI (탭 구성) ---
tabs = st.tabs(["🚀 전략 스캔", "📅 알림 설정", "🛠️ 전략 가이드", "⚙️ 시스템"])

with tabs[0]:
    st.title("⚡ 지능형 다중 전략 스캐너")
    with st.sidebar:
        st.header("🎯 스캔 설정")
        category = st.selectbox("분석 단위", ["월봉 전략", "주봉 전략", "일봉 전략"])
        
        if "월봉" in category:
            strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
            period = 'M'
        elif "주봉" in category:
            strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파"]
            period = 'W'
        else:
            strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수"]
            period = 'D'
            
        sel_strats = st.multiselect("전략 선택", strats, default=[strats[0]])
        target = st.radio("대상", ["주식", "ETF"])
        
        min_cap = 0; min_amt = 0
        if target == "주식":
            min_cap = st.slider("최소 시총 (억)", 0, 5000, 500, 100)
            min_amt = st.slider("최소 거래대금 (억)", 0, 500, 50, 10)
        
        limit = st.slider("최대 분석 수", 10, 500, 100)

    # 스캔 로직
    if st.button("🚀 스캔 시작", use_container_width=True):
        df_list = logic.get_listing_data(target)
        if not df_list.empty:
            # 필터링
            if target == "주식":
                df_list = df_list[(df_list.get('시총(억)', 0) >= min_cap) & (df_list.get('거래대금(억)', 0) >= min_amt)]
            
            targets = df_list.head(limit)
            results = []
            p_bar = st.progress(0)
            with ThreadPoolExecutor(max_workers=15) as exe:
                futures = {exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, sel_strats, period): r for r in targets.itertuples()}
                for i, f in enumerate(as_completed(futures)):
                    res = f.result()
                    if res: results.append(res)
                    p_bar.progress((i+1)/len(targets))
            
            if results:
                df_res = pd.DataFrame(results).sort_values(by=["점수", "승률"], ascending=False)
                st.session_state['last_results'] = df_res
                st.success(f"{len(results)}개 종목 포착!")
            else: st.warning("포착된 종목이 없습니다.")

    if 'last_results' in st.session_state:
        df_res = st.session_state['last_results']
        st.dataframe(df_res, use_container_width=True)
        sel_name = st.selectbox("차트 보기", df_res['종목명'].tolist())
        if sel_name:
            code = df_res[df_res['종목명']==sel_name]['코드'].values[0]
            df_chart = logic.get_processed_data(code, period)
            if df_chart is not None:
                st.plotly_chart(logic.create_advanced_chart(df_chart, sel_name, sel_strats))

with tabs[1]:
    st.title("📅 자동 알림 스케줄")
    import uuid
    with st.expander("➕ 새 알림 추가"):
        col1, col2 = st.columns(2)
        f = col1.selectbox("주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        t = col2.time_input("시간", datetime.strptime("09:00", "%H:%M").time())
        s = st.selectbox("전략", ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "주봉 5/20 골든크로스", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수"])
        if st.button("💾 알림 저장"):
            new_s = {"id": str(uuid.uuid4())[:8], "freq": f, "time": t.strftime("%H:%M"), "strategy": s, "target": "주식", "limit": 100}
            config['schedules'].append(new_s)
            logic.save_config(config)
            logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
            st.success("저장 및 동기화 완료!")
            st.rerun()

    for i, s in enumerate(config.get('schedules', [])):
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.write(f"### 📡 {s['freq']} {s['time']} | {s['strategy']}")
            if c2.button("📡 발송", key=f"t_{s['id']}"):
                with st.spinner("발송 중..."):
                    # 요약 분석 및 전송 로직 (중복 제거를 위해 logic 함수 활용 권장)
                    df_l = logic.get_listing_data("주식").head(100)
                    res = []
                    with ThreadPoolExecutor(max_workers=10) as exe:
                        futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [s['strategy']], 'W') for r in df_l.itertuples()]
                        for f in as_completed(futures):
                            if f.result(): res.append(f.result())
                    if logic.send_telegram_all(TG_TOKEN, TG_CHAT_ID, res, [s['strategy']], "주식"):
                        st.success("발송 성공!")
            if c3.button("🗑️ 삭제", key=f"d_{s['id']}"):
                config['schedules'].pop(i)
                logic.save_config(config)
                logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
                st.rerun()

with tabs[2]:
    st.title("🛠️ 전략 가이드")
    s_list = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수"]
    sel = st.selectbox("전략 선택", s_list)
    st.info(logic.get_strategy_desc(sel))

with tabs[3]:
    st.title("⚙️ 시스템 정보")
    st.write(f"**연동 저장소:** `{GH_REPO}`")
    st.write(f"**텔레그램 상태:** `{'연결됨' if TG_TOKEN else '미설정'}`")
    
    if st.button("🚀 GitHub 강제 동기화"):
        if logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)):
            st.success("동기화 성공!")
            
    st.divider()
    st.subheader("📜 실행 이력")
    if config.get('history'):
        st.table(pd.DataFrame(config['history']).head(10))
    else: st.write("이력이 없습니다.")
