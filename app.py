import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [0] 세션 초기화 및 토큰 관리 ---
if "gh_token" not in st.session_state:
    try:
        st.session_state["gh_token"] = st.secrets.get("GH_TOKEN", "")
    except:
        st.session_state["gh_token"] = ""

if "gh_repo" not in st.session_state:
    try:
        st.session_state["gh_repo"] = st.secrets.get("GH_REPO", "ydpapazzang/stock-screener999")
    except:
        st.session_state["gh_repo"] = "ydpapazzang/stock-screener999"

def auto_sync_github():
    """알람 변경 시 자동으로 GitHub에 동기화 시도"""
    token = st.session_state["gh_token"]
    repo = st.session_state["gh_repo"]
    if not token or not repo:
        st.warning("⚠️ 자동 동기화를 위해 [⚙️ 시스템 설정] 탭에서 GitHub 토큰을 먼저 입력해주세요.")
        return False
    try:
        import json
        config_content = json.dumps(config, ensure_ascii=False, indent=4)
        success = logic.update_config_to_github(
            token=token.strip(), repo=repo.strip(), path="config.json",
            message="Auto-sync schedules via Streamlit UI", content=config_content
        )
        if success:
            st.toast("✅ GitHub 자동 동기화 성공!", icon="🚀")
            return True
        else:
            st.error("❌ 자동 동기화 실패. 토큰 권한을 확인하세요.")
            return False
    except Exception as e:
        st.error(f"❌ 동기화 오류: {e}")
        return False

# --- [1] 기본 설정 ---
st.set_page_config(page_title="Pro Strategic Screener", layout="wide", page_icon="⚡")
config = logic.load_config()

# --- [로그인 시스템] ---
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def check_password():
    if st.session_state["password_input"] == config["password"]:
        st.session_state["authenticated"] = True
        del st.session_state["password_input"]
    else:
        st.error("❌ 비밀번호가 틀렸습니다.")

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    st.text_input("접속 비밀번호를 입력하세요", type="password", key="password_input", on_change=check_password)
    st.stop()

# --- [2] 메인 UI ---
tab1, tab2, tab_strat, tab3 = st.tabs(["🚀 복합 전략 스캔 & 차트", "📅 자동 알림 설정", "🛠️ 전략 관리 & 생성", "⚙️ 시스템 설정"])

with tab1:
    st.title("⚡ 다중 필터 스케닝 & 타점 분석")
    with st.sidebar:
        st.header("🎯 전략 조합 설정 (AND 필터)")
        category = st.selectbox("분석 단위", ["월봉 전략 조합", "주봉 전략 조합", "일봉 전략 조합"])
        if "월봉" in category:
            all_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
            period_key = 'M'
        elif "주봉" in category:
            all_strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파"]
            period_key = 'W'
        else:
            all_strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수"]
            period_key = 'D'
        selected_strategies = st.multiselect("사용할 전략들을 선택하세요", all_strats, default=[all_strats[0]])
        if selected_strategies:
            st.divider()
            st.subheader("📝 선택된 전략 설명")
            for s_name in selected_strategies:
                with st.expander(f"🔹 {s_name}", expanded=True):
                    st.write(logic.get_strategy_desc(s_name))
        st.divider()
        target_type = st.radio("분석 대상", ["주식 (KOSPI 200)", "ETF"])
        
        if "주식" in target_type:
            st.subheader("🔍 필터 조건 (주식 전용)")
            min_marcap = st.slider("최소 시가총액 (억원)", 0, 5000, 500, step=100)
            min_amount = st.slider("최소 거래대금 (억원/일)", 0, 500, 50, step=10)
            max_scan = 500
        else:
            min_marcap, min_amount = 0, 0
            max_scan = 1000
            
        scan_limit = st.slider("최대 분석 종목 수", 10, max_scan, 100)
        
        st.divider()
        st.header("📲 텔레그램 설정")
        if config.get("tg_token") and config.get("tg_chat_id"):
            st.success("✅ 텔레그램 활성화")
            if st.button("🗑️ 설정 초기화"):
                config.update({"tg_token": "", "tg_chat_id": ""}); logic.save_config(config); st.rerun()
        else:
            new_token = st.text_input("Bot Token", type="password")
            new_chat_id = st.text_input("Chat ID")
            if st.button("💾 저장"):
                config.update({"tg_token": new_token, "tg_chat_id": new_chat_id}); logic.save_config(config); st.rerun()

    target_key = "KOSPI" if "주식" in target_type else "ETF"
    df_list = logic.get_listing_data(target_key)
    if st.button(f"🚀 {len(selected_strategies)}개 복합 전략 스캔 시작", use_container_width=True):
        if not selected_strategies:
            st.warning("최소 하나 이상의 전략을 선택하세요.")
        elif df_list.empty:
            st.error("데이터 로드 실패")
        else:
            # [필터 적용] 시총 및 거래대금 기준
            if "주식" in target_type:
                df_filtered = df_list.copy()
                if '시총(억)' in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered['시총(억)'] >= min_marcap]
                if '거래대금(억)' in df_filtered.columns:
                    df_filtered = df_filtered[df_filtered['거래대금(억)'] >= min_amount]
                targets = df_filtered.iloc[:scan_limit]
            else:
                targets = df_list.iloc[:scan_limit]
            
            if targets.empty:
                st.warning("필터 조건에 맞는 종목이 없습니다. 조건을 완화해보세요.")
            else:
                results = []
                p_bar = st.progress(0)
                start_time = time.time()
                
                with ThreadPoolExecutor(max_workers=15) as executor:
                    futures = {executor.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, selected_strategies, period_key): r for r in targets.itertuples()}
                    for i, future in enumerate(as_completed(futures)):
                        res = future.result()
                        if res: results.append(res)
                        p_bar.progress((i + 1) / len(targets))
                
                p_bar.empty()
                st.session_state['multi_scan_results'] = results
                st.session_state['multi_period_key'] = period_key
                st.session_state['used_strategies'] = selected_strategies
                st.success(f"✅ 분석 완료! {len(results)}개 종목 포착 ({time.time()-start_time:.1f}초)")

    # 결과 및 차트 섹션
    if st.session_state.get('multi_scan_results'):
        df_res = pd.DataFrame(st.session_state['multi_scan_results'])
        # [정렬] 점수 높은 순 -> 신규감지(Y) -> 승률 순
        sort_cols = []
        if '점수' in df_res.columns: sort_cols.append('점수')
        if '신규감지' in df_res.columns: sort_cols.append('신규감지')
        if '승률' in df_res.columns: sort_cols.append('승률')
        
        df_res = df_res.sort_values(by=sort_cols, ascending=[False] * len(sort_cols))
        st.subheader(f"🎯 전략 적중 종목 (총 {len(df_res)}개)")
        st.caption("💡 점수(Score)는 선택한 전략 중 일치하는 비율입니다. (100점 = 모든 전략 일치)")
        st.dataframe(df_res, use_container_width=True)
        st.divider()
        st.subheader("📊 신호 발생 타점 차트 시각화")
        selected_name = st.selectbox("종목을 선택하면 차트에 타점이 표시됩니다", df_res['종목명'].tolist())
        if selected_name:
            selected_code = df_res[df_res['종목명'] == selected_name]['코드'].values[0]
            with st.spinner("전문 차트 생성 중..."):
                df_chart = logic.get_processed_data(selected_code, st.session_state['multi_period_key'])
                if df_chart is not None:
                    fig = logic.create_advanced_chart(df_chart, selected_name, st.session_state['used_strategies'])
                    st.plotly_chart(fig, use_container_width=True)

# --- 탭 2 자동 알림 설정 ---
with tab2:
    st.title("📅 자동 알림 스케줄 관리")
    import uuid
    with st.expander("➕ 새 자동 알림 추가"):
        c1, c2 = st.columns(2)
        new_freq = c1.selectbox("실행 주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        new_time = c2.time_input("실행 시간", value=datetime.strptime("09:00", "%H:%M").time())
        all_strat_list = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수"]
        new_strat = st.selectbox("전략", all_strat_list)
        new_target = st.radio("대상", ["주식 (KOSPI 200)", "ETF"], key="sched_target")
        if st.button("🔔 알림 등록"):
            new_schedule = {"id": str(uuid.uuid4())[:8], "freq": new_freq, "time": new_time.strftime("%H:%M"), "strategy": new_strat, "target": new_target, "limit": 100}
            if 'schedules' not in config: config['schedules'] = []
            config['schedules'].append(new_schedule); logic.save_config(config); st.success("등록됨!"); auto_sync_github(); st.rerun()

    schedules = config.get("schedules", [])
    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            col1, col2, col3 = st.columns([4, 1, 1])
            target_info = sched.get('target', '정보 없음')
            col1.markdown(f"### 📡 {sched['freq']} {sched['time']} | {sched['strategy']}")
            col1.caption(f"🎯 대상: {target_info} | 스캔 제한: {sched.get('limit', 100)}개")
            if col2.button("📡 발송", key=f"test_{sched['id']}"):
                with st.spinner("발송 중..."):
                    t_key = "KOSPI" if "주식" in sched['target'] else "ETF"
                    df_l = logic.get_listing_data(t_key)
                    res_list = []; t_limit = int(sched.get('limit', 100))
                    m_s = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
                    d_s = ["5일 연속 상승세", "외인/기관 쌍끌이 매수"]
                    p_key = 'M' if sched['strategy'] in m_s else ('D' if sched['strategy'] in d_s else 'W')
                    with ThreadPoolExecutor(max_workers=10) as exec:
                        futures = {exec.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [sched['strategy']], p_key): r for r in df_l.iloc[:t_limit].itertuples()}
                        for f in as_completed(futures):
                            r = f.result(); 
                            if r: res_list.append(r)
                    if config.get("tg_token") and config.get("tg_chat_id"):
                        msg_text = logic.format_tg_message(res_list, [sched['strategy']], sched['target']) if res_list else f"🔍 *[{sched['strategy']}]* 결과 없음"
                        
                        # 텍스트 메시지 먼저 발송
                        if logic.send_telegram_message(config["tg_token"], config["tg_chat_id"], msg_text): 
                            # 차트 이미지 발송 (결과가 있을 때만)
                            if res_list:
                                sorted_res = sorted(res_list, key=lambda x: x.get('승률', 0), reverse=True)
                                for item in sorted_res[:3]:
                                    df_c = logic.get_processed_data(item['코드'], p_key)
                                    if df_c is not None:
                                        fname = f"test_{item['코드']}.png"
                                        if logic.save_chart_image(df_c, item['종목명'], fname):
                                            logic.send_telegram_photo(config["tg_token"], config["tg_chat_id"], fname, caption=f"📊 *{item['종목명']}* 분석 차트 (수동테스트)")
                                            import os
                                            if os.path.exists(fname): os.remove(fname)

                            st.success("발송 성공!")
                            # 실행 이력 추가 (수동 발송 기록)
                            log_entry = {
                                "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "strategy": sched['strategy'] + " (수동)",
                                "target": sched['target'],
                                "count": len(res_list),
                                "status": "Success"
                            }
                            if "history" not in config: config["history"] = []
                            config["history"] = ([log_entry] + config["history"])[:10]
                            logic.save_config(config)
                            auto_sync_github() # GitHub 동기화하여 UI 업데이트
                        else: st.error("발송 실패")
            if col3.button("🗑️ 삭제", key=f"del_{sched['id']}"):
                config['schedules'].pop(idx); logic.save_config(config); auto_sync_github(); st.rerun()

# --- 탭 전략 관리 ---
with tab_strat:
    st.title("🛠️ 전략 관리 및 상세 설명")
    st.write("시스템 전략의 로직을 확인합니다.")
    all_existing = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수"]
    sel_info = st.selectbox("전략 선택", all_existing, key="strat_desc_select")
    if sel_info: st.info(f"### 📖 {sel_info}\n\n{logic.get_strategy_desc(sel_info)}")

# --- 탭 시스템 설정 ---
with tab3:
    st.title("⚙️ 시스템 설정")
    gh_token_input = st.text_input("GitHub PAT", value=st.session_state["gh_token"], type="password")
    gh_repo_input = st.text_input("GitHub Repo", value=st.session_state["gh_repo"])
    if gh_token_input != st.session_state["gh_token"] or gh_repo_input != st.session_state["gh_repo"]:
        st.session_state["gh_token"] = gh_token_input; st.session_state["gh_repo"] = gh_repo_input
    if st.button("🚀 GitHub 강제 동기화"): auto_sync_github()
    
    st.divider()
    st.subheader("📜 최근 자동 알림 실행 이력")
    history = config.get("history", [])
    if history:
        df_hist = pd.DataFrame(history)
        df_hist.columns = ["실행 시간", "전략", "대상", "포착수", "상태"]
        st.table(df_hist)
    else:
        st.write("실행 기록이 없습니다.")

    st.divider()
    current_pw = st.text_input("현재 비번", type="password")
    new_pw = st.text_input("새 비번", type="password")
    if st.button("비번 변경"):
        if current_pw == config.get("password"): config["password"] = new_pw; logic.save_config(config); st.success("변경 완료")
        else: st.error("비번 불일치")
    if st.button("🔓 로그아웃"): st.session_state["authenticated"] = False; st.rerun()
