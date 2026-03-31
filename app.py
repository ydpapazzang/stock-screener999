import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [0] 세션 초기화 및 토큰 관리 ---
if "gh_token" not in st.session_state:
    # 1. Streamlit Secrets에서 먼저 로드 시도 (보안 및 편의성)
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
            token=token.strip(),
            repo=repo.strip(),
            path="config.json",
            message="Auto-sync schedules via Streamlit UI",
            content=config_content
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
        category = st.selectbox("분석 단위", ["월봉 전략 조합", "주봉 전략 조합"])
        
        if "월봉" in category:
            all_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "저평가 성장주 (퀀트)"]
            period_key = 'M'
        else:
            all_strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파", "5일 연속 상승세", "외인/기관 쌍끌이 매수"]
            period_key = 'W'
            
        selected_strategies = st.multiselect("사용할 전략들을 선택하세요 (모두 만족 시 포착)", all_strats, default=[all_strats[0]])
        
        # 선택한 모든 전략의 설명을 사이드바에 표시
        if selected_strategies:
            st.divider()
            st.subheader("📝 선택된 전략 설명")
            for s_name in selected_strategies:
                with st.expander(f"🔹 {s_name}", expanded=True):
                    st.write(logic.get_strategy_desc(s_name))
        
        st.divider()
        target_type = st.radio("분석 대상", ["주식 (KOSPI 200)", "ETF"])
        max_scan = 200 if "주식" in target_type else 1000
        scan_limit = st.slider("스캔 종목 수", 10, max_scan, 100)

        st.divider()
        st.header("📲 텔레그램 설정")
        if config.get("tg_token") and config.get("tg_chat_id"):
            st.success("✅ 텔레그램 활성화")
            if st.button("🗑️ 설정 초기화"):
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

    if st.button(f"🚀 {len(selected_strategies)}개 복합 전략 스캔 시작", use_container_width=True):
        if not selected_strategies:
            st.warning("최소 하나 이상의 전략을 선택하세요.")
        elif df_list.empty:
            st.error("데이터 로드 실패")
        else:
            results = []
            p_bar = st.progress(0)
            targets = df_list.iloc[:scan_limit]
            
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
        # 신규감지(Y/N) 내림차순 정렬 (Y가 N보다 먼저 옴) 후 승률 내림차순 정렬
        df_res = df_res.sort_values(by=["신규감지", "승률"], ascending=[False, False])
        st.subheader("🎯 포착된 종목 리스트")
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
                    st.caption("💡 녹색 화살표(▲)는 선택한 모든 전략 조건이 동시에 만족되었던 시점입니다.")

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
            config['schedules'].append(new_schedule)
            logic.save_config(config)
            st.success("등록됨!")
            auto_sync_github()
            st.rerun()

    schedules = config.get("schedules", [])
    for idx, sched in enumerate(schedules):
        with st.container(border=True):
            col1, col2 = st.columns([4, 1])
            target_info = sched.get('target', '정보 없음')
            col1.markdown(f"### 📡 {sched['freq']} {sched['time']} | {sched['strategy']}")
            col1.caption(f"🎯 대상: {target_info} | 스캔 제한: {sched.get('limit', 100)}개")
            
            if col2.button("🗑️ 삭제", key=f"del_{sched['id']}"):
                with st.spinner("저장소 동기화 중..."):
                    config['schedules'].pop(idx)
                    logic.save_config(config)
                    auto_sync_github()
                st.rerun()

# --- 탭 전략 관리 ---
with tab_strat:
    st.title("🛠️ 전략 관리 및 상세 설명")
    st.write("시스템에 내장된 전략들의 상세 매매 로직과 활용법을 확인합니다.")
    
    st.divider()
    all_existing = [
        "정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", 
        "대시세 초입 (20선 돌파)", "월봉 MA12 돌파", "주봉 5/20 골든크로스", 
        "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착", "와인스타인 2단계 돌파",
        "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수"
    ]
    
    sel_info = st.selectbox("설명을 보려는 전략을 선택하세요", all_existing, key="strat_desc_select")
    if sel_info:
        st.info(f"### 📖 {sel_info}\n\n{logic.get_strategy_desc(sel_info)}")

    st.divider()
    st.subheader("➕ 나만의 복합 전략 만들기")
    st.write("여러 조건을 조합하여 나만의 필터를 만들 수 있습니다.")
    
    with st.expander("💡 복합 전략 가이드", expanded=False):
        st.markdown("""
        **복합 전략이란?**
        - 메인 화면의 사이드바에서 여러 개의 전략을 동시에 선택하는 것을 의미합니다.
        - 선택된 모든 조건이 **AND(교집합)**로 적용되어, 훨씬 정교한 타점을 찾아냅니다.
        
        **추천 조합:**
        1. **[추세 + 거래량]**: '와인스타인 2단계' + '거래량 폭발'
        2. **[낙폭과대 + 반등]**: '주봉 볼린저 하단' + '주봉 RSI 과매도 탈출'
        3. **[이평선 돌파 + 안착]**: '대시세 초입' + '주봉 20선 돌파'
        """)

# --- 탭 시스템 설정 ---
with tab3:
    st.title("⚙️ 시스템 설정")
    st.subheader("🔄 GitHub 동기화 (Cloud 전용)")
    st.info("웹에서 변경한 스케줄이 GitHub Actions에 반영되려면 GitHub API를 통해 동기화해야 합니다.")
    
    gh_token_input = st.text_input("GitHub Personal Access Token (PAT)", 
                                   value=st.session_state["gh_token"],
                                   type="password", 
                                   help="repo 권한이 있는 토큰이 필요합니다.")
    gh_repo_input = st.text_input("GitHub Repository", 
                                  value=st.session_state["gh_repo"],
                                  help="계정명/저장소명 형식으로 입력하세요.")
    
    if gh_token_input != st.session_state["gh_token"] or gh_repo_input != st.session_state["gh_repo"]:
        st.session_state["gh_token"] = gh_token_input
        st.session_state["gh_repo"] = gh_repo_input
    
    if st.button("🚀 설정 파일을 GitHub에 동기화 (API Push)"):
        auto_sync_github()

    st.divider()
    st.subheader("🔑 비밀번호 변경")
    current_pw = st.text_input("현재 비밀번호", type="password")
    new_pw = st.text_input("새 비밀번호", type="password")
    if st.button("비밀번호 변경"):
        if current_pw == config.get("password"):
            config["password"] = new_pw; logic.save_config(config); st.success("변경 완료")
        else: st.error("비번 불일치")
    if st.button("🔓 로그아웃"):
        st.session_state["authenticated"] = False; st.rerun()
