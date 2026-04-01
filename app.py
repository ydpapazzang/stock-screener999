import streamlit as st
import pandas as pd
import logic
import json
import uuid
import re
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- [0] 보안 설정 ---
GH_TOKEN = logic.get_secret("GH_TOKEN", "")
GH_REPO = logic.get_secret("GH_REPO", "ydpapazzang/stock-screener999")
TG_TOKEN = logic.get_secret("TELEGRAM_TOKEN", "")
TG_CHAT_ID = logic.get_secret("TELEGRAM_CHAT_ID", "")
ACCESS_PW = logic.get_secret("ACCESS_PASSWORD", "1234")

st.set_page_config(page_title="Strategic Screener Pro", layout="wide", page_icon="⚡")
config = logic.load_config()

# 세션 상태 초기화
if "authenticated" not in st.session_state: st.session_state["authenticated"] = False
if "active_tab_idx" not in st.session_state: st.session_state["active_tab_idx"] = 0
if "scanning" not in st.session_state: st.session_state["scanning"] = False
if "temp_conditions" not in st.session_state: st.session_state["temp_conditions"] = []
if "editing_idx" not in st.session_state: st.session_state["editing_idx"] = None

# --- [1] 사이드바 ---
with st.sidebar:
    st.title("🎯 전역 컨트롤")
    cat = st.selectbox("분석 단위", ["일봉 전략", "주봉 전략", "월봉 전략"])
    # 주기 결정 (일봉='D', 주봉='W', 월봉='M')
    if "일봉" in cat: period = 'D'
    elif "주봉" in cat: period = 'W'
    else: period = 'M'
    
    custom_list = [f"🔴 {s['name']}" for s in config.get('custom_strategies', []) if s.get('timeframe') == cat[:2]]
    base_options = ["정석 정배열 (추세추종)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
    all_opts = base_options + custom_list
    sel_labels = st.multiselect("전략 선택", all_opts, default=[all_opts[0]] if all_opts else [])
    sel_strats = [s.replace("🔴 ", "") for s in sel_labels]
    
    # 관심종목 제거됨
    target = st.radio("대상 시장", ["KOSPI/KOSDAQ", "한국 ETF", "미국 나스닥", "미국 ETF"])
    min_cap = st.slider("최소 시총 (억)", 0, 10000, 500, 100)
    limit = st.slider("최대 분석 수", 10, 1000, 100)
    
    st.divider()
    if st.session_state["authenticated"]:
        if st.session_state["scanning"]: st.button("⏳ 분석 중...", disabled=True, use_container_width=True)
        else:
            if st.button("🔍 즉시 스캔 실행", use_container_width=True, type="primary"):
                st.session_state['last_results'] = pd.DataFrame()
                st.session_state["scanning"] = True; st.rerun()
    else: st.error("🔒 보안 접속 필요")

# --- [1.5] 스캔 로직 실행 ---
if st.session_state["scanning"]:
    df_l = logic.get_listing_data(target)
    if not df_l.empty:
        if '시총(억)' in df_l.columns and "ETF" not in target: df_l = df_l[df_l['시총(억)'] >= min_cap]
        targets = df_l.head(limit)
        results = []
        with st.spinner(f"🚀 {len(targets)}개 분석 중..."):
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(logic.process_stock_multi_worker, getattr(r,'Symbol',getattr(r,'Index','')), getattr(r,'Name',''), sel_strats, period): r for r in targets.itertuples()}
                for f in as_completed(futures):
                    res = f.result()
                    if res: results.append(res)
        st.session_state['last_results'] = pd.DataFrame(results).sort_values(by=["코드"]) if results else pd.DataFrame()
        st.session_state['last_query_strats'] = ", ".join(sel_strats)
        st.session_state["active_tab_idx"] = 0
    st.session_state["scanning"] = False; st.rerun()

# --- [2] 보안 접속 화면 ---
if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    pw_in = st.text_input("비밀번호", type="password")
    if st.button("접속") or (pw_in == ACCESS_PW): 
        if pw_in == ACCESS_PW: st.session_state["authenticated"] = True; st.rerun()
    st.stop()

# --- [3] 메인 UI (내비게이션) ---
menu = ["🚀 전략 스캔", "📅 알림 설정", "🛠️ 전략 커스텀", "⚙️ 시스템"]
sel_menu = st.segmented_control("메뉴", menu, selection_mode="single", default=menu[st.session_state["active_tab_idx"]])
if sel_menu: st.session_state["active_tab_idx"] = menu.index(sel_menu)
curr_tab = menu[st.session_state["active_tab_idx"]]

if curr_tab == "🚀 전략 스캔":
    # ... (전략 스캔 로직)
    if 'last_results' in st.session_state:
        df = st.session_state['last_results']
        if not df.empty:
            st.success(f"✅ 스캔 완료 | 전략: `{st.session_state.get('last_query_strats')}` | 총 {len(df)}건")
            df_d = df.copy(); df_d.index = range(1, len(df)+1)
            st.dataframe(df_d, use_container_width=True)
            c1, c2 = st.columns([2, 1])
            sel_s = c1.selectbox("상세 분석", df['종목명'].tolist())
            if sel_s:
                row = df[df['종목명']==sel_s].iloc[0]
                links = logic.get_external_link(row['코드'])
                with c2:
                    st.write("🔗 **외부 링크**")
                    cols = st.columns(len(links))
                    for idx, (site, url) in enumerate(links.items()): cols[idx].link_button(site, url)
                df_c = logic.get_processed_data(row['코드'], period)
                if df_c is not None: st.plotly_chart(logic.create_advanced_chart(df_c, sel_s, sel_strats))
        else: st.warning("포착된 종목이 없습니다.")
    else: st.info("사이드바에서 [즉시 스캔 실행] 버튼을 눌러주세요.")

elif curr_tab == "📅 알림 설정":
    # ... (알림 설정 로직)
    st.title("📅 자동 알림 스케줄")
    with st.expander("➕ 새 알림 추가"):
        f = st.selectbox("주기", ["매일", "매주 (월요일)", "매월 (1일)"])
        all_s = ["정석 정배열 (추세추종)", "거래량 폭발 (세력개입)"] + [s['name'] for s in config.get('custom_strategies', [])]
        s_choice = st.selectbox("전략", all_s)
        t_choice = st.selectbox("대상 시장", ["KOSPI/KOSDAQ", "한국 ETF", "미국 나스닥", "미국 ETF"])
        if st.button("💾 저장"):
            config['schedules'].append({"id":str(uuid.uuid4())[:8], "freq":f, "time":"06:00", "strategy":s_choice, "target":t_choice})
            logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()
    for i, s in enumerate(config.get('schedules', [])):
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 1, 1])
            t_m = s.get('target', 'KOSPI/KOSDAQ')
            c1.write(f"### {s['freq']} | {s['strategy']} ({t_m})")
            if c2.button("📡 발송", key=f"snd_{s['id']}"):
                with st.spinner("분석 및 발송 중..."):
                    strat_name = s['strategy']
                    s_period = 'D'
                    for cs in config.get('custom_strategies', []):
                        if cs['name'] == strat_name:
                            s_period = 'M' if cs['timeframe'] == "월봉" else ('W' if cs['timeframe'] == "주봉" else 'D')
                    df_l = logic.get_listing_data(t_m).head(100)
                    results = []
                    with ThreadPoolExecutor(max_workers=10) as exe:
                        futures = {exe.submit(logic.process_stock_multi_worker, getattr(r,'Symbol',getattr(r,'Index','')), getattr(r,'Name',''), [strat_name], s_period): r for r in df_l.itertuples()}
                        for f in as_completed(futures):
                            res = f.result()
                            if res: results.append(res)
                    if results:
                        logic.send_telegram_all(TG_TOKEN, TG_CHAT_ID, results, [strat_name], t_m)
                        st.success(f"✅ {t_m} 시장 {len(results)}건 포착 알림을 발송했습니다.")
                    else:
                        logic.send_telegram_all(TG_TOKEN, TG_CHAT_ID, [], [strat_name], t_m)
                        st.warning("포착 종목이 없어 요약 알림만 발송되었습니다.")
            if c3.button("🗑️ 삭제", key=f"del_{s['id']}"):
                config['schedules'].pop(i); logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()

elif curr_tab == "🛠️ 전략 커스텀":
    # ... (기존 전략 커스텀 로직 유지)
    st.title("🛠️ 나만의 전략 커스텀")
    if "custom_strategies" not in config: config["custom_strategies"] = []
    is_edit = st.session_state.editing_idx is not None
    
    with st.expander("📝 전략 작성 및 검증", expanded=True):
        d_name = config["custom_strategies"][st.session_state.editing_idx]["name"] if is_edit else ""
        c_name = st.text_input("전략명", value=d_name)
        c_unit = st.selectbox("캔들 단위", ["일봉", "주봉", "월봉"], index=["일봉", "주봉", "월봉"].index(config["custom_strategies"][st.session_state.editing_idx]["timeframe"]) if is_edit else 0)
        
        t_tabs = st.tabs(["📈 이동평균 (MA)", "📊 RSI", "🔊 거래량"])
        with t_tabs[0]:
            col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 2, 1, 2, 2])
            ma_p_v = col1.selectbox("기간", [f"{i}봉" for i in range(11)], key="ma_pv")
            ma_p_t = col2.selectbox("타입", ["N봉전", "N봉 이내"], key="ma_pt")
            ma_a = col3.selectbox("비교 A", ["종가"]+[f"MA{i}" for i in range(1,101)], key="ma_a")
            ma_op = col4.selectbox("조건", [">=", "<=", ">", "<"], key="ma_op")
            ma_b = col5.selectbox("비교 B", [f"MA{i}" for i in range(1,366)], index=19, key="ma_b")
            ma_disp = col6.number_input("최대 이격도 (%)", 0.0, 100.0, 0.0, 0.1, help="0은 무시", key="ma_disp")
            
            if st.button("➕ MA 조건 추가", use_container_width=True):
                st.session_state.temp_conditions.append({
                    "a":ma_a, "b":ma_b, "op":ma_op, 
                    "period":int(ma_p_v.replace("봉","")), 
                    "p_type":"ago" if ma_p_t=="N봉전" else "within",
                    "disparity": ma_disp if ma_disp > 0 else None
                })
                st.rerun()
        with t_tabs[1]:
            col1, col2, col3, col4 = st.columns([2, 2, 1, 2])
            rsi_p_v = col1.selectbox("기간", [f"{i}봉" for i in range(11)], key="rsi_pv")
            rsi_p_t = col2.selectbox("타입", ["N봉전", "N봉 이내"], key="rsi_pt")
            rsi_op = col3.selectbox("조건", [">=", "<=", ">", "<"], key="rsi_op")
            rsi_v = col4.number_input("RSI 값", 0, 100, 30, key="rsi_v")
            if st.button("➕ RSI 추가"):
                st.session_state.temp_conditions.append({"a":"RSI", "b":str(rsi_v), "op":rsi_op, "period":int(rsi_p_v.replace("봉","")), "p_type":"ago" if rsi_p_t=="N봉전" else "within"})
                st.rerun()
        with t_tabs[2]:
            col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 2, 1])
            vol_p_v = col1.selectbox("기간", [f"{i}봉" for i in range(11)], key="vol_pv")
            vol_p_t = col2.selectbox("타입", ["N봉전", "N봉 이내"], key="vol_pt")
            vol_op = col3.selectbox("조건", [">=", "<=", ">", "<"], key="vol_op")
            vol_b = col4.selectbox("비교 대상", ["VMA5", "VMA20", "VMA60"], key="vol_b")
            vol_m = col5.number_input("배수", 0.1, 10.0, 2.0, 0.1, key="vol_m")
            if st.button("➕ 거래량 추가"):
                st.session_state.temp_conditions.append({"a":"거래량", "b":f"{vol_b} * {vol_m}", "op":vol_op, "period":int(vol_p_v.replace("봉","")), "p_type":"ago" if vol_p_t=="N봉전" else "within"})
                st.rerun()

        if st.session_state.temp_conditions:
            st.divider()
            for i, cd in enumerate(st.session_state.temp_conditions):
                c1, c2 = st.columns([5, 1])
                lbl = "봉전" if cd.get('p_type','ago')=='ago' else "봉이내"
                c1.info(f"{cd['period']}{lbl} {cd['a']} {cd['op']} {cd['b']}")
                if c2.button("❌", key=f"rm_{i}"): st.session_state.temp_conditions.pop(i); st.rerun()
            
            st.subheader("📊 전략 성과 검증")
            t_stocks = ["삼성전자 (005930)", "SK하이닉스 (000660)", "KODEX 200 (069500)", "NVIDIA (NVDA)", "Apple (AAPL)", "TSLA (TSLA)", "QQQ"]
            sel_t = st.selectbox("검증 종목 선택", t_stocks)
            t_tick = re.search(r'\((.*?)\)', sel_t).group(1) if "(" in sel_t else sel_t
            
            b1, b2, b3 = st.columns([2, 2, 1])
            if b1.button(f"📊 {sel_t} 검증 (3년)", use_container_width=True):
                with st.spinner("분석 중..."):
                    df_bt = logic.get_processed_data(t_tick, period)
                    win, ret, cnt = logic.run_backtest(df_bt, [c_name if c_name else "임시"])
                    st.metric(f"성과 ({sel_t})", f"승률 {win}%", f"수익 {ret}%")
                    st.info(f"3년간 총 {cnt}회 포착됨")
            if b2.button("💾 전략 저장", type="primary", use_container_width=True):
                new_s = {"name":c_name, "timeframe":c_unit, "conditions":st.session_state.temp_conditions.copy()}
                if is_edit: config["custom_strategies"][st.session_state.editing_idx] = new_s
                else: config["custom_strategies"].append(new_s)
                logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
                st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.rerun()
            if b3.button("🧹 초기화", use_container_width=True):
                st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.rerun()

    st.write("---")
    c1, c2, c3 = st.columns([3, 2, 1])
    c1.subheader("📋 내 커스텀 전략 목록")
    sel_bt = c2.selectbox("일괄 검증 종목", ["삼성전자 (005930)", "SK하이닉스 (000660)", "QQQ", "NVDA", "AAPL"], key="batch_stock")
    bt_tick = re.search(r'\((.*?)\)', sel_bt).group(1) if "(" in sel_bt else sel_bt
    
    if config.get("custom_strategies") and c3.button("📊 일괄 검증", type="primary", use_container_width=True):
        with st.spinner("모든 전략 분석 중..."):
            all_r = []
            df_bt_full = logic.get_processed_data(bt_tick, period)
            for cs in config["custom_strategies"]:
                win, ret, cnt = logic.run_backtest(df_bt_full, [cs['name']])
                all_r.append({"전략명":cs['name'], "승률":f"{win}%", "수익":f"{ret}%", "횟수":f"{cnt}회"})
            st.session_state["all_bt"] = all_r
    if "all_bt" in st.session_state:
        st.table(pd.DataFrame(st.session_state["all_bt"]))
        if st.button("결과 닫기"): del st.session_state["all_bt"]; st.rerun()
    for i, cs in enumerate(config.get("custom_strategies", [])):
        with st.container(border=True):
            col1, col2, col3 = st.columns([4, 1, 1])
            col1.write(f"### {cs['name']} ({cs['timeframe']})")
            col1.caption(" & ".join([f"[{c['period']}봉전 {c['a']} {c.get('op','>=')} {c['b']}]" for c in cs['conditions']]))
            if col2.button("📝 수정", key=f"edit_{i}"): st.session_state.editing_idx = i; st.session_state.temp_conditions = cs['conditions'].copy(); st.rerun()
            if col3.button("🗑️ 삭제", key=f"del_{i}"): config["custom_strategies"].pop(i); logic.save_config(config); st.rerun()

elif curr_tab == "⚙️ 시스템":
    # ... (기존 시스템 로직 유지)
    st.title("⚙️ 시스템")
    if st.button("🚀 GitHub 강제 동기화"): 
        if logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)): st.success("동기화 성공")
    if config.get('history'): st.table(pd.DataFrame(config['history']).head(10))
