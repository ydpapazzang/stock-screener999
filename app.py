import streamlit as st
import pandas as pd
import logic
import json
import uuid
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
for k, v in {"authenticated":False, "active_tab_idx":0, "scanning":False, "temp_conditions":[], "editing_idx":None}.items():
    if k not in st.session_state: st.session_state[k] = v

# --- [1] 사이드바 ---
with st.sidebar:
    st.title("🎯 전역 컨트롤")
    cat = st.selectbox("분석 단위", ["일봉 전략", "주봉 전략", "월봉 전략"])
    period = 'D' if "일봉" in cat else ('W' if "주봉" in cat else 'M')
    
    custom_list = [f"🔴 {s['name']}" for s in config.get('custom_strategies', []) if s.get('timeframe') == cat[:2]]
    base_options = ["정석 정배열 (추세추종)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
    all_opts = base_options + custom_list
    sel_labels = st.multiselect("전략 선택", all_opts, default=[all_opts[0]] if all_opts else [])
    sel_strats = [s.replace("🔴 ", "") for s in sel_labels]
    
    target = st.radio("대상", ["KOSPI/KOSDAQ", "한국 ETF", "미국 나스닥", "미국 ETF", "⭐ 관심종목"])
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

# --- [1.5] 스캔 로직 ---
if st.session_state["scanning"]:
    df_l = pd.DataFrame(config.get('watchlist', [])) if target == "⭐ 관심종목" else logic.get_listing_data(target)
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

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    p_in = st.text_input("비밀번호", type="password")
    if st.button("접속") or (p_in == ACCESS_PW): 
        if p_in == ACCESS_PW: st.session_state["authenticated"] = True; st.rerun()
    st.stop()

# --- [2] 메인 UI ---
menu = ["🚀 전략 스캔", "⭐ 관심종목", "📅 알림 설정", "🛠️ 전략 커스텀", "💰 배당 계산기", "⚙️ 시스템"]
sel_menu = st.segmented_control("메뉴", menu, selection_mode="single", default=menu[st.session_state["active_tab_idx"]])
if sel_menu: st.session_state["active_tab_idx"] = menu.index(sel_menu)
curr = menu[st.session_state["active_tab_idx"]]

if curr == "🚀 전략 스캔":
    if 'last_results' in st.session_state:
        df = st.session_state['last_results']
        if not df.empty:
            st.success(f"✅ 스캔 완료 | 전략: `{st.session_state.get('last_query_strats')}` | 총 {len(df)}건")
            df_d = df.copy(); df_d.index = range(1, len(df)+1)
            st.dataframe(df_d, use_container_width=True)
            c1, c2 = st.columns([2, 1])
            sel = c1.selectbox("상세 분석", df['종목명'].tolist())
            if sel:
                row = df[df['종목명']==sel].iloc[0]
                links = logic.get_external_link(row['코드'])
                with c2:
                    st.write("🔗 **외부 링크**")
                    cols = st.columns(len(links))
                    for i, (site, url) in enumerate(links.items()): cols[i].link_button(site, url)
                df_c = logic.get_processed_data(row['코드'], period)
                if df_c is not None: st.plotly_chart(logic.create_advanced_chart(df_c, sel, sel_strats))
        else: st.warning("포착된 종목이 없습니다.")
    else: st.info("사이드바에서 [즉시 스캔 실행] 버튼을 눌러주세요.")

elif curr == "⭐ 관심종목":
    st.title("⭐ 관심종목 관리")
    if "watchlist" not in config: config["watchlist"] = []
    with st.expander("➕ 관심종목 추가", expanded=True):
        col1, col2 = st.columns([3, 1])
        if "full_list" not in st.session_state: st.session_state.full_list = logic.get_searchable_list()
        new = col1.selectbox("종목 선택", st.session_state.full_list, index=None)
        if col2.button("추가") and new:
            import re
            sym = re.search(r'\((.*?)\)', new).group(1); name = new.split(" (")[0]
            if not any(x['Symbol'] == sym for x in config['watchlist']):
                config['watchlist'].append({"Symbol": sym, "Name": name}); logic.save_config(config); st.rerun()
    if config['watchlist']:
        df_w = pd.DataFrame(config['watchlist']); df_w.index += 1; st.dataframe(df_w, use_container_width=True)
        if st.button("🗑️ 전체 삭제"): config['watchlist'] = []; logic.save_config(config); st.rerun()

elif curr == "📅 알림 설정":
    st.title("📅 자동 알림 스케줄")
    with st.expander("➕ 새 알림 추가"):
        f = st.selectbox("주기", ["매일", "매주 (월요일)", "매월 (1일)"])
        all_s = ["정석 정배열 (추세추종)", "거래량 폭발 (세력개입)"] + [s['name'] for s in config.get('custom_strategies', [])]
        s_c = st.selectbox("전략", all_s)
        if st.button("💾 저장"):
            config['schedules'].append({"id":str(uuid.uuid4())[:8], "freq":f, "time":"06:00", "strategy":s_c, "target":"KOSPI/KOSDAQ"})
            logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()
    for i, s in enumerate(config.get('schedules', [])):
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.write(f"### {s['freq']} | {s['strategy']}")
            if c2.button("📡 발송", key=f"snd_{s['id']}"):
                with st.spinner("발송 중..."):
                    targets = logic.get_listing_data("KOSPI/KOSDAQ").head(30)
                    for r in targets.itertuples():
                        df_d = logic.get_processed_data(r.Symbol, 'D')
                        if df_d is not None and logic.check_multi_signals(df_d, [s['strategy']]).iloc[-1]:
                            logic.send_telegram_with_chart(TG_TOKEN, TG_CHAT_ID, r.Symbol, r.Name, df_d, [s['strategy']])
                    st.success("발송 완료!")
            if c3.button("🗑️ 삭제", key=f"del_{s['id']}"):
                config['schedules'].pop(i); logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()

elif curr == "🛠️ 전략 커스텀":
    st.title("🛠️ 나만의 전략 커스텀")
    if "custom_strategies" not in config: config["custom_strategies"] = []
    is_edit = st.session_state.editing_idx is not None
    
    with st.expander("📝 전략 작성 및 검증", expanded=True):
        d_name = config["custom_strategies"][st.session_state.editing_idx]["name"] if is_edit else ""
        c_name = st.text_input("전략명", value=d_name)
        c_unit = st.selectbox("캔들 단위", ["일봉", "주봉", "월봉"], index=["일봉", "주봉", "월봉"].index(config["custom_strategies"][st.session_state.editing_idx]["timeframe"]) if is_edit else 0)
        
        t_tabs = st.tabs(["📈 이동평균 (MA)", "📊 RSI", "🔊 거래량"])
        with t_tabs[0]:
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])
            ma_p_t = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="ma_pt")
            ma_p_v = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="ma_pv")
            ma_a = col3.selectbox("비교 A", ["종가"]+[f"MA{i}" for i in range(1,101)], key="ma_a")
            ma_op = col4.selectbox("조건", [">=", "<=", ">", "<"], key="ma_op")
            ma_b = col5.selectbox("비교 B", [f"MA{i}" for i in range(1,366)], index=19, key="ma_b")
            if st.button("➕ MA 조건 추가"):
                st.session_state.temp_conditions.append({"a":ma_a, "b":ma_b, "op":ma_op, "period":int(ma_p_v.replace("봉","")), "p_type":"ago" if ma_p_t=="N봉전" else "within"})
                st.rerun()
        with t_tabs[1]:
            col1, col2, col3, col4 = st.columns([2, 2, 1, 2])
            rsi_p_t = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="rsi_pt")
            rsi_p_v = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="rsi_pv")
            rsi_op = col3.selectbox("조건", [">=", "<=", ">", "<"], key="rsi_op")
            rsi_v = col4.number_input("RSI 값", 0, 100, 30, key="rsi_v")
            if st.button("➕ RSI 추가"):
                st.session_state.temp_conditions.append({"a":"RSI", "b":str(rsi_v), "op":rsi_op, "period":int(rsi_p_v.replace("봉","")), "p_type":"ago" if rsi_p_t=="N봉전" else "within"})
                st.rerun()
        with t_tabs[2]:
            col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 2, 1])
            vol_p_t = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="vol_pt")
            vol_p_v = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="vol_pv")
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
            b1, b2, b3 = st.columns([2, 2, 1])
            if b1.button("📊 성과 검증 (3년)", use_container_width=True):
                with st.spinner("분석 중..."):
                    ts = "005930" if c_unit != "월봉" else "AAPL"
                    win, ret, cnt = logic.run_backtest(logic.get_processed_data(ts, period), [c_name])
                    st.metric(f"{ts} (3년) 성과", f"승률 {win}%", f"수익 {ret}%")
            if b2.button("💾 저장", type="primary", use_container_width=True):
                new_s = {"name":c_name, "timeframe":c_unit, "conditions":st.session_state.temp_conditions.copy()}
                if is_edit: config["custom_strategies"][st.session_state.editing_idx] = new_s
                else: config["custom_strategies"].append(new_s)
                logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
                st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.rerun()
            if b3.button("🧹 초기화", use_container_width=True):
                st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.rerun()

    st.write("---")
    c1, c2 = st.columns([4, 1])
    c1.subheader("📋 내 커스텀 전략 목록")
    if config.get("custom_strategies") and c2.button("📊 일괄 검증", type="primary"):
        with st.spinner("검증 중..."):
            all_r = []
            for cs in config["custom_strategies"]:
                ts = "005930" if cs['timeframe'] != "월봉" else "AAPL"
                win, ret, cnt = logic.run_backtest(logic.get_processed_data(ts, period), [cs['name']])
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
            if col2.button("📝 수정", key=f"e_{i}"): st.session_state.editing_idx = i; st.session_state.temp_conditions = cs['conditions'].copy(); st.rerun()
            if col3.button("🗑️ 삭제", key=f"d_{i}"): config["custom_strategies"].pop(i); logic.save_config(config); st.rerun()

elif curr == "💰 배당 계산기":
    st.title("💰 배당금 계산기")
    if "portfolio" not in st.session_state: st.session_state.portfolio = []
    with st.expander("➕ 보유 종목 추가", expanded=True):
        c1,c2,c3 = st.columns(3)
        if "s_list" not in st.session_state: st.session_state.s_list = logic.get_searchable_list()
        sel = c1.selectbox("종목 검색", st.session_state.s_list, index=None)
        qty = c2.number_input("수량", min_value=1, value=10)
        price = c3.number_input("평균단가", min_value=0.0, value=50000.0)
        if st.button("추가") and sel:
            import re
            sym = re.search(r'\((.*?)\)', sel).group(1)
            det = logic.get_dividend_details(sym)
            if det: det.update({"qty": qty, "avg_price": price}); st.session_state.portfolio.append(det); st.rerun()
    if st.session_state.portfolio:
        df_p = pd.DataFrame(st.session_state.portfolio)
        invest = (df_p['qty'] * df_p['avg_price']).sum()
        div = (df_p['qty'] * df_p['dps']).sum()
        m1,m2,m3,m4 = st.columns(4)
        m1.metric("총 투자", f"{invest:,.0f} {df_p['currency'].iloc[0]}")
        m2.metric("연 배당", f"{div:,.0f} {df_p['currency'].iloc[0]}")
        m3.metric("수익률", f"{(div/invest*100):.2f}%")
        m4.metric("평균Payout", f"{df_p['payout'].mean():.1f}%")
        st.subheader("🗓️ 월별 배당 분포")
        monthly = {m: 0 for m in range(1, 13)}
        for p in st.session_state.portfolio:
            if p['months']:
                for m in p['months']: monthly[m] += (p['qty']*p['dps'])/len(p['months'])
        df_m = pd.DataFrame({"Month":[f"{m}월" for m in range(1,13)], "Amount":list(monthly.values())})
        st.plotly_chart(px.bar(df_m, x="Month", y="Amount", color="Amount", title="월별 배당금"), use_container_width=True)
        if st.button("초기화"): st.session_state.portfolio = []; st.rerun()

elif curr == "⚙️ 시스템":
    st.title("⚙️ 시스템")
    if st.button("🚀 GitHub 동기화"): logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
    if config.get('history'): st.table(pd.DataFrame(config['history']).head(10))
