import streamlit as st
import pandas as pd
import time
import logic
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import plotly.express as px

# --- [0] 보안 설정 ---
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
if "active_tab_idx" not in st.session_state:
    st.session_state["active_tab_idx"] = 0
if "scanning" not in st.session_state:
    st.session_state["scanning"] = False

# --- [2] 사이드바 전역 설정 ---
with st.sidebar:
    st.title("🎯 전역 컨트롤")
    category = st.selectbox("분석 단위", ["월봉 전략", "주봉 전략", "일봉 전략"])
    if "월봉" in category:
        base_strats = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "대시세 초입 (20선 돌파)", "월봉 MA12 돌파"]
        period = 'M'
    elif "주봉" in category:
        base_strats = ["주봉 5/20 골든크로스", "주봉 RSI 과매도 탈출", "주봉 볼린저 하단 터치", "주봉 20선 돌파 및 안착"]
        period = 'W'
    else:
        base_strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
        period = 'D'
        
    custom_list = [f"🔴 {s['name']}" for s in config.get('custom_strategies', []) if s.get('timeframe') == category[:2]]
    all_options = base_strats + custom_list
    sel_labels = st.multiselect("전략 선택", all_options, default=[all_options[0]] if all_options else [])
    sel_strats = [s.replace("🔴 ", "") for s in sel_labels]
    
    target = st.radio("대상", ["KOSPI/KOSDAQ", "한국 ETF", "미국 나스닥", "미국 ETF"])
    min_cap = st.slider("최소 시총 (억)", 0, 10000, 500, 100, help="미국 주식은 대략적인 원화 환산 기준")
    limit = st.slider("최대 분석 수", 10, 500, 100)
    
    st.divider()
    if st.session_state.get("authenticated"):
        if st.session_state["scanning"]:
            st.button("⏳ 종목 분석 중...", disabled=True, use_container_width=True)
        else:
            if st.button("🔍 즉시 스캔 실행", use_container_width=True, type="primary"):
                # 즉시 그리드 초기화
                st.session_state['last_results'] = pd.DataFrame()
                st.session_state['last_query_strats'] = ""
                st.session_state["scanning"] = True
                st.rerun()
    else:
        st.error("🔒 보안 접속이 필요합니다.")
        st.button("🔍 즉시 스캔 실행 (잠김)", disabled=True, use_container_width=True)

# --- [2.5] 실제 스캔 로직 실행 ---
if st.session_state["scanning"]:
    df_list = logic.get_listing_data(target)
    if not df_list.empty:
        # 시총 필터링
        if '시총(억)' in df_list.columns and target not in ["미국 ETF", "한국 ETF"]:
            df_list = df_list[df_list['시총(억)'] >= min_cap]
        
        targets = df_list.head(limit)
        results = []
        with st.spinner(f"🚀 {target} {len(targets)}개 종목 분석 중..."):
            # Symbol, Name 컬럼을 안전하게 추출
            for r in targets.itertuples():
                s_code = getattr(r, 'Symbol', getattr(r, 'Index', ''))
                s_name = getattr(r, 'Name', s_code)
                
                # 병렬 처리를 위해 worker 호출 (여기서는 단순화를 위해 루프 내 처리 또는 ThreadPool 유지)
                res = logic.process_stock_multi_worker(s_code, s_name, sel_strats, period)
                if res: results.append(res)
        
        if results:
            st.session_state['last_results'] = pd.DataFrame(results).sort_values(by=["점수"], ascending=False)
            st.session_state['last_query_strats'] = ", ".join(sel_strats)
        else:
            st.session_state['last_results'] = pd.DataFrame()
            st.session_state['last_query_strats'] = f"{', '.join(sel_strats)} (포착 없음)"
            st.warning(f"⚠️ {target} 시장에서 조건에 맞는 종목을 찾지 못했습니다.")
            
        st.session_state["active_tab_idx"] = 0 
    
    st.session_state["scanning"] = False
    st.rerun()

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    pw_input = st.text_input("비밀번호", type="password")
    if st.button("접속") or pw_input:
        if pw_input == ACCESS_PW:
            st.session_state["authenticated"] = True
            st.rerun()
        else: st.error("비밀번호 불일치")
    st.stop()

# --- [3] 메인 UI (탭 내비게이션) ---
menu_options = ["🚀 전략 스캔", "📅 알림 설정", "🛠️ 전략 커스텀", "💰 배당 계산기", "⚙️ 시스템"]
selected_menu = st.segmented_control("메뉴", menu_options, selection_mode="single", default=menu_options[st.session_state["active_tab_idx"]])

if selected_menu:
    st.session_state["active_tab_idx"] = menu_options.index(selected_menu)

curr_tab = menu_options[st.session_state["active_tab_idx"]]

if curr_tab == "🚀 전략 스캔":
    if 'last_results' in st.session_state:
        applied_strats = st.session_state.get('last_query_strats', "알 수 없음")
        df_res = st.session_state['last_results']
        
        if not df_res.empty:
            st.success(f"✅ **스캔 완료** | 적용 전략: `{applied_strats}` | **총 {len(df_res)}건 포착**")
            st.title("🚀 전략 스캔 결과")
            
            # 인덱스를 1부터 시작하도록 조정
            df_display = df_res.copy()
            df_display.index = range(1, len(df_display) + 1)
            st.dataframe(df_display, use_container_width=True)
            
            sel_name = st.selectbox("상세 차트 보기", df_res['종목명'].tolist())
            if sel_name:
                clean_name = sel_name.split(" (")[0]
                code = df_res[df_res['종목명']==sel_name]['코드'].values[0]
                df_chart = logic.get_processed_data(code, period)
                if df_chart is not None:
                    st.plotly_chart(logic.create_advanced_chart(df_chart, clean_name, [applied_strats]))
        else:
            st.warning(f"✅ **스캔 완료** | 적용 전략: `{applied_strats}`")
            st.info("검색된 결과가 0건입니다.")

elif curr_tab == "📅 알림 설정":
    st.title("📅 자동 알림 스케줄")
    import uuid
    with st.expander("➕ 새 알림 추가"):
        f = st.selectbox("주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        base_all = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
        custom_all = [s['name'] for s in config.get('custom_strategies', [])]
        s_choice = st.selectbox("전략", base_all + custom_all)
        st.info("💡 **알림 시간 안내**: 모든 알림은 오전 06:00 (KST)에 일괄 발송됩니다.")
        if st.button("💾 알림 저장"):
            new_s = {"id": str(uuid.uuid4())[:8], "freq": f, "time": "06:00", "strategy": s_choice, "target": "주식", "limit": 100}
            config['schedules'].append(new_s); logic.save_config(config)
            logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
            st.success("저장 완료!"); st.rerun()

    for i, s_item in enumerate(config.get('schedules', [])):
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.write(f"### 📡 {s_item['freq']} {s_item['time']} | {s_item['strategy']}")
            if c2.button("📡 발송", key=f"t_{s_item['id']}"):
                with st.spinner("발송 중..."):
                    df_l = logic.get_listing_data("주식").head(50)
                    res = []
                    s_period = 'D'
                    for cs in config.get('custom_strategies', []):
                        if cs['name'] == s_item['strategy']:
                            s_period = 'M' if cs['timeframe'] == "월봉" else ('W' if cs['timeframe'] == "주봉" else 'D')
                    with ThreadPoolExecutor(max_workers=5) as exe:
                        futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [s_item['strategy']], s_period) for r in df_l.itertuples()]
                        for f in as_completed(futures):
                            if f.result(): res.append(f.result())
                    logic.send_telegram_all(TG_TOKEN, TG_CHAT_ID, res, [s_item['strategy']], "주식")
                    st.success("발송 완료!")
            if c3.button("🗑️ 삭제", key=f"d_{s_item['id']}"):
                config['schedules'].pop(i); logic.save_config(config)
                logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()

elif curr_tab == "🛠️ 전략 커스텀":
    st.title("🛠️ 나만의 전략 커스텀")
    if "custom_strategies" not in config: config["custom_strategies"] = []
    if "temp_conditions" not in st.session_state: st.session_state.temp_conditions = []
    if "editing_idx" not in st.session_state: st.session_state.editing_idx = None
    
    is_edit_mode = st.session_state.editing_idx is not None
    edit_header = "📝 전략 수정 중" if is_edit_mode else "✨ 새 커스텀 전략 만들기"
    
    with st.expander(edit_header, expanded=True):
        d_name = ""
        d_tf_idx = 0
        if is_edit_mode:
            curr = config["custom_strategies"][st.session_state.editing_idx]
            d_name = curr["name"]
            d_tf_idx = ["일봉", "주봉", "월봉"].index(curr["timeframe"])
            
        c_name = st.text_input("전략명", value=d_name, placeholder="예: 골든크로스 + 정배열")
        c_unit = st.selectbox("캔들 단위", ["일봉", "주봉", "월봉"], index=d_tf_idx)
        st.write("---")
        st.subheader("🎯 조건 구성")
        c_tabs = st.tabs(["📈 이동평균 (MA)", "📊 RSI", "🔊 거래량"])
        with c_tabs[0]:
            col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 1, 2])
            ma_p_type = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="ma_pt")
            ma_p_val = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="ma_pv")
            ma_a = col3.selectbox("비교 A", ["종가"] + [f"MA{i}" for i in range(1, 101)], key="ma_a")
            ma_op = col4.selectbox("조건", [">=", "<=", ">", "<"], key="ma_op")
            ma_b = col5.selectbox("비교 B", [f"MA{i}" for i in range(1, 366)], index=19, key="ma_b")
            if st.button("➕ MA 조건 추가", use_container_width=True):
                st.session_state.temp_conditions.append({"a": ma_a, "b": ma_b, "op": ma_op, "period": int(ma_p_val.replace("봉", "")), "p_type": "ago" if ma_p_type == "N봉전" else "within"})
                st.toast("MA 조건 추가됨")
        with c_tabs[1]:
            col1, col2, col3, col4 = st.columns([2, 2, 1, 2])
            rsi_p_type = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="rsi_pt")
            rsi_p_val = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="rsi_pv")
            rsi_op = col3.selectbox("조건", [">=", "<=", ">", "<"], key="rsi_op")
            rsi_val = col4.number_input("RSI 값", 0, 100, 30, key="rsi_val")
            if st.button("➕ RSI 조건 추가", use_container_width=True):
                st.session_state.temp_conditions.append({"a": "RSI", "b": str(rsi_val), "op": rsi_op, "period": int(rsi_p_val.replace("봉", "")), "p_type": "ago" if rsi_p_type == "N봉전" else "within"})
                st.toast("RSI 조건 추가됨")
        with c_tabs[2]:
            col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 2, 1])
            vol_p_type = col1.selectbox("타입", ["N봉전", "N봉 이내"], key="vol_pt")
            vol_p_val = col2.selectbox("기간", [f"{i}봉" for i in range(11)], key="vol_pv")
            vol_op = col3.selectbox("조건", [">=", "<=", ">", "<"], key="vol_op")
            vol_base = col4.selectbox("비교 대상", ["VMA5", "VMA20", "VMA60"], key="vol_base")
            vol_mult = col5.number_input("배수", 0.1, 10.0, 2.0, 0.1, key="vol_mult")
            if st.button("➕ 거래량 조건 추가", use_container_width=True):
                st.session_state.temp_conditions.append({"a": "거래량", "b": f"{vol_base} * {vol_mult}", "op": vol_op, "period": int(vol_p_val.replace("봉", "")), "p_type": "ago" if vol_p_type == "N봉전" else "within"})
                st.toast("거래량 조건 추가됨")

        if st.session_state.temp_conditions:
            st.divider()
            for idx, cond in enumerate(st.session_state.temp_conditions):
                c1, c2 = st.columns([5, 1])
                t_label = "봉전" if cond.get('p_type', 'ago') == "ago" else "봉 이내"
                c1.info(f"조건 {idx+1}: {cond['period']}{t_label} {cond['a']} {cond.get('op', '>=')} {cond['b']}")
                if c2.button("❌", key=f"del_temp_{idx}"):
                    st.session_state.temp_conditions.pop(idx); st.rerun()
            
            save_btn_label = "💾 수정 내용 업데이트" if is_edit_mode else "💾 전체 전략 저장"
            b_col1, b_col2 = st.columns(2)
            if b_col1.button(save_btn_label, type="primary", use_container_width=True):
                if not c_name: st.error("전략명을 입력하세요.")
                else:
                    new_cs = {"name": c_name, "timeframe": c_unit, "conditions": st.session_state.temp_conditions.copy()}
                    if is_edit_mode: config["custom_strategies"][st.session_state.editing_idx] = new_cs
                    else: config["custom_strategies"].append(new_cs)
                    logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
                    st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.success("저장 완료!"); st.rerun()
            if b_col2.button("🧹 초기화 및 취소", use_container_width=True):
                st.session_state.temp_conditions = []; st.session_state.editing_idx = None; st.rerun()
        else: st.warning("조건을 추가하세요.")

    st.write("---")
    st.subheader("📋 내 커스텀 전략 목록")
    for i, cs in enumerate(config.get("custom_strategies", [])):
        with st.container(border=True):
            col1, col2, col3 = st.columns([4, 1, 1])
            cond_desc = " AND ".join([f"[{c['period']}{'봉전' if c.get('p_type','ago')=='ago' else '봉이내'} {c['a']} {c.get('op','>=')} {c['b']}]" for c in cs['conditions']])
            col1.write(f"### {cs['name']} ({cs['timeframe']})")
            col1.info(f"🔍 전체 조건: {cond_desc}")
            if col2.button("📝 수정", key=f"edit_cs_{i}", use_container_width=True):
                st.session_state.editing_idx = i
                st.session_state.temp_conditions = cs['conditions'].copy()
                st.rerun()
            if col3.button("🗑️ 삭제", key=f"del_cs_{i}", use_container_width=True):
                config["custom_strategies"].pop(i)
                logic.save_config(config); logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
                st.rerun()

elif curr_tab == "💰 배당 계산기":
    st.title("💰 스마트 배당금 계산기")
    if "portfolio" not in st.session_state: st.session_state.portfolio = []
    with st.expander("➕ 보유 종목 추가", expanded=True):
        c1, c2, c3 = st.columns(3)
        if "search_list" not in st.session_state:
            with st.spinner("종목 리스트 로드 중..."): st.session_state.search_list = logic.get_searchable_list()
        selected_stock = c1.selectbox("종목 검색", options=st.session_state.search_list, index=None, placeholder="종목명 또는 티커 입력")
        in_qty = c2.number_input("보유 수량", min_value=1, value=10)
        in_price = c3.number_input("평균 단가", min_value=0.0, value=50000.0)
        if st.button("포트폴리오에 추가"):
            if selected_stock:
                import re
                match = re.search(r'\((.*?)\)', selected_stock)
                in_symbol = match.group(1) if match else selected_stock
                with st.spinner(f"{in_symbol} 데이터 조회 중..."):
                    details = logic.get_dividend_details(in_symbol)
                    if details:
                        details.update({"qty": in_qty, "avg_price": in_price})
                        st.session_state.portfolio.append(details); st.success(f"{details['name']} 추가됨!"); st.rerun()
                    else: st.error("종목 정보를 찾을 수 없습니다.")
            else: st.warning("종목을 선택해주세요.")
    if st.session_state.portfolio:
        df_port = pd.DataFrame(st.session_state.portfolio)
        st.divider()
        total_invest = (df_port['qty'] * df_port['avg_price']).sum()
        total_div = (df_port['qty'] * df_port['dps']).sum()
        yoc = (total_div / total_invest * 100) if total_invest > 0 else 0
        m1, m2, c3, m4 = st.columns(4)
        m1.metric("총 투자금액", f"{total_invest:,.0f} {df_port['currency'].iloc[0]}")
        m2.metric("연간 예상 배당금", f"{total_div:,.0f} {df_port['currency'].iloc[0]}")
        c3.metric("월 평균 수령액", f"{total_div/12:,.0f}")
        m4.metric("평균 배당수익률(YOC)", f"{yoc:.2f}%")
        st.subheader("🗓️ 월별 배당 캘린더")
        monthly_data = {m: 0 for m in range(1, 13)}
        for p in st.session_state.portfolio:
            if p['months']:
                d_per_month = (p['qty'] * p['dps']) / len(p['months'])
                for m in p['months']: monthly_data[m] += d_per_month
        df_month = pd.DataFrame({"Month": [f"{m}월" for m in range(1, 13)], "Amount": list(monthly_data.values())})
        fig_cal = px.bar(df_month, x="Month", y="Amount", title="월별 배당금 분포", color="Amount", color_continuous_scale="Viridis")
        st.plotly_chart(fig_cal, use_container_width=True)
        st.subheader("📈 배당 재투자(DRIP) 시뮬레이션")
        years = st.slider("시뮬레이션 기간 (년)", 1, 30, 10)
        reinvest_rate = st.slider("배당 재투자 비율 (%)", 0, 100, 100) / 100
        values = [total_invest]
        current_val = total_invest
        for y in range(years):
            div = current_val * (yoc/100)
            current_val += (div * reinvest_rate) + (current_val * 0.05)
            values.append(current_val)
        fig_drip = px.line(x=list(range(years+1)), y=values, title=f"{years}년 후 예상 자산 변화 (재투자 포함)", labels={"x": "경과 년수", "y": "자산 가치"})
        st.plotly_chart(fig_drip, use_container_width=True)
        st.subheader("📋 포트폴리오 상세 및 세금 분석")
        df_display = df_port[['name', 'dps', 'yield', 'payout', 'qty']].copy()
        df_display['실수령액(세후)'] = (df_port['qty'] * df_port['dps'] * 0.846).round(0)
        st.dataframe(df_display, use_container_width=True)
        if st.button("🗑️ 포트폴리오 초기화"): st.session_state.portfolio = []; st.rerun()
    else: st.info("보유 종목을 추가하여 배당 대시보드를 생성하세요.")

elif curr_tab == "⚙️ 시스템":
    st.title("⚙️ 시스템 정보")
    if st.button("🚀 GitHub 강제 동기화"):
        if logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)): st.success("동기화 성공!")
    st.divider()
    st.subheader("📜 실행 이력")
    if config.get('history'): st.table(pd.DataFrame(config['history']).head(10))
    else: st.write("이력이 없습니다.")
