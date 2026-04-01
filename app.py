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

if not st.session_state["authenticated"]:
    st.title("🔒 보안 접속")
    pw_input = st.text_input("비밀번호", type="password")
    if st.button("접속") or pw_input:
        if pw_input == ACCESS_PW:
            st.session_state["authenticated"] = True
            st.rerun()
        else: st.error("비밀번호 불일치")
    st.stop()

# --- [2] 메인 UI ---
tabs = st.tabs(["🚀 전략 스캔", "📅 알림 설정", "💰 배당 계산기", "🛠️ 전략 가이드", "⚙️ 시스템"])

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
            strats = ["5일 연속 상승세", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
            period = 'D'
        sel_strats = st.multiselect("전략 선택", strats, default=[strats[0]])
        target = st.radio("대상", ["주식", "ETF"])
        min_cap = st.slider("최소 시총 (억)", 0, 5000, 500, 100) if target=="주식" else 0
        min_amt = st.slider("최소 거래대금 (억)", 0, 500, 50, 10) if target=="주식" else 0
        limit = st.slider("최대 분석 수", 10, 500, 100)

    if st.button("🚀 스캔 시작", use_container_width=True):
        df_list = logic.get_listing_data(target)
        if not df_list.empty:
            if target == "주식":
                df_list = df_list[(df_list.get('시총(억)', 0) >= min_cap) & (df_list.get('거래대금(억)', 0) >= min_amt)]
            targets = df_list.head(limit)
            results = []
            p_bar = st.progress(0)
            with ThreadPoolExecutor(max_workers=10) as exe:
                futures = {exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, sel_strats, period): r for r in targets.itertuples()}
                for i, f in enumerate(as_completed(futures)):
                    res = f.result()
                    if res: results.append(res)
                    p_bar.progress((i+1)/len(targets))
            if results:
                st.session_state['last_results'] = pd.DataFrame(results).sort_values(by=["점수"], ascending=False)
                st.success(f"{len(results)}개 종목 포착!")
            else: st.warning("포착된 종목이 없습니다.")

    if 'last_results' in st.session_state:
        st.dataframe(st.session_state['last_results'], use_container_width=True)
        sel_name = st.selectbox("차트 보기", st.session_state['last_results']['종목명'].tolist())
        if sel_name:
            clean_name = sel_name.split(" (")[0]
            code = st.session_state['last_results'][st.session_state['last_results']['종목명']==sel_name]['코드'].values[0]
            df_chart = logic.get_processed_data(code, period)
            if df_chart is not None:
                st.plotly_chart(logic.create_advanced_chart(df_chart, clean_name, sel_strats))

with tabs[1]:
    st.title("📅 자동 알림 스케줄")
    import uuid
    with st.expander("➕ 새 알림 추가"):
        f = st.selectbox("주기", ["매일", "매주 (월요일)", "매월 (1일)", "매월 (말일)"])
        all_s = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
        s = st.selectbox("전략", all_s)
        st.info("💡 **알림 시간 안내**: 서버의 안정적인 실행을 위해 모든 알림은 **오전 06:00 (KST)**에 일괄 발송되도록 고정됩니다.")
        if st.button("💾 알림 저장"):
            new_s = {"id": str(uuid.uuid4())[:8], "freq": f, "time": "06:00", "strategy": s, "target": "주식", "limit": 100}
            config['schedules'].append(new_s); logic.save_config(config)
            logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4))
            st.success("저장 완료!"); st.rerun()

    for i, s in enumerate(config.get('schedules', [])):
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 1, 1])
            c1.write(f"### 📡 {s['freq']} {s['time']} | {s['strategy']}")
            if c2.button("📡 발송", key=f"t_{s['id']}"):
                with st.spinner("발송 중..."):
                    df_l = logic.get_listing_data("주식").head(50)
                    res = []
                    with ThreadPoolExecutor(max_workers=5) as exe:
                        futures = [exe.submit(logic.process_stock_multi_worker, r.Symbol, r.Name, [s['strategy']], 'D') for r in df_l.itertuples()]
                        for f in as_completed(futures):
                            if f.result(): res.append(f.result())
                    logic.send_telegram_all(TG_TOKEN, TG_CHAT_ID, res, [s['strategy']], "주식")
                    st.success("발송 완료!")
            if c3.button("🗑️ 삭제", key=f"d_{s['id']}"):
                config['schedules'].pop(i); logic.save_config(config)
                logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)); st.rerun()

with tabs[2]:
    st.title("💰 스마트 배당금 계산기")
    
    if "portfolio" not in st.session_state:
        st.session_state.portfolio = []

    # [1] 입력 섹션
    with st.expander("➕ 보유 종목 추가", expanded=True):
        c1, c2, c3 = st.columns(3)
        
        # 검색용 리스트 로드 (캐시 활용 권장)
        if "search_list" not in st.session_state:
            with st.spinner("종목 리스트 로드 중..."):
                st.session_state.search_list = logic.get_searchable_list()
        
        selected_stock = c1.selectbox("종목 검색", 
                                     options=st.session_state.search_list,
                                     index=None,
                                     placeholder="종목명 또는 티커 입력",
                                     help="한국 및 미국(S&P 500) 종목 검색이 가능합니다.")
        
        in_qty = c2.number_input("보유 수량", min_value=1, value=10)
        in_price = c3.number_input("평균 단가", min_value=0.0, value=50000.0)
        
        if st.button("포트폴리오에 추가"):
            if selected_stock:
                # "Samsung Electronics (005930)" -> "005930" 추출
                import re
                match = re.search(r'\((.*?)\)', selected_stock)
                in_symbol = match.group(1) if match else selected_stock
                
                with st.spinner(f"{in_symbol} 데이터 조회 중..."):
                    details = logic.get_dividend_details(in_symbol)
                    if details:
                        details.update({"qty": in_qty, "avg_price": in_price})
                        st.session_state.portfolio.append(details)
                        st.success(f"{details['name']} 추가됨!")
                        st.rerun()
                    else: st.error("종목 정보를 찾을 수 없습니다.")
            else:
                st.warning("종목을 선택해주세요.")

    if st.session_state.portfolio:
        df_port = pd.DataFrame(st.session_state.portfolio)
        
        # [2] 요약 대시보드
        st.divider()
        total_invest = (df_port['qty'] * df_port['avg_price']).sum()
        total_div = (df_port['qty'] * df_port['dps']).sum()
        yoc = (total_div / total_invest * 100) if total_invest > 0 else 0
        
        m1, m2, c3, m4 = st.columns(4)
        m1.metric("총 투자금액", f"{total_invest:,.0f} {df_port['currency'].iloc[0]}")
        m2.metric("연간 예상 배당금", f"{total_div:,.0f} {df_port['currency'].iloc[0]}")
        c3.metric("월 평균 수령액", f"{total_div/12:,.0f}")
        m4.metric("평균 배당수익률(YOC)", f"{yoc:.2f}%")

        # [기능 1] 월별 배당 캘린더
        st.subheader("🗓️ 월별 배당 캘린더")
        monthly_data = {m: 0 for m in range(1, 13)}
        for p in st.session_state.portfolio:
            if p['months']:
                d_per_month = (p['qty'] * p['dps']) / len(p['months'])
                for m in p['months']: monthly_data[m] += d_per_month
        
        df_month = pd.DataFrame({"Month": [f"{m}월" for m in range(1, 13)], "Amount": list(monthly_data.values())})
        fig_cal = px.bar(df_month, x="Month", y="Amount", title="월별 배당금 분포", color="Amount", color_continuous_scale="Viridis")
        st.plotly_chart(fig_cal, use_container_width=True)

        # [기능 2] 배당 재투자 시뮬레이션
        st.subheader("📈 배당 재투자(DRIP) 시뮬레이션")
        years = st.slider("시뮬레이션 기간 (년)", 1, 30, 10)
        reinvest_rate = st.slider("배당 재투자 비율 (%)", 0, 100, 100) / 100
        
        values = [total_invest]
        current_val = total_invest
        for y in range(years):
            div = current_val * (yoc/100)
            current_val += (div * reinvest_rate) + (current_val * 0.05) # 연 5% 주가상승 가정
            values.append(current_val)
        
        fig_drip = px.line(x=list(range(years+1)), y=values, title=f"{years}년 후 예상 자산 변화 (재투자 포함)", labels={"x": "경과 년수", "y": "자산 가치"})
        st.plotly_chart(fig_drip, use_container_width=True)

        # [기능 3 & 4] 리스트 및 세금
        st.subheader("📋 포트폴리오 상세 및 세금 분석")
        df_display = df_port[['name', 'dps', 'yield', 'payout', 'qty']].copy()
        df_display['실수령액(세후)'] = (df_port['qty'] * df_port['dps'] * 0.846).round(0) # 15.4% 세금
        st.dataframe(df_display, use_container_width=True)
        
        if st.button("🗑️ 포트폴리오 초기화"):
            st.session_state.portfolio = []; st.rerun()
    else:
        st.info("보유 종목을 추가하여 배당 대시보드를 생성하세요.")

with tabs[3]:
    st.title("🛠️ 전략 가이드")
    all_s = ["정석 정배열 (추세추종)", "20월선 눌림목 (조정매수)", "거래량 폭발 (세력개입)", "5일 연속 상승세", "저평가 성장주 (퀀트)", "외인/기관 쌍끌이 매수", "꾸준한 배당주"]
    sel = st.selectbox("전략 선택", all_s)
    st.info(logic.get_strategy_desc(sel))

with tabs[4]:
    st.title("⚙️ 시스템 정보")
    if st.button("🚀 GitHub 강제 동기화"):
        if logic.update_config_to_github(GH_TOKEN, GH_REPO, json.dumps(config, indent=4)): st.success("동기화 성공!")
    st.divider()
    st.subheader("📜 실행 이력")
    if config.get('history'): st.table(pd.DataFrame(config['history']).head(10))
    else: st.write("이력이 없습니다.")
