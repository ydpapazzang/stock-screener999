# VERSION: 1.0.3 (Cleanup & Fix)
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import json
import os
from datetime import datetime, timedelta
import calendar
import streamlit as st
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

CONFIG_FILE = "config.json"

# --- [0] 보안 및 설정 관리 ---
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {"schedules": [], "history": []}

def save_config(config_data):
    # 기존 파일 로드 (secrets 보존을 위해)
    current_config = load_config()
    current_config.update(config_data)
    
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(current_config, f, ensure_ascii=False, indent=4)

def get_secret(key, default=None):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    return os.environ.get(key, default)

# --- [1] 데이터 및 지표 엔진 ---
def get_processed_data(symbol, period='M'):
    try:
        if period == 'M': days = 365*15 # 충분한 데이터 확보
        elif period == 'W': days = 365*7
        else: days = 365*2 # MA365를 위해 2년치 로드
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        df = fdr.DataReader(symbol, start_date)
        if df is None or len(df) < 10: return None
        
        if period == 'M': 
            df_res = df.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        elif period == 'W':
            df_res = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        else:
            df_res = df
        
        # 기본 지표들 (성능을 위해 필요한 것만 계산, 커스텀은 나중에 추가)
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma60'] = df_res['Close'].rolling(60).mean()
        df_res['vol_ma5'] = df_res['Volume'].rolling(5).mean()
        
        return df_res.dropna()
    except: return None

def get_indicator_val(df, key):
    """지표 이름(종가, MA20 등)에 따른 컬럼 또는 계산값 반환"""
    if key == "종가": return df['Close']
    if key.startswith("MA"):
        try:
            n = int(key[2:])
            return df['Close'].rolling(n).mean()
        except: return None
    return None

def check_multi_signals(df, strategy_list):
    if df is None or len(df) < 2: return pd.Series(False, index=df.index if df is not None else [])
    final_cond = pd.Series(True, index=df.index)
    
    config = load_config()
    custom_strats = {s['name']: s for s in config.get('custom_strategies', [])}
    
    for strategy in strategy_list:
        if strategy in custom_strats:
            # --- [커스텀 전략 처리] ---
            s_data = custom_strats[strategy]
            c_cond = pd.Series(True, index=df.index)
            for cond in s_data.get('conditions', []):
                # cond: {"a": "종가", "b": "MA20", "period": 0}
                val_a = get_indicator_val(df, cond['a'])
                val_b = get_indicator_val(df, cond['b'])
                period = int(cond.get('period', 0))
                
                if val_a is not None and val_b is not None:
                    # shift(period)를 통해 n봉전 비교 구현
                    c_cond &= (val_a.shift(period) >= val_b.shift(period))
            cond = c_cond
        elif strategy == "정석 정배열 (추세추종)":
            ma5 = df['Close'].rolling(5).mean()
            ma20 = df['Close'].rolling(20).mean()
            ma60 = df['Close'].rolling(60).mean()
            cond = (ma5 > ma20) & (ma20 > ma60) & (df['Close'] > ma5)
        elif strategy == "20월선 눌림목 (조정매수)":
            ma20 = df['Close'].rolling(20).mean()
            ma60 = df['Close'].rolling(60).mean()
            disp = (df['Close'] / ma20 - 1) * 100
            cond = (ma60 > ma60.shift(1)) & (disp >= -2.0) & (disp <= 3.0)
        elif strategy == "거래량 폭발 (세력개입)":
            vol_ma5 = df['Volume'].rolling(5).mean()
            cond = (df['Volume'] > vol_ma5 * 2.0) & (df['Close'] > df['Open'])
        elif strategy == "5일 연속 상승세":
            c = (df['Close'] > df['Close'].shift(1))
            cond = c & c.shift(1) & c.shift(2) & c.shift(3) & c.shift(4)
        elif strategy == "외인/기관 쌍끌이 매수":
            vol_ma5 = df['Volume'].rolling(5).mean()
            cond = (df['Close'] > df['Close'].shift(1)) & (df['Volume'] > vol_ma5 * 1.5)
        elif strategy == "꾸준한 배당주":
            ma60 = df['Close'].rolling(60).mean()
            ma20 = df['Close'].rolling(20).mean()
            std = df['Close'].rolling(20).std()
            bb_lower = ma20 - (std * 2)
            bb_w = (ma20 - bb_lower) / ma20
            cond = (bb_w < 0.06) & (df['Close'] > ma60 * 0.95)
        else:
            cond = pd.Series(True, index=df.index)
        final_cond &= cond
    return final_cond

def get_fundamental_dividend(symbol):
    try:
        yf_sym = f"{symbol}.KS" if not symbol.isdigit() or int(symbol) < 900000 else f"{symbol}.KQ"
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        div_yield = info.get('dividendYield', 0) or 0
        payout = info.get('payoutRatio', 0) or 0
        if div_yield < 0.03 or payout < 0.3: return False, 0
        earnings = ticker.financials.loc['Net Income']
        if len(earnings) >= 3:
            if earnings.iloc[0] > earnings.iloc[1] > earnings.iloc[2]:
                return True, round(div_yield * 100, 1)
        return False, 0
    except: return False, 0

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        if not check_multi_signals(df_data, strategy_list).iloc[-1]: return None
        div_info = ""
        if "꾸준한 배당주" in strategy_list:
            is_good_div, yield_val = get_fundamental_dividend(symbol)
            if not is_good_div: return None
            div_info = f" (수익률: {yield_val}%)"
        curr = df_data.iloc[-1]
        return {
            "코드": symbol, "종목명": name + div_info, "점수": 100, "현재가": f"{int(curr['Close']):,}",
            "승률": "N/A", "평균수익": "N/A", "신규감지": "Y" if not check_multi_signals(df_data, strategy_list).iloc[-2] else "N",
            "일치전략": ", ".join(strategy_list)
        }
    return None

def get_dividend_details(symbol):
    try:
        yf_sym = f"{symbol}.KS" if symbol.isdigit() and int(symbol) < 900000 else (f"{symbol}.KQ" if symbol.isdigit() else symbol)
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        dps = info.get('trailingAnnualDividendRate', 0) or info.get('dividendRate', 0) or 0
        div_yield = info.get('dividendYield', 0) or 0
        payout = info.get('payoutRatio', 0) or 0
        history = ticker.dividends
        months = []
        if not history.empty:
            months = sorted(list(set(history.tail(4).index.month)))
        return {
            "name": info.get('shortName', symbol), "dps": dps, "yield": round(div_yield * 100, 2),
            "payout": round(payout * 100, 1), "months": months, "growth": round(info.get('dividendGrowthRate', 0)*100, 1),
            "currency": info.get('currency', 'KRW')
        }
    except: return None

def send_telegram_all(token, chat_id, results, strategy_names, target_type):
    if not token or not chat_id:
        print("Telegram Error: Token or Chat ID missing.")
        return False
    
    if not results:
        msg = f"🔔 <b>[알림]</b> {target_type}\n🎯 전략: {', '.join(strategy_names)}\n\n현재 조건에 일치하는 종목이 없습니다."
    else:
        msg = f"🚀 <b>[전략 포착]</b> {target_type}\n🎯 전략: {', '.join(strategy_names)}\n📊 포착: {len(results)}개\n\n"
        for i, item in enumerate(results[:10]):
            msg += f"{i+1}. <b>{item['종목명']}</b>\n   - 현재가: {item['현재가']} | 신규: {item['신규감지']}\n"
        msg += f"\n🔗 <a href='https://stock-screener999-ztg2dqzbktgsfn5xxguc7t.streamlit.app/'>스크리너 접속</a>"
    
    res = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage", 
        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    )
    if res.status_code != 200:
        print(f"Telegram API Error: {res.status_code} - {res.text}")
        return False
    return True

def get_searchable_list():
    """검색용 통합 종목 리스트 (KRX + 주요 미국주식)"""
    try:
        # 1. 한국 종목
        df_kr = fdr.StockListing('KRX')[['Symbol', 'Name']]
        kr_list = [f"{row.Name} ({row.Symbol})" for row in df_kr.itertuples()]
        
        # 2. 주요 미국 종목 (S&P 500)
        df_us = fdr.StockListing('S&P500')[['Symbol', 'Name']]
        us_list = [f"{row.Name} ({row.Symbol})" for row in df_us.itertuples()]
        
        return sorted(list(set(kr_list + us_list)))
    except:
        return ["Samsung Electronics (005930)", "Apple (AAPL)", "Microsoft (MSFT)"]

def update_config_to_github(token, repo, content):
    if not token or not repo: return False
    url = f"https://api.github.com/repos/{repo}/contents/config.json"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    res = requests.get(url, headers=headers)
    sha = res.json().get("sha") if res.status_code == 200 else ""
    import base64
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    payload = {"message": "Update config", "content": encoded, "sha": sha}
    return requests.put(url, headers=headers, json=payload).status_code in [200, 201]

def get_strategy_desc(s):
    descs = {
        "정석 정배열 (추세추종)": "5 > 20 > 60 이평선 정배열 구간.",
        "20월선 눌림목 (조정매수)": "장기 추세 살아있는 상태에서 20월선 근처 조정.",
        "거래량 폭발 (세력개입)": "거래량이 5일 평균의 2배 이상 폭증.",
        "5일 연속 상승세": "5거래일 연속 종가 상승 (단기 모멘텀).",
        "외인/기관 쌍끌이 매수": "수급 주체의 동반 매수로 추정되는 강세.",
        "꾸준한 배당주": "배당수익률 3%↑, 배당성향 30%↑, 순이익 3년 연속 증가 우량주."
    }
    return descs.get(s, "설명 없음")

def create_advanced_chart(df, name, strats):
    df_p = df.tail(48)
    fig = go.Figure(data=[go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="Price")])
    fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma20'], name="MA20", line=dict(color='red')))
    fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma60'], name="MA60", line=dict(color='purple')))
    fig.update_layout(title=f"{name} Chart", template="plotly_dark", xaxis_rangeslider_visible=False)
    return fig

def get_listing_data(target):
    try:
        df = fdr.StockListing('ETF/KR' if target == "ETF" else 'KRX')
        df = df.rename(columns={'Code': 'Symbol', 'Marcap': '시가총액', 'Amount': '거래대금'})
        if '시가총액' in df.columns: df['시총(억)'] = (df['시가총액'] / 100000000).round(0)
        if '거래대금' in df.columns: df['거래대금(억)'] = (df['거래대금'] / 100000000).round(0)
        return df
    except: return pd.DataFrame()
