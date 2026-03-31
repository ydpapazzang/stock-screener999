# VERSION: 1.0.2 (Fundamental Support)
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
    safe_config = {
        "schedules": config_data.get("schedules", []),
        "history": config_data.get("history", [])
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_config, f, ensure_ascii=False, indent=4)

def get_secret(key, default=None):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    return os.environ.get(key, default)

# --- [1] 데이터 및 지표 엔진 ---
def get_processed_data(symbol, period='M'):
    try:
        if period == 'M': days = 365*10
        elif period == 'W': days = 365*5
        else: days = 365*1
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        df = fdr.DataReader(symbol, start_date)
        if df is None or len(df) < 10: return None
        
        if period == 'M': 
            df_res = df.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        elif period == 'W':
            df_res = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        else:
            df_res = df
        
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma60'] = df_res['Close'].rolling(60).mean()
        df_res['ma30'] = df_res['Close'].rolling(30).mean() # 와인스타인용
        df_res['vol_ma5'] = df_res['Volume'].rolling(5).mean()
        
        delta = df_res['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df_res['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        
        df_res['std'] = df_res['Close'].rolling(20).std()
        df_res['bb_lower'] = df_res['ma20'] - (df_res['std'] * 2)
        
        return df_res.dropna()
    except: return None

def check_multi_signals(df, strategy_list):
    if df is None or len(df) < 2: return pd.Series(False, index=df.index if df is not None else [])
    final_cond = pd.Series(True, index=df.index)
    
    for strategy in strategy_list:
        if strategy == "정석 정배열 (추세추종)":
            cond = (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['Close'] > df['ma5'])
        elif strategy == "20월선 눌림목 (조정매수)":
            disp = (df['Close'] / df['ma20'] - 1) * 100
            cond = (df['ma60'] > df['ma60'].shift(1)) & (disp >= -2.0) & (disp <= 3.0)
        elif strategy == "거래량 폭발 (세력개입)":
            cond = (df['Volume'] > df['vol_ma5'] * 2.0) & (df['Close'] > df['Open'])
        elif strategy == "5일 연속 상승세":
            c = (df['Close'] > df['Close'].shift(1))
            cond = c & c.shift(1) & c.shift(2) & c.shift(3) & c.shift(4)
        elif strategy == "외인/기관 쌍끌이 매수":
            cond = (df['Close'] > df['Close'].shift(1)) & (df['Volume'] > df['vol_ma5'] * 1.5)
        elif strategy == "꾸준한 배당주":
            # 기술적 필터: 변동성 수축 및 바닥권 지지
            bb_w = (df['ma20'] - df['bb_lower']) / df['ma20']
            cond = (bb_w < 0.06) & (df['Close'] > df['ma60'] * 0.95)
        else:
            # 주봉/월봉 기타 전략들 (생략된 경우 True로 처리하여 영향 없게 함)
            cond = pd.Series(True, index=df.index)
        final_cond &= cond
    return final_cond

def get_fundamental_dividend(symbol):
    """yfinance를 이용해 배당 및 재무 데이터 검증"""
    try:
        # 한국 종목 코드 변환 (005930 -> 005930.KS)
        yf_sym = f"{symbol}.KS" if not symbol.isdigit() or int(symbol) < 900000 else f"{symbol}.KQ"
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        
        div_yield = info.get('dividendYield', 0) or 0
        payout = info.get('payoutRatio', 0) or 0
        
        # 1. 배당수익률 3% 이상 (0.03)
        # 2. 배당성향 30% 이상 (0.3)
        if div_yield < 0.03 or payout < 0.3:
            return False, 0
            
        # 순이익 데이터 확인 (최근 3년 연속 증가)
        earnings = ticker.financials.loc['Net Income']
        if len(earnings) >= 3:
            # earnings는 최신순이므로 역순으로 비교 (과거 < 최근)
            if earnings.iloc[0] > earnings.iloc[1] > earnings.iloc[2]:
                return True, round(div_yield * 100, 1)
        
        return False, 0
    except:
        return False, 0

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        # 1. 기술적 지표 체크
        if not check_multi_signals(df_data, strategy_list).iloc[-1]:
            return None
            
        # 2. '꾸준한 배당주' 전략 포함 시 재무 체크
        div_info = ""
        if "꾸준한 배당주" in strategy_list:
            is_good_div, yield_val = get_fundamental_dividend(symbol)
            if not is_good_div: return None
            div_info = f" (수익률: {yield_val}%)"
            
        # 3. 결과 요약
        curr = df_data.iloc[-1]
        match_count = len(strategy_list) # 기술적 필터 통과한 경우
        score = 100 # 기본 100점 (AND 필터 기준)
        
        return {
            "코드": symbol, "종목명": name + div_info, "점수": score, "현재가": f"{int(curr['Close']):,}",
            "승률": "N/A", "평균수익": "N/A", "신규감지": "Y" if not check_multi_signals(df_data, strategy_list).iloc[-2] else "N",
            "일치전략": ", ".join(strategy_list)
        }
    return None

def send_telegram_all(token, chat_id, results, strategy_names, target_type):
    if not token or not chat_id or not results: return False
    msg = f"🚀 *[전략 포착]* {target_type}\n🎯 전략: {', '.join(strategy_names)}\n📊 포착: {len(results)}개\n\n"
    sorted_res = sorted(results, key=lambda x: x.get('점수', 0), reverse=True)
    for i, item in enumerate(sorted_res[:10]):
        msg += f"{i+1}. *{item['종목명']}*\n   - 현재가: {item['현재가']} | 신규: {item['신규감지']}\n"
    msg += f"\n🔗 [스크리너 접속](https://stock-screener999-ztg2dqzbktgsfn5xxguc7t.streamlit.app/)"
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    return True

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
