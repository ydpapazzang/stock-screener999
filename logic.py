# VERSION: 1.0.6 (Full Feature Integration)
import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import json
import os
import io
import base64
from datetime import datetime, timedelta
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

CONFIG_FILE = "config.json"

# --- [0] 설정 및 보안 ---
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {"schedules": [], "history": [], "custom_strategies": [], "watchlist": []}

def save_config(config_data):
    curr = load_config()
    curr.update(config_data)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(curr, f, ensure_ascii=False, indent=4)

def get_secret(key, default=None):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    return os.environ.get(key, default)

# --- [1] 데이터 엔진 ---
@st.cache_data(ttl=3600)
def get_listing_data(target):
    try:
        mapping = {"한국 ETF": "ETF/KR", "미국 나스닥": "NASDAQ", "미국 ETF": "ETF/US", "KOSPI/KOSDAQ": "KRX"}
        market = mapping.get(target, "KRX")
        df = fdr.StockListing(market)
        df = df.rename(columns={'Code': 'Symbol', 'Marcap': '시가총액', 'Name': 'Name'})
        
        if '시가총액' in df.columns: df['시총(억)'] = (df['시가총액'] / 100000000).round(0)
        elif 'MarketCap' in df.columns: df['시총(억)'] = (df['MarketCap'] * 1350 / 100000000).round(0)
        else: df['시총(억)'] = 0
        return df[['Symbol', 'Name', '시총(억)']]
    except: return pd.DataFrame()

@st.cache_data(ttl=1800)
def get_processed_data(symbol, period='D'):
    try:
        is_kr = symbol.isdigit() and len(symbol) == 6
        yf_sym = f"{symbol}.KS" if is_kr and int(symbol) < 900000 else (f"{symbol}.KQ" if is_kr else symbol)
        
        ticker = yf.Ticker(yf_sym)
        df = ticker.history(period="5y", interval="1d")
        if df.empty: return None
        
        df.index = pd.to_datetime(df.index).tz_localize(None)
        if period == 'M': df = df.resample('ME').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        elif period == 'W': df = df.resample('W').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        
        # 지표 선계산
        for n in [5, 10, 20, 60, 120]: df[f'ma{n}'] = df['Close'].rolling(n).mean()
        df['vol_ma5'] = df['Volume'].rolling(5).mean()
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        return df.dropna()
    except: return None

# --- [2] 전략 엔진 ---
def get_indicator_val(df, key):
    m = {"종가":"Close", "거래량":"Volume", "RSI":"rsi"}
    if key in m: return df[m[key]]
    if key.startswith("MA"): return df[f'ma{key[2:]}'] if f'ma{key[2:]}' in df.columns else df['Close'].rolling(int(key[2:])).mean()
    if key.startswith("VMA"): return df['Volume'].rolling(int(key[3:])).mean()
    try: return pd.Series(float(key), index=df.index)
    except: return None

def check_multi_signals(df, strategy_list):
    if df is None or len(df) < 2: return pd.Series(False, index=df.index if df is not None else [])
    final = pd.Series(True, index=df.index)
    customs = {s['name']: s for s in load_config().get('custom_strategies', [])}
    
    for s_name in strategy_list:
        if s_name in customs:
            c_cond = pd.Series(True, index=df.index)
            for cond in customs[s_name]['conditions']:
                v_a = get_indicator_val(df, cond['a'])
                v_b = get_indicator_val(df, cond['b'].split(" * ")[0])
                if " * " in str(cond['b']): v_b *= float(cond['b'].split(" * ")[1])
                
                op = cond.get('op', '>=')
                res = (v_a >= v_b) if op==">=" else ((v_a <= v_b) if op=="<=" else (v_a > v_b if op==">" else v_a < v_b))
                if cond.get('p_type') == "within": c_cond &= res.rolling(int(cond['period'])+1, min_periods=1).max().astype(bool)
                else: c_cond &= res.shift(int(cond['period']))
            cond_res = c_cond
        elif s_name == "정석 정배열 (추세추종)": cond_res = (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['Close'] > df['ma5'])
        elif s_name == "거래량 폭발 (세력개입)": cond_res = (df['Volume'] > df['vol_ma5'] * 2.0) & (df['Close'] > df['Open'])
        else: cond_res = pd.Series(True, index=df.index)
        final &= cond_res
    return final

# --- [3] 고급 기능 (백테스트, 알림, 링크) ---
def run_backtest(df, strategy_list):
    try:
        sig = check_multi_signals(df, strategy_list)
        res, in_pos, buy_p = [], False, 0
        for i in range(1, len(df)):
            if not in_pos and sig.iloc[i] and not sig.iloc[i-1]: buy_p, in_pos = df.iloc[i]['Close'], True
            elif in_pos and (not sig.iloc[i] or i == len(df)-1):
                res.append((df.iloc[i]['Close']/buy_p - 1)*100); in_pos = False
        if not res: return 0, 0, 0
        return round(len([r for r in res if r>0])/len(res)*100, 1), round(sum(res)/len(res), 2), len(res)
    except: return 0, 0, 0

def send_telegram_with_chart(token, chat_id, symbol, name, df, strategy_names):
    try:
        df_p = df.tail(60)
        fig = go.Figure(data=[go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'])])
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma20'], name="MA20", line=dict(color='yellow', width=1)))
        fig.update_layout(template="plotly_dark", xaxis_rangeslider_visible=False, margin=dict(l=5,r=5,t=5,b=5), height=400)
        img = fig.to_image(format="png", width=700, height=400)
        cap = f"🚀 <b>{name} ({symbol})</b>\n🎯 {', '.join(strategy_names)}\n💰 현재가: {df.iloc[-1]['Close']:,.2f}"
        return requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo':io.BytesIO(img)}, data={'chat_id':chat_id, 'caption':cap, 'parse_mode':'HTML'}).status_code == 200
    except: return False

def get_external_link(symbol):
    is_kr = symbol.isdigit() and len(symbol) == 6
    if is_kr: return {"Naver": f"https://finance.naver.com/item/main.naver?code={symbol}", "TradingView": f"https://www.tradingview.com/symbols/KRX-{symbol}/"}
    return {"Yahoo": f"https://finance.yahoo.com/quote/{symbol}", "TradingView": f"https://www.tradingview.com/symbols/NASDAQ-{symbol}/"}

def get_dividend_details(symbol):
    try:
        is_kr = symbol.isdigit() and len(symbol) == 6
        yf_sym = f"{symbol}.KS" if is_kr and int(symbol) < 900000 else (f"{symbol}.KQ" if is_kr else symbol)
        t = yf.Ticker(yf_sym); i = t.info
        hist = t.dividends
        months = sorted(list(set(hist.tail(8).index.month))) if not hist.empty else []
        return {"name": i.get('shortName', symbol), "dps": i.get('dividendRate', 0) or 0, "yield": round((i.get('dividendYield', 0) or 0)*100, 2), "currency": i.get('currency', 'KRW'), "payout": round(i.get('payoutRatio', 0)*100, 1), "months": months}
    except: return None

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    try:
        df = get_processed_data(symbol, period_key)
        if df is not None and len(df) >= 2:
            sig = check_multi_signals(df, strategy_list)
            if not sig.iloc[-1]: return None
            return {"코드": symbol, "종목명": name, "점수": 100, "현재가": f"{df.iloc[-1]['Close']:,.2f}", "신규": "Y" if not sig.iloc[-2] else "N", "일치전략": ", ".join(strategy_list)}
    except: pass
    return None

def get_searchable_list():
    try:
        df = fdr.StockListing('KRX')[['Symbol', 'Name']]
        return sorted([f"{r.Name} ({r.Symbol})" for r in df.itertuples()])
    except: return ["삼성전자 (005930)"]

def update_config_to_github(token, repo, content):
    if not token or not repo: return False
    url = f"https://api.github.com/repos/{repo}/contents/config.json"
    h = {"Authorization": f"token {token}"}
    r = requests.get(url, headers=h)
    sha = r.json().get("sha") if r.status_code == 200 else ""
    return requests.put(url, headers=h, json={"message":"Update config", "content": base64.b64encode(content.encode()).decode(), "sha":sha}).status_code in [200, 201]

def create_advanced_chart(df, name, strats):
    df_p = df.tail(60)
    fig = go.Figure(data=[go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'])])
    for ma, color in {'ma5':'white','ma20':'yellow','ma60':'orange','ma120':'purple'}.items():
        if ma in df_p.columns: fig.add_trace(go.Scatter(x=df_p.index, y=df_p[ma], name=ma.upper(), line=dict(color=color, width=1)))
    fig.update_layout(title=f"📈 {name} 분석 차트", template="plotly_dark", xaxis_rangeslider_visible=False, height=500)
    return fig
