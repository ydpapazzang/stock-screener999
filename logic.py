# VERSION: 1.1.2 (Universal Global Search Fix)
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
import matplotlib.pyplot as plt

CONFIG_FILE = "config.json"

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

@st.cache_data(ttl=86400) # 24시간 캐싱
def get_searchable_list():
    """한국/미국 주식 및 ETF 전체 리스트 통합 생성"""
    final_list = []
    
    # 1. 한국 시장 (주식 + ETF)
    try:
        df_krx = fdr.StockListing('KRX')[['Symbol', 'Name']]
        final_list += [f"{r.Name} ({r.Symbol})" for r in df_krx.itertuples()]
        df_kr_etf = fdr.StockListing('ETF/KR')[['Symbol', 'Name']]
        final_list += [f"[ETF] {r.Name} ({r.Symbol})" for r in df_kr_etf.itertuples()]
    except: pass

    # 2. 미국 시장 (NASDAQ + NYSE + ETF)
    try:
        # 나스닥
        df_nas = fdr.StockListing('NASDAQ')[['Symbol', 'Name']]
        final_list += [f"{r.Name} ({r.Symbol})" for r in df_nas.itertuples()[:2000]] # 주요종목 2000개
        # NYSE
        df_nyse = fdr.StockListing('NYSE')[['Symbol', 'Name']]
        final_list += [f"{r.Name} ({r.Symbol})" for r in df_nyse.itertuples()[:1000]]
        # 미국 ETF
        df_us_etf = fdr.StockListing('ETF/US')[['Symbol', 'Name']]
        final_list += [f"[US ETF] {r.Name} ({r.Symbol})" for r in df_us_etf.itertuples()[:500]]
    except: pass

    if not final_list:
        return ["삼성전자 (005930)", "SK하이닉스 (000660)", "Apple (AAPL)", "NVIDIA (NVDA)", "Tesla (TSLA)"]
    
    return sorted(list(set(final_list)))

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
        sym_str = str(symbol)
        is_kr = sym_str.isdigit() and len(sym_str) == 6
        yf_sym = f"{sym_str}.KS" if is_kr and int(sym_str) < 900000 else (f"{sym_str}.KQ" if is_kr else sym_str)
        
        ticker = yf.Ticker(yf_sym)
        df = ticker.history(period="5y", interval="1d")
        if df.empty: return None
        
        df.index = pd.to_datetime(df.index).tz_localize(None)
        if period == 'M': df = df.resample('ME').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        elif period == 'W': df = df.resample('W').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'})
        
        for n in [5, 10, 20, 60, 120]: df[f'ma{n}'] = df['Close'].rolling(n, min_periods=1).mean()
        df['vol_ma5'] = df['Volume'].rolling(5, min_periods=1).mean()
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14, min_periods=1).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14, min_periods=1).mean()
        df['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        return df
    except: return None

def get_indicator_val(df, key):
    k = str(key).upper()
    if k == "종가": return df['Close']
    if k == "거래량": return df['Volume']
    if k == "RSI": return df['rsi']
    if k.startswith("MA"):
        n = int(k[2:])
        col = f'ma{n}'
        return df[col] if col in df.columns else df['Close'].rolling(n, min_periods=1).mean()
    if k.startswith("VMA"): return df['Volume'].rolling(int(k[3:]), min_periods=1).mean()
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
                b_raw = str(cond['b'])
                if " * " in b_raw:
                    bk, mult = b_raw.split(" * ")
                    v_b = get_indicator_val(df, bk) * float(mult)
                else: v_b = get_indicator_val(df, b_raw)
                op = cond.get('op', '>=')
                if op == ">=": res = (v_a >= v_b)
                elif op == "<=": res = (v_a <= v_b)
                elif op == ">": res = (v_a > v_b)
                else: res = (v_a < v_b)
                if cond.get('p_type') == "within": c_cond &= res.rolling(int(cond['period'])+1, min_periods=1).max().fillna(0).astype(bool)
                else: c_cond &= res.shift(int(cond['period'])).fillna(False)
            cond_res = c_cond
        elif s_name == "정석 정배열 (추세추종)": cond_res = (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['Close'] > df['ma5'])
        elif s_name == "거래량 폭발 (세력개입)": cond_res = (df['Volume'] > df['vol_ma5'] * 2.0) & (df['Close'] > df['Open'])
        else: cond_res = pd.Series(True, index=df.index)
        final &= cond_res
    return final.fillna(False)

def run_backtest(df, strategy_list):
    try:
        sig = check_multi_signals(df, strategy_list)
        res, in_pos, buy_p = [], False, 0
        for i in range(1, len(df)):
            if not in_pos and sig.iloc[i] and not sig.iloc[i-1]: buy_p, in_pos = df.iloc[i]['Close'], True
            elif in_pos and (not sig.iloc[i] or i == len(df)-1):
                res.append((df.iloc[i]['Close']/buy_p - 1)*100); in_pos = False
        if not res: return 0, 0, 0
        win = len([r for r in res if r>0])/len(res)*100
        return round(win, 1), round(sum(res)/len(res), 2), len(res)
    except: return 0, 0, 0

def send_telegram_all(token, chat_id, results, strategy_names, target_type):
    if not token or not chat_id: return False
    msg = f"🚀 <b>[{target_type}] 포착</b>\n🎯 전략: {', '.join(strategy_names)}\n📊 포착: {len(results)}개\n\n"
    if not results: msg = f"🔔 <b>[{target_type}]</b>\n🎯 전략: {', '.join(strategy_names)}\n\n현재 조건에 맞는 종목이 없습니다."
    else:
        for i, item in enumerate(results[:15]): msg += f"{i+1}. <b>{item['종목명']}</b> ({item['현재가']})\n"
        if len(results) > 15: msg += f"\n...외 {len(results)-15}건 더보기"
    msg += f"\n\n🔗 <a href='https://stock-screener999-ztg2dqzbktgsfn5xxguc7t.streamlit.app/'>스크리너 접속하기</a>"
    try: return requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, timeout=15).status_code == 200
    except: return False

def send_telegram_with_chart(token, chat_id, symbol, name, df, strategy_names):
    if not token or not chat_id: return False
    try:
        plt.style.use('dark_background')
        df_p = df.tail(60)
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(df_p.index, df_p['Close'], label='Price', color='white', linewidth=1.5)
        if 'ma20' in df_p.columns: ax.plot(df_p.index, df_p['ma20'], label='MA20', color='yellow', linewidth=1, alpha=0.8)
        if 'ma60' in df_p.columns: ax.plot(df_p.index, df_p['ma60'], label='MA60', color='orange', linewidth=1, alpha=0.8)
        ax.set_title(f"🚀 {name} ({symbol})", fontsize=12, color='cyan')
        ax.legend(loc='best', fontsize=8); ax.grid(True, alpha=0.2)
        plt.tight_layout()
        buf = io.BytesIO(); plt.savefig(buf, format='png', dpi=100); plt.close(fig); buf.seek(0)
        cap = f"🚀 <b>{name} ({symbol})</b>\n🎯 {', '.join(strategy_names)}\n💰 현재가: {df.iloc[-1]['Close']:,.2f}"
        return requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files={'photo': buf}, data={'chat_id':chat_id, 'caption':cap, 'parse_mode':'HTML'}, timeout=30).status_code == 200
    except: return False

def get_external_link(symbol):
    is_kr = str(symbol).isdigit() and len(str(symbol)) == 6
    if is_kr: return {"Naver": f"https://finance.naver.com/item/main.naver?code={symbol}", "TradingView": f"https://www.tradingview.com/symbols/KRX-{symbol}/"}
    return {"Yahoo": f"https://finance.yahoo.com/quote/{symbol}", "TradingView": f"https://www.tradingview.com/symbols/NASDAQ-{symbol}/"}

def get_dividend_details(symbol):
    try:
        sym_str = str(symbol)
        is_kr = sym_str.isdigit() and len(sym_str) == 6
        yf_sym = f"{sym_str}.KS" if is_kr and int(sym_str) < 900000 else (f"{sym_str}.KQ" if is_kr else sym_str)
        t = yf.Ticker(yf_sym); i = t.info; h = t.dividends
        m = sorted(list(set(h.tail(8).index.month))) if not h.empty else []
        return {"name": i.get('shortName', symbol), "dps": i.get('dividendRate', 0) or 0, "yield": round((i.get('dividendYield', 0) or 0)*100, 2), "currency": i.get('currency', 'KRW'), "payout": round(i.get('payoutRatio', 0)*100, 1), "months": m}
    except: return None

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    try:
        df = get_processed_data(symbol, period_key)
        if df is not None and len(df) >= 2:
            sig = check_multi_signals(df, strategy_list)
            if sig.iloc[-1]:
                df_3y = df.tail(252*3) if period_key=='D' else (df.tail(52*3) if period_key=='W' else df.tail(12*3))
                win, ret, cnt = run_backtest(df_3y, strategy_list)
                status = "🚀 최초진입" if not sig.iloc[-2] else "📈 추세유지"
                return {"코드": symbol, "종목명": name, "현재가": f"{df.iloc[-1]['Close']:,.2f}", "상태": status, "승률": f"{win}%", "수익률": f"{ret}%", "일치전략": ", ".join(strategy_list)}
    except: pass
    return None

def update_config_to_github(token, repo, content):
    if not token or not repo: return False
    url = f"https://api.github.com/repos/{repo}/contents/config.json"
    h = {"Authorization": f"token {token}"}; r = requests.get(url, headers=h)
    sha = r.json().get("sha") if r.status_code == 200 else ""
    return requests.put(url, headers=h, json={"message":"Update config", "content": base64.b64encode(content.encode()).decode(), "sha":sha}).status_code in [200, 201]

def create_advanced_chart(df, name, strats):
    df_p = df.tail(60)
    fig = go.Figure(data=[go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'])])
    for ma, color in {'ma5':'white','ma20':'yellow','ma60':'orange'}.items():
        if ma in df_p.columns: fig.add_trace(go.Scatter(x=df_p.index, y=df_p[ma], name=ma.upper(), line=dict(color=color, width=1)))
    fig.update_layout(title=f"📈 {name} 분석 차트", template="plotly_dark", xaxis_rangeslider_visible=False, height=500)
    return fig
