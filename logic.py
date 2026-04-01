# VERSION: 1.0.4 (Multi-Market Fix)
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
    return {"schedules": [], "history": [], "custom_strategies": []}

def save_config(config_data):
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
def run_backtest(df, strategy_list):
    """전략이 TRUE인 구간 동안 보유했을 때의 성과 계산 (Buy on TRUE, Sell on FALSE)"""
    try:
        signals = check_multi_signals(df, strategy_list)
        results = []
        
        in_position = False
        buy_price = 0
        
        for i in range(1, len(df)):
            # 진입: 이전은 False, 현재는 True
            if not in_position and signals.iloc[i] and not signals.iloc[i-1]:
                buy_price = df.iloc[i]['Close']
                in_position = True
            
            # 청산: 이전에 True였는데 현재 False로 변함 (또는 마지막 데이터)
            elif in_position:
                if not signals.iloc[i] or i == len(df) - 1:
                    sell_price = df.iloc[i]['Close']
                    profit = (sell_price / buy_price - 1) * 100
                    results.append(profit)
                    in_position = False
        
        if not results: return 0, 0, 0
        
        win_rate = len([r for r in results if r > 0]) / len(results) * 100
        avg_return = sum(results) / len(results)
        return round(win_rate, 1), round(avg_return, 2), len(results)
    except: return 0, 0, 0

def get_external_link(symbol, market_type='KR'):
    """외부 금융 사이트 연결 링크 생성"""
    is_kr = symbol.isdigit() and len(symbol) == 6
    if is_kr:
        return {
            "Naver": f"https://finance.naver.com/item/main.naver?code={symbol}",
            "TradingView": f"https://www.tradingview.com/symbols/KRX-{symbol}/",
            "Yahoo": f"https://finance.yahoo.com/quote/{symbol}.KS" if int(symbol) < 900000 else f"https://finance.yahoo.com/quote/{symbol}.KQ"
        }
    else:
        return {
            "Yahoo": f"https://finance.yahoo.com/quote/{symbol}",
            "TradingView": f"https://www.tradingview.com/symbols/NASDAQ-{symbol}/",
            "Finviz": f"https://finviz.com/quote.ashx?t={symbol}"
        }

def send_telegram_with_chart(token, chat_id, symbol, name, df, strategy_names):
    """차트 이미지와 함께 텔레그램 알림 전송"""
    try:
        import io
        # 차트 생성 (최근 60봉)
        df_p = df.tail(60)
        fig = go.Figure(data=[go.Candlestick(
            x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="Price"
        )])
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma20'], name="MA20", line=dict(color='yellow', width=1)))
        fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma60'], name="MA60", line=dict(color='orange', width=1)))
        
        fig.update_layout(
            title=f"🚀 {name} ({symbol}) - {', '.join(strategy_names)}",
            template="plotly_dark", xaxis_rangeslider_visible=False,
            margin=dict(l=10, r=10, t=40, b=10)
        )
        
        # 이미지를 바이트로 변환
        img_bytes = fig.to_image(format="png", width=800, height=500)
        
        caption = f"🚀 <b>[전략 포착] {name} ({symbol})</b>\n🎯 전략: {', '.join(strategy_names)}\n💰 현재가: {df.iloc[-1]['Close']:,.0f}\n\n#주식스캐너 #자동알림"
        
        files = {'photo': ('chart.png', io.BytesIO(img_bytes), 'image/png')}
        data = {'chat_id': chat_id, 'caption': caption, 'parse_mode': 'HTML'}
        
        requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", files=files, data=data)
        return True
    except Exception as e:
        print(f"Chart Send Error: {e}")
        return False

def get_processed_data(symbol, period='M'):
    try:
        # KR/US 접미사 보정 (yfinance 호환성)
        is_kr = symbol.isdigit() and len(symbol) == 6
        yf_sym = symbol
        if is_kr:
            yf_sym = f"{symbol}.KS" if int(symbol) < 900000 else f"{symbol}.KQ"
            
        if period == 'M': days = 365*15
        elif period == 'W': days = 365*7
        else: days = 365*2
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        # fdr 대신 yfinance로 통일 (미국주식 안정성 및 차트 데이터 위해)
        ticker = yf.Ticker(yf_sym)
        df = ticker.history(start=start_date, interval='1d')
        
        if df is None or len(df) < 10: return None
        
        # 컬럼명 표준화 (Capitalized)
        df.index = pd.to_datetime(df.index).tz_localize(None)
        
        if period == 'M': 
            df_res = df.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        elif period == 'W':
            df_res = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        else:
            df_res = df
        
        # 지표 계산
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma60'] = df_res['Close'].rolling(60).mean()
        df_res['vol_ma5'] = df_res['Volume'].rolling(5).mean()
        
        delta = df_res['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df_res['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-9))))
        
        return df_res.dropna()
    except: return None

def get_indicator_val(df, key):
    if key == "종가": return df['Close']
    if key == "거래량": return df['Volume']
    if key == "RSI": return df['rsi']
    if key.startswith("MA"):
        try: return df['Close'].rolling(int(key[2:])).mean()
        except: return None
    if key.startswith("VMA"):
        try: return df['Volume'].rolling(int(key[3:])).mean()
        except: return None
    try: return pd.Series(float(key), index=df.index)
    except: return None

def check_multi_signals(df, strategy_list):
    if df is None or len(df) < 2: return pd.Series(False, index=df.index if df is not None else [])
    final_cond = pd.Series(True, index=df.index)
    config = load_config()
    custom_strats = {s['name']: s for s in config.get('custom_strategies', [])}
    
    for strategy in strategy_list:
        if strategy in custom_strats:
            s_data = custom_strats[strategy]
            c_cond = pd.Series(True, index=df.index)
            for cond in s_data.get('conditions', []):
                val_a = get_indicator_val(df, cond['a'])
                b_key = cond['b']
                if " * " in str(b_key):
                    base_b, mult = b_key.split(" * ")
                    val_b = get_indicator_val(df, base_b) * float(mult)
                else:
                    val_b = get_indicator_val(df, b_key)
                
                if val_a is not None and val_b is not None:
                    p_type = cond.get('p_type', 'ago')
                    period = int(cond.get('period', 0))
                    op = cond.get('op', '>=')
                    if op == ">=": base_cond = (val_a >= val_b)
                    elif op == "<=": base_cond = (val_a <= val_b)
                    elif op == ">": base_cond = (val_a > val_b)
                    elif op == "<": base_cond = (val_a < val_b)
                    else: base_cond = (val_a >= val_b)
                    
                    if p_type == "within":
                        c_cond &= base_cond.rolling(window=period + 1, min_periods=1).max().astype(bool)
                    else:
                        c_cond &= base_cond.shift(period)
            cond = c_cond
        elif strategy == "정석 정배열 (추세추종)":
            ma5, ma20, ma60 = df['Close'].rolling(5).mean(), df['Close'].rolling(20).mean(), df['Close'].rolling(60).mean()
            cond = (ma5 > ma20) & (ma20 > ma60) & (df['Close'] > ma5)
        elif strategy == "거래량 폭발 (세력개입)":
            vol_ma5 = df['Volume'].rolling(5).mean()
            cond = (df['Volume'] > vol_ma5 * 2.0) & (df['Close'] > df['Open'])
        else: cond = pd.Series(True, index=df.index)
        final_cond &= cond
    return final_cond

def get_fundamental_dividend(yf_sym):
    try:
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        div_yield = info.get('dividendYield', 0) or 0
        if div_yield < 0.03: return False, 0
        return True, round(div_yield * 100, 1)
    except: return False, 0

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    is_kr = symbol.isdigit() and len(symbol) == 6
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        if not check_multi_signals(df_data, strategy_list).iloc[-1]: return None
        div_info = ""
        if "꾸준한 배당주" in strategy_list:
            yf_sym = f"{symbol}.KS" if is_kr and int(symbol) < 900000 else (f"{symbol}.KQ" if is_kr else symbol)
            is_good, y_val = get_fundamental_dividend(yf_sym)
            if is_good: div_info = f" ({y_val}%)"
        curr = df_data.iloc[-1]
        return {
            "코드": symbol, "종목명": name + div_info, "점수": 100, 
            "현재가": f"{curr['Close']:,.2f}" if not is_kr else f"{int(curr['Close']):,}",
            "신규감지": "Y" if not check_multi_signals(df_data, strategy_list).iloc[-2] else "N",
            "일치전략": ", ".join(strategy_list)
        }
    return None

def get_listing_data(target):
    try:
        if target == "한국 ETF": market = 'ETF/KR'
        elif target == "미국 나스닥": market = 'NASDAQ'
        elif target == "미국 ETF": market = 'ETF/US'
        else: market = 'KRX' 
        
        df = fdr.StockListing(market)
        df = df.rename(columns={'Code': 'Symbol', 'Marcap': '시가총액', 'Amount': '거래대금'})
        
        if '시가총액' in df.columns: df['시총(억)'] = (df['시가총액'] / 100000000).round(0)
        if target in ["미국 나스닥", "미국 ETF"] and 'MarketCap' in df.columns:
            df['시총(억)'] = (df['MarketCap'] * 1350 / 100000000).round(0)
        elif '시총(억)' not in df.columns: df['시총(억)'] = 0
        return df
    except: return pd.DataFrame()

def get_dividend_details(symbol):
    try:
        is_kr = symbol.isdigit() and len(symbol) == 6
        yf_sym = f"{symbol}.KS" if is_kr and int(symbol) < 900000 else (f"{symbol}.KQ" if is_kr else symbol)
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        return {
            "name": info.get('shortName', symbol), "dps": info.get('dividendRate', 0) or 0,
            "yield": round((info.get('dividendYield', 0) or 0) * 100, 2),
            "currency": info.get('currency', 'KRW')
        }
    except: return None

def send_telegram_all(token, chat_id, results, strategy_names, target_type):
    if not token or not chat_id: return False
    msg = f"🚀 <b>[{target_type}] 포착</b>\n🎯 전략: {', '.join(strategy_names)}\n📊 포착: {len(results)}개\n\n"
    if not results: msg = f"🔔 <b>[{target_type}]</b>\n🎯 전략: {', '.join(strategy_names)}\n\n조건에 맞는 종목이 없습니다."
    else:
        for i, item in enumerate(results[:10]):
            msg += f"{i+1}. <b>{item['종목명']}</b> ({item['현재가']})\n"
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})
    return True

def get_searchable_list():
    try:
        df_kr = fdr.StockListing('KRX')[['Symbol', 'Name']]
        return [f"{r.Name} ({r.Symbol})" for r in df_kr.itertuples()]
    except: return ["Samsung (005930)"]

def update_config_to_github(token, repo, content):
    if not token or not repo: return False
    url = f"https://api.github.com/repos/{repo}/contents/config.json"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    res = requests.get(url, headers=headers)
    sha = res.json().get("sha") if res.status_code == 200 else ""
    import base64
    payload = {"message": "Update config", "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"), "sha": sha}
    return requests.put(url, headers=headers, json=payload).status_code in [200, 201]

def get_strategy_desc(s):
    descs = {"정석 정배열 (추세추종)": "5 > 20 > 60 이평선 정배열.", "거래량 폭발 (세력개입)": "거래량 5일 평균의 2배 폭증."}
    return descs.get(s, "설명 없음")

def create_advanced_chart(df, name, strats):
    df_p = df.tail(48)
    fig = go.Figure(data=[go.Candlestick(x=df_p.index, open=df_p['Open'], high=df_p['High'], low=df_p['Low'], close=df_p['Close'], name="Price")])
    fig.add_trace(go.Scatter(x=df_p.index, y=df_p['ma20'], name="MA20", line=dict(color='red')))
    fig.update_layout(title=f"{name} Chart", template="plotly_dark", xaxis_rangeslider_visible=False)
    return fig
