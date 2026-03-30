import FinanceDataReader as fdr
import pandas as pd
import numpy as np
import requests
import json
import os
import sqlite3
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

CONFIG_FILE = "config.json"
DB_FILE = "stock_cache.db"

# --- [0] 기본 유틸리티 ---
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {"tg_token": "", "tg_chat_id": "", "schedules": [], "password": "1234"}

def save_config(config_data):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config_data, f, ensure_ascii=False, indent=4)

# --- [1] 데이터 및 지표 계산 ---
def get_processed_data(symbol, period='M'):
    try:
        days = 365*10 if period == 'M' else 365*5
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        df = fdr.DataReader(symbol, start_date)
        if df is None or len(df) < 70: return None
        
        rule = 'ME' if period == 'M' else 'W'
        df_res = df.resample(rule).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        
        # 지표 계산
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma12'] = df_res['Close'].rolling(12).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma60'] = df_res['Close'].rolling(60).mean()
        df_res['vol_ma5'] = df_res['Volume'].rolling(5).mean()
        
        delta = df_res['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df_res['rsi'] = 100 - (100 / (1 + (gain / loss)))
        
        df_res['std'] = df_res['Close'].rolling(20).std()
        df_res['bb_lower'] = df_res['ma20'] - (df_res['std'] * 2)
        
        return df_res
    except: return None

# --- [2] 다중 전략 엔진 (Intersection Logic) ---
def check_multi_signals(df, strategy_list):
    """여러 전략 리스트를 받아 교집합(AND) 신호를 반환"""
    if df is None or len(df) < 65: return pd.Series(False, index=df.index if df is not None else [])
    
    final_cond = pd.Series(True, index=df.index) # 기본값 True (교집합 시작)
    
    for strategy in strategy_list:
        if strategy == "정석 정배열 (추세추종)":
            cond = (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['Close'] > df['ma5'])
        elif strategy == "20월선 눌림목 (조정매수)":
            disp_20 = (df['Close'] / df['ma20'] - 1) * 100
            cond = (df['ma60'] > df['ma60'].shift(1)) & (disp_20 >= -2.0) & (disp_20 <= 3.0)
        elif strategy == "거래량 폭발 (세력개입)":
            cond = (df['Volume'] > df['vol_ma5'] * 2.0) & (df['Close'] > df['Open'])
        elif strategy == "대시세 초입 (20선 돌파)":
            cond = (df['Close'].shift(1) < df['ma20'].shift(1)) & (df['Close'] > df['ma20'])
        elif strategy == "월봉 MA12 돌파":
            cond = (df['Close'].shift(1) < df['ma12'].shift(1)) & (df['Close'] > df['ma12'])
        elif strategy == "주봉 5/20 골든크로스":
            cond = (df['ma5'].shift(1) < df['ma20'].shift(1)) & (df['ma5'] > df['ma20'])
        elif strategy == "주봉 RSI 과매도 탈출":
            cond = (df['rsi'].shift(1) < 30) & (df['rsi'] > 30)
        elif strategy == "주봉 볼린저 하단 터치":
            cond = (df['Low'] <= df['bb_lower'])
        elif strategy == "주봉 20선 돌파 및 안착":
            # 조건: 지난주 종가 < 20선 AND 이번주 종가 > 20선 AND 거래량 > 5주 평균거래량
            cond = (df['Close'].shift(1) < df['ma20'].shift(1)) & (df['Close'] > df['ma20']) & (df['Volume'] > df['vol_ma5'])
        else:
            cond = pd.Series(False, index=df.index)
        
        final_cond &= cond # AND 조건 결합
        
    return final_cond

def fast_backtest_multi(df, strategy_list, period='M'):
    if df is None or len(df) < 65: return 0, 0, 0
    hold = 6 if period == 'M' else 4
    signals = check_multi_signals(df, strategy_list)
    testable = signals.iloc[:-hold]
    matches = testable[testable == True].index
    if len(matches) == 0: return 0, 0, 0
    profits = [(df.iloc[df.index.get_loc(d) + hold]['Close'] / df.iloc[df.index.get_loc(d)]['Close'] - 1) * 100 for d in matches]
    return round(np.mean(np.array(profits) > 0) * 100, 1), round(np.mean(profits), 1), len(profits)

# --- [3] 타점 표시 차트 로직 ---
def create_advanced_chart(df, name, strategy_list):
    """Plotly 차트에 신호 타점 표시 추가"""
    df_plot = df.tail(36) # 최근 3년(주/월) 표시
    signals = check_multi_signals(df, strategy_list)
    signals_plot = signals.reindex(df_plot.index, fill_value=False)
    
    fig = go.Figure(data=[go.Candlestick(
        x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'],
        name="Price"
    )])
    
    # 신호 타점 (Buy Arrows) 추가
    buy_signals = df_plot[signals_plot]
    if not buy_signals.empty:
        fig.add_trace(go.Scatter(
            x=buy_signals.index, y=buy_signals['Low'] * 0.95,
            mode='markers', name='Buy Signal',
            marker=dict(symbol='triangle-up', size=15, color='lime', line=dict(width=2, color='white'))
        ))

    # 이평선
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma5'], name="MA5", line=dict(color='orange', width=1)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma20'], name="MA20", line=dict(color='red', width=1.5)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma60'], name="MA60", line=dict(color='purple', width=1)))

    fig.update_layout(title=f"{name} (포착 타점 표시)", template="plotly_dark", xaxis_rangeslider_visible=False, height=600)
    return fig

# --- [4] 기타 및 병렬 처리 ---
@st.cache_data(ttl=3600)
def get_listing_data(target):
    try:
        if target == "ETF":
            df = fdr.StockListing('ETF/KR')
            return df[['Symbol', 'Name']]
        else:
            df = fdr.StockListing('KOSPI')
            df = df.rename(columns={'Code': 'Symbol', 'Marcap': 'MarCap'})
            if 'MarCap' in df.columns:
                df = df.sort_values(by='MarCap', ascending=False)
            return df[['Symbol', 'Name', 'MarCap']].head(200)
    except: return pd.DataFrame()

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        signals = check_multi_signals(df_data, strategy_list)
        if signals.iloc[-1]:
            is_new = "Y" if not signals.iloc[-2] else "N"
            curr = df_data.iloc[-1]
            disp = (curr['Close'] / curr['ma20'] - 1) * 100
            win, ret, cnt = fast_backtest_multi(df_data, strategy_list, period_key)
            return {"코드": symbol, "종목명": name, "현재가": f"{int(curr['Close']):,}", "승률": win, "평균수익": f"{ret}%", "신호수": cnt, "신규감지": is_new, "이격도": f"{disp:+.1f}%"}
    return None

def send_telegram_message(token, chat_id, message):
    if not token or not chat_id: return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        res = requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=10)
        return res.status_code == 200
    except: return False

def format_tg_message(results, strategy_names, target_type):
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    strategies_str = ", ".join(strategy_names)
    msg = f"🚀 *[복합 전략]* 스캔 결과\n"
    msg += f"🎯 전략: {strategies_str}\n"
    msg += f"📊 포착: {len(results)}개 | {target_type}\n\n"
    sorted_res = sorted(results, key=lambda x: x.get('승률', 0), reverse=True)
    for i, item in enumerate(sorted_res[:10]):
        msg += f"{i+1}. *{item['종목명']}* (승률: {item['승률']}%)\n"
        msg += f"   - 가격: {item['현재가']}원 | 신규: {item['신규감지']}\n"
    return msg
