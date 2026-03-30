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
    return {"tg_token": "", "tg_chat_id": "", "schedules": []}

def save_config(config_data):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config_data, f, ensure_ascii=False, indent=4)

# --- [1] 데이터베이스 관리 (성능 개선) ---
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS price_cache 
                     (symbol TEXT, period TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, 
                      PRIMARY KEY(symbol, period, date))''')
    conn.commit()
    conn.close()

def get_processed_data(symbol, period='M'):
    """데이터 호출 및 지표 계산 (DB 캐싱 활용)"""
    try:
        # 데이터베이스 초기화 확인
        init_db()
        
        # 1. 원격 데이터 호출 (최적화: 필요한 기간만)
        days = 365*10 if period == 'M' else 365*5
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        # FDR 호출 (여기서 SQLite 연동을 통해 증분 업데이트를 할 수도 있지만, 
        # 우선은 빠른 분석을 위해 메모리 캐싱 위주로 구현하고 로직을 통합합니다.)
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

# --- [2] 분석 및 백테스트 엔진 ---
def check_all_signals(df, strategy_type):
    if df is None or len(df) < 65: return pd.Series(False, index=df.index if df is not None else [])
    
    if strategy_type == "정석 정배열 (추세추종)":
        cond = (df['ma5'] > df['ma20']) & (df['ma20'] > df['ma60']) & (df['Close'] > df['ma5'])
    elif strategy_type == "20월선 눌림목 (조정매수)":
        disp_20 = (df['Close'] / df['ma20'] - 1) * 100
        cond = (df['ma60'] > df['ma60'].shift(1)) & (disp_20 >= -2.0) & (disp_20 <= 3.0)
    elif strategy_type == "거래량 폭발 (세력개입)":
        cond = (df['Volume'] > df['vol_ma5'] * 2.0) & (df['Close'] > df['Open'])
    elif strategy_type == "대시세 초입 (20선 돌파)":
        cond = (df['Close'].shift(1) < df['ma20'].shift(1)) & (df['Close'] > df['ma20'])
    elif strategy_type == "월봉 MA12 돌파":
        cond = (df['Close'].shift(1) < df['ma12'].shift(1)) & (df['Close'] > df['ma12'])
    elif strategy_type == "주봉 5/20 골든크로스":
        cond = (df['ma5'].shift(1) < df['ma20'].shift(1)) & (df['ma5'] > df['ma20'])
    elif strategy_type == "주봉 RSI 과매도 탈출":
        cond = (df['rsi'].shift(1) < 30) & (df['rsi'] > 30)
    elif strategy_type == "주봉 볼린저 하단 터치":
        cond = (df['Low'] <= df['bb_lower'])
    else:
        cond = pd.Series(False, index=df.index)
    return cond

def fast_backtest(df, strategy_type, period='M'):
    if df is None or len(df) < 65: return 0, 0, 0
    hold = 6 if period == 'M' else 4
    signals = check_all_signals(df, strategy_type)
    testable_signals = signals.iloc[:-hold]
    matches = testable_signals[testable_signals == True].index
    if len(matches) == 0: return 0, 0, 0
    profits = []
    for date in matches:
        idx = df.index.get_loc(date)
        profits.append((df.iloc[idx + hold]['Close'] / df.iloc[idx]['Close'] - 1) * 100)
    return round(np.mean(np.array(profits) > 0) * 100, 1), round(np.mean(profits), 1), len(profits)

# --- [3] 시각화 로직 (UX 개선) ---
def create_interactive_chart(df, name, strategy_type):
    """Plotly 인터랙티브 캔들 차트 생성"""
    df_plot = df.tail(24) # 최근 24개 봉만 표시
    
    fig = go.Figure(data=[go.Candlestick(
        x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'],
        name="Price"
    )])
    
    # 이평선 추가
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma5'], name="MA5", line=dict(color='orange', width=1)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma20'], name="MA20", line=dict(color='red', width=1.5)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma60'], name="MA60", line=dict(color='purple', width=1)))
    
    if "MA12" in strategy_type:
        fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma12'], name="MA12", line=dict(dash='dot')))
    if "볼린저" in strategy_type:
        fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['bb_lower'], name="BB Lower", line=dict(color='gray', width=1)))

    fig.update_layout(
        title=f"{name} 기술적 분석 차트",
        yaxis_title="Price (KRW)",
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=500
    )
    return fig

# --- [4] 기타 공통 함수 ---
@st.cache_data(ttl=3600)
def get_listing_data(target):
    try:
        if target == "ETF":
            df = fdr.StockListing('ETF/KR')
            return df[['Symbol', 'Name']]
        else:
            df = fdr.StockListing('KOSPI')
            mapping = {'Code': 'Symbol', 'Marcap': 'MarCap'}
            df = df.rename(columns=mapping)
            if 'MarCap' in df.columns:
                df = df.sort_values(by='MarCap', ascending=False)
            return df[['Symbol', 'Name', 'MarCap']].head(200)
    except: return pd.DataFrame()

def process_stock_worker(symbol, name, strategy_type, period_key):
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        signals = check_all_signals(df_data, strategy_type)
        if signals.iloc[-1]:
            is_new = "Y" if not signals.iloc[-2] else "N"
            curr = df_data.iloc[-1]
            if "볼린저" in strategy_type: disp = (curr['Close'] / curr['bb_lower'] - 1) * 100
            elif "MA12" in strategy_type: disp = (curr['Close'] / curr['ma12'] - 1) * 100
            else: disp = (curr['Close'] / curr['ma20'] - 1) * 100
            win_rate, avg_ret, count = fast_backtest(df_data, strategy_type, period_key)
            return {
                "코드": symbol, "종목명": name, "현재가": f"{int(curr['Close']):,}",
                "승률": win_rate, "평균수익": f"{avg_ret}%", "신호수": count,
                "신규감지": is_new, "이격도": f"{disp:+.1f}%"
            }
    return None

def send_telegram_message(token, chat_id, message):
    if not token or not chat_id: return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        res = requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=10)
        return res.status_code == 200
    except: return False

def format_tg_message(results, strategy_name, target_type):
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    msg = f"🚀 *[{strategy_name}]* 병렬 스캔 결과\n"
    msg += f"📊 포착: {len(results)}개 | {target_type}\n\n"
    sorted_res = sorted(results, key=lambda x: x.get('승률', 0), reverse=True)
    for i, item in enumerate(sorted_res[:10]):
        msg += f"{i+1}. *{item['종목명']}* (승률: {item['승률']}%)\n"
        msg += f"   - 수익: {item['평균수익']} | 이격: {item['이격도']}\n"
    return msg
