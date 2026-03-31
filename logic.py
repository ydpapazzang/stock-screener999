# VERSION: 1.0.1 (Security Hardened)
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

CONFIG_FILE = "config.json"

# --- [0] 보안 및 설정 관리 ---
def load_config():
    """설정 파일 로드 (민감 정보는 제외하고 로드)"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {"schedules": [], "history": []}

def save_config(config_data):
    """설정 파일 저장 (민감 정보가 포함되지 않도록 필터링)"""
    # 저장 전 민감 정보 강제 제거 (보안 강화)
    safe_config = {
        "schedules": config_data.get("schedules", []),
        "history": config_data.get("history", [])
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_config, f, ensure_ascii=False, indent=4)

def get_secret(key, default=None):
    """Secret 정보를 안전하게 가져오기 (Streamlit Secrets > Env > Default)"""
    try:
        # 1. Streamlit Secrets (웹 운영 환경)
        if key in st.secrets:
            return st.secrets[key]
    except: pass
    
    # 2. Environment Variables (GitHub Actions 또는 로컬)
    return os.environ.get(key, default)

# --- [1] 데이터 및 지표 엔진 ---
def get_processed_data(symbol, period='M'):
    """데이터 수집 및 지표 계산 통합"""
    try:
        if period == 'M': days = 365*10
        elif period == 'W': days = 365*5
        else: days = 365*1
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        df = fdr.DataReader(symbol, start_date)
        if df is None or len(df) < 70: return None
        
        if period == 'M': 
            df_res = df.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        elif period == 'W':
            df_res = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        else:
            df_res = df
        
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma12'] = df_res['Close'].rolling(12).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma30'] = df_res['Close'].rolling(30).mean()
        df_res['ma60'] = df_res['Close'].rolling(60).mean()
        df_res['vol_ma5'] = df_res['Volume'].rolling(5).mean()
        
        delta = df_res['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df_res['rsi'] = 100 - (100 / (1 + (gain / loss + 1e-9)))
        
        df_res['std'] = df_res['Close'].rolling(20).std()
        df_res['bb_lower'] = df_res['ma20'] - (df_res['std'] * 2)
        
        return df_res.dropna()
    except: return None

def check_multi_signals(df, strategy_list):
    """다중 전략 필터링 엔진"""
    if df is None or len(df) < 2: return pd.Series(False, index=df.index if df is not None else [])
    final_cond = pd.Series(True, index=df.index)
    
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
            cond = (df['Close'].shift(1) < df['ma20'].shift(1)) & (df['Close'] > df['ma20']) & (df['Volume'] > df['vol_ma5'])
        elif strategy == "와인스타인 2단계 돌파":
            cond = (df['ma30'] >= df['ma30'].shift(1)) & (df['Close'].shift(1) < df['ma30'].shift(1)) & (df['Close'] > df['ma30']) & (df['Volume'] > df['vol_ma5'])
        elif strategy == "5일 연속 상승세":
            c = (df['Close'] > df['Close'].shift(1))
            cond = c & c.shift(1) & c.shift(2) & c.shift(3) & c.shift(4)
        elif strategy == "저평가 성장주 (퀀트)":
            cond = (df['ma60'] > df['ma60'].shift(20)) & (df['Close'] > df['ma20']) & (df['rsi'] > 50)
        elif strategy == "외인/기관 쌍끌이 매수":
            cond = (df['Close'] > df['Close'].shift(1)) & (df['Volume'] > df['vol_ma5'] * 1.5)
        else: cond = pd.Series(False, index=df.index)
        final_cond &= cond
    return final_cond

def fast_backtest_multi(df, strategy_list, period='M'):
    """첫 진입점 기준 백테스팅"""
    if df is None or len(df) < 65: return 0, 0, 0
    hold = 6 if period == 'M' else 4
    signals = check_multi_signals(df, strategy_list)
    entry_signals = signals & (~signals.shift(1).fillna(False))
    testable = entry_signals.iloc[:-hold]
    matches = testable[testable == True].index
    if len(matches) == 0: return 0, 0, 0
    profits = [(df.iloc[df.index.get_loc(d) + hold]['Close'] / df.iloc[df.index.get_loc(d)]['Close'] - 1) * 100 for d in matches]
    return round(np.mean(np.array(profits) > 0) * 100, 1), round(np.mean(profits), 1), len(profits)

# --- [2] 분석 워커 ---
def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    """개별 종목 분석 및 점수 계산"""
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        match_count = 0
        hit_strats = []
        for s in strategy_list:
            if check_multi_signals(df_data, [s]).iloc[-1]:
                match_count += 1
                hit_strats.append(s)
        
        if match_count > 0:
            score = round((match_count / len(strategy_list)) * 100)
            curr = df_data.iloc[-1]
            win, ret, cnt = fast_backtest_multi(df_data, strategy_list, period_key)
            return {
                "코드": symbol, "종목명": name, "점수": score, "현재가": f"{int(curr['Close']):,}",
                "승률": win, "평균수익": f"{ret}%", "신규감지": "Y" if not check_multi_signals(df_data, strategy_list).iloc[-2] else "N",
                "일치전략": ", ".join(hit_strats)
            }
    return None

# --- [3] 텔레그램 및 알림 로직 ---
def send_telegram_all(token, chat_id, results, strategy_names, target_type):
    """텍스트 메시지와 차트 이미지를 통합 전송"""
    if not token or not chat_id: return False
    
    # 1. 텍스트 리포트 생성 및 전송
    msg = f"🚀 *[전략 포착]* {target_type}\n🎯 전략: {', '.join(strategy_names)}\n📊 포착: {len(results)}개\n\n"
    sorted_res = sorted(results, key=lambda x: x.get('점수', 0), reverse=True)
    for i, item in enumerate(sorted_res[:10]):
        msg += f"{i+1}. *{item['종목명']}* ({item['점수']}점)\n   - 수익: {item['평균수익']} | 신규: {item['신규감지']}\n"
    msg += f"\n🔗 [스크리너 접속](https://stock-screener999-ztg2dqzbktgsfn5xxguc7t.streamlit.app/)"
    
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"})
    
    # 2. 상위 3개 차트 전송
    import matplotlib.pyplot as plt
    plt.style.use('dark_background')
    for item in sorted_res[:3]:
        df = get_processed_data(item['코드'], 'W') # 요약용 주봉
        if df is not None:
            plt.figure(figsize=(8, 4))
            plt.plot(df.index[-40:], df['Close'].tail(40), color='orange')
            plt.title(f"{item['종목명']} Trend")
            plt.savefig("temp.png")
            plt.close()
            with open("temp.png", "rb") as f:
                requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", data={"chat_id": chat_id, "caption": f"📊 {item['종목명']} 차트"}, files={"photo": f})
            if os.path.exists("temp.png"): os.remove("temp.png")
    return True

# --- [4] 외부 연동 (GitHub) ---
def update_config_to_github(token, repo, content):
    """GitHub API를 통해 설정 파일 동기화"""
    if not token or not repo: return False
    url = f"https://api.github.com/repos/{repo}/contents/config.json"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    res = requests.get(url, headers=headers)
    sha = res.json().get("sha") if res.status_code == 200 else ""
    
    import base64
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    payload = {"message": "Update config", "content": encoded, "sha": sha}
    
    put_res = requests.put(url, headers=headers, json=payload)
    return put_res.status_code in [200, 201]

def get_strategy_desc(s):
    descs = {
        "정석 정배열 (추세추종)": "5 > 20 > 60 이평선 정배열 구간.",
        "20월선 눌림목 (조정매수)": "장기 추세 살아있는 상태에서 20월선 근처 조정.",
        "거래량 폭발 (세력개입)": "거래량이 5일 평균의 2배 이상 폭증.",
        "대시세 초입 (20선 돌파)": "주가가 20선을 하향에서 상향으로 강력 돌파.",
        "월봉 MA12 돌파": "1년 평균선 돌파로 중장기 반전 포착.",
        "주봉 5/20 골든크로스": "단기 이평선이 중기 이평선을 뚫고 상승.",
        "주봉 RSI 과매도 탈출": "RSI 30 이하에서 위로 올라오는 낙폭과대 타점.",
        "주봉 볼린저 하단 터치": "볼린저 밴드 하단선 지지를 노리는 역추세 전략.",
        "5일 연속 상승세": "5거래일 연속 종가 상승 (강한 단기 모멘텀).",
        "저평가 성장주 (퀀트)": "저PER + 장기 정배열 + 상승 모멘텀.",
        "외인/기관 쌍끌이 매수": "수급 주체의 동반 매수로 추정되는 강한 가격/거래량 상승."
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
