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
def safe_cache(ttl=3600):
    """Streamlit 환경에서만 캐시 적용, 아니면 그냥 함수 반환"""
    def decorator(func):
        try:
            import streamlit as st
            return st.cache_data(ttl=ttl)(func)
        except:
            return func
    return decorator

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
        # 기간 설정: 월봉(10년), 주봉(5년), 일봉(1년)
        if period == 'M': days = 365*10
        elif period == 'W': days = 365*5
        else: days = 365*1 # 일봉
        
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        df = fdr.DataReader(symbol, start_date)
        if df is None or len(df) < 70: return None
        
        # 리샘플링 규칙: ME(월말), W(주봉), D(일봉 - 리샘플링 불필요)
        if period == 'M': 
            df_res = df.resample('ME').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        elif period == 'W':
            df_res = df.resample('W').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        else:
            df_res = df # 일봉은 그대로 사용
        
        # 지표 계산
        df_res['ma5'] = df_res['Close'].rolling(5).mean()
        df_res['ma12'] = df_res['Close'].rolling(12).mean()
        df_res['ma20'] = df_res['Close'].rolling(20).mean()
        df_res['ma30'] = df_res['Close'].rolling(30).mean()
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

# --- [2] 다중 전략 엔진 ---
def check_multi_signals(df, strategy_list):
    if df is None or len(df) < 65: return pd.Series(False, index=df.index if df is not None else [])
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
        
        # --- [신규 전략 추가] ---
        elif strategy == "5일 연속 상승세":
            # 5일 연속 종가가 전일보다 높음 + 1주일 전(5봉 전) 대비 상승
            c1 = (df['Close'] > df['Close'].shift(1))
            cond = c1 & c1.shift(1) & c1.shift(2) & c1.shift(3) & c1.shift(4)
            cond &= (df['Close'] >= df['Close'].shift(5))
        elif strategy == "저평가 성장주 (퀀트)":
            # 기술적으로는 장기 정배열 + 최근 3개월 수익률 우수 (재무 데이터 대용)
            # 실제 PER/매출은 Listing 데이터에서 보완 필요
            cond = (df['ma60'] > df['ma60'].shift(20)) & (df['Close'] > df['ma20']) & (df['rsi'] > 50)
        elif strategy == "외인/기관 쌍끌이 매수":
            # 수급 데이터 부재 시, 가격과 거래량의 강한 동반 상승으로 추정 (가격 등락률 > 0)
            cond = (df['Close'] > df['Close'].shift(1)) & (df['Volume'] > df['vol_ma5'] * 1.5)
        else:
            cond = pd.Series(False, index=df.index)
        final_cond &= cond
    return final_cond

def get_strategy_desc(strategy_name):
    """전략별 상세 설명을 반환"""
    descriptions = {
        "정석 정배열 (추세추종)": "5 > 20 > 60 이평선 정배열 상태에서 현재가가 5선 위에 있는 강력한 추세 구간입니다.",
        "20월선 눌림목 (조정매수)": "장기 추세(60선)가 살아있는 상태에서 20월선 근처까지 내려온 '싸게 살 기회'를 포착합니다.",
        "거래량 폭발 (세력개입)": "평소 거래량의 2배 이상이 터지며 양봉을 만든 종목으로, 세력의 매집이나 강한 모멘텀을 의미합니다.",
        "대시세 초입 (20선 돌파)": "오랫동안 하락하거나 횡보하던 주가가 20선(황금선)을 강력하게 뚫고 올라오는 시점입니다.",
        "월봉 MA12 돌파": "1년 평균선인 12월선을 돌파하는 시점으로, 중장기적인 추세 반전을 의미합니다.",
        "주봉 5/20 골든크로스": "단기 추세가 중기 추세를 돌파하며 상승 에너지가 응축되는 시점입니다.",
        "주봉 RSI 과매도 탈출": "RSI 30 이하에서 탈출하는 시점으로, 과도한 낙폭 후의 반등 타점입니다.",
        "주봉 볼린저 하단 터치": "볼린저 밴드 하단에 닿은 종목으로, 기술적 반등을 노리는 역추세 매매 타점입니다.",
        "주봉 20선 돌파 및 안착": "주봉상 주요 저항선인 20주선을 거래량과 함께 돌파하는 실질적인 상승 시작점입니다.",
        "와인스타인 2단계 돌파": "30주 이평선 우상향 + 가격 돌파 + 거래량 실림. 바닥권을 탈출하는 가장 정석적인 타점입니다.",
        "5일 연속 상승세": "5일 동안 단 하루도 쉬지 않고 상승한 종목입니다. 강력한 매수세가 유입되고 있음을 뜻합니다.",
        "저평가 성장주 (퀀트)": "낮은 PER과 높은 매출 성장성을 가진 종목 중, 차트상 우상향 추세가 확인된 종목을 고릅니다.",
        "외인/기관 쌍끌이 매수": "기관과 외국인이 동시에 매수하며 주가를 끌어올리는 종목으로, 수급의 힘이 가장 강한 상태입니다."
    }
    return descriptions.get(strategy_name, "설명이 등록되지 않은 전략입니다.")

def fast_backtest_multi(df, strategy_list, period='M'):
    """첫 진입점(Entry Point) 기준의 백테스팅 로직"""
    if df is None or len(df) < 65: return 0, 0, 0
    hold = 6 if period == 'M' else 4
    signals = check_multi_signals(df, strategy_list)
    
    # [수정] 신호가 False -> True로 바뀌는 시점만 추출 (첫 진입점)
    entry_signals = signals & (~signals.shift(1).fillna(False))
    
    testable = entry_signals.iloc[:-hold]
    matches = testable[testable == True].index
    
    if len(matches) == 0: return 0, 0, 0
    
    profits = []
    for d in matches:
        idx = df.index.get_loc(d)
        buy_price = df.iloc[idx]['Close']
        sell_price = df.iloc[idx + hold]['Close']
        profit = (sell_price / buy_price - 1) * 100
        profits.append(profit)
        
    return round(np.mean(np.array(profits) > 0) * 100, 1), round(np.mean(profits), 1), len(profits)

# --- [3] 타점 표시 차트 로직 ---
def create_advanced_chart(df, name, strategy_list):
    df_plot = df.tail(48) # 와인스타인 전략을 위해 조금 더 길게(약 1년) 표시
    signals = check_multi_signals(df, strategy_list)
    signals_plot = signals.reindex(df_plot.index, fill_value=False)
    
    fig = go.Figure(data=[go.Candlestick(
        x=df_plot.index, open=df_plot['Open'], high=df_plot['High'], low=df_plot['Low'], close=df_plot['Close'],
        name="Price"
    )])
    
    buy_signals = df_plot[signals_plot]
    if not buy_signals.empty:
        fig.add_trace(go.Scatter(
            x=buy_signals.index, y=buy_signals['Low'] * 0.95,
            mode='markers', name='Buy Signal',
            marker=dict(symbol='triangle-up', size=15, color='lime', line=dict(width=2, color='white'))
        ))

    # 이평선 (와인스타인용 30선 포함)
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma5'], name="MA5", line=dict(color='orange', width=1)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma20'], name="MA20", line=dict(color='red', width=1.5)))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma30'], name="MA30", line=dict(color='cyan', width=2, dash='dash')))
    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['ma60'], name="MA60", line=dict(color='purple', width=1)))

    fig.update_layout(title=f"{name} (타점 및 30주선 표시)", template="plotly_dark", xaxis_rangeslider_visible=False, height=600)
    return fig

# --- [4] 기타 및 병렬 처리 (Force Update 0331) ---
def get_listing_data(target):
    """종목 리스트 로드 (에러 방지 로직 강화)"""
    try:
        if target == "ETF":
            df = fdr.StockListing('ETF/KR')
            return df[['Symbol', 'Name']]
        else:
            # KOSPI 대신 KRX 전체를 시도해보고, 상위 종목만 필터링
            df = fdr.StockListing('KRX')
            
            # 컬럼명 대응 (Code/Symbol, Name/Name)
            if 'Code' in df.columns:
                df = df.rename(columns={'Code': 'Symbol'})
            
            # 시장 필터링 (주식만)
            if 'Market' in df.columns:
                df = df[df['Market'].isin(['KOSPI', 'KOSDAQ'])]
            
            # 시가총액순 정렬 (Marcap 또는 Stocks)
            sort_col = 'Marcap' if 'Marcap' in df.columns else (df.columns[df.columns.str.contains('시가총액|Marcap', case=False)][0] if any(df.columns.str.contains('시가총액|Marcap', case=False)) else None)
            
            if sort_col:
                df = df.sort_values(by=sort_col, ascending=False)
            
            return df[['Symbol', 'Name']].head(200)
    except Exception as e:
        print(f"DEBUG: 데이터 로드 중 에러 발생 - {e}")
        # 예비 수단: FinanceDataReader 내부의 다른 인덱스 시도
        try:
            df = fdr.StockListing('KOSPI')
            if 'Code' in df.columns: df = df.rename(columns={'Code': 'Symbol'})
            return df[['Symbol', 'Name']].head(200)
        except:
            return pd.DataFrame()

def process_stock_multi_worker(symbol, name, strategy_list, period_key):
    df_data = get_processed_data(symbol, period_key)
    if df_data is not None and len(df_data) >= 2:
        signals = check_multi_signals(df_data, strategy_list)
        if signals.iloc[-1]:
            is_new = "Y" if not signals.iloc[-2] else "N"
            curr = df_data.iloc[-1]
            # 전략에 따라 이격도 기준선 변경
            ref_ma = curr['ma30'] if "와인스타인" in "".join(strategy_list) else curr['ma20']
            disp = (curr['Close'] / ref_ma - 1) * 100
            win, ret, cnt = fast_backtest_multi(df_data, strategy_list, period_key)
            return {"코드": symbol, "종목명": name, "현재가": f"{int(curr['Close']):,}", "승률": win, "평균수익": f"{ret}%", "신호수": cnt, "신규감지": is_new, "이격도": f"{disp:+.1f}%"}
    return None

def format_tg_message(results, strategy_names, target_type):
    """텔레그램용 메시지 형식 생성"""
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    msg = f"🚀 *[전략 포착]* 스캔 결과\n"
    msg += f"🎯 전략: {', '.join(strategy_names)}\n"
    msg += f"📊 포착: {len(results)}개 | {target_type}\n\n"
    
    # 승률 높은 순으로 정렬
    sorted_res = sorted(results, key=lambda x: x.get('승률', 0), reverse=True)
    for i, item in enumerate(sorted_res[:10]):
        msg += f"{i+1}. *{item['종목명']}* ({item['코드']})\n"
        msg += f"   - 현재가: {item['현재가']} | 승률: {item['승률']}%\n"
        msg += f"   - 신규감지: {item['신규감지']} | 이격도: {item['이격도']}\n"
    
    if len(results) > 10:
        msg += f"\n외 {len(results)-10}개 종목이 더 포착되었습니다."
    return msg

def send_telegram_message(token, chat_id, message):
    if not token or not chat_id: return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        res = requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}, timeout=10)
        return res.status_code == 200
    except: return False

def update_config_to_github(token, repo, path, message, content):
    """GitHub API를 통해 파일을 직접 커밋/푸시"""
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    
    # 1. 기존 파일의 SHA 값 가져오기
    res = requests.get(url, headers=headers)
    sha = ""
    if res.status_code == 200:
        sha = res.json().get("sha")
    
    # 2. 파일 업데이트
    import base64
    encoded_content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    data = {
        "message": message,
        "content": encoded_content,
        "sha": sha
    }
    
    res = requests.put(url, headers=headers, json=data)
    return res.status_code in [200, 201]
