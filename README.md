# 📈 Multi-Strategy Stock Screener

월봉 및 주봉 기반의 다양한 매매 전략을 제공하고, 과거 데이터를 통한 백테스팅 및 텔레그램 알림 기능을 지원하는 주식/ETF 스캐너입니다.

## 주요 기능
- **다양한 전략 지원**: 월봉 정배열, 20월선 눌림목, 거래량 폭발, MA12 돌파, 주봉 골든크로스 등
- **초고속 분석**: 멀티스레딩 병렬 처리 및 벡터화 연산을 통한 빠른 스캔
- **백테스팅**: 각 종목별 과거 승률 및 평균 수익률 분석 제공
- **자동 알림**: 설정한 주기에 맞춰 텔레그램으로 분석 결과 자동 발송
- **KOSPI 200 & ETF**: 국내 주요 시장 데이터 실시간 분석

## 실행 방법
1. 저장소 클론
2. 필수 라이브러리 설치: `pip install -r requirements.txt`
3. 앱 실행: `streamlit run app.py`
4. (선택) 자동 알림 실행: `python scheduler.py`

## 기술 스택
- Python, Streamlit, FinanceDataReader, Pandas, APScheduler
