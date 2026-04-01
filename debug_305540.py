import logic
import pandas as pd
from datetime import datetime

symbol = "305540"
name = "TIGER 2차전지테마"
period = "M"
strategy_list = ["월봉 12선 돌파"] # 사용자가 설정한 전략명 (또는 그 조건)

print(f"--- Debugging {name} ({symbol}) ---")

# 1. 데이터 로드 및 전처리
df = logic.get_processed_data(symbol, period)
if df is None:
    print("Error: Could not fetch data.")
else:
    print(f"Data fetched. Last 3 months:\n{df.tail(3)[['Close', 'ma5', 'ma20']]}")
    
    # MA12 계산 (선계산 항목에 없으면 실시간 계산)
    df['MA12'] = df['Close'].rolling(12).mean()
    last_row = df.iloc[-1]
    
    print(f"\nLast Close: {last_row['Close']}")
    print(f"MA12 (12-month average): {last_row['MA12']}")
    
    if last_row['Close'] > last_row['MA12']:
        print("\n✅ Condition MET: Close > MA12")
    else:
        print("\n❌ Condition NOT MET: Close <= MA12")

# 2. 리스팅 데이터 확인
df_listing = logic.get_listing_data("한국 ETF")
match = df_listing[df_listing['Symbol'] == symbol]
if not match.empty:
    print(f"\nListing info found: {match.iloc[0].to_dict()}")
else:
    print("\n❌ Not found in '한국 ETF' listing.")
