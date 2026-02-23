import requests
import pandas as pd
from datetime import datetime, timedelta
import json
from sqlalchemy import create_engine
import time
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# MySQL 연결 정보 설정
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_url)

def get_daily_data(code, date):
    try:
        # API URL 및 파라미터 설정
        url = "https://crix-api-cdn.upbit.com/v1/crix/candles/days"
        params = {
            "code": f"CRIX.UPBIT.KRW-{code}",
            "count": 365,
            "to": date
        }

        # 헤더 설정
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://upbit.com",
            "Referer": "https://upbit.com/"
        }

        # API 요청
        response = requests.get(url, params=params, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} for date {date}")
            return None

        # JSON 데이터를 DataFrame으로 변환
        data = response.json()
        if not data:
            print(f"No data for date {date}")
            return None
            
        df = pd.DataFrame(data)

        # 필요한 컬럼만 선택
        columns = ['candleDateTimeKst', 'code', 'openingPrice', 'highPrice', 'lowPrice', 'tradePrice', 'candleAccTradeVolume', 'candleAccTradePrice']
        df = df[columns]

        # 날짜 형식 변환 (YYYY-MM-DD)
        df['candleDateTimeKst'] = pd.to_datetime(df['candleDateTimeKst']).dt.strftime('%Y-%m-%d')

        # code 컬럼 뒷 3자리만 추출
        df['code'] = df['code'].str[-3:]

        # 컬럼명 한글로 변경
        df.columns = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'trading_value']

        # 중복 데이터 확인
        existing_dates = pd.read_sql("SELECT DISTINCT date FROM coin_daily WHERE code = 'BTC'", engine)
        df = df[~df['date'].isin(existing_dates['date'])]

        if not df.empty:
            df.to_sql('coin_daily', engine, if_exists='append', index=False)
            print(f"Successfully saved data for {date}")
        else:
            print(f"No new data to save for {date}")

        return df

    except Exception as e:
        print(f"Error occurred for date {date}: {str(e)}")
        return None

def main():
    # 시작 날짜와 현재 날짜 설정
    start_date = datetime(2017, 10, 1)
    end_date = datetime.now()
    
    # 전체 날짜 수 계산
    total_days = (end_date - start_date).days
    print(f"\n전체 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"전체 날짜 수: {total_days}일")
    
    # 데이터 수집 시작
    current_date = end_date
    total_rows_saved = 0
    
    while current_date >= start_date:
        date_str = current_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        df = get_daily_data("XRP", date_str)
        
        if df is not None and not df.empty:
            total_rows_saved += len(df)
        
        # API 호출 제한을 위한 대기
        time.sleep(0.3)
        
        # 365일 전으로 이동
        current_date = current_date - timedelta(days=365)
    
    # 최종 결과 출력
    print("\n=== 데이터 수집 결과 ===")
    print(f"전체 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    print(f"전체 날짜 수: {total_days}일")
    print(f"저장된 데이터 행 수: {total_rows_saved}행")
    
    # 현재 DB에 저장된 총 데이터 수 확인
    total_db_rows = pd.read_sql("SELECT COUNT(*) as count FROM coin_daily WHERE code = 'BTC'", engine).iloc[0]['count']
    print(f"DB에 저장된 총 BTC 데이터 수: {total_db_rows}행")

if __name__ == "__main__":
    main()



