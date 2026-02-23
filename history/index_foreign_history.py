import yfinance as yf
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

def get_data(yahoo_ticker, code):
    dat = yf.Ticker(yahoo_ticker)

    data = dat.history(period='max')

    # 인덱스를 컬럼으로 변환하고 인덱스 초기화
    data = data.reset_index()

    # 원하는 컬럼만 선택: Date, Open, Low, Close, Volume
    data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # 컬럼명을 소문자로 변경 (선택사항)
    data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

    # code 컬럼 추가
    data['code'] = code

    # 열 순서 변경
    column_order = ['date', 'code', 'open', 'high', 'low', 'close', 'volume']
    data = data[column_order]
    
    # open, high, low, close 를 소숫점 2자리로 반올림
    data['open'] = data['open'].round(2)
    data['high'] = data['high'].round(2)
    data['low'] = data['low'].round(2)
    data['close'] = data['close'].round(2)

    return data

def send_sql(data):
    try:
        with engine.begin() as connection:
            df_future = data
            df_future.to_sql(name='index_foreign_daily', con=connection, if_exists='append', index=False)
            
        print("데이터가 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {str(e)}")
        raise

# 상품 매핑 딕셔너리
mapping = {
    "NI225": "^N225",
    "HSI": "^HSI",
}

# 모든 상품 데이터 수집 및 저장
for code, yahoo_ticker in mapping.items():
    try:
        print(f"\n{code} ({yahoo_ticker}) 데이터 수집 중...")
        data = get_data(yahoo_ticker, code)
        print(f"{code} 데이터 수집 완료: {len(data)} 행")
        print(data.tail())  # 마지막 5행 출력
        
        # 데이터베이스에 저장
        send_sql(data)
        print(f"{code} 데이터 저장 완료")
        
    except Exception as e:
        print(f"{code} 처리 중 오류 발생: {str(e)}")
        continue