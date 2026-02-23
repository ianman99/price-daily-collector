import yfinance as yf
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# User-Agent 설정으로 브라우저 위장
from curl_cffi import requests as curl_requests

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
    # curl_cffi 세션으로 브라우저 위장
    session = curl_requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    dat = yf.Ticker(yahoo_ticker, session=session)

    data = dat.history(period='15d')

    # 인덱스를 컬럼으로 변환하고 인덱스 초기화
    data = data.reset_index()

    # 원하는 컬럼만 선택: Date, Open, Low, Close, Volume
    data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]

    # 컬럼명을 소문자로 변경 (선택사항)
    data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']

    # code 컬럼 추가, code 는 NYMEX:CL1! 로 설정
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
            for _, row in data.iterrows():
                upsert_query = text("""
                    INSERT INTO index_foreign_daily (date, code, open, high, low, close, volume)
                    VALUES (:date, :code, :open, :high, :low, :close, :volume)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                """)
                
                connection.execute(upsert_query, {
                    'date': row['date'],
                    'code': row['code'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
            
            print(f"Upsert 완료: {len(data)} 행")
    except Exception as upsert_error:
        print(f"Upsert 중 오류 발생: {str(upsert_error)}")
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
        
        send_sql(data)
        print(f"{code} 데이터 저장 완료")
        
    except Exception as e:
        print(f"{code} 처리 중 오류 발생: {str(e)}")
        continue