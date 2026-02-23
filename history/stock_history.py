import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
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

def collect_krx_stock_data(date):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'trdDd': date,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0'
    }

    response = rq.get(url, params=params, headers=headers)
    data = response.json()
    df = pd.DataFrame(data['OutBlock_1'])
    df = df[['ISU_SRT_CD', 'MKT_NM', 'TDD_CLSPRC', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'LIST_SHRS']]
    df.rename(columns={
        'ISU_SRT_CD': 'code',
        'MKT_NM': 'market',
        'TDD_CLSPRC': 'close',
        'TDD_OPNPRC': 'open',
        'TDD_HGPRC': 'high',
        'TDD_LWPRC': 'low',
        'ACC_TRDVOL': 'volume',
        'ACC_TRDVAL': 'trading_value',
        'LIST_SHRS': 'listed_stocks'
    }, inplace=True)
    # 거래량 '-' 인 행 제거
    df = df[df['volume'] != '-']
    # KONEX 제거
    df = df[df['market'] != 'KONEX']
    # close, open, high, low, volume, trading_value, listed_stocks 쉽표 제거 후 정수로 변환
    df['close'] = df['close'].str.replace(',', '').astype(int)
    df['open'] = df['open'].str.replace(',', '').astype(int)
    df['high'] = df['high'].str.replace(',', '').astype(int)
    df['low'] = df['low'].str.replace(',', '').astype(int)
    df['volume'] = df['volume'].str.replace(',', '').astype(int)
    df['trading_value'] = df['trading_value'].str.replace(',', '').astype(int)
    df['listed_stocks'] = df['listed_stocks'].str.replace(',', '').astype(int)
    df['close'] = df['close'].astype(int)
    df['open'] = df['open'].astype(int) 
    df['high'] = df['high'].astype(int)
    df['low'] = df['low'].astype(int)
    df['volume'] = df['volume'].astype(int)
    df['trading_value'] = df['trading_value'].astype(int)
    df['listed_stocks'] = df['listed_stocks'].astype(int)
    # 거래량 0 제거
    df = df[df['volume'] != 0]
    df['date'] = date
    # 종목코드 앞에 A 추가
    df['code'] = 'A' + df['code']
    column_order = ['date', 'code', 'close', 'open', 'high', 'low', 'volume', 'trading_value', 'market', 'listed_stocks']
    df = df[column_order]
    return df


def main(date):
    try:
        with engine.begin() as connection:
            df = collect_krx_stock_data(date)
            df.to_sql(name='stock_daily_copy', con=connection, if_exists='append', index=False)
            
        print(f"{date} 데이터가 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {str(e)}")
        raise

# for year in range(2000, 2010):
#     start_date = date(year, 1, 1)
#     end_date = date(year, 12, 31) 
#     current_date = start_date
#     while current_date <= end_date:
#         date_str = current_date.strftime('%Y%m%d')
#         main(date_str)
#         time.sleep(0.5)
#         current_date += timedelta(days=1)
        
for year in range(2025, 2026):
    start_date = date(year, 6, 6)
    end_date = date(year, 6, 19) 
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        main(date_str)
        time.sleep(0.5)
        current_date += timedelta(days=1)
    