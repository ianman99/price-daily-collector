import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date
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

def collect_krx_index_data(index_code, strtDd, endDd):
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'tboxindIdx_finder_equidx0_8': index_code,
        'indIdx': '2',
        'indIdx2': '203',
        'codeNmindIdx_finder_equidx0_8': index_code,
        'param1indIdx_finder_equidx0_8': '',
        'strtDd': strtDd,
        'endDd': endDd,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00301'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd', "User-Agent": "Mozilla/5.0"}

    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    df = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
    df = df.drop(columns=['대비', '등락률'])
    df= df.dropna(subset=['종가'])
    df['지수명'] = index_code
    df['지수명'] = df['지수명'].str.replace(' ', '')
    df.rename(columns={
    '지수명': 'code',
    '종가': 'close',
    '시가': 'open',
    '고가': 'high',
    '저가': 'low',
    '거래량': 'volume',
    '거래대금': 'trading_value',
    '일자': 'date',
    '상장시가총액': 'market_cap'
    }, inplace=True)

    # 열 순서 변경
    column_order = ['date', 'code', 'open', 'high', 'low', 'close', 
                    'volume', 'trading_value', 'market_cap']
    df = df[column_order]

    return df


# print(collect_krx_index_data('코스피', '19940101', '19941231'))


def main():
    for year in range(2024, 2025):
        strtDd = f'{year}0101'
        endDd = f'{year}1231'
        print(f"{year}년 데이터 수집 중...")
        df = collect_krx_index_data('코스닥 150', strtDd, endDd)
        with engine.connect() as conn:
            for _, row in df.iterrows():
                date_value = row['date']
                code = row['code']
                open_ = None if pd.isna(row['open']) else row['open']
                high = None if pd.isna(row['high']) else row['high']
                low = None if pd.isna(row['low']) else row['low']
                close = None if pd.isna(row['close']) else row['close']
                volume = None if pd.isna(row['volume']) else row['volume']
                trading_value = None if pd.isna(row['trading_value']) else row['trading_value']
                market_cap = None if pd.isna(row['market_cap']) else row['market_cap']
                insert_query = text("""
                INSERT INTO index_daily (date, code, open, high, low, close, volume, trading_value, market_cap)
                VALUES (:date, :code, :open, :high, :low, :close, :volume, :trading_value, :market_cap)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    trading_value = VALUES(trading_value),
                    market_cap = VALUES(market_cap)
                """)
                conn.execute(insert_query, {
                    'date': date_value, 'code': code, 'open': open_, 'high': high, 'low': low, 'close': close,
                    'volume': volume, 'trading_value': trading_value, 'market_cap': market_cap
                })
            conn.commit()
        time.sleep(1)

main()