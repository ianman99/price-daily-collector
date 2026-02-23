import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from login_krx import get_krx_session

# 환경 변수 로드
load_dotenv()

# MySQL 연결 정보 설정
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_url)

# KRX 세션 가져오기
krx_session = get_krx_session()

def collect_krx_index_data(type_num):
    today_date = date.today().strftime("%Y%m%d")
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'idxIndMidclssCd': type_num,
        'trdDd': today_date,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'

    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd', "User-Agent": "Mozilla/5.0", "Cookie": f"JSESSIONID={krx_session}"}

    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    df = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
    df = df.drop(columns=['대비', '등락률'])
    df= df.dropna(subset=['시가', '종가'])

    
    df['기준일'] = today_date
    df['지수명'] = df['지수명'].str.replace(' ', '')
    # 지수명이 코스피, 코스피200, 코스닥, 코스닥150 인 행을 제외하고 나머지 행 삭제
    df = df[df['지수명'].isin(['코스피', '코스피200', '코스닥', '코스닥150'])]
    
    df.rename(columns={
    '지수명': 'code',
    '종가': 'close',
    '시가': 'open',
    '고가': 'high',
    '저가': 'low',
    '거래량': 'volume',
    '거래대금': 'trading_value',
    '기준일': 'date',
    '상장시가총액': 'market_cap'
    }, inplace=True)

    # 열 순서 변경
    column_order = ['date', 'code', 'open', 'high', 'low', 'close', 
                    'volume', 'trading_value', 'market_cap']
    df = df[column_order]

    return df

def collect_krx_stock_data():
    today_date = date.today().strftime("%Y%m%d")
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'trdDd': today_date,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd', "User-Agent": "Mozilla/5.0", "Cookie": f"JSESSIONID={krx_session}"}

    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)

    df = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')

    df = df.drop(columns=['소속부', '대비', '등락률'])
    df= df.dropna(subset=['시가', '종가'])
    df['기준일'] = today_date
    df['종목코드'] = 'A' + df['종목코드']
    df['시장구분'] = df['시장구분'].str.replace('KOSDAQ GLOBAL', 'KOSDAQ')
    df['거래대금'] = (df['거래대금'] / 1_000_000).round(0).astype(int)
    
    df.rename(columns={
    '종목코드': 'code',
    '시장구분': 'market',
    '종가': 'close',
    '시가': 'open',
    '고가': 'high',
    '저가': 'low',
    '거래량': 'volume',
    '거래대금': 'trading_value',
    '상장주식수': 'listed_stocks',
    '기준일': 'date'
    }, inplace=True)

    # 열 순서 변경
    column_order = ['date', 'code', 'open', 'high', 'low', 'close', 
                    'volume', 'trading_value', 'market', 'listed_stocks']
    df = df[column_order]

    return df

def main():
    try:
        with engine.begin() as connection:
            df_stock = collect_krx_stock_data()
            df_stock.to_sql(name='stock_daily', con=connection, if_exists='append', index=False)

            df_kospi = collect_krx_index_data('02')
            df_kospi.to_sql(name='index_daily', con=connection, if_exists='append', index=False)

            df_kosdaq = collect_krx_index_data('03')
            df_kosdaq.to_sql(name='index_daily', con=connection, if_exists='append', index=False)
            
        print("데이터가 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {str(e)}")
        raise

main()

