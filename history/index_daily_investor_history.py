import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import time
from calendar import monthrange
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

def collect_krx_index_data(start_date, end_date):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT02202',
        'locale': 'ko_KR',
        'inqTpCd': '2',
        'trdVolVal': '2',
        'askBid': '3',
        'mktId': 'STK',
        'etf': 'EF',
        'etn': 'EN',
        'elw': 'EW',
        'strtDd': start_date,
        'endDd': end_date,
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0'
    }
    response = rq.get(url, params=params, headers=headers)
    data = response.json()
    df = pd.DataFrame(data['output'])
    df = df[['TRD_DD', 'TRDVAL1', 'TRDVAL2', 'TRDVAL3', 'TRDVAL4']]
    df['code'] = 'KOSPI'
    df.rename(columns={
        'TRD_DD': 'date',
        'TRDVAL1': 'net_buy_institution',
        'TRDVAL2': 'net_buy_other_corporation',
        'TRDVAL3': 'net_buy_individual',
        'TRDVAL4': 'net_buy_foreign'
    }, inplace=True)
    time.sleep(1)
    url_2 = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params_2 = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT02202',
        'locale': 'ko_KR',
        'inqTpCd': '2',
        'trdVolVal': '2',
        'askBid': '3',
        'mktId': 'KSQ',
        'segTpCd': 'ALL',
        'strtDd': start_date,
        'endDd': end_date,
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_2 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0'
    }
    response_2 = rq.get(url_2, params=params_2, headers=headers_2)
    data_2 = response_2.json()
    df_2 = pd.DataFrame(data_2['output'])
    df_2 = df_2[['TRD_DD', 'TRDVAL1', 'TRDVAL2', 'TRDVAL3', 'TRDVAL4']]
    df_2['code'] = 'KOSDAQ'
    df_2.rename(columns={
        'TRD_DD': 'date',
        'TRDVAL1': 'net_buy_institution',
        'TRDVAL2': 'net_buy_other_corporation',
        'TRDVAL3': 'net_buy_individual',
        'TRDVAL4': 'net_buy_foreign'
    }, inplace=True)
    
    time.sleep(1)
    url_3 = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params_3 = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03601',
        'locale': 'ko_KR',
        'mktId': 'STK',
        'strtDd': start_date,
        'endDd': end_date,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_3 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0'
    }
    response_3 = rq.get(url_3, params=params_3, headers=headers_3)
    data_3 = response_3.json()
    df_3 = pd.DataFrame(data_3['block1'])
    df_3 = df_3[['TRD_DD', 'MKTCAP', 'FORN_HD_MKTCAP', 'MKTCAP_RTO', 'LIST_SHRS', 'FORN_HD_SHRS', 'LIST_SHRS_RTO']]
    df_3['code'] = 'KOSPI'
    df_3.rename(columns={
        'TRD_DD': 'date',
        'MKTCAP': 'market_cap_total',
        'FORN_HD_MKTCAP': 'market_cap_foreign_holding',
        'MKTCAP_RTO': 'market_cap_foreign_ratio',
        'LIST_SHRS': 'shares_total',
        'FORN_HD_SHRS': 'shares_foreign_holding',
        'LIST_SHRS_RTO': 'shares_foreign_ratio'
    }, inplace=True)
    
    time.sleep(1)
    url_4 = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params_4 = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03601',
        'locale': 'ko_KR',
        'mktId': 'KSQ',
        'segTpCd': 'ALL',
        'strtDd': start_date,
        'endDd': end_date,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_4 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0'
    }
    response_4 = rq.get(url_4, params=params_4, headers=headers_4)
    data_4 = response_4.json()
    df_4 = pd.DataFrame(data_4['block1'])
    df_4 = df_4[['TRD_DD', 'MKTCAP', 'FORN_HD_MKTCAP', 'MKTCAP_RTO', 'LIST_SHRS', 'FORN_HD_SHRS', 'LIST_SHRS_RTO']]
    df_4['code'] = 'KOSDAQ'
    df_4.rename(columns={
        'TRD_DD': 'date',
        'MKTCAP': 'market_cap_total',
        'FORN_HD_MKTCAP': 'market_cap_foreign_holding',
        'MKTCAP_RTO': 'market_cap_foreign_ratio',
        'LIST_SHRS': 'shares_total',
        'FORN_HD_SHRS': 'shares_foreign_holding',
        'LIST_SHRS_RTO': 'shares_foreign_ratio'
    }, inplace=True)
    
    df = pd.concat([df, df_2])
    df_3 = pd.concat([df_3, df_4])
    df = pd.merge(df, df_3, on=['date', 'code'], how='left')
    
    # 쉼표 제거, 숫자 변경, 원단위를 백만원 단위로 반올림
    df['net_buy_institution'] = df['net_buy_institution'].str.replace(',', '').replace('', '0').astype(int)
    df['net_buy_other_corporation'] = df['net_buy_other_corporation'].str.replace(',', '').replace('', '0').astype(int)
    df['net_buy_individual'] = df['net_buy_individual'].str.replace(',', '').replace('', '0').astype(int)
    df['net_buy_foreign'] = df['net_buy_foreign'].str.replace(',', '').replace('', '0').astype(int)
    df['net_buy_institution'] = (df['net_buy_institution'] / 1000000).round(0)
    df['net_buy_other_corporation'] = (df['net_buy_other_corporation'] / 1000000).round(0)
    df['net_buy_individual'] = (df['net_buy_individual'] / 1000000).round(0)
    df['net_buy_foreign'] = (df['net_buy_foreign'] / 1000000).round(0)
    
    # 쉼표 제거, 숫자 변경
    df['market_cap_total'] = df['market_cap_total'].str.replace(',', '').replace('', '0').astype(int)
    df['market_cap_foreign_holding'] = df['market_cap_foreign_holding'].str.replace(',', '').replace('', '0').astype(int)
    df['market_cap_foreign_ratio'] = df['market_cap_foreign_ratio'].str.replace(',', '').replace('', '0').astype(float)
    df['market_cap_total'] = (df['market_cap_total'] / 1000000).round(0)
    df['market_cap_foreign_holding'] = (df['market_cap_foreign_holding'] / 1000000).round(0)
    
    # 쉼표 제거, 숫자 변경
    df['shares_total'] = df['shares_total'].str.replace(',', '').replace('', '0').astype(int)
    df['shares_foreign_holding'] = df['shares_foreign_holding'].str.replace(',', '').replace('', '0').astype(int)
    df['shares_foreign_ratio'] = df['shares_foreign_ratio'].str.replace(',', '').replace('', '0').astype(float)
    df['shares_total'] = (df['shares_total'] / 1000).round(0)
    df['shares_foreign_holding'] = (df['shares_foreign_holding'] / 1000).round(0)
    
    # 열 순서 변경
    column_order = ['date', 'code', 'net_buy_institution', 'net_buy_other_corporation', 'net_buy_individual', 'net_buy_foreign', 'market_cap_total', 'market_cap_foreign_holding', 'market_cap_foreign_ratio', 'shares_total', 'shares_foreign_holding', 'shares_foreign_ratio']
    df = df[column_order]
    return df

# print(collect_krx_index_data('20250101', '20250605'))

def main(start_date, end_date):
    try:
        with engine.begin() as connection:
            df = collect_krx_index_data(start_date, end_date)
            df.to_sql(name='index_daily_investor', con=connection, if_exists='append', index=False)
            
        print("데이터가 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {str(e)}")
        raise
    
# main('20120101', '20121231')

for year in range(2006, 2010):
    for month in range(1, 13):
        start_date = f'{year}{month:02d}01'
        last_day = monthrange(year, month)[1]
        end_date = f'{year}{month:02d}{last_day:02d}'
        main(start_date, end_date)
        time.sleep(1)
    