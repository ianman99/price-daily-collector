import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import exchange_calendars as xcals
import time
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

def collect_krx_index_data(today_date):
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
        'strtDd': today_date,
        'endDd': today_date,
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f"JSESSIONID={krx_session}"
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
        'strtDd': today_date,
        'endDd': today_date,
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_2 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f"JSESSIONID={krx_session}"
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
        'strtDd': today_date,
        'endDd': today_date,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_3 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f"JSESSIONID={krx_session}"
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
        'strtDd': today_date,
        'endDd': today_date,
        'share': '2',
        'money': '3',
        'csvxls_isNo': 'false'
    }
    headers_4 = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f"JSESSIONID={krx_session}"
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
    df['net_buy_institution'] = df['net_buy_institution'].str.replace(',', '').astype(int)
    df['net_buy_other_corporation'] = df['net_buy_other_corporation'].str.replace(',', '').astype(int)
    df['net_buy_individual'] = df['net_buy_individual'].str.replace(',', '').astype(int)
    df['net_buy_foreign'] = df['net_buy_foreign'].str.replace(',', '').astype(int)
    df['net_buy_institution'] = (df['net_buy_institution'] / 1000000).round(0)
    df['net_buy_other_corporation'] = (df['net_buy_other_corporation'] / 1000000).round(0)
    df['net_buy_individual'] = (df['net_buy_individual'] / 1000000).round(0)
    df['net_buy_foreign'] = (df['net_buy_foreign'] / 1000000).round(0)
    
    # 쉼표 제거, 숫자 변경
    df['market_cap_total'] = df['market_cap_total'].str.replace(',', '').astype(int)
    df['market_cap_foreign_holding'] = df['market_cap_foreign_holding'].str.replace(',', '').astype(int)
    df['market_cap_foreign_ratio'] = df['market_cap_foreign_ratio'].str.replace(',', '').astype(float)
    df['market_cap_total'] = (df['market_cap_total'] / 1000000).round(0)
    df['market_cap_foreign_holding'] = (df['market_cap_foreign_holding'] / 1000000).round(0)
    
    # 쉼표 제거, 숫자 변경
    df['shares_total'] = df['shares_total'].str.replace(',', '').astype(int)
    df['shares_foreign_holding'] = df['shares_foreign_holding'].str.replace(',', '').astype(int)
    df['shares_foreign_ratio'] = df['shares_foreign_ratio'].str.replace(',', '').astype(float)
    df['shares_total'] = (df['shares_total'] / 1000).round(0)
    df['shares_foreign_holding'] = (df['shares_foreign_holding'] / 1000).round(0)
    
    # 열 순서 변경
    column_order = ['date', 'code', 'net_buy_institution', 'net_buy_other_corporation', 'net_buy_individual', 'net_buy_foreign', 'market_cap_total', 'market_cap_foreign_holding', 'market_cap_foreign_ratio', 'shares_total', 'shares_foreign_holding', 'shares_foreign_ratio']
    df = df[column_order]
    return df

def main():
    today = date.today()
    krx = xcals.get_calendar("XKRX")
    prev_trading_day = krx.previous_session(pd.Timestamp(today)).strftime("%Y%m%d")
    date_list = [prev_trading_day, today.strftime("%Y%m%d")]

    for date_str in date_list:
        print(f"{date_str} 처리 시작")
        try:
            df = collect_krx_index_data(date_str)
            with engine.begin() as connection:
                df.to_sql(name='index_daily_investor', con=connection, if_exists='append', index=False)
            print(f"{date_str} 저장 완료")
        except Exception as e:
            print(f"{date_str} 오류: {str(e)}")
    
main()
    