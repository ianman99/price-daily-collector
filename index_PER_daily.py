import requests as rq
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
import exchange_calendars as xcals
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

def collect_krx_index_data(type_num: str, date_str: str) -> pd.DataFrame:
    """단일 날짜의 KRX 지수 PER 데이터 수집"""
    url = 'https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT00701',
        'locale': 'ko_KR',
        'searchType': 'A',
        'idxIndMidclssCd': type_num,
        'trdDd': date_str,
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201010107',
        'User-Agent': 'Mozilla/5.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': f"JSESSIONID={krx_session}"
    }

    try:
        response = rq.post(url, data=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data.get('output'):
            print(f"No data for date: {date_str} (type {type_num})")
            return pd.DataFrame()

        df = pd.DataFrame(data['output'])
        if df.empty:
            return df

        df = df[['IDX_NM', 'WT_PER', 'WT_STKPRC_NETASST_RTO', 'DIV_YD']]
        df = df.rename(columns={
            'IDX_NM': 'code',
            'WT_PER': 'PER',
            'WT_STKPRC_NETASST_RTO': 'PBR',
            'DIV_YD': 'dividend_yield'
        })

        # 지수명 공백 제거
        df['code'] = df['code'].str.replace(' ', '')
        # 지수명이 코스피, 코스피200, 코스닥, 코스닥150 인 행을 제외하고 나머지 행 삭제
        df = df[df['code'].isin(['코스피', '코스피200', '코스닥', '코스닥150'])].copy()

        df['date'] = date_str

        # 쉼표 제거 후 데이터 타입 최적화
        for col in ['PER', 'PBR', 'dividend_yield']:
            df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')

        # PER 이 0 인 행 삭제
        df = df[df['PER'] != 0]

        print(f"Collected {len(df)} records for {date_str} (type {type_num})")
        return df[['date', 'code', 'PER', 'PBR', 'dividend_yield']]

    except Exception as e:
        print(f"Error collecting data for {date_str} (type {type_num}): {e}")
        return pd.DataFrame()

def main():
    list = ['02', '03']
    today = date.today()
    krx = xcals.get_calendar("XKRX")
    prev_trading_day = krx.previous_session(pd.Timestamp(today)).strftime("%Y%m%d")
    date_list = [prev_trading_day, today.strftime("%Y%m%d")]
    for date_str in date_list:
        for i in list:
            df = collect_krx_index_data(i, date_str)
            if df.empty:
                continue
            with engine.connect() as conn:
                for _, row in df.iterrows():
                    date_value = row['date']
                    code = row['code']
                    per = 'NULL' if pd.isna(row['PER']) else row['PER']
                    pbr = 'NULL' if pd.isna(row['PBR']) else row['PBR']
                    dividend_yield = 'NULL' if pd.isna(row['dividend_yield']) else row['dividend_yield']
                    update_query = f"""
                    UPDATE index_daily
                    SET PER = {per}, PBR = {pbr}, dividend_yield = {dividend_yield}
                    WHERE date = '{date_value}' AND code = '{code}'
                    """
                    conn.execute(text(update_query))
                conn.commit()

main()

