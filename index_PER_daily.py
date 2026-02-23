import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
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

def collect_krx_index_data(type_num, date_str):
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'searchType': 'A',
        'idxIndMidclssCd': type_num,
        'trdDd': date_str,
        'param1indTpCd_finder_equidx0_5': '',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00701'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd', "User-Agent": "Mozilla/5.0", "Cookie": f"JSESSIONID={krx_session}"}
    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    df = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
    
    # 지수명 공백 제거
    df['지수명'] = df['지수명'].str.replace(' ', '')
    # 지수명이 코스피, 코스피200, 코스닥, 코스닥150 인 행을 제외하고 나머지 행 삭제
    df = df[df['지수명'].isin(['코스피', '코스피200', '코스닥', '코스닥150'])]
    
    # 지수명, PER, PBR, 배당수익률 컬럼만 남기기
    df = df[['지수명', 'PER', 'PBR', '배당수익률']]
    df['date'] = date_str
    df.rename(columns={
        '지수명': 'code',
        'PER': 'PER',
        'PBR': 'PBR',
        '배당수익률': 'dividend_yield'
    }, inplace=True)
    # PER 이 0 인 행 삭제
    df= df[df['PER'] != 0]
    
    return df

def main():
    list = ['02', '03']
    date_list = [(date.today() - timedelta(days=i)).strftime("%Y%m%d") for i in range(5)]
    for date_str in date_list:
        for i in list:
            df = collect_krx_index_data(i, date_str)
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

