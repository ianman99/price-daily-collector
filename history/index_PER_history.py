import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
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
    today_date = date.today().strftime("%Y%m%d")
    gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    gen_otp_stk = {
        'locale': 'ko_KR',
        'searchType': 'P',
        'idxIndMidclssCd': '03',
        'trdDd': endDd,
        'tboxindTpCd_finder_equidx0_5': index_code,
        'indTpCd': '1',
        'indTpCd2': '028',
        'codeNmindTpCd_finder_equidx0_5': index_code,
        'param1indTpCd_finder_equidx0_5': '',
        'strtDd': strtDd,
        'endDd': endDd,
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00702'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd', "User-Agent": "Mozilla/5.0"}

    otp_stk = rq.post(gen_otp_url, gen_otp_stk, headers=headers).text
    
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_sector_stk = rq.post(down_url, {'code': otp_stk}, headers=headers)
    df = pd.read_csv(BytesIO(down_sector_stk.content), encoding='EUC-KR')
    df = df[['일자', 'PER', 'PBR', '배당수익률']]
    # 코드 추가
    df['code'] = index_code
    df['code'] = df['code'].str.replace(' ', '')
    df.rename(columns={
        '일자': 'date',
        'PER': 'PER',
        'PBR': 'PBR',
        '배당수익률': 'dividend_yield'
    }, inplace=True)
    # 열 순서 변경
    column_order = ['date', 'code', 'PER', 'PBR', 'dividend_yield']
    df = df[column_order]

    return df


# print(collect_krx_index_data('코스닥 150', '20240101', '20241231'))

df = collect_krx_index_data('코스닥', '20240101', '20241231')
with engine.connect() as conn:
    for _, row in df.iterrows():
        date = row['date']
        code = row['code']
        per = 'NULL' if pd.isna(row['PER']) else row['PER']
        pbr = 'NULL' if pd.isna(row['PBR']) else row['PBR']
        dividend_yield = 'NULL' if pd.isna(row['dividend_yield']) else row['dividend_yield']
        update_query = f"""
        UPDATE index_daily
        SET PER = {per}, PBR = {pbr}, dividend_yield = {dividend_yield}
        WHERE date = '{date}' AND code = '{code}'
        """
        conn.execute(text(update_query))
    conn.commit()



# for year in range(2010, 2025):
#     start_date = f'{year}0101'
#     end_date = f'{year}1231'
#     df = collect_krx_index_data('코스피 200', start_date, end_date)
#     with engine.connect() as conn:
#         for _, row in df.iterrows():
#             date_value = row['date']
#             code = row['code']
#             per = 'NULL' if pd.isna(row['PER']) else row['PER']
#             pbr = 'NULL' if pd.isna(row['PBR']) else row['PBR']
#             dividend_yield = 'NULL' if pd.isna(row['dividend_yield']) else row['dividend_yield']
#             update_query = f"""
#             UPDATE index_daily
#             SET PER = {per}, PBR = {pbr}, dividend_yield = {dividend_yield}
#             WHERE date = '{date_value}' AND code = '{code}'
#             """
#             conn.execute(text(update_query))
#         conn.commit()
    