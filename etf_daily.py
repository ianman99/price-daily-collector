import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text
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

def collect_krx_etf_data():
    today_date = date.today().strftime("%Y%m%d")
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT04301',
        'locale': 'ko_KR',
        'trdDd': today_date,
        'share': '1',
        'money': '1',
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
    df = df[['ISU_SRT_CD', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'LIST_SHRS', 'NAV']]

    # 쉼표 제거 및 숫자 변환
    numeric_cols = ['TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'TDD_CLSPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'LIST_SHRS']
    for col in numeric_cols:
        df[col] = df[col].str.replace(',', '').astype(int)
    df['NAV'] = df['NAV'].str.replace(',', '').astype(float)
    df['ACC_TRDVAL'] = (df['ACC_TRDVAL'] / 1_000_000).round().astype(int)

    # 열 이름 변경
    df.columns = ['code', 'open', 'high', 'low', 'close', 'volume', 'trading_value', 'listed_stocks', 'NAV']
    
    # 종목코드 A 추가
    df['code'] = 'A' + df['code']

    # date 열 추가
    df.insert(0, 'date', today_date)
    return df

def main():
    try:
        with engine.begin() as connection:
            df = collect_krx_etf_data()
            df.to_sql(name='etf_daily', con=connection, if_exists='append', index=False)
            print(df)
            
        print("데이터가 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류가 발생했습니다: {str(e)}")
        raise
    
main()
    