import requests as rq
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

def collect_krx_index_future_data(code):
    today_date = date.today().strftime("%Y%m%d")
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'http://data.krx.co.kr',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050103',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': f"JSESSIONID={krx_session}"
    }
    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT12701',
        'locale': 'ko_KR',
        'prodId': code,
        'strtDd': today_date,
        'endDd': today_date,
        'aggBasTpCd': '0',
        'share': '1',
        'money': '3',
        'csvxls_isNo': 'false',
    }
    response = rq.post(url, headers=headers, data=data)
    response.raise_for_status()
    json_data = response.json()
    df = pd.DataFrame(json_data['output'])

    # 공백 및 (주간) 제거
    df['TRD_DD'] = df['TRD_DD'].str.replace(' ', '')
    df['TRD_DD'] = df['TRD_DD'].str.replace('(주간)', '')
    df['ISU_NM'] = df['ISU_NM'].str.replace(' ', '')
    
    # code와 maturity 분리
    df[['code', 'maturity']] = df['ISU_NM'].str.split('F', expand=True)
    df['code'] = df['code'] + 'F'  # code에 'F' 추가
    df['maturity'] = df['maturity'].str[:4] + '-' + df['maturity'].str[4:6]

    # 컬럼명 매핑
    df.rename(columns={
        'TRD_DD': 'date',
        'TDD_OPNPRC': 'open',
        'TDD_HGPRC': 'high',
        'TDD_LWPRC': 'low',
        'TDD_CLSPRC': 'close',
        'ACC_TRDVOL': 'volume',
        'ACC_TRDVAL': 'trading_value',
        'ACC_OPNINT_QTY': 'unsettled',
    }, inplace=True)

    # 불필요 컬럼 제거
    df = df.drop(columns=['ISU_CD', 'ISU_SRT_CD', 'ISU_NM', 'FLUC_TP_CD', 'CMPPREVDD_PRC', 'SETL_PRC'])

    # 열 순서 변경 전에 trading_value를 백만 단위로 반올림
    df['trading_value'] = (
        pd.to_numeric(df['trading_value'].astype(str).str.replace(',', ''), errors='coerce') / 1_000_000
    ).round().astype('Int64')

    # 숫자형 컬럼 일괄 변환
    num_cols = ['open', 'high', 'low', 'close', 'volume', 'trading_value', 'unsettled']
    for col in num_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

    column_order = ['date', 'code', 'open', 'high', 'low', 'close', 
                    'volume', 'trading_value', 'maturity', 'unsettled']
    df = df[column_order]

    return df

# print(collect_krx_index_future_data('KR___FUK2I', '20250101', '20250613')) 
# print(collect_krx_index_future_data('KR___FUKQI', '20250101', '20250613')) 

df_1 = collect_krx_index_future_data('KR___FUK2I')
df_1.to_sql(name='index_future_daily', con=engine, if_exists='append', index=False)

df_2 = collect_krx_index_future_data('KR___FUKQI')
df_2.to_sql(name='index_future_daily', con=engine, if_exists='append', index=False)
