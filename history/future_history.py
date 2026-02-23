import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.mysql import insert
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

def collect_krx_index_future_data(code, strtDd, endDd):
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
    }
    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT12701',
        'locale': 'ko_KR',
        'prodId': code,
        'strtDd': strtDd,
        'endDd': endDd,
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
    
    # 만약 ISU_NM에 '선물'이 포함되어 있으면 'KOSPI 200 선물' 을 '코스피200 F' 로 변경
    df['ISU_NM'] = df['ISU_NM'].str.replace('선물', 'F')
    df['ISU_NM'] = df['ISU_NM'].str.replace('KOSPI', '코스피')

    # code, maturity 분리
    df[['code', 'maturity']] = df['ISU_NM'].str.split('F', expand=True)
    df['code'] = df['code'] + 'F'
    # maturity 처리: 길이가 4면 앞자리 따라 20 또는 19를 붙이고, 아니면 기존처럼
    def format_maturity(x):
        if pd.isna(x):
            return x
        x = str(x)
        if len(x) == 4:
            if x[0] == '0':
                return '20' + x[:2] + '-' + x[2:]
            elif x[0] == '9':
                return '19' + x[:2] + '-' + x[2:]
            else:
                return x  # 예외처리: 예상치 못한 경우 원본 반환
        else:
            return x[:4] + '-' + x[4:6]
    df['maturity'] = df['maturity'].apply(format_maturity)

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

def upsert_index_future_daily(df, engine):
    """
    DataFrame을 index_future_daily 테이블에 (date, code) 프라이머리 키 기준으로 업설트합니다.
    """
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
            maturity = None if pd.isna(row['maturity']) else row['maturity']
            unsettled = None if pd.isna(row['unsettled']) else row['unsettled']
            insert_query = text("""
            INSERT INTO index_future_daily (date, code, open, high, low, close, volume, trading_value, maturity, unsettled)
            VALUES (:date, :code, :open, :high, :low, :close, :volume, :trading_value, :maturity, :unsettled)
            ON DUPLICATE KEY UPDATE
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume),
                trading_value = VALUES(trading_value),
                maturity = VALUES(maturity),
                unsettled = VALUES(unsettled)
            """)
            conn.execute(insert_query, {
                'date': date_value, 'code': code, 'open': open_, 'high': high, 'low': low, 'close': close,
                'volume': volume, 'trading_value': trading_value, 'maturity': maturity, 'unsettled': unsettled
            })
        conn.commit()

# print(collect_krx_index_future_data('KR___FUK2I', '20250101', '20250613')) 
# print(collect_krx_index_future_data('KR___FUKQI', '20250101', '20250613')) 

df_1 = collect_krx_index_future_data('KR___FUK2I', '20250601', '20250619')
upsert_index_future_daily(df_1, engine)

df_2 = collect_krx_index_future_data('KR___FUKQI', '20250601', '20250619')
upsert_index_future_daily(df_2, engine)

# for year in range(2015, 2025):
#     start_date = f'{year}0101'
#     end_date = f'{year}1231'
#     df = collect_krx_index_future_data('KR___FUKQI', start_date, end_date)
#     upsert_index_future_daily(df, engine)
    
#     time.sleep(1)

# for year in range(2020, 2025):
#     start_date = f'{year}0101'
#     end_date = f'{year}1231'
#     df = collect_krx_index_future_data('KR___FUK2I', start_date, end_date)
#     upsert_index_future_daily(df, engine)
#     print(f'{year} 년 데이터 업데이트 완료')
    
#     time.sleep(1)