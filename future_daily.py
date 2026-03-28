import requests as rq
import pandas as pd
from datetime import date, timedelta
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

def collect_krx_index_future_data(code, aggBasTpCd='0'):
    today = date.today()
    strtDd = (today - timedelta(days=7)).strftime("%Y%m%d")
    endDd = (today + timedelta(days=7)).strftime("%Y%m%d")
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f'JSESSIONID={krx_session}',
    }
    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT12701',
        'locale': 'ko_KR',
        'prodId': code,
        'strtDd': strtDd,
        'endDd': endDd,
        'aggBasTpCd': aggBasTpCd,
        'share': '1',
        'money': '3',
        'csvxls_isNo': 'false',
    }
    response = rq.post(url, headers=headers, data=data)
    response.raise_for_status()
    json_data = response.json()
    df = pd.DataFrame(json_data['output'])

    # 공백 및 (주간)/(야간) 제거
    df['TRD_DD'] = df['TRD_DD'].str.replace(' ', '')
    df['TRD_DD'] = df['TRD_DD'].str.replace('(주간)', '')
    df['TRD_DD'] = df['TRD_DD'].str.replace('(야간)', '')

    # 야간선물: T+1 영업일 → T일로 변환 (KRX 캘린더 기준)
    if aggBasTpCd == '2':
        krx = xcals.get_calendar("XKRX")
        df['TRD_DD'] = df['TRD_DD'].apply(
            lambda x: krx.previous_session(pd.Timestamp(x)).strftime('%Y/%m/%d')
        )
    df['ISU_NM'] = df['ISU_NM'].str.replace(' ', '')

    # 만약 ISU_NM에 '선물'이 포함되어 있으면 'KOSPI 200 선물' 을 '코스피200 F' 로 변경
    df['ISU_NM'] = df['ISU_NM'].str.replace('선물', 'F')
    df['ISU_NM'] = df['ISU_NM'].str.replace('KOSPI', '코스피')

    # code, maturity 분리
    df[['code', 'maturity']] = df['ISU_NM'].str.split('F', expand=True)
    df['code'] = df['code'] + 'F'
    if aggBasTpCd == '2':
        df['code'] = df['code'] + 'N'
    df['maturity'] = df['maturity'].apply(
        lambda x: x[:4] + '-' + x[4:6] if not pd.isna(x) else x
    )

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
    if df.empty:
        return
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

# 주간선물
df_1 = collect_krx_index_future_data('KR___FUK2I')
upsert_index_future_daily(df_1, engine)

df_2 = collect_krx_index_future_data('KR___FUKQI')
upsert_index_future_daily(df_2, engine)

# 야간선물
df_3 = collect_krx_index_future_data('KR___FUK2I', '2')
upsert_index_future_daily(df_3, engine)

df_4 = collect_krx_index_future_data('KR___FUKQI', '2')
upsert_index_future_daily(df_4, engine)
