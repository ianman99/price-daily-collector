import requests as rq
import pandas as pd
from datetime import date
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

def collect_krx_stock_data_per(date_str: str) -> pd.DataFrame:
    """단일 날짜의 KRX 주식 PER 데이터 수집"""
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03501',
        'locale': 'ko_KR',
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': date_str,
        'csvxls_isNo': 'false'
    }
    headers = {
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd',
        'User-Agent': 'Mozilla/5.0',
        'Cookie': f"JSESSIONID={krx_session}"
    }

    try:
        response = rq.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('output'):
            print(f"No data for date: {date_str}")
            return pd.DataFrame()
            
        df = pd.DataFrame(data['output'])
        if df.empty:
            return df
            
        df = df[['ISU_SRT_CD', 'PER', 'PBR', 'DVD_YLD']]
        df['date'] = date_str
        df['code'] = 'A' + df['ISU_SRT_CD']
        column_order = ['date', 'code', 'PER', 'PBR', 'DVD_YLD']
        df = df[column_order]
        
        # 쉽표 제거
        df['PER'] = df['PER'].str.replace(',', '')
        df['PBR'] = df['PBR'].str.replace(',', '')
        df['DVD_YLD'] = df['DVD_YLD'].str.replace(',', '')
        
        # 데이터 타입 최적화
        df['PER'] = pd.to_numeric(df['PER'], errors='coerce')
        df['PBR'] = pd.to_numeric(df['PBR'], errors='coerce')
        df['DVD_YLD'] = pd.to_numeric(df['DVD_YLD'], errors='coerce')
        
        params_2 = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT03701',
            'locale': 'ko_KR',
            'searchType': '1',
            'mktId': 'ALL',
            'trdDd': date_str,
            'share': '1',
            'csvxls_isNo': 'false'
        }
        
        response_2 = rq.get(url, params=params_2, headers=headers, timeout=30)
        response_2.raise_for_status()
        data_2 = response_2.json()
        
        df_2 = pd.DataFrame(data_2['output'])
        df_2 = df_2[['ISU_SRT_CD', 'FORN_HD_QTY', 'FORN_SHR_RT', 'FORN_ORD_LMT_QTY']]
        df_2['date'] = date_str
        df_2['code'] = 'A' + df_2['ISU_SRT_CD']
        
        # 컬럼명을 SQL 테이블에 맞게 변경
        df_2 = df_2.rename(columns={
            'FORN_HD_QTY': 'shares_foreign_holding',
            'FORN_SHR_RT': 'shares_foreign_ratio', 
            'FORN_ORD_LMT_QTY': 'shares_foreign_limit'
        })
        
        column_order = ['date', 'code', 'shares_foreign_holding', 'shares_foreign_ratio', 'shares_foreign_limit']
        df_2 = df_2[column_order]
        
        # 쉽표 제거
        df_2['shares_foreign_holding'] = df_2['shares_foreign_holding'].str.replace(',', '')
        df_2['shares_foreign_ratio'] = df_2['shares_foreign_ratio'].str.replace(',', '')
        df_2['shares_foreign_limit'] = df_2['shares_foreign_limit'].str.replace(',', '')
        
        # 데이터 타입 최적화
        df_2['shares_foreign_holding'] = pd.to_numeric(df_2['shares_foreign_holding'], errors='coerce')
        df_2['shares_foreign_ratio'] = pd.to_numeric(df_2['shares_foreign_ratio'], errors='coerce')
        df_2['shares_foreign_limit'] = pd.to_numeric(df_2['shares_foreign_limit'], errors='coerce')
        
        # 두 데이터프레임 합치기(날짜, 코드 기준)
        df = pd.merge(df, df_2, on=['date', 'code'], how='outer')
        
        # PER, PBR, DVD_YLD, 외국인 보유 정보 데이터가 있는 행만 포함
        df = df[df['PER'].notna() | df['PBR'].notna() | df['DVD_YLD'].notna() | df['shares_foreign_holding'].notna() | df['shares_foreign_ratio'].notna() | df['shares_foreign_limit'].notna()]
        
        print(f"Collected {len(df)} records for {date_str}")
        return df
        
    except Exception as e:
        print(f"Error collecting data for {date_str}: {e}")
        return pd.DataFrame()

def upsert_data(df: pd.DataFrame) -> None:
    """데이터 업서트"""
    if df.empty:
        return
    
    # 컬럼명 변경: DVD_YLD -> dividend_yield
    df = df.rename(columns={'DVD_YLD': 'dividend_yield'})
    
    print(f"Starting upsert for {len(df)} records")
    
    with engine.begin() as connection:
        # 기존 임시 테이블이 있다면 삭제
        drop_temp_table_query = text("DROP TEMPORARY TABLE IF EXISTS temp_stock_daily")
        connection.execute(drop_temp_table_query)
        
        # 임시 테이블 생성
        temp_table_query = text("""
            CREATE TEMPORARY TABLE temp_stock_daily (
                date VARCHAR(8),
                code VARCHAR(10),
                PER DECIMAL(10,2),
                PBR DECIMAL(10,2),
                dividend_yield DECIMAL(10,4),
                shares_foreign_holding BIGINT,
                shares_foreign_ratio DECIMAL(10,4),
                shares_foreign_limit BIGINT,
                INDEX idx_temp_date_code (date, code)
            )
        """)
        connection.execute(temp_table_query)
        
        # 임시 테이블에 데이터 삽입
        df.to_sql(
            name='temp_stock_daily', 
            con=connection, 
            if_exists='append', 
            index=False,
            method='multi'
        )
        
        # 임시 테이블에서 메인 테이블로 upsert
        upsert_query = text("""
            INSERT INTO stock_daily (date, code, PER, PBR, dividend_yield, shares_foreign_holding, shares_foreign_ratio, shares_foreign_limit)
            SELECT date, code, PER, PBR, dividend_yield, shares_foreign_holding, shares_foreign_ratio, shares_foreign_limit FROM temp_stock_daily
            ON DUPLICATE KEY UPDATE
                PER = VALUES(PER),
                PBR = VALUES(PBR),
                dividend_yield = VALUES(dividend_yield),
                shares_foreign_holding = VALUES(shares_foreign_holding),
                shares_foreign_ratio = VALUES(shares_foreign_ratio),
                shares_foreign_limit = VALUES(shares_foreign_limit)
        """)
        
        result = connection.execute(upsert_query)
        print(f"Upsert completed. Affected rows: {result.rowcount}")

def main():
    today_date = date.today().strftime("%Y%m%d")
    
    print(f"Starting data collection for {today_date}")
    
    # 오늘 날짜 데이터 수집
    df = collect_krx_stock_data_per(today_date)
    
    # 데이터 업서트
    if not df.empty:
        upsert_data(df)
        print("데이터가 성공적으로 저장되었습니다.")
    else:
        print("수집된 데이터가 없습니다.")

if __name__ == '__main__':
    main()
    