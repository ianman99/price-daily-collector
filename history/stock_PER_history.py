import requests as rq
from io import BytesIO
import pandas as pd
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, text
import time
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import List, Tuple
import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MySQL 연결 정보 설정
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(
    db_url,
    pool_size=20,          # 연결 풀 크기 증가
    max_overflow=30,       # 추가 연결 허용
    pool_pre_ping=True,    # 연결 상태 확인
    pool_recycle=3600      # 1시간마다 연결 재생성
)

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
        'User-Agent': 'Mozilla/5.0'
    }

    try:
        response = rq.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('output'):
            logger.warning(f"No data for date: {date_str}")
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
        
        logger.info(f"Collected {len(df)} records for {date_str}")
        return df
        
    except Exception as e:
        logger.error(f"Error collecting data for {date_str}: {e}")
        return pd.DataFrame()

def collect_data_batch(date_list: List[str]) -> List[pd.DataFrame]:
    """배치로 데이터 수집 (멀티스레딩 사용)"""
    results = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:  # API 서버 부하를 고려하여 5개로 제한
        future_to_date = {executor.submit(collect_krx_stock_data_per, date_str): date_str 
                         for date_str in date_list}
        
        for future in as_completed(future_to_date):
            date_str = future_to_date[future]
            try:
                df = future.result()
                if not df.empty:
                    results.append(df)
                time.sleep(0.1)  # API 서버 부하 방지를 위한 짧은 지연
            except Exception as e:
                logger.error(f"Error processing {date_str}: {e}")
    
    return results

def bulk_upsert_data(df_list: List[pd.DataFrame]) -> None:
    """배치로 데이터 업서트"""
    if not df_list:
        return
        
    # 모든 DataFrame 합치기
    combined_df = pd.concat(df_list, ignore_index=True)
    
    if combined_df.empty:
        return
    
    # 컬럼명 변경: DVD_YLD -> dividend_yield
    combined_df = combined_df.rename(columns={'DVD_YLD': 'dividend_yield'})
    
    logger.info(f"Starting bulk upsert for {len(combined_df)} records")
    
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
        
        # 임시 테이블에 데이터 삽입 (배치 처리)
        batch_size = 1000
        for i in range(0, len(combined_df), batch_size):
            batch_df = combined_df.iloc[i:i+batch_size]
            batch_df.to_sql(
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
        logger.info(f"Bulk upsert completed. Affected rows: {result.rowcount}")

def get_business_days(start_date: str, end_date: str) -> List[str]:
    """영업일 목록 생성 (주말 제외)"""
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    
    business_days = []
    current = start
    
    while current <= end:
        # 주말 제외 (월요일=0, 일요일=6)
        if current.weekday() < 5:
            business_days.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    
    return business_days

def get_missing_dates(start_date: str, end_date: str) -> List[str]:
    """DB에 없는 날짜 찾기"""
    business_days = get_business_days(start_date, end_date)
    
    with engine.connect() as connection:
        # DB에 있는 날짜 조회
        existing_dates_query = text("""
            SELECT DISTINCT date 
            FROM stock_daily 
            WHERE date BETWEEN :start_date AND :end_date
        """)
        
        result = connection.execute(existing_dates_query, {
            'start_date': start_date,
            'end_date': end_date
        })
        
        existing_dates = {row[0] for row in result}
    
    # 없는 날짜만 반환
    missing_dates = [date for date in business_days if date not in existing_dates]
    logger.info(f"Found {len(missing_dates)} missing dates out of {len(business_days)} business days")
    
    return missing_dates

def main():
    start_date = '20000101'
    end_date = '20241231'
    
    logger.info(f"Starting data collection from {start_date} to {end_date}")
    
    # 1. 먼저 누락된 날짜만 찾기
    missing_dates = get_missing_dates(start_date, end_date)
    
    if not missing_dates:
        logger.info("No missing dates found. All data is up to date.")
        return
    
    # 2. 날짜를 배치로 나누기 (한 번에 너무 많이 처리하지 않도록)
    batch_size = 30  # 한 달 정도씩 처리
    total_batches = (len(missing_dates) + batch_size - 1) // batch_size
    
    logger.info(f"Processing {len(missing_dates)} dates in {total_batches} batches")
    
    for i in range(0, len(missing_dates), batch_size):
        batch_dates = missing_dates[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_dates)} dates)")
        
        # 3. 배치별로 데이터 수집
        df_list = collect_data_batch(batch_dates)
        
        # 4. 배치별로 DB 업데이트
        if df_list:
            bulk_upsert_data(df_list)
        
        # 5. 배치 간 짧은 휴식 (API 서버 부하 방지)
        if i + batch_size < len(missing_dates):
            time.sleep(2)
    
    logger.info("Data collection and update completed!")

if __name__ == '__main__':
    main()
    