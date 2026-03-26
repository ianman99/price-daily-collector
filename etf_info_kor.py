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
DB_NAME = os.getenv('DB_NAME_FIN')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_url)

# KRX 세션 가져오기
krx_session = get_krx_session()

def collect_etf_info_data():
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'http://data.krx.co.kr',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201050103',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': f"JSESSIONID={krx_session}"
    }
    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT04601',
        'locale': 'ko_KR',
        'share': '1',
        'csvxls_isNo': 'false',
    }
    response = rq.post(url, headers=headers, data=data)
    response.raise_for_status()
    json_data = response.json()
    df = pd.DataFrame(json_data['output'])
    
    # 필요한 컬럼만 남기기
    df = df[['ISU_SRT_CD', 'ISU_CD', 'ISU_ABBRV', 'ETF_OBJ_IDX_NM', 'LIST_DD']]
    df.rename(columns={
        'ISU_CD': 'st_code',
        'ISU_ABBRV': 'name',
        'ETF_OBJ_IDX_NM': 'benchmark',
        'ISU_SRT_CD': 'code',
        'LIST_DD': 'list_dt',
    }, inplace=True)
    df['code'] = 'A' + df['code']

    return df

# 기존 테이블 내용 삭제 후 새 데이터 삽입
try:
    # 기존 데이터 삭제
    with engine.connect() as conn:
        # 데이터베이스 선택
        conn.execute(text("USE fin_db"))
        # 안전모드 해제
        conn.execute(text("SET SQL_SAFE_UPDATES = 0"))
        
        # 기존 데이터 삭제
        conn.execute(text("DELETE FROM etf_info_kor"))
        
        # 안전모드 다시 활성화
        conn.execute(text("SET SQL_SAFE_UPDATES = 1"))
        conn.commit()
    print("데이터베이스 작업 완료")
    
    # 새 데이터 삽입
    df = collect_etf_info_data()
    df.to_sql('etf_info_kor', engine, if_exists='append', index=False)
    print(f"corp_info_kor 테이블에 {len(df)}개 데이터 삽입 완료")
    
except Exception as e:
    print(f"데이터베이스 작업 중 오류 발생: {e}")
