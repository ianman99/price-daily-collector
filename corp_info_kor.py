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

def collect_corp_info_data(market_type: str):
    today_date = date.today().strftime("%Y%m%d")
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
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03901',
        'locale': 'ko_KR',
        'mktId': market_type,
        'trdDd': today_date,
        'money': '1',
        'csvxls_isNo': 'false',
    }
    response = rq.post(url, headers=headers, data=data)
    response.raise_for_status()
    json_data = response.json()
    df = pd.DataFrame(json_data['block1'])
    
    # 필요한 컬럼만 남기기
    df = df[['ISU_SRT_CD', 'ISU_ABBRV', 'MKT_TP_NM', 'IDX_IND_NM', 'MKTCAP']]
    df.rename(columns={
        'ISU_SRT_CD': 'code',
        'ISU_ABBRV': 'name',
        'MKT_TP_NM': 'market',
        'IDX_IND_NM': 'sector',
        'MKTCAP': 'market_cap',
    }, inplace=True)
    df['code'] = 'A' + df['code']
    df['market_cap'] = df['market_cap'].str.replace(',', '').astype(int)
    
    column_order = ['code', 'name', 'market', 'sector', 'market_cap']
    df = df[column_order]

    return df

def collect_corp_stcode_data():
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
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
    }
    response = rq.post(url, headers=headers, data=data)
    response.raise_for_status()
    json_data = response.json()
    df = pd.DataFrame(json_data['OutBlock_1'])
    
    # 필요한 컬럼만 남기기
    df = df[['ISU_CD', 'ISU_SRT_CD', 'LIST_DD']]
    df.rename(columns={
        'ISU_CD': 'st_code',
        'ISU_SRT_CD': 'code',
        'LIST_DD': 'list_dt',
    }, inplace=True)
    df['code'] = 'A' + df['code']
    column_order = ['st_code', 'code', 'list_dt']
    df = df[column_order]

    return df

# STK와 KSQ 데이터를 수집하고 수직으로 합치기
stk_data = collect_corp_info_data('STK')
ksq_data = collect_corp_info_data('KSQ')
stcode_data = collect_corp_stcode_data()

# 두 데이터프레임을 수직으로 합치기
combined_data = pd.concat([stk_data, ksq_data], ignore_index=True)

# stcode_data와 combined_data를 수평으로 합치기 combined_data에 st_code 추가 (combined_data에 존재하는 code에 해당하는 st_code 추가)
combined_data = pd.merge(combined_data, stcode_data, on='code', how='left')

print(f"STK 데이터 개수: {len(stk_data)}")
print(f"KSQ 데이터 개수: {len(ksq_data)}")
print(f"STCODE 데이터 개수: {len(stcode_data)}")
print(f"합친 데이터 개수: {len(combined_data)}")

# 기존 테이블 내용 삭제 후 새 데이터 삽입
try:
    # 기존 데이터 삭제
    with engine.connect() as conn:
        # 데이터베이스 선택
        conn.execute(text("USE fin_db"))
        # 안전모드 해제
        conn.execute(text("SET SQL_SAFE_UPDATES = 0"))
        
        # 기존 데이터 삭제
        conn.execute(text("DELETE FROM corp_info_kor"))
        
        # 안전모드 다시 활성화
        conn.execute(text("SET SQL_SAFE_UPDATES = 1"))
        conn.commit()
    print("데이터베이스 작업 완료")
    
    # 새 데이터 삽입
    combined_data.to_sql('corp_info_kor', engine, if_exists='append', index=False)
    print(f"corp_info_kor 테이블에 {len(combined_data)}개 데이터 삽입 완료")
    
except Exception as e:
    print(f"데이터베이스 작업 중 오류 발생: {e}")
