import requests
from login_krx import get_krx_session
import pandas as pd
import time
import os
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_url)

krx_session = get_krx_session()

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": f"JSESSIONID={krx_session}",
}

def get_etf_investor_trend(isu_cd, strt_dd, end_dd):
    resp = requests.post(
        "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
        headers=headers,
        data={
            "bld": "dbms/MDC/STAT/standard/MDCSTAT04902",
            "locale": "ko_KR",
            "inqTpCd": "2",
            "inqCondTpCd1": "1",
            "inqCondTpCd2": "1",
            "isuCd": isu_cd,
            "strtDd": strt_dd,
            "endDd": end_dd,
            "money": "1",
            "csvxls_isNo": "false",
        },
    )
    output = resp.json().get('output', [])
    if output:
        return pd.DataFrame(output)
    return pd.DataFrame()

# ETF 종목 리스트 조회
resp = requests.post(
    "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
    headers=headers,
    data={
        "bld": "dbms/MDC/STAT/standard/MDCSTAT04601",
        "locale": "ko_KR",
        "share": "1",
        "csvxls_isNo": "false",
    },
)
output = resp.json().get('output', [])
if output:
    df_list = pd.DataFrame(output)
    isu_cd_map = dict(zip(df_list['ISU_CD'], df_list['ISU_SRT_CD']))
    print(f"ETF 종목 조회: {len(isu_cd_map)}건")
else:
    isu_cd_map = {}
    print("ETF 종목 리스트 조회 실패")

# NUM_ITM_VAL21=기관합계, NUM_ITM_VAL22=기타법인, NUM_ITM_VAL23=개인, NUM_ITM_VAL24=외국인합계
COL_MAP = {
    'NUM_ITM_VAL21': 'inst_net_buy',
    'NUM_ITM_VAL22': 'corp_net_buy',
    'NUM_ITM_VAL23': 'retail_net_buy',
    'NUM_ITM_VAL24': 'foreign_net_buy',
}

def parse_val(v):
    """쉼표 제거 후 백만원 단위로 변환"""
    return int(round(float(str(v).replace(',', '')) / 1_000_000))

today_date = date.today().strftime("%Y%m%d")

upsert_sql = text("""
    INSERT INTO etf_daily (date, code, inst_net_buy, corp_net_buy, retail_net_buy, foreign_net_buy)
    VALUES (:date, :code, :inst_net_buy, :corp_net_buy, :retail_net_buy, :foreign_net_buy)
    ON DUPLICATE KEY UPDATE
        inst_net_buy = VALUES(inst_net_buy),
        corp_net_buy = VALUES(corp_net_buy),
        retail_net_buy = VALUES(retail_net_buy),
        foreign_net_buy = VALUES(foreign_net_buy)
""")

total = len(isu_cd_map)
items = list(isu_cd_map.items())

for i, (isu_cd, srt_cd) in enumerate(items, 1):
    code = 'A' + srt_cd
    try:
        df = get_etf_investor_trend(isu_cd, today_date, today_date)
        if df.empty:
            time.sleep(0.3)
            continue

        rows = []
        for _, row in df.iterrows():
            trd_date = row['TRD_DD'].replace('/', '')
            rows.append({
                'date': trd_date,
                'code': code,
                'inst_net_buy': parse_val(row['NUM_ITM_VAL21']),
                'corp_net_buy': parse_val(row['NUM_ITM_VAL22']),
                'retail_net_buy': parse_val(row['NUM_ITM_VAL23']),
                'foreign_net_buy': parse_val(row['NUM_ITM_VAL24']),
            })

        with engine.begin() as conn:
            conn.execute(upsert_sql, rows)
        print(f"[{i}/{total}] {code} → {len(rows)}건 업서트 완료")
    except Exception as e:
        print(f"[{i}/{total}] {code} 오류: {str(e)}")
    time.sleep(0.3)

print(f"\n전체 완료! ({total}종목)")
