import requests
from login_krx import get_krx_session
import pandas as pd
import json
import time
import os
from datetime import date, timedelta
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
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": f"JSESSIONID={krx_session}",
}

def get_investor_trend(isu_cd, strt_dd, end_dd):
    resp = requests.post(
        "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
        headers=headers,
        data={
            "bld": "dbms/MDC/STAT/standard/MDCSTAT02302",
            "locale": "ko_KR",
            "inqTpCd": "2",
            "trdVolVal": "2",
            "askBid": "3",
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

ISU_CD_FILE = "isu_cd_list.json"

# JSON 파일이 있으면 이어서, 없으면 KRX에서 새로 조회
if os.path.exists(ISU_CD_FILE):
    with open(ISU_CD_FILE, 'r') as f:
        isu_cd_map = json.load(f)
    print(f"JSON 파일에서 로드: 잔여 {len(isu_cd_map)}건")
else:
    resp = requests.post(
        "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
        headers=headers,
        data={
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01901",
            "locale": "ko_KR",
            "mktId": "ALL",
            "share": "1",
            "csvxls_isNo": "false",
        },
    )
    output = resp.json().get('OutBlock_1', [])
    if output:
        df_list = pd.DataFrame(output)
        isu_cd_map = dict(zip(df_list['ISU_CD'], df_list['ISU_SRT_CD']))
        with open(ISU_CD_FILE, 'w') as f:
            json.dump(isu_cd_map, f, ensure_ascii=False, indent=2)
        print(f"KRX 조회 → JSON 저장: {len(isu_cd_map)}건")
    else:
        isu_cd_map = {}
        print("종목 리스트 조회 실패")

# TRDVAL1=기관합계, TRDVAL2=기타법인, TRDVAL3=개인, TRDVAL4=외국인합계
COL_MAP = {
    'TRDVAL1': 'inst_net_buy',
    'TRDVAL2': 'corp_net_buy',
    'TRDVAL3': 'retail_net_buy',
    'TRDVAL4': 'foreign_net_buy',
}

def parse_val(v):
    """쉼표 제거 후 백만원 단위로 변환"""
    return int(round(float(str(v).replace(',', '')) / 1_000_000))

GLOBAL_START = date(2000, 1, 1)
GLOBAL_END = date(2026, 2, 5)

# 1년 단위 기간 분할
def make_yearly_ranges(start, end):
    ranges = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor.replace(year=cursor.year + 1) - timedelta(days=1), end)
        ranges.append((cursor.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cursor = chunk_end + timedelta(days=1)
    return ranges

date_ranges = make_yearly_ranges(GLOBAL_START, GLOBAL_END)
print(f"기간 분할: {len(date_ranges)}구간 ({date_ranges[0][0]} ~ {date_ranges[-1][1]})")

upsert_sql = text("""
    INSERT INTO stock_daily (date, code, inst_net_buy, corp_net_buy, retail_net_buy, foreign_net_buy)
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
    total_rows = 0

    for strt_dd, end_dd in date_ranges:
        df = get_investor_trend(isu_cd, strt_dd, end_dd)
        if df.empty:
            time.sleep(0.3)
            continue

        rows = []
        for _, row in df.iterrows():
            trd_date = row['TRD_DD'].replace('/', '')
            rows.append({
                'date': trd_date,
                'code': code,
                'inst_net_buy': parse_val(row['TRDVAL1']),
                'corp_net_buy': parse_val(row['TRDVAL2']),
                'retail_net_buy': parse_val(row['TRDVAL3']),
                'foreign_net_buy': parse_val(row['TRDVAL4']),
            })

        with engine.begin() as conn:
            conn.execute(upsert_sql, rows)
        total_rows += len(rows)
        time.sleep(0.3)

    # 업서트 완료 → JSON에서 제거 후 저장
    del isu_cd_map[isu_cd]
    with open(ISU_CD_FILE, 'w') as f:
        json.dump(isu_cd_map, f, ensure_ascii=False, indent=2)

    print(f"[{i}/{total}] {code} → {total_rows}건 업서트 완료, JSON에서 삭제 (잔여: {len(isu_cd_map)})")
    time.sleep(0.3)

# 모두 완료되면 JSON 파일 삭제
if os.path.exists(ISU_CD_FILE) and len(isu_cd_map) == 0:
    os.remove(ISU_CD_FILE)
    print(f"\n전체 완료! JSON 파일 삭제됨 ({total}종목)")
else:
    print(f"\n중단됨. 잔여: {len(isu_cd_map)}건 (재실행 시 이어서 처리)")
