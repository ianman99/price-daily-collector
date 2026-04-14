import requests
from login_krx import get_krx_session
from datetime import date, timedelta
import pandas as pd
import exchange_calendars as xcals
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
db_url = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(db_url)

krx_session = get_krx_session()

# 날짜 범위 생성 (전 거래일 + 오늘)
today = date.today()
krx = xcals.get_calendar("XKRX")
prev_trading_day = krx.previous_session(pd.Timestamp(today)).strftime("%Y%m%d")
target_dates = [prev_trading_day, today.strftime("%Y%m%d")]

print(f"처리 대상: {len(target_dates)}일 (주말 제외)")

url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303",
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": f"JSESSIONID={krx_session}",
}

def get_investor_data(category_code, target_date):
    data = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02401",
        "locale": "ko_KR",
        "mktId": "ALL",
        "invstTpCd": category_code,
        "strtDd": target_date,
        "endDd": target_date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
    }
    resp = requests.post(url, headers=headers, data=data)
    try:
        output = resp.json().get('output', [])
    except Exception:
        return pd.DataFrame(columns=['code', 'net_buy'])
    if not output:
        return pd.DataFrame(columns=['code', 'net_buy'])
    df = pd.DataFrame(output)
    df['code'] = 'A' + df['ISU_SRT_CD']
    df['net_buy'] = (df['NETBID_TRDVAL'].str.replace(',', '').astype(float) / 1000000).round(0).astype(int)
    return df[['code', 'net_buy']]

total = len(target_dates)
for i, today_date in enumerate(target_dates, 1):
    print(f"\n[{i}/{total}] {today_date} 처리 시작")

    # 기관합계 먼저 호출하여 휴장일 판단
    df_inst = get_investor_data('7050', today_date)
    if df_inst.empty:
        print(f"{today_date} 휴장일 → 스킵")
        time.sleep(0.3)
        continue

    # 나머지 4개 카테고리 병렬 호출
    with ThreadPoolExecutor(max_workers=4) as executor:
        f_corp = executor.submit(get_investor_data, '7100', today_date)
        f_retail = executor.submit(get_investor_data, '8000', today_date)
        f_foreign = executor.submit(get_investor_data, '9000', today_date)
        f_foreign_other = executor.submit(get_investor_data, '9001', today_date)

        df_corp = f_corp.result()
        df_retail = f_retail.result()
        df_foreign = f_foreign.result()
        df_foreign_other = f_foreign_other.result()

    df_foreign_total = pd.concat([df_foreign, df_foreign_other]).groupby('code')['net_buy'].sum().reset_index()

    update_map = {
        'inst_net_buy': df_inst,
        'corp_net_buy': df_corp,
        'retail_net_buy': df_retail,
        'foreign_net_buy': df_foreign_total,
    }

    with engine.begin() as conn:
        for col_name, df_inv in update_map.items():
            if df_inv.empty:
                continue
            query = text(f"UPDATE stock_daily SET {col_name} = :val WHERE date = :date AND code = :code")
            params = [{"val": int(row['net_buy']), "date": today_date, "code": row['code']}
                      for _, row in df_inv.iterrows()]
            conn.execute(query, params)

    print(f"{today_date} 완료")
    time.sleep(0.5)

print("\n전체 완료!")
