import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from sqlalchemy import create_engine, text
import aiohttp
import asyncio
from typing import List
import time
import os
from dotenv import load_dotenv

# ======================== 환경 변수 로드 ========================
load_dotenv()

# ======================== 로깅 설정 ========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ======================== DB 연결 설정 ========================
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME_PRICE')
DB_URL = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
engine = create_engine(DB_URL)

# ======================== 종목 리스트 ========================
coins = ["BTC", "ETH", "XRP"]

# ======================== 데이터 수집 함수 ========================
async def get_daily_data(session: aiohttp.ClientSession, code: str, date: str) -> pd.DataFrame | None:
    url = "https://crix-api-cdn.upbit.com/v1/crix/candles/days"
    params = {
        "code": f"CRIX.UPBIT.KRW-{code}",
        "count": 4,
        "to": date
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://upbit.com",
        "Referer": "https://upbit.com/"
    }

    try:
        async with session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()

            if not data:
                logging.warning(f"[{code}] No data for {date}")
                return None

            df = pd.DataFrame(data)
            df = df[[
                'candleDateTimeKst', 'code', 'openingPrice',
                'highPrice', 'lowPrice', 'tradePrice',
                'candleAccTradeVolume', 'candleAccTradePrice'
            ]]

            df['candleDateTimeKst'] = pd.to_datetime(df['candleDateTimeKst']).dt.strftime('%Y-%m-%d')
            df['code'] = df['code'].str[-3:]  # e.g. KRW-BTC → BTC

            df.columns = ['date', 'code', 'open', 'high', 'low', 'close', 'volume', 'trading_value']
            return df

    except Exception as e:
        logging.error(f"[{code}] Error: {e}")
        return None

# ======================== DB 저장 함수 ========================
def upsert_to_db(df: pd.DataFrame, table_name: str = "coin_daily"):
    if df is None or df.empty:
        logging.info("No data to upsert.")
        return

    sql = f"""
        INSERT INTO {table_name} 
            (date, code, open, high, low, close, volume, trading_value)
        VALUES 
            (:date, :code, :open, :high, :low, :close, :volume, :trading_value)
        ON DUPLICATE KEY UPDATE 
            open = VALUES(open),
            high = VALUES(high),
            low = VALUES(low),
            close = VALUES(close),
            volume = VALUES(volume),
            trading_value = VALUES(trading_value)
    """

    try:
        with engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(text(sql), row.to_dict())
        logging.info(f"Upserted {len(df)} rows into `{table_name}`.")

    except Exception as e:
        logging.error(f"Upsert DB error: {e}")

# ======================== 메인 함수 ========================
async def process_coin(session: aiohttp.ClientSession, coin: str, current_time: str) -> pd.DataFrame | None:
    logging.info(f"Processing {coin}...")
    df = await get_daily_data(session, coin, current_time)
    return df

async def main():
    start_time = time.time()
    timeout = 30 * 60  # 30분 타임아웃
    
    while time.time() - start_time < timeout:
        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        today = datetime.now().strftime("%Y-%m-%d")
        
        async with aiohttp.ClientSession() as session:
            tasks = [process_coin(session, coin, current_time) for coin in coins]
            results = await asyncio.gather(*tasks)
            
            # 모든 결과가 None이 아니고, 데이터프레임이 있는지 확인
            if all(df is not None and not df.empty and len(df) >= 4 for df in results):
                # 모든 코인의 첫 번째 행이 오늘 날짜인지 확인
                first_rows_today = all(df.iloc[0]['date'] == today for df in results)
                
                if first_rows_today:
                    logging.info("오늘 날짜가 확인되었습니다. 최근 3일치 데이터를 업설트합니다.")
                    for df in results:
                        # 2~4번째 행 (최근 3일치 데이터)
                        last_three_days = df.iloc[1:4]
                        upsert_to_db(last_three_days)
                    break
                else:
                    logging.info("날짜 조건이 만족되지 않았습니다. 1초 후 재시도합니다.")
                    await asyncio.sleep(1)
            else:
                logging.info("일부 데이터를 가져오지 못했습니다. 1초 후 재시도합니다.")
                await asyncio.sleep(1)
    
    if time.time() - start_time >= timeout:
        logging.warning("30분 타임아웃으로 인해 프로그램이 종료되었습니다.")

# ======================== 실행 ========================
if __name__ == "__main__":
    asyncio.run(main())
