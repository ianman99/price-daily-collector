import websocket
import json
import ssl
import uuid
import re
import time
import threading
from datetime import datetime
import pytz
import pandas as pd
from sqlalchemy import create_engine, text
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

# WebSocket URL
tv_ws_url = "wss://data.tradingview.com/socket.io/websocket"

# Headers from the curl request
headers = {
    "Origin": "https://kr.tradingview.com",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}

# 채권 리스트
bond_list = ["TVC:KR03Y" ,"TVC:KR10Y", "TVC:KR30Y","TVC:US02Y" ,"TVC:US10Y", "TVC:US30Y","TVC:JP02Y" ,"TVC:JP10Y", "TVC:JP30Y"]
# 통화 리스트
currency_list = ["FX_IDC:USDKRW", "FX_IDC:JPYKRW", "FX_IDC:CNYKRW", "ICEUS:DXY"]
# 원자재 선물 리스트
commodity_future_list = ["NYMEX:CL1!", "NYMEX:BZ1!", "NYMEX:NG1!", "COMEX:GC1!", "COMEX:SI1!", "COMEX:HG1!", "CBOT:ZC1!", "CBOT:ZW1!", "CBOT:ZS1!"]
# 지수 선물, 지수 리스트
index_list = ["CME_MINI:ES1!", "CME_MINI:NQ1!", "SP:SPX", "NASDAQ:IXIC", "DJ:DJI"]

timeframe = "1D"  # 일봉
bars_count = 10   # 가져올 바의 개수

# 전역 변수들
current_symbol = None
current_table_type = None
data_received = False

def send_sql_currency(data):
    """currency_daily 테이블에 데이터 upsert"""
    try:
        with engine.begin() as connection:
            for _, row in data.iterrows():
                upsert_query = text("""
                    INSERT INTO currency_daily (date, code, open, high, low, close)
                    VALUES (:date, :code, :open, :high, :low, :close)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close)
                """)
                
                connection.execute(upsert_query, {
                    'date': row['date'],
                    'code': row['code'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close']
                })
            
            print(f"currency_daily 테이블 upsert 완료: {len(data)} 행")
    except Exception as upsert_error:
        print(f"currency_daily upsert 중 오류 발생: {str(upsert_error)}")
        raise

def send_sql_commodity(data):
    """commodity_future_daily 테이블에 데이터 upsert (volume 포함)"""
    try:
        with engine.begin() as connection:
            for _, row in data.iterrows():
                upsert_query = text("""
                    INSERT INTO commodity_future_daily (date, code, open, high, low, close, volume)
                    VALUES (:date, :code, :open, :high, :low, :close, :volume)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                """)
                
                connection.execute(upsert_query, {
                    'date': row['date'],
                    'code': row['code'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
            
            print(f"commodity_future_daily 테이블 upsert 완료: {len(data)} 행")
    except Exception as upsert_error:
        print(f"commodity_future_daily upsert 중 오류 발생: {str(upsert_error)}")
        raise

def send_sql_index(data):
    """index_foreign_daily 테이블에 데이터 upsert (volume 포함)"""
    try:
        with engine.begin() as connection:
            for _, row in data.iterrows():
                upsert_query = text("""
                    INSERT INTO index_foreign_daily (date, code, open, high, low, close, volume)
                    VALUES (:date, :code, :open, :high, :low, :close, :volume)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close),
                        volume = VALUES(volume)
                """)
                
                connection.execute(upsert_query, {
                    'date': row['date'],
                    'code': row['code'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                })
            
            print(f"index_foreign_daily 테이블 upsert 완료: {len(data)} 행")
    except Exception as upsert_error:
        print(f"index_foreign_daily upsert 중 오류 발생: {str(upsert_error)}")
        raise

def send_sql_bond(data):
    """bond_daily 테이블에 데이터 upsert"""
    try:
        with engine.begin() as connection:
            for _, row in data.iterrows():
                upsert_query = text("""
                    INSERT INTO bond_daily (date, code, open, high, low, close)
                    VALUES (:date, :code, :open, :high, :low, :close)
                    ON DUPLICATE KEY UPDATE
                        open = VALUES(open),
                        high = VALUES(high),
                        low = VALUES(low),
                        close = VALUES(close)
                """)
                
                connection.execute(upsert_query, {
                    'date': row['date'],
                    'code': row['code'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close']
                })
            
            print(f"bond_daily 테이블 upsert 완료: {len(data)} 행")
    except Exception as upsert_error:
        print(f"bond_daily upsert 중 오류 발생: {str(upsert_error)}")
        raise

def get_upsert_function(table_type):
    """테이블 타입에 따라 적절한 upsert 함수 반환"""
    if table_type == "currency":
        return send_sql_currency
    elif table_type == "commodity":
        return send_sql_commodity
    elif table_type == "index":
        return send_sql_index
    elif table_type == "bond":
        return send_sql_bond
    else:
        raise ValueError(f"알 수 없는 테이블 타입: {table_type}")

# 메시지 전송 함수
def send_message(ws, message):
    """TradingView 프로토콜에 맞춰 메시지 전송"""
    json_msg = json.dumps(message)
    formatted_message = f"~m~{len(json_msg)}~m~{json_msg}"
    ws.send(formatted_message)

# 메시지 파싱 함수
def parse_messages(raw_message):
    """수신된 메시지를 파싱하여 JSON 객체 리스트로 반환"""
    messages = []
    # ~m~숫자~m~JSON 패턴을 찾아서 추출
    pattern = r"~m~(\d+)~m~({.*?})(?=~m~|\Z)"
    matches = re.findall(pattern, raw_message, re.DOTALL)
    
    for length, json_str in matches:
        try:
            message = json.loads(json_str)
            messages.append(message)
        except json.JSONDecodeError as e:
            pass
    
    return messages

def on_open(ws):
    global current_symbol
    
    # 세션 ID 생성 (각 연결마다 새로 생성)
    chart_session_id = f"cs_{str(uuid.uuid4()).replace('-', '')[:16]}"
    ws.chart_session_id = chart_session_id  # ws 객체에 저장
    
    # 1. 차트 세션 생성
    chart_create_msg = {
        "m": "chart_create_session",
        "p": [chart_session_id, ""]
    }
    send_message(ws, chart_create_msg)
    
    # 2. 타임존 설정
    timezone_msg = {
        "m": "switch_timezone",
        "p": [chart_session_id, "Asia/Seoul"]
    }
    send_message(ws, timezone_msg)
    
    # 3. 심볼 해결
    resolve_symbol_msg = {
        "m": "resolve_symbol",
        "p": [chart_session_id, "sds_sym_1", f"={{\"adjustment\":\"splits\",\"symbol\":\"{current_symbol}\"}}"]
    }
    send_message(ws, resolve_symbol_msg)
    
    # 4. 시리즈 생성 (히스토리 데이터 요청)
    create_series_msg = {
        "m": "create_series",
        "p": [chart_session_id, "sds_1", "s1", "sds_sym_1", timeframe, bars_count, ""]
    }
    send_message(ws, create_series_msg)

def parse_ohlcv_data(series_data, symbol, table_type):
    """TradingView 시리즈 데이터를 파싱하여 DataFrame으로 변환"""
    if not isinstance(series_data, list):
        return []
    
    parsed_bars = []
    for bar_item in series_data:
        if isinstance(bar_item, dict) and 'v' in bar_item:
            bar_values = bar_item['v']
            if len(bar_values) >= 5:  # [timestamp, open, high, low, close, volume]
                try:
                    timestamp = bar_values[0]
                    # UTC timestamp를 서울 시간대로 변환
                    utc_dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                    seoul_tz = pytz.timezone('Asia/Seoul')
                    seoul_dt = utc_dt.astimezone(seoul_tz)
                    
                    bar = {
                        'date': seoul_dt.strftime('%Y-%m-%d'),
                        'code': symbol.split(':')[1],
                        'open': bar_values[1],
                        'high': bar_values[2],
                        'low': bar_values[3],
                        'close': bar_values[4],
                    }
                    
                    # commodity와 index의 경우 volume 데이터도 포함
                    if table_type in ["commodity", "index"] and len(bar_values) >= 6:
                        bar['volume'] = bar_values[5]
                    
                    parsed_bars.append(bar)
                except (IndexError, TypeError, ValueError) as e:
                    continue
    
    return parsed_bars

def on_message(ws, message):
    global current_symbol, current_table_type, data_received
    
    # 하트비트 메시지 처리
    if "~h~" in message:
        ws.send(message)  # 하트비트 응답
        return
    
    # 메시지 파싱
    parsed_messages = parse_messages(message)
    
    for msg in parsed_messages:
        method = msg.get('m')
        params = msg.get('p', [])
        
        if method == "symbol_resolved":
            pass
            
        elif method == "series_completed":
            pass
            
        elif method == "timescale_update":
            if len(params) >= 2:
                session_id = params[0]
                series_data = params[1]
                
                # OHLCV 데이터 파싱
                for series_key, series_info in series_data.items():
                    if isinstance(series_info, dict) and 's' in series_info:
                        bars_data = series_info['s']
                        parsed_bars = parse_ohlcv_data(bars_data, current_symbol, current_table_type)
                        
                        if parsed_bars:
                            # DataFrame 생성
                            df = pd.DataFrame(parsed_bars)
                            
                            print(f"{current_symbol} 데이터 수집 완료: {len(df)} 행")
                            print(df.tail())  # 마지막 5행 출력
                            
                            # 데이터베이스에 저장
                            try:
                                upsert_func = get_upsert_function(current_table_type)
                                upsert_func(df)
                                print(f"{current_symbol} 데이터 저장 완료")
                            except Exception as e:
                                print(f"{current_symbol} 데이터 저장 중 오류 발생: {str(e)}")
                            
                            # 데이터 수신 완료 표시
                            data_received = True
                            
                            # WebSocket 연결 종료
                            ws.close()
                            return
                
        elif method == "series_loading":
            pass
            
        elif method == "series_error":
            print(f"시리즈 오류: {params}")
            data_received = True
            ws.close()
            
        elif method == "critical_error":
            print(f"크리티컬 오류: {params}")
            data_received = True
            ws.close()

def on_error(ws, error):
    global data_received
    print(f"WebSocket 오류: {error}")
    data_received = True

def on_close(ws, close_status_code, close_msg):
    global data_received
    data_received = True

def get_tradingview_data(symbol, table_type):
    """단일 심볼에 대한 TradingView 데이터 수집"""
    global current_symbol, current_table_type, data_received
    
    current_symbol = symbol
    current_table_type = table_type
    data_received = False
    
    print(f"\n{symbol} 데이터 수집 시작...")
    
    # WebSocket 연결 생성
    ws = websocket.WebSocketApp(
        tv_ws_url,
        header=headers,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # WebSocket 실행 (SSL 인증서 검증 비활성화)
    try:
        ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
    except KeyboardInterrupt:
        print(f"{symbol} 데이터 수집 중단됨")
        ws.close()
    except Exception as e:
        print(f"{symbol} 데이터 수집 중 오류: {str(e)}")
    
    # 연결 종료 후 잠시 대기
    time.sleep(1)

def main():
    """메인 실행 함수"""
    
    print("=== TradingView 데이터 수집 시작 ===")
    
    # 통화 데이터 수집
    print("\n--- 통화 데이터 수집 ---")
    for symbol in currency_list:
        try:
            get_tradingview_data(symbol, "currency")
        except Exception as e:
            print(f"{symbol} 처리 중 오류 발생: {str(e)}")
            continue
    
    # 원자재 선물 데이터 수집
    print("\n--- 원자재 선물 데이터 수집 ---")
    for symbol in commodity_future_list:
        try:
            get_tradingview_data(symbol, "commodity")
        except Exception as e:
            print(f"{symbol} 처리 중 오류 발생: {str(e)}")
            continue
    
    # 지수 데이터 수집
    print("\n--- 지수 데이터 수집 ---")
    for symbol in index_list:
        try:
            get_tradingview_data(symbol, "index")
        except Exception as e:
            print(f"{symbol} 처리 중 오류 발생: {str(e)}")
            continue
    
    # 채권 데이터 수집
    print("\n--- 채권 데이터 수집 ---")
    for symbol in bond_list:
        try:
            get_tradingview_data(symbol, "bond")
        except Exception as e:
            print(f"{symbol} 처리 중 오류 발생: {str(e)}")
            continue
    
    print("\n=== 모든 데이터 수집 완료 ===")

if __name__ == "__main__":
    main()