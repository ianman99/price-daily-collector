# 금융 시세 데이터 자동 수집기

주식, 지수, 암호화폐, 선물, 통화, 원자재, 채권 등 다양한 금융 자산의 일일 시세 데이터를 자동으로 수집하여 MySQL 데이터베이스에 저장하는 파이프라인입니다.

## 주요 기능

- **국내 주식 시세**: KRX에서 코스피/코스닥 전 종목 일일 시세, PER/PBR, 투자자별 순매수 수집
- **국내 지수**: 코스피, 코스피200, 코스닥, 코스닥150 시세 및 PER/PBR/배당수익률
- **해외 지수**: Nikkei225, HSI 및 TradingView를 통한 S&P500, NASDAQ, Dow Jones
- **암호화폐**: Upbit API를 통한 BTC, ETH, XRP 일일 시세
- **선물**: 코스피200/코스닥 지수선물 시세
- **통화/원자재/채권**: TradingView WebSocket을 통한 실시간 데이터 수집

## 데이터 소스

| 소스 | 수집 대상 | 방식 |
|------|----------|------|
| KRX (한국거래소) | 주식 시세, 지수, 선물, 투자자별 매매 | REST API + 세션 인증 |
| Upbit | 암호화폐 (BTC, ETH, XRP) | REST API (비동기) |
| Yahoo Finance | 해외 지수 (Nikkei225, HSI) | yfinance |
| TradingView | 통화, 원자재선물, 해외지수, 채권 | WebSocket |

## 데이터베이스 구조

### price DB

| 테이블 | 설명 |
|--------|------|
| `stock_daily` | 개별 주식 시세 + PER/PBR + 투자자별 순매수 |
| `index_daily` | 국내 지수 시세 + PER/PBR/배당수익률 |
| `index_future_daily` | 지수선물 시세 |
| `index_daily_investor` | 지수 투자자별 순매수 + 외국인 보유 현황 |
| `index_foreign_daily` | 해외 지수 시세 |
| `coin_daily` | 암호화폐 시세 |
| `currency_daily` | 통화 (USDKRW, JPYKRW, DXY 등) |
| `commodity_future_daily` | 원자재선물 (원유, 금, 은, 구리 등) |
| `bond_daily` | 채권 금리 (한국, 미국, 일본) |

### fin_db

| 테이블 | 설명 |
|--------|------|
| `corp_info_kor` | 한국 상장사 기본정보 (종목코드, 종목명, 시장, 섹터, 시가총액) |

### 테이블 데이터 예시

> 실제 적재되는 데이터의 예시입니다.

**stock_daily** (개별 주식 시세)

| date | code | open | high | low | close | volume | trading_value | market | listed_stocks | PER | PBR | dividend_yield | inst_net_buy | corp_net_buy | retail_net_buy | foreign_net_buy |
|------|------|------|------|-----|-------|--------|---------------|--------|---------------|-----|-----|----------------|-------------|-------------|---------------|----------------|
| 2026-02-20 | A950210 | 11,430 | 11,880 | 11,300 | 11,760 | 133,572 | 1,554 | KOSPI | 60,096,155 | - | - | - | 372 | 0 | -320 | -52 |

**index_daily** (국내 지수)

| date | code | open | high | low | close | volume | trading_value | market_cap | PER | PBR | dividend_yield |
|------|------|------|------|-----|-------|--------|---------------|-----------|-----|-----|----------------|
| 2026-02-20 | 코스피 | 5,696.89 | 5,809.91 | 5,684.58 | 5,808.53 | 1,748,650 | 33,117,327 | 4,802,598,957 | 24.3 | 1.87 | 0.94 |
| 2026-02-20 | 코스피200 | 842.56 | 860.22 | 839.98 | 859.59 | 352,723 | 25,929,799 | 4,392,583,158 | 23.66 | 2.05 | 0.91 |

**index_future_daily** (지수선물)

| date | code | open | high | low | close | volume | trading_value | maturity | unsettled |
|------|------|------|------|-----|-------|--------|---------------|----------|-----------|
| 2026-02-20 | 코스피200F | 842.5 | 860.85 | 840.45 | 859.6 | 168,931 | 35,991,812 | 2026-03 | 210,555 |
| 2026-02-20 | 코스닥150F | 2,050.0 | 2,055.7 | 2,010.5 | 2,024.0 | 125,709 | 2,553,600 | 2026-03 | 471,045 |

**index_daily_investor** (지수 투자자별 순매수, 단위: 백만원)

| date | code | net_buy_institution | net_buy_other_corporation | net_buy_individual | net_buy_foreign | market_cap_total | market_cap_foreign_holding | market_cap_foreign_ratio |
|------|------|--------------------|--------------------------|--------------------|----------------|-----------------|---------------------------|--------------------------|
| 2026-02-20 | KOSPI | 903,343 | 154,665 | -345,127 | -712,880 | 4,803,601,346 | 1,802,463,362 | 37.52 |
| 2026-02-20 | KOSDAQ | 23,122 | -47,538 | 231,759 | -207,343 | 632,352,782 | 65,108,740 | 10.3 |

**coin_daily** (암호화폐, 단위: 원)

| date | code | open | high | low | close | volume | trading_value |
|------|------|------|------|-----|-------|--------|---------------|
| 2026-02-22 | BTC | 100,033,000 | 100,263,000 | 98,912,000 | 99,542,000 | 698.64 | 69,650,850,513 |
| 2026-02-22 | ETH | 2,902,000 | 2,916,000 | 2,850,000 | 2,881,000 | 17,009.28 | 49,052,409,205 |

**currency_daily** (통화)

| date | code | open | high | low | close |
|------|------|------|------|-----|-------|
| 2026-02-23 | USDKRW | 1,445.98 | 1,448.94 | 1,439.68 | 1,443.78 |
| 2026-02-23 | JPYKRW | 9.308 | 9.346 | 9.308 | 9.343 |

**commodity_future_daily** (원자재선물)

| date | code | open | high | low | close | volume |
|------|------|------|------|-----|-------|--------|
| 2026-02-23 | ZW1! | 578.0 | 579.0 | 574.5 | 576.5 | 3,695 |
| 2026-02-23 | ZS1! | 1,149.5 | 1,149.75 | 1,145.0 | 1,145.5 | 6,335 |

**bond_daily** (채권 금리, 단위: %)

| date | code | open | high | low | close |
|------|------|------|------|-----|-------|
| 2026-02-23 | KR10Y | 3.554 | 3.562 | 3.537 | 3.562 |
| 2026-02-23 | KR30Y | 3.478 | 3.491 | 3.467 | 3.491 |

**corp_info_kor** (상장사 기본정보)

| code | name | market | sector | market_cap |
|------|------|--------|--------|-----------|
| A000020 | 동화약품 | KOSPI | 제약 | 187,140,849,000 |
| A000040 | KR모터스 | KOSPI | 운송장비·부품 | 35,413,825,440 |
| A000050 | 경방 | KOSPI | 유통 | 291,424,320,100 |

## 프로젝트 구조

```
├── login_krx.py                  # KRX 세션 인증 및 쿠키 관리
├── stock_daily.py                # 주식 일일 시세 수집 (코스피/코스닥)
├── stock_PER_daily.py            # 주식 PER/PBR/배당수익률 수집
├── stock_daily_investor.py       # 주식 투자자별 순매수 수집
├── index_PER_daily.py            # 지수 PER/PBR/배당수익률 수집
├── index_daily_investor.py       # 지수 투자자별 순매수 + 외국인 보유 수집
├── index_foreign_daily.py        # 해외 지수 시세 수집 (Yahoo Finance)
├── future_daily.py               # 지수선물 시세 수집
├── coin_daily.py                 # 암호화폐 시세 수집 (Upbit)
├── corp_info_kor.py              # 상장사 기본정보 수집
├── tradingview.py                # 통화/원자재/채권/해외지수 WebSocket 수집
├── history/                      # 히스토리 데이터 일괄 수집 스크립트
│   ├── stock_history.py
│   ├── stock_PER_history.py
│   ├── stock_investor_history.py
│   ├── index_history.py
│   ├── index_PER_history.py
│   ├── index_daily_investor_history.py
│   ├── index_foreign_history.py
│   ├── future_history.py
│   └── coin_history.py
└── .env                          # 환경 변수 (DB, KRX 로그인 정보)
```

## 데이터 처리 흐름

```
KRX API → login_krx.py (세션 인증)
  ├─ stock_daily.py          → stock_daily, index_daily
  ├─ stock_PER_daily.py      → stock_daily (UPDATE)
  ├─ stock_daily_investor.py → stock_daily (UPDATE)
  ├─ index_PER_daily.py      → index_daily (UPDATE)
  ├─ index_daily_investor.py → index_daily_investor
  ├─ future_daily.py         → index_future_daily
  └─ corp_info_kor.py        → corp_info_kor

Upbit API → coin_daily.py → coin_daily

Yahoo Finance → index_foreign_daily.py → index_foreign_daily

TradingView WebSocket → tradingview.py
  ├─ currency_daily
  ├─ commodity_future_daily
  ├─ index_foreign_daily
  └─ bond_daily
```

## 설치 및 실행

### 1. 패키지 설치

```bash
pip install requests pandas sqlalchemy mysql-connector-python aiohttp yfinance curl_cffi websocket-client python-dotenv pytz
```

### 2. 환경 변수 설정

`.env` 파일을 생성하고 아래 항목을 설정합니다.

```env
# Database
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=your_db_host
DB_NAME_PRICE=price
DB_NAME_FIN=fin_db

# KRX Login (한국거래소)
KRX_ID=your_krx_id
KRX_PW=your_krx_password
```

### 3. 실행

각 스크립트를 개별적으로 실행합니다.

```bash
# 주식 시세 수집
python stock_daily.py

# 암호화폐 시세 수집
python coin_daily.py

# 통화/원자재/채권 수집
python tradingview.py
```

## 기술 스택

- **Python 3**
- **pandas** — 데이터 가공
- **SQLAlchemy / mysql-connector-python** — DB 연동
- **aiohttp** — 비동기 HTTP 요청 (암호화폐)
- **yfinance** — Yahoo Finance 데이터
- **websocket-client** — TradingView WebSocket 통신
- **concurrent.futures** — 멀티스레딩 병렬 처리
