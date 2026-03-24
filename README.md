# 컴퓨터 가격 모니터링 시스템

컴퓨터 부품 가격 비교 사이트 3곳을 크롤링하여 일일 가격을 수집하고, 가격 변동을 시각화하는 웹 대시보드.

## 아키텍처

```
Airflow DAG (매일 21:00, 22:00 KST)
    │
    ├── Step 1: 크롤링 (다나와, 컴퓨존, 견적왕)
    ├── Step 2: Snowflake Raw 적재
    ├── Step 3: Staging 변환 (정규화, 가격 파싱)
    ├── Step 4: 변경 감지 & 알림 (NEW_LOW, PRICE_DROP 등)
    └── Step 5: Analytics 집계 (일별/주별 요약)
                    │
              Dashboard ← Snowflake Analytics
```

## 크롤링 대상

| 사이트 | URL | 코드명 |
|--------|-----|--------|
| 샵다나와 | shop.danawa.com | `danawa` |
| 컴퓨존 | compuzone.co.kr | `compuzone` |
| 견적왕 | pc-estimate.com | `pc_estimate` |

## 기술 스택

| 레이어 | 기술 |
|--------|------|
| 크롤링 | Python, BeautifulSoup |
| 오케스트레이션 | Apache Airflow |
| DWH | Snowflake (3-Layer: Raw → Staging → Analytics) |
| 메타DB | PostgreSQL 16 (Airflow 전용) |
| 시각화 | Dash (Plotly) |
| 인프라 | Docker Compose (3개 서비스) |

## 빠른 시작

### 사전 요구사항

- Docker Desktop
- Git

### 설정

```bash
# 저장소 클론
git clone https://github.com/ssb3204/computer-price.git
cd computer-price

# 환경변수 설정
cp .env.example .env
# .env 파일을 열어 값 수정 (특히 AIRFLOW_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD 필수)
```

### 실행

```bash
# 전체 서비스 기동 (3개: postgres, airflow, dashboard)
docker compose up -d
```

### 접속

| 서비스 | URL | 비고 |
|--------|-----|------|
| 대시보드 | http://localhost:8050 | 가격 모니터링 UI |
| Airflow | http://localhost:8081 | .env의 ID/PW 사용 |

## 프로젝트 구조

```
computer_price/
├── src/
│   ├── common/          # 공유 모듈 (models, config, snowflake_client)
│   ├── crawlers/        # 사이트별 크롤러 (다나와, 컴퓨존, 견적왕)
│   ├── dashboard/       # Dash 웹 대시보드 (Snowflake 연동)
│   └── airflow_dags/    # Airflow DAG (5단계 파이프라인)
├── docker/              # 서비스별 Dockerfile
├── snowflake/           # Snowflake DDL (3-Layer)
├── scripts/             # 수동 실행 스크립트
├── tests/               # 테스트 (unit)
└── docker-compose.yml
```

## 데이터 모델 (Snowflake 3-Layer)

### Raw — 크롤링 원본
- **RAW_CRAWLED_PRICES** — 가공 없는 원본 데이터 (가격 텍스트 보존)

### Staging — 정제/정규화
- **DIM_SITES** — 사이트 차원 (다나와, 컴퓨존, 견적왕)
- **DIM_CATEGORIES** — 카테고리 차원 (CPU, GPU, RAM, SSD)
- **STG_PRODUCTS** — 사이트별 상품 목록
- **STG_DAILY_PRICES** — 일별 가격 이력 (append-only)
- **STG_LATEST_PRICES** — 상품별 최신 가격
- **STG_ALERTS** — 가격 변동 알림 (NEW_LOW, NEW_HIGH, PRICE_DROP, PRICE_SPIKE)

### Analytics — 집계
- **DAILY_SUMMARY** — 일별 최저/최고/평균 가격
- **WEEKLY_SUMMARY** — 주별 요약
- **PRODUCT_STATS** — 상품별 전체 통계

## 대시보드 기능 (localhost:8050)

- **대시보드 개요** — 추적 제품 수, 카테고리, 사이트, 오늘 수집 건수
- **전체 가격표** — 상품명 링크, 사이트별 가격, 재고 상태
- **카테고리 요약** — 카테고리별 최저/최고/평균 가격
- **상품 통계** — 상품별 전체 기간 통계
- **가격 추이** — 사이트별 최저가 라인 차트, 크롤링 비교 테이블

## 개발

```bash
# Python 가상환경
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# 테스트
pytest

# 린트
ruff check src/
black --check src/
```
