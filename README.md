# 컴퓨터 가격 모니터링 시스템

컴퓨터 부품 가격 비교 사이트 3곳을 크롤링하여 일일 가격을 수집하고, 가격 변동을 시각화하는 웹 대시보드.

## 아키텍처

```
Airflow (스케줄러)
    │
    ├── Crawler: 다나와
    ├── Crawler: 컴퓨존
    └── Crawler: 견적왕
            │
        Kafka [raw-prices]
            │
    Change Detector (PostgreSQL 기반 변경 감지)
            │
        Kafka [price-changes]
            │
    ├── Alert Service → PostgreSQL (alerts)
    ├── Snowflake Loader → Snowflake (DWH)
    └── Dashboard ← PostgreSQL + Snowflake
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
| 스트리밍 | Apache Kafka (변경분만 전달) |
| 운영 DB | PostgreSQL 16 |
| 데이터 웨어하우스 | Snowflake |
| 시각화 | Dash (Plotly) |
| 인프라 | Docker Compose |

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
# 전체 서비스 기동 (9개)
docker compose up -d

# Airflow DB 초기화 (최초 1회)
docker compose run --rm airflow bash -c "airflow db init"
docker compose restart airflow

# PostgreSQL 스키마 적용
docker compose exec postgres psql -U computer_price -d computer_price \
  -f /dev/stdin < migrations/versions/001_initial_schema.sql
docker compose exec postgres psql -U computer_price -d computer_price \
  -f /dev/stdin < migrations/versions/002_index_and_constraint_fixes.sql
docker compose exec postgres psql -U computer_price -d computer_price \
  -f /dev/stdin < migrations/versions/003_price_history_site_constraint.sql
```

### 접속

| 서비스 | URL | 비고 |
|--------|-----|------|
| 대시보드 | http://localhost:8050 | 가격 모니터링 UI |
| Airflow | http://localhost:8081 | .env의 ID/PW 사용 |
| PostgreSQL | localhost:5432 | .env의 인증정보 사용 |
| Kafka | localhost:29092 | 호스트 접속용 |

## 프로젝트 구조

```
computer_price/
├── src/
│   ├── common/          # 공유 모듈 (models, config, kafka, db)
│   ├── crawlers/        # 사이트별 크롤러
│   ├── consumers/       # Kafka 컨슈머 (변경감지, 알림, Snowflake)
│   ├── dashboard/       # Dash 웹 대시보드
│   └── airflow_dags/    # Airflow DAG
├── docker/              # 서비스별 Dockerfile
├── migrations/          # PostgreSQL 마이그레이션
├── snowflake/           # Snowflake DDL
├── tests/               # 테스트 (unit, integration, e2e)
└── docker-compose.yml
```

## 데이터 모델

### PostgreSQL (운영)

- **products** — 정규화된 제품 카탈로그
- **latest_prices** — 제품/사이트별 최신 가격 (변경 감지 기준)
- **price_history** — 전체 가격 이력 (append-only)
- **alerts** — 가격 알림 (NEW_LOW, NEW_HIGH, PRICE_DROP, PRICE_SPIKE)

### Kafka 토픽

| 토픽 | 파티션 | 용도 |
|------|--------|------|
| `raw-prices` | 3 | 크롤러 → 원시 가격 |
| `price-changes` | 6 | 변경분만 필터링 |
| `crawl-events` | 1 | 크롤링 상태 |

## 대시보드 기능

- **대시보드 개요** — 추적 제품 수, 가격 변동, 알림 현황
- **제품 상세** — 주간/월간/분기 가격 추이 차트, 최적 구매 시점 표시
- **사이트 비교** — 제품별 사이트 간 가격 비교, 최저가 표시
- **알림** — 최저가/최고가 갱신, 급락/급등 알림

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
