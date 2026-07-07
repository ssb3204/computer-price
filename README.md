# 컴퓨터 가격 모니터링 시스템

컴퓨터 부품 가격 비교 사이트 3곳을 크롤링하여 일일 가격을 수집하고, 가격 변동을 시각화하는 웹 대시보드.

## 아키텍처

```
GitHub Actions (하루 4회: 00:00/05:00/10:00/15:00 KST)
    │
    └── run_pipeline.py
           ├── Step 1: 크롤링 (다나와, 컴퓨존, 견적왕)
           ├── Step 2: MySQL Raw 적재
           ├── Step 3: Staging 변환 (정규화, 가격 파싱, 이상치 제거)
           ├── Step 3.5: 품질 검증 (레이어 정합성, 사이트 간 가격 편차)
           ├── Step 4: 변경 감지 & 알림 (NEW_LOW, PRICE_DROP 등)
           ├── Step 5: Slack 실패 알림
           └── Step 6: Analytics 집계 (일별/주별 요약)
                           │
                     Dashboard ← MySQL
```

## 크롤링 대상

| 사이트 | URL | 코드명 |
|--------|-----|--------|
| 다나와 | danawa.com | `danawa` |
| 컴퓨존 | compuzone.co.kr | `compuzone` |
| 견적왕 | kjwwang.com | `pc_estimate` |

## 기술 스택

| 레이어 | 기술 |
|--------|------|
| 크롤링 | Python, BeautifulSoup |
| 오케스트레이션 | GitHub Actions (하루 4회) |
| DWH | MySQL 8.0 로컬 (단일 DB `computer_price`, 3-Layer: 테이블 접두사 raw_/stg_/ans_) |
| 시각화 | Dash (Plotly) |
| 인프라 | Docker Compose (대시보드 단일 서비스) |

## 빠른 시작

### 사전 요구사항

- Docker Desktop
- MySQL 8.0 (로컬 설치, `price_app` 전용 계정 — root 계정은 사용하지 않음)

### 설정

```bash
git clone https://github.com/ssb3204/computer-price.git
cd computer-price

cp .env.example .env
# .env 파일에 MySQL 연결 정보 입력 (MYSQL_PASSWORD 등)
```

### 실행

```bash
# 대시보드 서비스 기동
docker compose up -d

# 수동 파이프라인 실행
python run_pipeline.py
```

### 접속

| 서비스 | URL |
|--------|-----|
| 대시보드 | http://localhost:8050 |

## 프로젝트 구조

```
computer_price/
├── src/
│   ├── common/          # 공유 모듈 (models, config, mysql_client)
│   ├── crawlers/        # 사이트별 크롤러 (다나와, 컴퓨존, 견적왕)
│   ├── pipeline/        # 파이프라인 스텝 (crawl, load_raw, transform, quality, detect, analytics, slack)
│   └── dashboard/       # Dash 웹 대시보드
│       ├── layouts/     # 페이지별 레이아웃 (5개 페이지)
│       └── data_access/ # MySQL 쿼리
├── mysql/               # MySQL DDL (3-Layer)
├── tests/
│   ├── unit/            # 크롤러 유닛 테스트
│   └── integration/     # 로컬 MySQL 통합 테스트
├── .github/workflows/   # CI (유닛+통합 테스트), 크롤링 스케줄
├── run_pipeline.py      # 파이프라인 진입점 (6단계 + 품질 검증)
└── docker-compose.yml
```

## 데이터 모델 (MySQL 3-Layer, 테이블 접두사 기반)

### Raw (`raw_`) — 크롤링 원본
- **raw_crawled_prices** — 가공 없는 원본 데이터 (가격 텍스트 보존)
- **raw_transform_failures** — Staging 변환 실패 감사 로그

### Staging (`stg_`) — 정제/정규화
- **stg_products** — 사이트별 상품 목록 (URL 최신값으로 유지)
- **stg_price_history** — 일별 가격 이력 (append-only)
- **stg_latest_prices** — 상품별 최신 가격 **(VIEW, stg_price_history에서 동적 도출)**
- **stg_price_alerts** — 가격 변동 알림
- **stg_watchlist** — 관심 상품 목록

### Analytics (`ans_`) — 집계
- **ans_daily_price_stats** — 일별 최저/최고/평균 가격
- **ans_weekly_price_stats** — 주별 요약
- **ans_product_stats** — 상품별 전체 통계 (min_price_ever/max_price_ever)

## 대시보드 기능 (localhost:8050)

| 페이지 | URL | 내용 |
|--------|-----|------|
| 대시보드 | `/` | 추적 제품 수, 카테고리, 사이트, 오늘 수집 건수 + 카테고리별 가격 요약 |
| 가격 정보 | `/prices` | 사이트/카테고리 필터, 상품별 최신 가격 |
| 가격 추이 | `/trends` | 키워드 검색, 사이트별 최저가 라인 차트, 당일 크롤링 비교 |
| 가격 알림 | `/alerts` | 날짜별 구분, 유형/카테고리 필터 (NEW_LOW/NEW_HIGH/PRICE_DROP/PRICE_SPIKE) |
| 크롤링 대상 | `/watchlist` | 관심 상품 추가/삭제 관리, 카테고리별 검색 |
| 파이프라인 ↗ | GitHub Actions | 크롤링 스케줄 실행 이력 (외부 링크) |

## 알림 기준

| 유형 | 조건 |
|------|------|
| NEW_LOW | 역대 최저가 갱신 |
| NEW_HIGH | 역대 최고가 갱신 |
| PRICE_DROP | 직전 대비 5% 이상 하락 |
| PRICE_SPIKE | 직전 대비 10% 이상 상승 |

- 1% 미만 변동 무시
- 단일 변동 70% 초과는 데이터 이상치로 간주하여 알림 제외

## 개발

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# 유닛 테스트
python -m pytest tests/unit/ -v -o "addopts="

# 통합 테스트 (로컬 MySQL 연결 필요)
python -m pytest tests/integration/ -v -o "addopts=" -m integration
```
