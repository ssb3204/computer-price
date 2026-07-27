# 컴퓨터 가격 모니터링 시스템

컴퓨터 부품 가격 비교 사이트 3곳을 크롤링하여 일일 가격을 수집하고, 사용자별 관심 상품의 가격 변동을 추적하는 시스템.

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
           └── Step 6: Analytics 집계 (일별 요약)
                           │
                    API + 웹 UI ← MySQL
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
| DWH | MySQL 8.0 (Oracle Cloud VM 상시가동, 단일 DB `computer_price`, 3-Layer: 테이블 접두사 raw_/stg_/ans_) |
| 웹 | FastAPI + 정적 HTML |
| 인프라 | Docker Compose (api 단일 서비스) |

## 빠른 시작

### 사전 요구사항

- Docker Desktop
- MySQL 8.0 (`price_app` 전용 계정 — root 계정은 사용하지 않음)

### 설정

```bash
git clone https://github.com/ssb3204/computer-price.git
cd computer-price

cp .env.example .env
# .env 파일에 MySQL 연결 정보 입력 (MYSQL_PASSWORD 등)
```

### 실행

```bash
# api 서비스 기동
docker compose up -d

# 수동 파이프라인 실행
python run_pipeline.py
```

### 접속

| 서비스 | URL |
|--------|-----|
| 웹 UI | http://localhost:8001 |
| API 문서 | http://localhost:8001/docs |

## 프로젝트 구조

```
computer_price/
├── src/
│   ├── common/          # 공유 모듈 (models, config, mysql_client)
│   ├── crawlers/        # 사이트별 크롤러 (다나와, 컴퓨존, 견적왕)
│   ├── pipeline/        # 파이프라인 스텝 (crawl, load_raw, transform, quality, detect, analytics, slack)
│   └── api/             # FastAPI (users, watchlist)
│       └── static/      # 웹 UI (로그인/회원가입/홈/마이페이지/워치리스트)
├── mysql/               # MySQL DDL (3-Layer)
├── tests/
│   ├── unit/            # 크롤러 유닛 테스트
│   └── integration/     # 로컬 MySQL 통합 테스트
├── .github/workflows/   # CI (린트+유닛+통합 테스트), 크롤링 스케줄
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
  - 주별/전체기간 통계는 별도 테이블 없이 이 테이블을 즉석 GROUP BY 해서 구한다

### Users
- **users** — 회원 계정
- **user_watchlist** — 사용자와 관심 상품 연결

## 웹 UI (localhost:8001)

| 페이지 | URL | 내용 |
|--------|-----|------|
| 로그인 | `/` | 계정 로그인 |
| 회원가입 | `/signup` | 계정 생성 |
| 홈 | `/home` | 메인 화면 |
| 마이페이지 | `/mypage` | 프로필 관리 |
| 워치리스트 | `/watchlist` | 관심 상품 검색·추가·삭제, 가격 이력 조회 |
| API 문서 | `/docs` | FastAPI 자동 생성 문서 |

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

# 통합 테스트 (실제 MySQL 연결 필요 — .env 의 접속 정보 사용)
python -m pytest tests/integration/ -v -o "addopts=" -m integration
```

CI 에서는 통합 테스트용 MySQL 8.0 컨테이너를 러너 안에 띄우고 `mysql/ddl/` 을
적용해서 돌린다. 운영 DB 는 건드리지 않는다.
