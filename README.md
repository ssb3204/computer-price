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

| 사이트 | URL | `site` 코드 | 크롤러 모듈 |
|--------|-----|------------|------------|
| 다나와 | danawa.com | `danawa` | `src/crawlers/danawa.py` |
| 컴퓨존 | compuzone.co.kr | `compuzone` | `src/crawlers/compuzone.py` |
| 견적왕 | kjwwang.com | `kjwwang` | `src/crawlers/pc_estimate.py` |

견적왕만 모듈명(`pc_estimate`)과 `site` 코드(`kjwwang`)가 다르다. DB에 저장되는 값은
`kjwwang` 이다 — 모듈명을 코드로 쓰지 말 것.

### `site` 값의 어휘가 테이블마다 다르다

| 테이블 | `site` 값 | 예 |
|--------|----------|-----|
| `raw_crawled_prices`, `stg_products` | **영문 코드** | `danawa` / `compuzone` / `kjwwang` |
| `stg_watchlist` | **한글 표기** | `다나와` / `컴퓨존` / `견적왕` |

워치리스트는 사용자가 화면에서 고르는 값이라 한글로 통일돼 있고, 크롤링 결과는
크롤러가 넣는 값이라 영문 코드다. 두 테이블을 조인할 때 `site` 로 직접 매칭하면 안 된다
(대상 매칭은 사이트 고유 ID — pcode/ProductNo/pd_no — 로 한다).

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
│   └── api/             # FastAPI (users, watchlist, builds)
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

### Users / Builds (접두사 없음 — 사용자 데이터)
- **users** — 회원 계정
- **user_watchlist** — 사용자와 관심 상품 연결
- **builds** — 부품 조합(공개 게시물). `UNIQUE(user_id, name)`
- **build_items** — 조합과 상품 연결. 조합 삭제 시 `ON DELETE CASCADE`

조합 총액 이력은 별도 테이블로 두지 않고 `stg_price_history`에서 즉석 집계한다
(일별 1점 기준 연 수천 행 규모라 캐시 테이블 이득이 없다. 근거는 `docs/benchmark_results.md`).

## 웹 UI (localhost:8001)

| 페이지 | URL | 내용 |
|--------|-----|------|
| 로그인 | `/` | 계정 로그인 |
| 회원가입 | `/signup` | 계정 생성 |
| 홈 | `/home` | 메인 화면 (사이드바: 가격 추이 · 부품 조합 · 크롤링 대상 관리) |
| 마이페이지 | `/mypage` | 프로필 관리 |
| 워치리스트 | `/watchlist` | 관심 상품 검색·추가·삭제, 가격 이력 조회 |
| API 문서 | `/docs` | FastAPI 자동 생성 문서 |

### 부품 조합 (공개 게시물)

여러 부품을 묶어 이름을 붙이고, 그 조합의 **총액이 어떻게 변해왔는지** 본다.
조합은 게시물처럼 **누구나 볼 수 있고, 수정은 만든 사람만** 할 수 있다.

총액 추이 집계 규칙:

- 하루가 한 점. 같은 날 여러 번 크롤링되므로 그날 마지막 값을 쓴다
- 수집이 없는 날은 직전 가격을 이어 쓴다(forward fill) — 크롤링 실패로 선이 끊기지 않게
- **모든 부품이 가격을 가진 날부터** 그린다. 있는 것만 더하면 뒤늦게 담긴 부품이
  합류하는 날 총액이 급등해 가격이 오른 것처럼 보인다

조합 화면은 별도 페이지가 아니라 홈(`/home`) 사이드바의 **부품 조합** 메뉴에서 열린다.
차트는 가격 추이 화면과 같은 inline SVG로 그린다(외부 차트 라이브러리 미사용).

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
```

### ⚠ 통합 테스트는 일회용 MySQL 에 돌린다

`tests/integration/` 은 `.env` 의 접속 정보를 그대로 쓴다. `.env` 가 운영 DB(Oracle Cloud VM)를
가리키는 상태에서 실행하면 **운영 DB 에 붙는다.**

정리 픽스처(`conftest.py`)가 지우는 것은 `IT_TEST_` 접두사 행뿐이라 실데이터가 삭제되지는 않는다.
문제는 삭제가 아니라 **쓰기**다 — 테스트가 `transform_staging()` / `aggregate_analytics()` /
`detect_changes()` 를 직접 호출하는데, 이 함수들은 테스트 행만이 아니라 **미처리 상태인 실제 행
전부**를 대상으로 돈다. 멱등이라 데이터가 깨지지는 않지만 운영 데이터의 처리 상태가 바뀌고,
정리 픽스처는 그것을 되돌리지 않는다.

CI 와 같은 방식으로 일회용 MySQL 을 띄워서 돌린다:

```bash
docker run -d --name it_mysql -e MYSQL_ROOT_PASSWORD=it_root_pw \
  -e MYSQL_DATABASE=computer_price -p 3307:3306 mysql:8.0

docker exec -i it_mysql mysql -u root -pit_root_pw <<'SQL'
CREATE USER IF NOT EXISTS 'price_app'@'%' IDENTIFIED BY 'it_app_pw';
GRANT ALL PRIVILEGES ON `computer_price`.* TO 'price_app'@'%';
SQL

for f in mysql/ddl/schema.sql mysql/ddl/users.sql mysql/ddl/user_watchlist.sql mysql/ddl/builds.sql; do
  docker exec -i it_mysql mysql -u price_app -pit_app_pw computer_price < "$f"
done

MYSQL_HOST=127.0.0.1 MYSQL_PORT=3307 MYSQL_USER=price_app \
MYSQL_PASSWORD=it_app_pw MYSQL_DATABASE=computer_price \
  python -m pytest tests/integration/ -v -o "addopts=" -m integration

docker rm -f it_mysql
```

환경변수가 `.env` 보다 우선하므로(`pydantic-settings`) 위 명령은 `.env` 를 고치지 않아도 된다.

CI 도 같은 구조다 — 러너 안에 MySQL 8.0 service 컨테이너를 띄우고 `mysql/ddl/` 을 적용해서
돌리므로 운영 DB 를 건드리지 않는다.
