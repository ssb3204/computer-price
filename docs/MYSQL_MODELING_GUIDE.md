# MySQL 마이그레이션 모델링 가이드

> **목적**: Snowflake(3-Layer, 스키마 분리) 기반 파이프라인을 로컬 **MySQL 8.0 단일 DB(`computer_price`)**로 이관하기 위한 데이터 모델링 기준 문서.
> 이 문서는 **Phase 2 통합 DDL 작성의 근거**가 된다. 실제 DDL은 이 문서 확정 후 별도로 생성한다.
> 근거 소스: `snowflake/ddl/002~010`, `src/pipeline/{transform,analytics,detect,load_raw}.py`, `src/crawlers/*`, `src/common/models.py`.

---

## 1. 범위

### 이관 대상 (현재 실제 구현된 것만)
- **RAW**: `raw_crawled_prices`, `raw_transform_failures`
- **STAGING**: `stg_products`, `stg_price_history`, `stg_price_alerts`, `stg_watchlist`, 뷰 `stg_latest_prices`
- **ANALYTICS**: `ans_daily_price_stats`, `ans_weekly_price_stats`, `ans_product_stats`

### 이관 제외 (확정)
| 제외 대상 | 사유 |
|-----------|------|
| `USER`, `BUILD`, `BUILD_ITEM`, `BUILD_PRICE_HISTORY`, `LISTING` 등 조합/견적 테이블 | **미구현**. 코드·DDL에 존재하지 않고 목표 모델링 개념으로만 언급됨 |
| `PIPELINE_RUNS`, `PIPELINE_STEP_RUNS` (구 005 DDL) | 이를 사용하던 `observability.py`가 삭제(커밋 8077f01)되어 **코드 참조 0건인 죽은 테이블** |
| `STOCK_STATUS` 컬럼 (raw_crawled_prices, stg_price_history) | DTO·INSERT 어디에도 없어 항상 NULL/`'unknown'`인 **죽은 컬럼** — 이관 시 제거 |

---

## 2. 설계 원칙 (확정)

1. **단일 DB + 테이블 접두사**: 스키마 분리 대신 `computer_price` 단일 DB에서 `raw_` / `stg_` / `ans_` 접두사로 계층 구분. **`USE DATABASE` / `USE SCHEMA` 구문 전면 제거.**
2. **대리키(surrogate PK)**: 모든 기본 테이블은 `BIGINT AUTO_INCREMENT` 대리키 PK.
3. **자연키는 UNIQUE 제약으로**: 멱등성 판정용 자연키는 `UNIQUE KEY`로 명시 → `INSERT ... ON DUPLICATE KEY UPDATE`가 이 UNIQUE에 걸린다.
4. **증분 처리**: 기존 MERGE 방식 유지. **`batch_id` 도입 안 함.** `stg_price_history.raw_id`로 원본(`raw_crawled_prices.id`) 추적.
5. **append-only 이력**: `raw_crawled_prices`, `stg_price_history`, `stg_price_alerts`는 삽입 위주. `raw_id` / `crawled_prices_id`는 **의도적으로 FK 아님**(원본 삭제 시에도 이력 유지).
6. **스토리지 표준**: 전 테이블 `ENGINE=InnoDB`, `CHARACTER SET utf8mb4`(한글 상품명), `COLLATE utf8mb4_0900_ai_ci`.
7. **시간 저장**: 모든 타임스탬프는 **앱에서 UTC로 통일**해 `DATETIME`에 저장(타임존 미보존). 크롤러가 `datetime.now(timezone.utc)`로 생성하는 계약 유지.

---

## 3. 타입 매핑표 (Snowflake → MySQL 8.0)

| Snowflake | MySQL 8.0 | 적용 대상 예 |
|-----------|-----------|-------------|
| `NUMBER AUTOINCREMENT` | `BIGINT AUTO_INCREMENT` | 모든 대리키 PK |
| `NUMBER` (정수: 가격/카운트) | `BIGINT` | price, min/max_price, record_count, raw_id |
| `NUMBER(12,2)` | `DECIMAL(12,2)` | avg_price |
| `NUMBER(8,4)` | `DECIMAL(8,4)` | change_pct |
| `VARCHAR(n)` | `VARCHAR(n)` (utf8mb4) | 문자열 전반 — **인덱스 바이트 한계 주의(§7)** |
| `BOOLEAN` | `TINYINT(1)` (`DEFAULT TRUE`→`DEFAULT 1`) | is_active |
| `TIMESTAMP_TZ` | `DATETIME` (UTC 저장) | crawled_at, created_at, updated_at, loaded_at |
| `TIMESTAMP_NTZ` | `DATETIME` (UTC 저장) | transform_failures.crawled_at, failed_at |
| `DATE` | `DATE` | price_date, week_start |
| `DEFAULT CURRENT_TIMESTAMP()` | `DEFAULT CURRENT_TIMESTAMP` | 각종 생성 시각 |

> 식별자는 소문자 snake_case로 통일한다(Snowflake는 대문자였음). MySQL 컬럼명은 대소문자 무시, 테이블명은 `lower_case_table_names`(Windows 기본 1) 영향을 받으므로 **소문자 테이블명**이 안전.

---

## 4. 문법·함수 이식 매핑표 (확정)

| Snowflake 구문 | MySQL 8.0 대체 | 사용처 |
|----------------|----------------|--------|
| `QUALIFY ROW_NUMBER() OVER(...) = 1` | **서브쿼리 + `ROW_NUMBER()` 필터** (아래 §6 `stg_latest_prices`) | LATEST_PRICES 뷰 |
| `MERGE INTO ... WHEN MATCHED / NOT MATCHED` | **`INSERT ... ON DUPLICATE KEY UPDATE`** (대상 UNIQUE 필요) | transform(products, price_history), analytics(3종) |
| `CRAWLED_AT::DATE` | `DATE(crawled_at)` | daily 집계 |
| `DATE_TRUNC('WEEK', CRAWLED_AT)::DATE` | `DATE_SUB(DATE(crawled_at), INTERVAL WEEKDAY(crawled_at) DAY)` (**월요일 시작 확정**, §7-3) | weekly 집계 |
| `USE DATABASE COMPUTER_PRICE` / `USE SCHEMA X` | **제거** (단일 DB, 접두사 테이블명 직접 참조) | 크롤러, load_raw, transform |
| `STREAM (CRAWLED_PRICES_STREAM, APPEND_ONLY)` | **§8 별도 결정 필요** (MySQL엔 Stream 없음) | transform 증분 소비 |
| `SCHEMA.TABLE` 참조 (예: `STAGING.PRODUCTS`) | `stg_products` (접두사 단일명) | 전 계층 |

### MERGE → ON DUPLICATE KEY UPDATE 예시 (패턴 참고용, DDL 아님)
```sql
-- 기존(Snowflake): MERGE INTO PRODUCTS ... ON (SITE, PRODUCT_NAME)
INSERT INTO stg_products (site, category, product_name, url)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    url = IF(VALUES(url) <> '', VALUES(url), url),
    updated_at = CURRENT_TIMESTAMP;
```

---

## 5. 계층 개요

| 계층 | 접두사 | 오브젝트 | 쓰는 주체 |
|------|--------|----------|-----------|
| RAW | `raw_` | `raw_crawled_prices`, `raw_transform_failures` | `load_raw.py`, `transform.py`(실패기록) |
| STAGING | `stg_` | `stg_products`, `stg_price_history`, `stg_price_alerts`, `stg_watchlist`, `stg_latest_prices`(뷰) | `transform.py`, `detect.py`, 대시보드(watchlist) |
| ANALYTICS | `ans_` | `ans_daily_price_stats`, `ans_weekly_price_stats`, `ans_product_stats` | `analytics.py` |

---

## 6. 테이블별 상세 스펙

> 표기: 컬럼은 소문자 snake_case, 타입은 MySQL 확정 타입. `↓제거` = 이관 시 삭제, `↑추가` = MySQL 신규 필요.

### 6.1 `raw_crawled_prices` (RAW)
크롤러 산출 `RawCrawledPrice` DTO 원본 적재. `load_raw.py`가 INSERT.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `id` | BIGINT | PK, AUTO_INCREMENT | |
| `site` | VARCHAR(50) | NOT NULL | **영문**(`danawa`/`compuzone`/`kjwwang`) |
| `category` | VARCHAR(100) | NOT NULL | 원본 카테고리 |
| `product_name` | VARCHAR(500) | NOT NULL | 상품명 원본 (2000→500 축소 확정, §7-1) |
| `price_text` | VARCHAR(100) | NOT NULL | **가격 원본 텍스트 — 불변 계약(§9)** |
| `brand` | VARCHAR(200) | NULL | watchlist에서 주입 |
| `url` | VARCHAR(4000) | NOT NULL | |
| ~~`stock_status`~~ | — | **↓제거** | 죽은 컬럼 |
| `crawled_at` | DATETIME | NOT NULL | UTC |
| `loaded_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

- **자연키(멱등)**: `UNIQUE (site, category, product_name, crawled_at)` — 기존 `UQ_RAW_CRAWLED` 대응.
- ✅ **인덱스 길이 해결(§7-1 확정)**: `product_name`을 **VARCHAR(500)**으로 축소해 자연키 4개 컬럼 합계가 InnoDB 인덱스 한계(3072 byte) 이내 → **해시 컬럼 없이 직접 UNIQUE** 생성.

### 6.2 `raw_transform_failures` (RAW)
transform 파싱/이상치 실패 감사(append-only). `transform.py`가 INSERT.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `id` | BIGINT | PK, AUTO_INCREMENT | |
| `crawled_prices_id` | BIGINT | NULL | `raw_crawled_prices.id` 참조값(**FK 아님**) |
| `site` | VARCHAR(50) | NULL | |
| `category` | VARCHAR(100) | NULL | |
| `product_name` | VARCHAR(500) | NULL | raw_crawled_prices와 통일(§7-1) |
| `price_text` | VARCHAR(100) | NULL | |
| `crawled_at` | DATETIME | NULL | |
| `reject_reason` | VARCHAR(200) | NOT NULL | 예: "가격 파싱 실패", "카테고리 범위 초과" |
| `failed_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

- 자연키 없음(순수 감사 로그).

### 6.3 `stg_products` (STAGING)
정제된 상품 마스터. `transform.py`가 UPSERT.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `product_id` | BIGINT | PK, AUTO_INCREMENT | |
| `site` | VARCHAR(50) | NOT NULL | **한글 표시명**(다나와/컴퓨존/견적왕) |
| `category` | VARCHAR(100) | NOT NULL | CPU/GPU/RAM/SSD |
| `product_name` | VARCHAR(500) | NOT NULL | 공백 정규화된 이름 |
| ~~`brand`~~ | — | **↓이미 제거됨(010)** | 이관 안 함 |
| ~~`model_number`~~ | — | **↓이미 제거됨(009)** | 이관 안 함 |
| `url` | VARCHAR(4000) | NULL | |
| `created_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |
| `updated_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | UPSERT 시 앱이 명시 세팅 |

- **자연키(멱등)**: `UNIQUE (site, product_name)` — `UQ_PRODUCTS` 대응. `site(50)+product_name(500)` utf8mb4 = 2200 byte < 3072 → **문제 없음**.

### 6.4 `stg_price_history` (STAGING)
상품별 가격 이력(append-only). `transform.py`가 UPSERT.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `id` | BIGINT | PK, AUTO_INCREMENT | detect가 daily_price_id로 참조 |
| `product_id` | BIGINT | NOT NULL, **FK → stg_products** | |
| `raw_id` | BIGINT | NULL | `raw_crawled_prices.id` 추적(**FK 아님**) |
| `price` | BIGINT | NOT NULL | KRW 정수(parse_korean_price 결과) |
| ~~`stock_status`~~ | — | **↓제거** | 죽은 컬럼 |
| `crawled_at` | DATETIME | NOT NULL | UTC |

- **자연키(멱등)**: `UNIQUE (product_id, crawled_at)` ↑**신규 추가 확정(§7-2)**.
  - Snowflake 원본엔 이 UNIQUE가 **없었고** MERGE의 `ON` 절로만 판정했다. MySQL `ON DUPLICATE KEY UPDATE`는 UNIQUE가 있어야 동작하므로 **반드시 신규 추가한다**.

### 6.5 `stg_price_alerts` (STAGING)
가격 변동 알림(append-only). `detect.py`가 INSERT.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `alert_id` | BIGINT | PK, AUTO_INCREMENT | |
| `product_id` | BIGINT | NOT NULL, **FK → stg_products** | |
| `daily_price_id` | BIGINT | NULL, **FK → stg_price_history(id)** | 중복 알림 방지 키(detect의 NOT EXISTS) |
| `alert_type` | VARCHAR(30) | NOT NULL | NEW_LOW / NEW_HIGH / PRICE_DROP / PRICE_SPIKE |
| `old_price` | BIGINT | NULL | |
| `new_price` | BIGINT | NOT NULL | |
| `change_pct` | DECIMAL(8,4) | NULL | |
| `created_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |
| ~~`is_read`~~ | — | **↓이미 제거됨(009)** | 이관 안 함 |

- 자연키 없음(append). detect가 `NOT EXISTS (… daily_price_id …)`로 중복 억제.

### 6.6 `stg_watchlist` (STAGING)
크롤링 대상 목록. 대시보드가 관리, 크롤러가 읽기.

| 컬럼 | 타입 | 제약/기본값 | 비고 |
|------|------|-------------|------|
| `id` | BIGINT | PK, AUTO_INCREMENT | |
| `` `query` `` | VARCHAR(500) | NOT NULL | 검색어 — DDL에서 **백틱 인용 확정(§7-4)** |
| `pcode` | VARCHAR(50) | NOT NULL, **UNIQUE** | 사이트별 상품ID 공용 컬럼(pcode/product_no/pd_no) |
| `product_name` | VARCHAR(500) | NULL | |
| `category` | VARCHAR(100) | NOT NULL | |
| `brand` | VARCHAR(200) | NULL | raw로 전달됨 |
| `site` | VARCHAR(20) | NOT NULL DEFAULT '다나와' | **한글값**(008에서 컬럼 추가, 009에서 값 한글 통일) |
| `is_active` | TINYINT(1) | NOT NULL DEFAULT 1 | |
| `added_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP | |

- 크롤러 로드 쿼리: `SELECT query, pcode, category, brand FROM stg_watchlist WHERE is_active = 1 AND site = '<한글명>'`.

### 6.7 `stg_latest_prices` (STAGING · **뷰**)
상품별 최신가 파생 뷰. `QUALIFY` → 서브쿼리 재작성.

| 컬럼 | 타입 | 비고 |
|------|------|------|
| `product_id` | (파생) | |
| `price` | (파생) | |
| `crawled_at` | (파생) | |

**이식 정의(패턴 참고용):**
```sql
CREATE OR REPLACE VIEW stg_latest_prices AS
SELECT product_id, price, crawled_at
FROM (
    SELECT product_id, price, crawled_at,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY crawled_at DESC) AS rn
    FROM stg_price_history
) ranked
WHERE rn = 1;
```

### 6.8 `ans_daily_price_stats` (ANALYTICS)
일별 집계. `analytics.py`가 UPSERT (`GROUP BY product_id, DATE(crawled_at)`).

| 컬럼 | 타입 | 제약/기본값 |
|------|------|-------------|
| `id` | BIGINT | PK, AUTO_INCREMENT |
| `product_id` | BIGINT | NOT NULL |
| `price_date` | DATE | NOT NULL |
| `min_price` | BIGINT | NOT NULL |
| `max_price` | BIGINT | NOT NULL |
| `avg_price` | DECIMAL(12,2) | NOT NULL |
| `record_count` | BIGINT | NOT NULL |

- **자연키(멱등)**: `UNIQUE (product_id, price_date)`.

### 6.9 `ans_weekly_price_stats` (ANALYTICS)
주별 집계. `analytics.py`가 UPSERT. 주 시작일은 `DATE_TRUNC('WEEK')` → **월요일 시작 확정(§7-3)**.

| 컬럼 | 타입 | 제약/기본값 |
|------|------|-------------|
| `id` | BIGINT | PK, AUTO_INCREMENT |
| `product_id` | BIGINT | NOT NULL |
| `week_start` | DATE | NOT NULL |
| `min_price` | BIGINT | NOT NULL |
| `max_price` | BIGINT | NOT NULL |
| `avg_price` | DECIMAL(12,2) | NOT NULL |
| `record_count` | BIGINT | NOT NULL |

- **자연키(멱등)**: `UNIQUE (product_id, week_start)`.

### 6.10 `ans_product_stats` (ANALYTICS)
상품 전기간 통계. `analytics.py`가 UPSERT (`GROUP BY product_id`). detect가 NEW_LOW/NEW_HIGH 판정에 읽음.

| 컬럼 | 타입 | 제약/기본값 |
|------|------|-------------|
| `product_id` | BIGINT | **PK** (자연키=대리키 일치), FK → stg_products |
| `avg_price` | DECIMAL(12,2) | NOT NULL |
| `min_price_ever` | BIGINT | NOT NULL |
| `max_price_ever` | BIGINT | NOT NULL |
| `first_crawled_at` | DATETIME | NOT NULL |
| `last_crawled_at` | DATETIME | NOT NULL |
| `total_records` | BIGINT | NOT NULL |
| `updated_at` | DATETIME | NOT NULL DEFAULT CURRENT_TIMESTAMP |

- `product_id`가 PK이므로 `ON DUPLICATE KEY UPDATE`가 PK에 걸린다(별도 UNIQUE 불필요).

---

## 7. MySQL 특유 결정사항 (모두 확정)

### 7-1. `raw_crawled_prices` 자연키 인덱스 길이 — ✅ 확정: product_name VARCHAR(500), 직접 UNIQUE
`product_name`을 **VARCHAR(500)**으로 축소 확정한다. 자연키 `UNIQUE (site, category, product_name, crawled_at)`의 인덱스 바이트 합계(50+100+500 char × 4 + DATETIME)가 InnoDB 한계(3072 byte) 이내이므로 **해시 컬럼 없이 직접 UNIQUE**를 생성한다.
- **폐기**: 해시 컬럼 방식(구 (A) `name_hash`)은 도입하지 않는다.
- `raw_transform_failures.product_name`도 동일하게 **VARCHAR(500)**으로 통일한다(§6.2).
- 근거: 크롤러가 저장하는 상품명(상세 title 기반)은 500자 이내로 충분하며, 파싱은 transform 단독 책임이라 RAW 이름 길이는 정합에 영향 없음(불변 계약 §9-1).

### 7-2. `stg_price_history` UNIQUE 신규 추가 — ✅ 확정
Snowflake엔 없던 `UNIQUE (product_id, crawled_at)`를 **신규 추가한다**. 이 UNIQUE가 있어야 `INSERT ... ON DUPLICATE KEY UPDATE`가 기존 MERGE(`ON product_id, crawled_at`)와 동일하게 동작한다(§6.4).

### 7-3. 주 시작일 정의 — ✅ 확정: 월요일 시작
Snowflake `DATE_TRUNC('WEEK', …)`(기본 월요일 시작)를 **월요일 기준으로 확정** 재현한다:
```sql
DATE_SUB(DATE(crawled_at), INTERVAL WEEKDAY(crawled_at) DAY)   -- WEEKDAY: 월=0 → 그 주 월요일
```
`analytics.py`의 weekly 집계 `GROUP BY`와 `ans_weekly_price_stats.week_start`에 이 식을 적용한다.

### 7-4. 예약어 / 식별자 인용 — ✅ 확정: 백틱 일괄 적용
- 생성 DDL 및 코드의 SQL에서 **모든 식별자(테이블·컬럼)를 백틳(`` ` ``)으로 인용한다** (예: `` `query` ``, `` `stg_watchlist` ``).
- 이관 대상 중 MySQL 8.0 예약어와 실제 충돌하는 이름은 없으나(`query`/`status`/`site`/`url`/`category` 모두 비예약어, 예약어 `rank`/`user`는 미사용), 백틱 일괄 적용으로 향후 이름 추가 시의 리스크까지 방어한다.
- `USER`/`BUILD` 계열은 애초에 이관 제외라 예약어 이슈 없음.

---

## 8. 별도 결정 보류 항목 (이 문서 범위 밖, Phase 3에서 확정)

- **Stream(증분 소비) 대체 방식**: 현재 transform은 `CRAWLED_PRICES_STREAM`(APPEND_ONLY)에서 새 레코드만 소비한다. MySQL엔 Stream이 없으므로 대체 필요. 확정 원칙(§2-4)에 따라 `batch_id`는 도입하지 않으므로, `raw_id` 워터마크(마지막 처리 `raw_crawled_prices.id` 추적) 또는 "미처리 조인"(`LEFT JOIN stg_price_history ON raw_id` where null) 등으로 재설계 → **Phase 3-2에서 별도 확정**. (본 문서는 스키마만 확정)

---

## 9. 불변 계약 (반드시 준수)

1. **`price_text` 원본 보존**: `raw_crawled_prices.price_text`는 사이트별 상이한 형식("1,234,500원" / "1234500" / "1,234,500")을 **가공 없이 그대로** 저장한다. 숫자 파싱은 **오직 `transform.py`의 `parse_korean_price()`(+`validate_price()`) 단독 책임**이며, 마이그레이션 중에도 이 로직을 **절대 변경하지 않는다**. RAW에 파싱된 숫자를 넣지 않는다.
2. **`site` 이중 표기 매핑 유지**: RAW는 **영문**(`danawa`/`compuzone`/`kjwwang`), STAGING(`stg_products`, `stg_watchlist`)은 **한글**(다나와/컴퓨존/견적왕). 변환은 `transform.py`의 `_SITE_DISPLAY_MAP`이 담당한다. 이 매핑과 각 계층의 표기 규칙을 그대로 보존한다.
3. **`USE DATABASE` / `USE SCHEMA` 제거**: 단일 DB이므로 모든 `USE …` 구문을 삭제하고, 테이블은 접두사 단일명(`stg_products` 등)으로 직접 참조한다. 크롤러의 `USE DATABASE COMPUTER_PRICE`, load_raw/transform의 `USE SCHEMA RAW/STAGING`이 대상.
4. **원본 추적 컬럼 비-FK 유지**: `stg_price_history.raw_id`, `raw_transform_failures.crawled_prices_id`는 FK로 걸지 않는다(원본 정리와 독립).
5. **UTC 통일**: 모든 시각은 앱에서 UTC로 생성·저장(`DATETIME`), DB는 타임존 변환을 하지 않는다.

---

## 10. 변경 이력
- 최초 작성: MySQL 마이그레이션 Phase 1 완료 시점 조사 결과 기반. Phase 2 DDL의 단일 근거 문서.
