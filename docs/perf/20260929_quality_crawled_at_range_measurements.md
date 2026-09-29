# 측정 원자료 — quality.py 오늘 범위 조건 (EXPLAIN ANALYZE)

- **해석·판단**: [20260929_quality_crawled_at_range.md](20260929_quality_crawled_at_range.md)
- **측정자**: 직접 측정
- **측정일**: 2026-09-29
- **측정 스키마**: `computer_price_perf` (perf 스키마, 개발 DB 아님)

이 문서는 **붙여넣은 원문만** 둔다. 요약·해석은 위 문서에 쓴다.

## 측정 환경

| 항목 | 값 |
|---|---|
| 컨테이너 | `perf-mysql` (운영 VM 사양 모사: 2 CPU, 1GB) |
| MySQL 버전 | 8.0.46 |
| `raw_crawled_prices` 행 수 | 약 351만 (3.51e+6, 실행 계획 기준) |
| `stg_price_history` 행 수 | 약 350만 (3.5e+6, 실행 계획 기준) |
| `innodb_buffer_pool_size` | 128MB |
| 측정 시점의 `UTC_DATE()` | 2026-09-29 |
| 측정 대상 날짜 | **2026-09-22 고정** (아래 참고) |

### 측정 방법

- 코드는 `UTC_DATE()` 를 쓰지만, perf 스키마 데이터가 2026-09-22 까지만 있어 측정 시점의
  `UTC_DATE()` (2026-09-29)로는 결과가 0건이 된다. 그래서 데이터가 있는 **2026-09-22 를
  리터럴로 고정**해 측정했다. 조건의 형태(`DATE()` 비교 vs 반열린 구간)는 코드와 같다.
- 순서: 세트마다 `docker restart perf-mysql` → 기동 후 대기 → 3회 연속 실행.
  raw 변경 전 → raw 변경 후 → stg 변경 전 → stg 변경 후.
- 3회분을 모두 남긴다. 1회차는 재시작 직후 값이다.

---

## 1. `raw_crawled_prices` — 오늘 건수

### 1-1. 변경 전 — `DATE(crawled_at) = ...`

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM `raw_crawled_prices`
WHERE DATE(`crawled_at`) = '2026-09-22';
```

인덱스 상태: `PRIMARY (id)`, `uq_raw_crawled (site, category, product_name, crawled_at)`,
`idx_raw_crawled_at (crawled_at)` — 인덱스는 있지만 함수 때문에 전체 스캔(index scan)

**1회차** (재시작 직후)
```text
-> Aggregate: count(0)  (cost=732808 rows=1) (actual time=394..394 rows=1 loops=1)
    -> Filter: (cast(raw_crawled_prices.crawled_at as date) = '2026-09-22')  (cost=390212 rows=3.43e+6) (actual time=393..394 rows=3204 loops=1)
        -> Covering index scan on raw_crawled_prices using idx_raw_crawled_at  (cost=390212 rows=3.43e+6) (actual time=0.103..282 rows=3.51e+6 loops=1)
```

**2회차**
```text
-> Aggregate: count(0)  (cost=732808 rows=1) (actual time=363..363 rows=1 loops=1)
    -> Filter: (cast(raw_crawled_prices.crawled_at as date) = '2026-09-22')  (cost=390212 rows=3.43e+6) (actual time=363..363 rows=3204 loops=1)
        -> Covering index scan on raw_crawled_prices using idx_raw_crawled_at  (cost=390212 rows=3.43e+6) (actual time=0.0252..250 rows=3.51e+6 loops=1)
```

**3회차**
```text
-> Aggregate: count(0)  (cost=732808 rows=1) (actual time=378..378 rows=1 loops=1)
    -> Filter: (cast(raw_crawled_prices.crawled_at as date) = '2026-09-22')  (cost=390212 rows=3.43e+6) (actual time=377..378 rows=3204 loops=1)
        -> Covering index scan on raw_crawled_prices using idx_raw_crawled_at  (cost=390212 rows=3.43e+6) (actual time=0.0248..261 rows=3.51e+6 loops=1)
```

### 1-2. 변경 후 — 반열린 구간 + `idx_raw_crawled_at`

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM `raw_crawled_prices`
WHERE `crawled_at` >= '2026-09-22'
  AND `crawled_at` <  '2026-09-23';
```

인덱스 상태: 1-1 과 동일 (`idx_raw_crawled_at` 존재)

**1회차** (재시작 직후)
```text
-> Aggregate: count(0)  (cost=967 rows=1) (actual time=0.661..0.661 rows=1 loops=1)
    -> Filter: ((raw_crawled_prices.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (raw_crawled_prices.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=647 rows=3204) (actual time=0.0193..0.578 rows=3204 loops=1)
        -> Covering index range scan on raw_crawled_prices using idx_raw_crawled_at over ('2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00')  (cost=647 rows=3204) (actual time=0.0172..0.35 rows=3204 loops=1)
```

**2회차**
```text
-> Aggregate: count(0)  (cost=967 rows=1) (actual time=0.573..0.573 rows=1 loops=1)
    -> Filter: ((raw_crawled_prices.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (raw_crawled_prices.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=647 rows=3204) (actual time=0.0205..0.5 rows=3204 loops=1)
        -> Covering index range scan on raw_crawled_prices using idx_raw_crawled_at over ('2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00')  (cost=647 rows=3204) (actual time=0.0188..0.311 rows=3204 loops=1)
```

**3회차**
```text
-> Aggregate: count(0)  (cost=967 rows=1) (actual time=0.709..0.71 rows=1 loops=1)
    -> Filter: ((raw_crawled_prices.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (raw_crawled_prices.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=647 rows=3204) (actual time=0.0192..0.634 rows=3204 loops=1)
        -> Covering index range scan on raw_crawled_prices using idx_raw_crawled_at over ('2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00')  (cost=647 rows=3204) (actual time=0.0178..0.399 rows=3204 loops=1)
```

### 1-3. raw 요약

| | 1회차 | 2회차 | 3회차 | 중앙값 | 접근 방식 | 스캔 행 수 | 결과 |
|---|---:|---:|---:|---:|---|---:|---:|
| 변경 전 | 394ms | 363ms | 378ms | 378ms | Covering index scan | 약 351만 | 3,204 |
| 변경 후 | 0.661ms | 0.573ms | 0.710ms | 0.661ms | Covering index range scan | 3,204 | 3,204 |

---

## 2. `stg_price_history` — 오늘 건수

### 2-1. 변경 전 — `DATE(crawled_at) = ...`

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM `stg_price_history`
WHERE DATE(`crawled_at`) = '2026-09-22';
```

인덱스 상태: `PRIMARY (id)`, `uq_price_history (product_id, crawled_at)`,
`uq_price_history_raw_id (raw_id)` — crawled_at 단독 인덱스 없음

**1회차** (재시작 직후)
```text
-> Aggregate: count(0)  (cost=710591 rows=1) (actual time=645..645 rows=1 loops=1)
    -> Filter: (cast(stg_price_history.crawled_at as date) = '2026-09-22')  (cost=361816 rows=3.49e+6) (actual time=4.5..645 rows=2403 loops=1)
        -> Covering index scan on stg_price_history using uq_price_history  (cost=361816 rows=3.49e+6) (actual time=2.74..535 rows=3.5e+6 loops=1)
```

**2회차**
```text
-> Aggregate: count(0)  (cost=710591 rows=1) (actual time=319..319 rows=1 loops=1)
    -> Filter: (cast(stg_price_history.crawled_at as date) = '2026-09-22')  (cost=361816 rows=3.49e+6) (actual time=0.625..319 rows=2403 loops=1)
        -> Covering index scan on stg_price_history using uq_price_history  (cost=361816 rows=3.49e+6) (actual time=0.139..206 rows=3.5e+6 loops=1)
```

**3회차**
```text
-> Aggregate: count(0)  (cost=710591 rows=1) (actual time=315..315 rows=1 loops=1)
    -> Filter: (cast(stg_price_history.crawled_at as date) = '2026-09-22')  (cost=361816 rows=3.49e+6) (actual time=0.467..315 rows=2403 loops=1)
        -> Covering index scan on stg_price_history using uq_price_history  (cost=361816 rows=3.49e+6) (actual time=0.114..203 rows=3.5e+6 loops=1)
```

### 2-2. 변경 후 — 반열린 구간 (인덱스 추가 없음)

```sql
EXPLAIN ANALYZE
SELECT COUNT(*) FROM `stg_price_history`
WHERE `crawled_at` >= '2026-09-22'
  AND `crawled_at` <  '2026-09-23';
```

인덱스 상태: 2-1 과 동일 (skip scan 이 나오는지 확인하는 것이 이 측정의 목적)

**1회차** (재시작 직후)
```text
-> Aggregate: count(0)  (cost=138274 rows=1) (actual time=12.2..12.2 rows=1 loops=1)
    -> Filter: ((stg_price_history.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (stg_price_history.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=99529 rows=387450) (actual time=0.143..12.1 rows=2403 loops=1)
        -> Covering index skip scan on stg_price_history using uq_price_history over '2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00'  (cost=99529 rows=387450) (actual time=0.142..11.9 rows=2403 loops=1)
```

**2회차**
```text
-> Aggregate: count(0)  (cost=138169 rows=1) (actual time=2.82..2.82 rows=1 loops=1)
    -> Filter: ((stg_price_history.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (stg_price_history.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=99424 rows=387450) (actual time=0.0314..2.74 rows=2403 loops=1)
        -> Covering index skip scan on stg_price_history using uq_price_history over '2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00'  (cost=99424 rows=387450) (actual time=0.0298..2.59 rows=2403 loops=1)
```

**3회차**
```text
-> Aggregate: count(0)  (cost=138169 rows=1) (actual time=3.4..3.4 rows=1 loops=1)
    -> Filter: ((stg_price_history.crawled_at >= TIMESTAMP'2026-09-22 00:00:00') and (stg_price_history.crawled_at < TIMESTAMP'2026-09-23 00:00:00'))  (cost=99424 rows=387450) (actual time=0.0333..3.34 rows=2403 loops=1)
        -> Covering index skip scan on stg_price_history using uq_price_history over '2026-09-22 00:00:00' <= crawled_at < '2026-09-23 00:00:00'  (cost=99424 rows=387450) (actual time=0.0316..3.16 rows=2403 loops=1)
```

### 2-3. stg 요약

| | 1회차 | 2회차 | 3회차 | 중앙값 | 접근 방식 | 결과 행 수 |
|---|---:|---:|---:|---:|---|---:|
| 변경 전 | 645ms | 319ms | 315ms | 319ms | Covering index scan (uq_price_history) | 2,403 |
| 변경 후 | 12.2ms | 2.82ms | 3.4ms | 3.4ms | Covering index skip scan (uq_price_history) | 2,403 |

`stg` 는 결과 행 수가 변경 전후로 같다 — 빨라진 것이지 덜 읽어서 적게 나온 것이 아님을
이 칸으로 확인한다.

---

## 3. 교차검증 쿼리 (선택)

`check_cross_site_prices` 의 JOIN 형태는 이번에 측정하지 않았다.

---

## 비고: 폐기한 1차 측정

처음에는 raw 변경 전·후를 번갈아 실행해 세트별 재시작 조건이 지켜지지 않았다
(변경 전 687 / 363 / 372ms, 변경 후 0.851 / 0.759 / 0.686ms). 결론은 같지만 측정 조건이
달라 대표값에서 제외하고 위 순서로 다시 측정했다.

재시작 직후 1회차가 2·3회차와 크게 차이 나지 않는 것은 MySQL 8.0 의 buffer pool
덤프/로드(`innodb_buffer_pool_load_at_startup`, 기본 ON)로 재시작 시 이전 페이지가 다시
적재되기 때문으로 보인다.
