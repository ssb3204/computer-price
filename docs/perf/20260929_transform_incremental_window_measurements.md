# 측정 원자료 — transform.py 증분 쿼리 최근 7일 창 (EXPLAIN ANALYZE)

- **해석·판단**: [20260929_transform_incremental_window.md](20260929_transform_incremental_window.md)
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
| `raw_transform_failures` 행 수 | 6,984 |
| `innodb_buffer_pool_size` | 128MB |
| `raw_crawled_prices.idx_raw_crawled_at` | 있음 |
| `stg_price_history.uq_price_history_raw_id` | 있음 (③에서만 INVISIBLE) |
| 창 기준 시각 | **`2026-09-23 00:00:00` 고정** (아래 참고) |

### 측정 방법

- 코드는 `UTC_TIMESTAMP() - INTERVAL 7 DAY` 를 쓰지만, perf 스키마 데이터가 2026-09-22 까지만 있어
  측정 시점(2026-09-29) 기준으로는 창 안에 데이터가 거의 없다. 그래서 데이터 마지막 날이 끝나는
  **`2026-09-23 00:00:00` 을 "지금"으로 고정**해 측정했다. 창 하한은 `2026-09-16 00:00:00` 이다.
- 순서: 세트마다 `docker restart perf-mysql` → 기동 후 대기 → 3회 연속 실행.
  ① 일반 → ② 전체 점검 → ③ 일반(raw_id 인덱스 숨김).
- ③은 운영 스키마(당시 `uq_price_history_raw_id` 없음)와 같은 조건을 재현하기 위해, 인덱스를 삭제하지
  않고 옵티마이저에서만 숨겼다. 측정 후 즉시 VISIBLE 로 되돌렸다.
  ```sql
  ALTER TABLE `stg_price_history` ALTER INDEX `uq_price_history_raw_id` INVISIBLE;
  -- ③ 측정
  ALTER TABLE `stg_price_history` ALTER INDEX `uq_price_history_raw_id` VISIBLE;
  ```
- 3회분을 모두 남긴다. 1회차는 재시작 직후 값이다.

## 요약

| 세트 | 1회차 | 2회차 | 3회차 | 중앙값 | raw 접근 | stg 조인 | 결과 |
|---|---:|---:|---:|---:|---|---|---:|
| ① 일반 | 579ms | 58.3ms | 57.8ms | **58.3ms** | index range scan, 22,428행 | 인덱스 lookup (uq_price_history_raw_id) | 801 |
| ② 전체 점검 | 10,039ms | 9,721ms | 10,048ms | **10,039ms** | table scan, 351만 행 | 인덱스 lookup (uq_price_history_raw_id) | 801 |
| ③ 일반, raw_id 인덱스 숨김 | 1,306ms | 844ms | 799ms | **844ms** | index range scan, 22,428행 | stg table scan 350만 행 → hash join | 801 |

- 세 세트 모두 결과 행 수 801 로 동일하다.
- `raw_transform_failures` 의 NOT EXISTS 는 세 세트 모두 `Materialize with deduplication` (loops=1, 6,984행)
  으로 한 번만 읽히고, 이후 `<auto_distinct_key>` 로 행마다 조회된다.

---

## ① 일반 모드 (최근 7일 창)

```sql
EXPLAIN ANALYZE
SELECT r.`id`, r.`site`, r.`category`, r.`product_name`,
       r.`price_text`, r.`url`, r.`crawled_at`
FROM `raw_crawled_prices` r
LEFT JOIN `stg_price_history` h ON h.`raw_id` = r.`id`
WHERE h.`raw_id` IS NULL
  AND r.`crawled_at` >= TIMESTAMP '2026-09-23 00:00:00' - INTERVAL 7 DAY
  AND NOT EXISTS (
      SELECT 1 FROM `raw_transform_failures` f
      WHERE f.`crawled_prices_id` = r.`id`
  );
```

**1회차** (재시작 직후)
```text
-> Filter: (h.raw_id is null)  (cost=62.2e+6 rows=311e+6) (actual time=21.2..579 rows=801 loops=1)
    -> Nested loop left join  (cost=62.2e+6 rows=311e+6) (actual time=21.1..578 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=20.4..334 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49777 rows=44432) (actual time=10.7..313 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1495..1495 rows=1) (actual time=828e-6..828e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1495..1495 rows=6989) (actual time=9.64..9.64 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=796 rows=6989) (actual time=1.04..8.7 rows=6984 loops=1)
                        -> Table scan on f  (cost=796 rows=6989) (actual time=1.04..8.37 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.0107..0.0108 rows=0.964 loops=22396)
```

**2회차**
```text
-> Filter: (h.raw_id is null)  (cost=62.2e+6 rows=311e+6) (actual time=11.3..58.3 rows=801 loops=1)
    -> Nested loop left join  (cost=62.2e+6 rows=311e+6) (actual time=11.2..57.7 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=11.2..31.8 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49079 rows=44432) (actual time=9.59..22.5 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1422..1422 rows=1) (actual time=314e-6..314e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1422..1422 rows=6989) (actual time=1.61..1.61 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=723 rows=6989) (actual time=0.0434..0.952 rows=6984 loops=1)
                        -> Table scan on f  (cost=723 rows=6989) (actual time=0.0423..0.618 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.00101..0.00103 rows=0.964 loops=22396)
```

**3회차**
```text
-> Filter: (h.raw_id is null)  (cost=62.2e+6 rows=311e+6) (actual time=11.2..57.8 rows=801 loops=1)
    -> Nested loop left join  (cost=62.2e+6 rows=311e+6) (actual time=11.1..57.2 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=11.1..31.3 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49079 rows=44432) (actual time=9.47..21.9 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1422..1422 rows=1) (actual time=320e-6..320e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1422..1422 rows=6989) (actual time=1.66..1.66 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=723 rows=6989) (actual time=0.0453..0.996 rows=6984 loops=1)
                        -> Table scan on f  (cost=723 rows=6989) (actual time=0.0441..0.625 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.00101..0.00104 rows=0.964 loops=22396)
```

---

## ② 전체 점검 모드 (`--full`, 창 없음)

```sql
EXPLAIN ANALYZE
SELECT r.`id`, r.`site`, r.`category`, r.`product_name`,
       r.`price_text`, r.`url`, r.`crawled_at`
FROM `raw_crawled_prices` r
LEFT JOIN `stg_price_history` h ON h.`raw_id` = r.`id`
WHERE h.`raw_id` IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM `raw_transform_failures` f
      WHERE f.`crawled_prices_id` = r.`id`
  );
```

**1회차** (재시작 직후)
```text
-> Filter: (h.raw_id is null)  (cost=4.79e+9 rows=23.9e+9) (actual time=25.8..10039 rows=801 loops=1)
    -> Nested loop left join  (cost=4.79e+9 rows=23.9e+9) (actual time=7.65..9955 rows=3.5e+6 loops=1)
        -> Nested loop antijoin  (cost=2.4e+9 rows=23.9e+9) (actual time=7.13..5897 rows=3.5e+6 loops=1)
            -> Table scan on r  (cost=390212 rows=3.43e+6) (actual time=0.626..4839 rows=3.51e+6 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1495..1495 rows=1) (actual time=218e-6..218e-6 rows=0.00199 loops=3.51e+6)
                -> Materialize with deduplication  (cost=1495..1495 rows=6989) (actual time=6.48..6.48 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=796 rows=6989) (actual time=0.723..5.15 rows=6984 loops=1)
                        -> Table scan on f  (cost=796 rows=6989) (actual time=0.723..4.82 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.00104..0.00106 rows=1 loops=3.5e+6)
```

**2회차**
```text
-> Filter: (h.raw_id is null)  (cost=4.79e+9 rows=23.9e+9) (actual time=17..9721 rows=801 loops=1)
    -> Nested loop left join  (cost=4.79e+9 rows=23.9e+9) (actual time=5.74..9637 rows=3.5e+6 loops=1)
        -> Nested loop antijoin  (cost=2.4e+9 rows=23.9e+9) (actual time=3.26..5500 rows=3.5e+6 loops=1)
            -> Table scan on r  (cost=385160 rows=3.43e+6) (actual time=0.086..4451 rows=3.51e+6 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1450..1450 rows=1) (actual time=216e-6..216e-6 rows=0.00199 loops=3.51e+6)
                -> Materialize with deduplication  (cost=1450..1450 rows=6989) (actual time=3.17..3.17 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=751 rows=6989) (actual time=0.077..2.53 rows=6984 loops=1)
                        -> Table scan on f  (cost=751 rows=6989) (actual time=0.0763..2.24 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.00106..0.00108 rows=1 loops=3.5e+6)
```

**3회차**
```text
-> Filter: (h.raw_id is null)  (cost=4.79e+9 rows=23.9e+9) (actual time=20.1..10048 rows=801 loops=1)
    -> Nested loop left join  (cost=4.79e+9 rows=23.9e+9) (actual time=6.83..9964 rows=3.5e+6 loops=1)
        -> Nested loop antijoin  (cost=2.4e+9 rows=23.9e+9) (actual time=4.81..5787 rows=3.5e+6 loops=1)
            -> Table scan on r  (cost=385147 rows=3.43e+6) (actual time=1.06..4731 rows=3.51e+6 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1450..1450 rows=1) (actual time=218e-6..218e-6 rows=0.00199 loops=3.51e+6)
                -> Materialize with deduplication  (cost=1450..1450 rows=6989) (actual time=3.74..3.74 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=751 rows=6989) (actual time=0.105..3.04 rows=6984 loops=1)
                        -> Table scan on f  (cost=751 rows=6989) (actual time=0.104..2.68 rows=6984 loops=1)
        -> Single-row covering index lookup on h using uq_price_history_raw_id (raw_id=r.id)  (cost=1 rows=1) (actual time=0.00108..0.00109 rows=1 loops=3.5e+6)
```

---

## ③ 일반 모드, `uq_price_history_raw_id` INVISIBLE

①과 같은 쿼리.

**1회차** (재시작 직후)
```text
-> Filter: (h.raw_id is null)  (cost=108e+12 rows=108e+12) (actual time=1066..1306 rows=801 loops=1)
    -> Left hash join (h.raw_id = r.id)  (cost=108e+12 rows=108e+12) (actual time=1030..1305 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=10.5..39.1 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49777 rows=44432) (actual time=8.44..30.6 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1495..1495 rows=1) (actual time=298e-6..298e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1495..1495 rows=6989) (actual time=2.08..2.08 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=796 rows=6989) (actual time=0.155..1.15 rows=6984 loops=1)
                        -> Table scan on f  (cost=796 rows=6989) (actual time=0.154..0.889 rows=6984 loops=1)
        -> Hash
            -> Table scan on h  (cost=23744 rows=3.49e+6) (actual time=2.47..808 rows=3.5e+6 loops=1)
```

**2회차**
```text
-> Filter: (h.raw_id is null)  (cost=108e+12 rows=108e+12) (actual time=613..844 rows=801 loops=1)
    -> Left hash join (h.raw_id = r.id)  (cost=108e+12 rows=108e+12) (actual time=582..843 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=9.8..35.6 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49079 rows=44432) (actual time=8.17..27.4 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1422..1422 rows=1) (actual time=280e-6..280e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1422..1422 rows=6989) (actual time=1.62..1.62 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=723 rows=6989) (actual time=0.0644..1.01 rows=6984 loops=1)
                        -> Table scan on f  (cost=723 rows=6989) (actual time=0.0632..0.752 rows=6984 loops=1)
        -> Hash
            -> Table scan on h  (cost=23465 rows=3.49e+6) (actual time=0.24..398 rows=3.5e+6 loops=1)
```

**3회차**
```text
-> Filter: (h.raw_id is null)  (cost=108e+12 rows=108e+12) (actual time=585..799 rows=801 loops=1)
    -> Left hash join (h.raw_id = r.id)  (cost=108e+12 rows=108e+12) (actual time=556..798 rows=22396 loops=1)
        -> Nested loop antijoin  (cost=31.1e+6 rows=311e+6) (actual time=8.85..33.7 rows=22396 loops=1)
            -> Index range scan on r using idx_raw_crawled_at over ('2026-09-16 00:00:00' <= crawled_at), with index condition: (r.crawled_at >= <cache>((TIMESTAMP'2026-09-23 00:00:00' - interval 7 day)))  (cost=49079 rows=44432) (actual time=7.37..25.8 rows=22428 loops=1)
            -> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)  (cost=1422..1422 rows=1) (actual time=268e-6..268e-6 rows=0.00143 loops=22428)
                -> Materialize with deduplication  (cost=1422..1422 rows=6989) (actual time=1.47..1.47 rows=6984 loops=1)
                    -> Filter: (f.crawled_prices_id is not null)  (cost=723 rows=6989) (actual time=0.0616..0.951 rows=6984 loops=1)
                        -> Table scan on f  (cost=723 rows=6989) (actual time=0.0602..0.696 rows=6984 loops=1)
        -> Hash
            -> Table scan on h  (cost=23464 rows=3.49e+6) (actual time=0.463..377 rows=3.5e+6 loops=1)
```
