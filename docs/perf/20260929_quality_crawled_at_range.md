# quality.py — 오늘 범위 조건을 sargable 범위 조건으로 교체

- **상태**: 적용 (2026-09-29)
- **대상 파일**: `src/pipeline/quality.py` (3곳), `mysql/ddl/schema.sql` (인덱스 1개)
- **관련 테스트**: `tests/integration/test_quality_date_boundary.py`
- **측정 원자료**: [20260929_quality_crawled_at_range_measurements.md](20260929_quality_crawled_at_range_measurements.md) — perf 스키마에서 잰 `EXPLAIN ANALYZE` 원문 (raw·stg 각각 변경 전·후 3회)

---

## 1. 문제 — 컬럼을 함수로 감싸 인덱스를 못 쓰고 있었다

`quality.py` 의 "오늘 수집분" 필터가 세 곳 모두 이 형태였다.

```sql
WHERE DATE(`crawled_at`) = UTC_DATE()
```

`crawled_at` 이 `DATE()` 안에 들어가 있으면 옵티마이저가 인덱스 순서를 쓸 수 없다
(sargable 하지 않다). 인덱스에 `crawled_at` 이 들어 있어도 **전 구간을 읽어
행마다 `DATE()` 를 계산해 비교**하는 index scan 이 된다. 데이터가 쌓일수록
비용이 선형으로 늘고, 파이프라인 4회/일 실행마다 그 비용을 낸다.

## 2. 변경 — 반열린 구간 `[오늘, 내일)`

```sql
WHERE `crawled_at` >= UTC_DATE()
  AND `crawled_at` <  UTC_DATE() + INTERVAL 1 DAY
```

컬럼이 벌거벗은 상태라 인덱스 범위 탐색이 가능하다. 결과 집합은 `DATE()` 비교와
완전히 동일하다 — 하한은 자정 `00:00:00` 을 포함하고, 상한은 다음날 `00:00:00` 을
제외하기 때문이다.

바꾼 위치 3곳:

| 위치 | 함수 | 대상 테이블 |
|---|---|---|
| `src/pipeline/quality.py:62` | `check_cross_site_prices` | `stg_price_history` (JOIN `stg_products`) |
| `src/pipeline/quality.py:121` | `check_layer_consistency` | `raw_crawled_prices` |
| `src/pipeline/quality.py:124` | `check_layer_consistency` | `stg_price_history` |

**손대지 않은 곳**: `src/pipeline/analytics.py:24,27` 과 `src/api/build_repo.py:224,227`.
이 둘의 `DATE(crawled_at)` 은 `SELECT` / `GROUP BY` / `PARTITION BY` 에만 쓰인다 —
행을 걸러내는 조건이 아니라 **묶는 키**라서 sargable 여부와 무관하고, 바꾸면
집계 단위가 달라진다.

## 3. 측정 결과

| 쿼리 | 이전 | 이후 | 접근 방식 | 스캔 행 수 |
|---|---:|---:|---|---|
| `raw_crawled_prices` 오늘 건수 | 378ms | **0.661ms** | index scan → **range scan** | 3,510,000행 → **3,204행** |
| `stg_price_history` 오늘 건수 | 319ms | **3.4ms** | index scan → **skip scan** | 결과 2,403행으로 동일 |

`raw` 는 약 **572배**, `stg` 는 약 **94배** 빨라졌다. `stg` 는 결과 행 수가
이전과 같음을 확인했다 — 빨라진 것이지 덜 읽어서 적게 나온 게 아니다.

## 4. 판단 — stg 에는 인덱스를 추가하지 않았다

`stg_price_history` 의 유일한 관련 인덱스는 자연키 `uq_price_history (product_id, crawled_at)` 이고,
`crawled_at` 은 두 번째 키 파트다. 그런데도 3.4ms 가 나온 이유는 MySQL 8.0 의
**skip scan** 덕분이다. 선행 컬럼 `product_id` 의 distinct 값이 적어서, 옵티마이저가
`product_id` 값을 하나씩 훑으며 각 값 안에서 `crawled_at` 범위 탐색을 반복한다.

즉 **skip scan 의 효율은 선행 컬럼의 카디널리티에 반비례한다.** 지금은 워치리스트
상품 수가 작아 충분히 빠르므로, `crawled_at` 단독 인덱스를 추가하지 않는다.
인덱스는 공짜가 아니다 — 크롤링이 4회/일 INSERT 하는 테이블이라 쓰기 비용과
저장 공간이 늘고, 지금 얻을 이득은 3.4ms 안쪽이다.

**재검토 트리거**: 상품 수(= `stg_price_history.product_id` 의 distinct 값)가 크게 늘면
skip scan 이 훑어야 할 선행 값이 그만큼 많아져 비용이 따라 오른다. 그때
`KEY (crawled_at)` 추가를 다시 판단한다.

## 5. 자정 경계 회귀 테스트

교체 전후의 결과가 같다는 것이 이 변경의 전제이므로, 경계를 테스트로 고정했다
(`tests/integration/test_quality_date_boundary.py`, 실제 MySQL 사용).

| 수집 시각 | 오늘로 집계되는가 |
|---|---|
| 어제 23:59:59 | ✗ |
| 오늘 00:00:00 | ✓ (하한 닫힘) |
| 오늘 23:59:59 | ✓ |
| 내일 00:00:00 | ✗ (상한 열림) |

변이 테스트로 실효성을 확인했다:

| 변이 | 잡히는 테스트 |
|---|---|
| `>=` → `>` (자정 배제) | `test_layer_consistency_counts_only_today_rows`, `test_cross_site_includes_midnight_boundary_rows` |
| `<` → `<=` (내일 자정 포함) | `test_layer_consistency_counts_only_today_rows`, `test_cross_site_excludes_rows_outside_today` |

건수는 절대값이 아니라 삽입 전후 **차이**로 검증한다 — 같은 DB 에 실제 크롤링
데이터가 함께 있어 오늘 건수의 절대값을 고정할 수 없다.

## 6. raw 에는 `crawled_at` 단독 인덱스를 추가한다

§3 의 `raw` 결과(range scan, 3,204행)는 **`crawled_at` 이 선행 컬럼인 인덱스가 있을 때만** 나온다.
기존 `raw_crawled_prices` 의 인덱스는 `PRIMARY (id)` 와
`uq_raw_crawled (site, category, product_name, crawled_at)` 둘뿐이고, 후자는 `crawled_at` 이
네 번째 키 파트다. MySQL 의 skip scan 은 선행 키 파트 1개까지만 건너뛰므로 이 인덱스로는
range scan 이 되지 않는다.

그래서 `mysql/ddl/schema.sql` 의 `raw_crawled_prices` 정의에 단독 인덱스를 추가했다.

```sql
KEY `idx_raw_crawled_at` (`crawled_at`)
```

§4 에서 `stg_price_history` 에는 인덱스를 추가하지 않기로 한 것과 판단이 갈리는 이유는,
`stg` 는 선행 컬럼 카디널리티가 낮아 skip scan 만으로 3.4ms 가 나오는 반면 `raw` 는
그 경로 자체가 없기 때문이다.

### 이 인덱스를 함께 쓰는 곳

| 사용처 | 조건 | 상태 |
|---|---|---|
| `src/pipeline/quality.py` | 오늘 범위 `[UTC_DATE(), +1일)` | 적용됨 (§2) |
| `src/pipeline/transform.py` | 증분 쿼리의 최근 7일 조건 | **예정 (다음 작업)** |

두 쿼리 모두 `crawled_at` 시각 범위로 자르므로 인덱스 하나를 공유한다. 쓰기 비용
(크롤링 4회/일 INSERT)을 한 번만 내고 두 경로가 함께 쓰는 구조다.

### 운영 반영

**운영 VM 에는 아직 적용하지 않았다 (2026-09-29 기준).** 이 저장소의 DDL 은
`CREATE TABLE IF NOT EXISTS` 선언형이라 이미 만들어진 테이블에는 반영되지 않는다.
기존 DB 에는 아래를 별도로 실행해야 한다.

```sql
ALTER TABLE `raw_crawled_prices` ADD KEY `idx_raw_crawled_at` (`crawled_at`);
```

같은 내용을 `schema.sql` 의 해당 위치 주석에도 남겨 두었다.

## 부록 — 이 저장소에서 확인한 것

§3 의 수치는 perf 스키마(약 351만 행)에서 잰 값이고, `EXPLAIN ANALYZE` 원문은
[20260929_quality_crawled_at_range_measurements.md](20260929_quality_crawled_at_range_measurements.md)
에 raw·stg 각각 변경 전·후 3회씩 남긴다.

현재 개발 DB 는 1,233행뿐이라 같은 수치가 재현되지 않는다(데이터가 작아 옵티마이저가
두 형태 모두 index scan 을 고른다). 이 저장소에서 확인한 것은 다음 세 가지다.

1. **결과 동등성** — 교체 전/후 쿼리의 `COUNT(*)` 가 동일 (raw·stg 모두 6건)
2. **`filtered` 개선** — `EXPLAIN` 의 `filtered` 가 `100.0` → `11.11` 로 바뀌어,
   옵티마이저가 조건을 범위로 인식하게 됐음을 확인
3. **전체 테스트 197건 통과** (기존 194 + 신규 경계 테스트 3)
