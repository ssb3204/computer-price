# transform.py — 증분 조회에 7일 창 + 전체 점검 모드

- **상태**: 적용 (2026-09-29)
- **측정 원자료**: [20260929_transform_incremental_window_measurements.md](20260929_transform_incremental_window_measurements.md) — perf 스키마에서 잰 `EXPLAIN ANALYZE` 원문 (세트 3개 × 3회)
- **대상 파일**: `src/pipeline/transform.py`, `run_pipeline.py`, `mysql/ddl/schema.sql` (인덱스 1개)
- **관련 테스트**: `tests/integration/test_transform_incremental_window.py`
- **관련 문서**: [20260929_quality_crawled_at_range.md](20260929_quality_crawled_at_range.md)
  — 여기서 추가한 `idx_raw_crawled_at` 을 이 쿼리가 함께 쓴다

---

## 1. 문제 — 미처리 조인은 시간이 지나도 좁아지지 않는다

증분 처리는 "처리 흔적이 없는 raw 행"을 매번 다시 찾는 구조다.

```sql
FROM `raw_crawled_prices` r
LEFT JOIN `stg_price_history` h ON h.`raw_id` = r.`id`
WHERE h.`raw_id` IS NULL
  AND NOT EXISTS (SELECT 1 FROM `raw_transform_failures` f WHERE f.`crawled_prices_id` = r.`id`)
```

처리된 행은 매번 걸러지지만, **걸러내려면 일단 전부 훑어야 한다.** 조건에 시각 하한이
없어 `raw_crawled_prices` 전체가 대상이고, 이 테이블은 append-only 라 하루 4회씩
영구히 자란다. 즉 증분 처리인데 스캔 범위는 증분이 아니었다.

여기에 더해, 어떤 이유로든 처리되지 않고 남은 행(예: `product_id` 매핑 실패 경로 —
`transform.py` 는 로그만 남기고 실패 테이블에 기록하지 않는다)은 **영구히 미처리로
남아 매 실행 재조회된다.** 시간이 지날수록 이 찌꺼기도 쌓인다.

## 2. 변경 — 최근 7일 창

```sql
AND r.`crawled_at` >= UTC_TIMESTAMP() - INTERVAL 7 DAY
```

`src/pipeline/transform.py` 상단 상수로 뺐다.

```python
INCREMENTAL_WINDOW_DAYS = 7
```

`stg_price_history` anti-join 과 `raw_transform_failures` NOT EXISTS 는 그대로 둔다 —
창은 **스캔 범위**만 줄이고, 무엇이 미처리인지에 대한 정의는 건드리지 않는다.

## 3. 왜 7일인가

두 방향의 비용이 맞서는 값이다.

| 창을 늘리면 | 창을 줄이면 |
|---|---|
| 매 실행 스캔 범위가 커진다 (원래 문제로 되돌아감) | 수집이 멈춘 기간을 넘기면 그 사이 행을 놓친다 |

7일로 정한 근거:

1. **수집이 며칠 멈춰도 버틴다.** 크롤링은 VM cron 4회/일이고, 그동안 사이트 차단
   (견적왕 Oracle 대역 403)·VM 재시작 같은 이유로 일부 사이트가 며칠 단위로 멈춘 전례가 있다.
   하루이틀짜리 창은 이런 정지 구간을 그대로 누락으로 만든다.
2. **주 단위 점검과 맞물린다.** 전체 점검을 주 1회 돌리면, 창 밖으로 밀려난 행도
   최대 1주 안에 회수된다 (§4).
3. **조정 가능한 상수다.** 운영하며 정지 구간이 더 길어지면 상수만 올리면 된다.
   값이 코드 한 곳에 있고 쿼리 문자열에 흩어져 있지 않다.

7일이 "안전한 상한"이라는 뜻은 아니다. **창은 누락을 줄이는 장치가 아니라 스캔을
줄이는 장치이고, 누락을 막는 것은 §4 의 전체 점검이다.**

## 4. 누락 보완 — 전체 점검 모드

```bash
python run_pipeline.py --full
```

`--full` 은 시각 하한 없이 미처리 행 전체를 훑는다(= 변경 전 동작). Staging 변환의
조회 범위만 달라지고 나머지 단계는 같다.

이 구조가 성립하는 전제는 **창 밖 행이 "실패"로 기록되지 않는다**는 점이다. 창에 걸려
건너뛴 행은 `raw_transform_failures` 에 남지 않으므로 여전히 미처리 상태이고,
전체 점검이 오면 그대로 대상이 된다. 만약 건너뛴 행을 실패로 기록했다면 NOT EXISTS 에
걸려 전체 점검으로도 회수할 수 없게 된다 — 이 점을 테스트로 고정해 두었다(§6).

### 주 1회 전체 점검 cron (예시 — 아직 등록하지 않았다)

기존 4회/일 크롤링(UTC 01/06/15/20시)과 겹치지 않는 시각으로 둔다.

```
0 3 * * 1 cd /home/ubuntu/price-pipeline && .venv/bin/python run_pipeline.py --full >> /home/ubuntu/crawl.log 2>&1
```

매주 월요일 03:00 UTC(= 12:00 KST). `cd` 를 빼면 `.env` 를 못 찾는다 — README 의
크롤링 스케줄 절과 같은 이유다. **운영 VM 의 crontab 은 아직 건드리지 않았다.**

전체 점검 1회의 Staging 조회 비용은 약 **10초**다(§5 세트 ②). 주 1회라면 감당할 수
있는 값이고, 이것이 창 밖으로 밀려난 행을 회수하기 위해 지불하는 대가다.

## 5. 측정 결과 — perf 스키마 (`computer_price_perf`)

`EXPLAIN ANALYZE` 원문은
[20260929_transform_incremental_window_measurements.md](20260929_transform_incremental_window_measurements.md)
에 세트당 3회씩 남겼다. 아래는 그 중앙값이다.

측정 환경: `perf-mysql` 컨테이너(운영 VM 사양 모사 2 CPU / 1GB, buffer pool 128MB),
MySQL 8.0.46, `raw_crawled_prices` 약 351만 행 / `stg_price_history` 약 350만 행 /
`raw_transform_failures` 6,984행.

perf 데이터가 2026-09-22 까지만 있어 `UTC_TIMESTAMP()` 대신 **`2026-09-23 00:00:00` 을
"지금"으로 고정**해 측정했다(창 하한 `2026-09-16 00:00:00`). 조건의 형태는 코드와 같다.

| 세트 | 중앙값 | `r` 접근 | `h` 조인 | 결과 |
|---|---:|---|---|---:|
| ① 일반 (7일 창) | **58.3ms** | index range scan, 22,428행 | 인덱스 lookup × 22,396 | 801 |
| ② 전체 점검 (`--full`) | **10,039ms** | table scan, 351만 행 | 인덱스 lookup × 350만 | 801 |
| ③ 일반, `raw_id` 인덱스 숨김 | **844ms** | index range scan, 22,428행 | table scan 350만 → hash join | 801 |

③은 운영 스키마와 같은 조건(당시 `uq_price_history_raw_id` 없음)을 재현하려고 인덱스를
삭제하지 않고 `INVISIBLE` 로만 숨겨 측정한 뒤 되돌린 것이다.

**세 세트 모두 결과 801건으로 같다.** 창은 스캔 범위만 줄였고 "무엇을 미처리로 볼지"는
바꾸지 않았다는 확인이다.

### 읽는 법 — 두 인덱스는 한 쌍이다

| | 창 있음 | 창 없음 |
|---|---:|---:|
| `raw_id` 인덱스 있음 | ① **58.3ms** | ② 10,039ms |
| `raw_id` 인덱스 없음 | ③ 844ms | (미측정, ②보다 나쁨) |

- **창의 효과(①↔②)**: 10,039ms → 58.3ms, 약 **172배**. ②가 곧 변경 전 동작이다.
- **`raw_id` 인덱스의 효과(①↔③)**: 844ms → 58.3ms, 약 **14배**.
- 한쪽만으로는 부족하다. 창만 넣고 인덱스가 없으면 844ms 에서 멈추고(③), 인덱스만 있고
  창이 없으면 10초다(②). 둘 다 있어야 58.3ms 가 된다.
- **전체 점검의 비용은 약 10초다.** 주 1회 실행이므로 감당할 수 있는 값이고, 이것이
  §4 의 누락 회수를 위해 지불하는 대가다.

7일 창 안에 raw 22,428행이 있고 그중 미처리가 801건이다. 즉 일반 실행이 실제로 건드리는
범위는 전체 351만 행의 0.6% 수준이다.

## 6. 실행 계획에서 확인한 것

### 6-1. `crawled_at` 조건은 range scan 이 된다

```
-> Index range scan on r using idx_raw_crawled_at
   over ('2026-09-16 00:00:00' <= crawled_at)  (actual time=9.59..22.5 rows=22428 loops=1)
```

`idx_raw_crawled_at` 이 있어야 이렇게 잡힌다. 없으면 `r` 이 전체 스캔이 되어 창을 넣어도
스캔 범위가 줄지 않는다 — **창의 효과가 이 인덱스에 걸려 있다.** 인덱스는
`mysql/ddl/schema.sql` 에 있지만 운영 DB 에는 `ALTER` 가 따로 필요하다
([20260929_quality_crawled_at_range.md](20260929_quality_crawled_at_range.md) §6).

전체 점검(②)에서는 같은 자리가 `Table scan on r ... rows=3.51e+6` 이 된다. 창이 없으니
당연한 결과이고, 이것이 ②가 10초 걸리는 주된 이유다.

### 6-2. `raw_transform_failures` NOT EXISTS — 인덱스 없이도 1회 materialize 로 풀린다

`raw_transform_failures.crawled_prices_id` 에는 인덱스가 없다(PK `id` 뿐). 행마다 이
테이블을 다시 뒤지면 O(N×M) 이 되지만, MySQL 8.0 은 그렇게 하지 않았다.

```
-> Single-row index lookup on <subquery2> using <auto_distinct_key> (crawled_prices_id=r.id)
   (actual time=314e-6..314e-6 rows=0.00143 loops=22428)
    -> Materialize with deduplication  (actual time=1.61..1.61 rows=6984 loops=1)
        -> Table scan on f  (actual time=0.0423..0.618 rows=6984 loops=1)
```

읽는 법:

- `Materialize with deduplication` 의 **`loops=1`** — `raw_transform_failures` 6,984행을
  **한 번만** 훑어 임시 테이블로 만든다. `f` 의 `Table scan` 은 행당 스캔이 아니라
  이 materialize 를 위한 1회 스캔이다.
- 바깥은 `<auto_distinct_key>` 로 행마다 probe 한다(`loops=22428`). probe 1회가
  0.3µs 수준이라 전체 기여가 미미하다.
- 인덱스가 없는 것을 옵티마이저가 materialize 로 보정한 형태다.
  **지금 규모(실패 6,984행)에서는 `crawled_prices_id` 인덱스가 필요 없다.**

재검토 트리거: 실패 행이 계속 쌓여 임시 테이블이 `tmp_table_size` 를 넘기면 디스크로
내려가 이 비용이 갑자기 커진다. 전체 점검(②)에서는 probe 가 351만 회라 그때 영향이 더 크다.

### 6-3 참고 — 옵티마이저 추정치는 크게 빗나가 있다

①의 최상위 노드는 `rows=311e+6` 으로 추정하는데 실제는 22,396행이다(`cost` 도 그에 비례해
부풀어 있다). 계획 자체는 적절하게 잡혔으므로 문제는 아니지만, **원자료의 `cost`/추정 `rows`
를 근거로 읽지 말 것** — 판단은 `actual time` 과 `loops` 로 한다.

### 6-4. `h` 의 anti-join — `raw_id` 인덱스를 스키마에 넣기로 했다

증분 조회의 나머지 한 축은 `stg_price_history` anti-join(`h.raw_id = r.id`)이다.
`raw_id` 에 인덱스가 없으면 미처리 행을 찾을 때마다 이 테이블 **전체**를 해시 조인해야
한다. perf 스키마 실측:

| `stg_price_history.raw_id` 인덱스 | 증분 쿼리 | 원자료 |
|---|---:|---|
| 없음 (전체 해시 조인) | 844ms | 세트 ③ |
| `uq_price_history_raw_id (raw_id)` | **58.3ms** | 세트 ① |

약 14배다. 창(`crawled_at`)이 `r` 쪽 스캔을 줄여도 `h` 쪽이 전체 조인으로 남으면
효과가 그만큼 깎인다 — 두 인덱스는 한 쌍으로 봐야 한다.

그래서 `mysql/ddl/schema.sql` 의 `stg_price_history` 에 추가했다.

```sql
UNIQUE KEY `uq_price_history_raw_id` (`raw_id`)
```

**왜 일반 KEY 가 아니라 UNIQUE 인가**: 조회 성능은 같지만, UNIQUE 면 같은 raw 행이 두 번
적재되는 것을 DB 가 막아 준다. 지금은 anti-join 이 그 역할을 하고 있어 동시 실행이
겹치면 뚫릴 수 있는데, 제약으로 두면 경합과 무관하게 보장된다.
`raw_id` 는 NULL 허용이고 UNIQUE 는 NULL 을 중복으로 보지 않으므로, 추적값이 없는
과거 행이 있어도 제약에 걸리지 않는다.

`transform.py` 의 적재는 `INSERT ... ON DUPLICATE KEY UPDATE id = id` 라, UNIQUE 키가
둘(`uq_price_history`, `uq_price_history_raw_id`)이 되어도 어느 쪽에 걸리든 동작은 no-op 로
같다. 적재 코드는 고치지 않았다.

### 운영 반영 — 중복 확인이 먼저다

**운영 VM 에는 아직 적용하지 않았다 (2026-09-29 기준).** `idx_raw_crawled_at` 과 달리
이쪽은 **제약**이라, 기존 데이터에 중복이 하나라도 있으면 `ALTER` 가 `ERROR 1062` 로
실패한다. 반드시 먼저 확인한다.

```sql
-- 1) 중복 확인 — 0건이어야 한다
SELECT `raw_id`, COUNT(*) AS cnt
FROM `stg_price_history`
WHERE `raw_id` IS NOT NULL
GROUP BY `raw_id`
HAVING COUNT(*) > 1;

-- 2) 0건일 때만 적용
ALTER TABLE `stg_price_history` ADD UNIQUE KEY `uq_price_history_raw_id` (`raw_id`);
```

중복이 나오면 인덱스를 먼저 넣지 말고 그 행들이 어떻게 생겼는지부터 본다 — 적재 경로에
문제가 있었다는 뜻이고, UNIQUE 로 덮으면 원인이 가려진다.

참고로 현재 개발 DB(1,233행)에는 중복 0건, `raw_id` NULL 0건이다. 운영 VM 은 데이터가
다르므로 그대로 믿지 말고 위 쿼리로 확인한다.

같은 내용을 `schema.sql` 의 해당 위치 주석에도 남겨 두었다.

## 7. 테스트

`tests/integration/test_transform_incremental_window.py` (실제 MySQL)

| 테스트 | 확인 |
|---|---|
| `test_row_outside_window_skipped_in_normal_run` | 10일 전 미처리 행은 일반 실행에서 변환되지 않고, **실패로도 기록되지 않는다** |
| `test_row_outside_window_processed_in_full_scan` | 같은 행이 `full_scan=True` 에서 변환된다 |
| `test_row_inside_window_processed_in_normal_run` | 2일 전 미처리 행은 일반 실행에서 변환된다 |

### 기존 테스트 영향

통합 테스트들이 `crawled_at=2000-01-01` 로 raw 를 넣고 있었다. 창이 생기면서 이 행들이
전부 대상에서 빠져 변환 0건이 되므로, 공용 시각 상수를 `tests/integration/conftest.py` 로
옮기고 창 안쪽(2일 전 / 1일 전 정오)으로 바꿨다. 날짜가 서로 달라야 하는 곳(일별 집계·LAG)이
있어 두 상수의 날짜를 하루 벌려 두었다.

전체 200건 통과 (기존 197 + 신규 3).
