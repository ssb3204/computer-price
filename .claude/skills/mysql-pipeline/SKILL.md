---
name: mysql-pipeline
description: "MySQL 파이프라인(Raw→Staging→Analytics) 및 Airflow DAG 개발·수정 스킬. SQL UPSERT(ON DUPLICATE KEY UPDATE), 미처리 조인 기반 증분 처리, LAG 윈도우 함수, 변환 로직, 변경감지, 파이프라인 관찰성, DAG 수정 등 모든 파이프라인 작업 시 반드시 이 스킬을 사용."
---

# MySQL 파이프라인 스킬

Raw → Staging → Analytics 3-Layer 파이프라인과 Airflow DAG를 개발·수정하는 지침.

## 파이프라인 구조

```
src/pipeline/
├── crawl.py        — Step 1: 크롤러 실행 → RAW 적재 준비
├── load_raw.py     — Step 2: MySQL raw_crawled_prices 적재
├── transform.py    — Step 3: RAW → STAGING 변환
├── quality.py      — Step 4: 품질 체크 (손실률, 누락 탐지)
├── detect.py       — Step 5: 변경 감지 (NEW_LOW/NEW_HIGH)
├── slack.py        — Step 6: Slack 알림
└── observability.py — 파이프라인 실행 이력 기록
```

## 핵심 패턴

### 1. UPSERT 멱등성 보장

모든 MySQL 적재는 `INSERT`가 아닌 `INSERT ... ON DUPLICATE KEY UPDATE`를 사용한다. 재실행해도 중복이 생기지 않아야 한다:
- 자연키에 UNIQUE 제약이 걸려 있어야 동작한다 (상품 식별자 + 날짜 + 사이트 등, 테이블마다 다름, 수정 전 확인)
- 매칭되면 UPDATE, 없으면 INSERT — INSERT만 필요한 곳은 no-op UPDATE(`col = col`)로 흉내

### 2. 증분 처리 — 미처리 조인

Raw → Staging 변환은 "미처리 조인"으로 증분 처리한다:
```sql
-- stg_price_history/raw_transform_failures에 아직 없는 raw 행만 대상
SELECT r.* FROM raw_crawled_prices r
LEFT JOIN stg_price_history h ON h.raw_id = r.id
WHERE h.raw_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM raw_transform_failures f WHERE f.crawled_prices_id = r.id);
```
처리 실패한 레코드는 `raw_transform_failures`에 기록한다 — 성공/실패 모두 다음 실행에서 재조회되지 않는다.

### 3. 변경 감지 (detect.py)

`LAG()` 윈도우 함수로 이전 가격과 비교 (MySQL 8.0 윈도우 함수 지원):
- `ans_product_stats` 테이블의 `min_price_ever`, `max_price_ever`와 비교하여 NEW_LOW/NEW_HIGH 판정
- 변경 감지 결과는 `stg_price_alerts`에 기록

### 4. 파이프라인 관찰성

각 단계 완료 후 `observability.py`로 `PIPELINE_STEP_RUNS` 테이블에 기록:
- `record_count`: 처리된 레코드 수 (0이면 데이터 누락 의심)
- 단계별 시작/종료 시간, 상태

### 5. Airflow DAG 주의사항

- Airflow 2.8 + SQLAlchemy <2.0 제약: 의존성 변경 시 `Dockerfile` 영향 확인
- DAG 파일 위치 확인 (CLAUDE.md 기준: `src/airflow_dags/`)

## 작업 절차

1. 수정 대상 파이프라인 단계의 소스 파일 읽기
2. **스키마 변경 전 실제 MySQL 테이블/컬럼 확인** (CLAUDE.md Verification)
3. 설계 제시 → 사용자 확인 → 구현
4. UNIQUE 키 충돌 여부 확인
5. 미처리 조인 로직에 영향이 없는지 확인
6. `observability.py` 기록 로직 포함 여부 확인

## 디버깅 체크리스트

파이프라인 실패 시:
- [ ] `PIPELINE_STEP_RUNS`에서 실패 단계와 record_count 확인
- [ ] `raw_transform_failures`에서 실패 레코드와 원인 확인
- [ ] 미처리 조인 대상에서 이미 처리된 행이 재조회되는지 확인
- [ ] UNIQUE 키 컬럼의 NULL 여부 확인
