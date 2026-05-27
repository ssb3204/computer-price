---
name: qa-integration
description: "통합 테스트 및 데이터 정합성 검증 스킬. pytest 실행, 크롤러 출력 검증, 파이프라인 레이어 간 손실률 확인, 대시보드 쿼리 검증 등 테스트·검증 작업 시 반드시 이 스킬을 사용. 모듈 완성 직후 점진적으로 실행."
---

# QA 통합 검증 스킬

크롤러 → Snowflake → 대시보드 전 레이어의 통합 테스트와 데이터 정합성 검증 지침.

## 검증 원칙

**존재 확인이 아닌 경계면 교차 비교**: 파일이 있는지 확인하는 것으로는 부족하다. 크롤러 출력 shape과 DB 적재 shape을 동시에 비교하고, 파이프라인 각 단계의 레코드 수를 교차 확인한다.

**점진적 검증**: 전체 완성 후 1회 검증이 아니라, 각 모듈이 완성되는 즉시 검증을 수행한다.

## 검증 레벨

### Level 1: pytest 단위·통합 테스트

```bash
python -m pytest tests/ -v -o "addopts="
```

실패 시 반드시 원인을 파악하고 해결 후 다음 단계로 진행한다.

### Level 2: 레이어 정합성 체크

**Raw → Staging 손실률:**
```sql
-- 손실률 확인 (5% 초과 시 이상)
SELECT
    COUNT(*) AS raw_count,
    (SELECT COUNT(*) FROM STAGING.PRICE_HISTORY
     WHERE DATE(CREATED_AT) = CURRENT_DATE) AS staging_count
FROM RAW.CRAWLED_PRICES
WHERE DATE(CRAWLED_AT) = CURRENT_DATE;
```

**Analytics 누락 탐지:**
```sql
-- Staging에는 있지만 Analytics에 없는 레코드
SELECT COUNT(*) AS missing_count
FROM STAGING.LATEST_PRICES lp
LEFT JOIN ANALYTICS.PRODUCT_PRICES ap ON lp.PRODUCT_ID = ap.PRODUCT_ID
WHERE ap.PRODUCT_ID IS NULL;
```

### Level 3: 파이프라인 실행 이력 확인

```sql
SELECT STEP_NAME, STATUS, RECORD_COUNT, START_TIME, END_TIME
FROM PIPELINE_STEP_RUNS
WHERE DATE(START_TIME) = CURRENT_DATE
ORDER BY START_TIME;
```

`RECORD_COUNT = 0`인 단계가 있으면 데이터 누락 의심 — 원인 조사 필요.

### Level 4: 크롤러 출력 샘플 검증

- 광고 필터: 결과에 `adReaderProductItem*`, `adPointProductItem*` ID가 없는지 확인
- 가격 범위: `0 < price < 10,000,000` 범위 확인
- 필수 필드 없음 없는지: `product_name`, `price`, `site_name`, `crawled_at`

## 검증 체크리스트

| 항목 | 기준 | 확인 방법 |
|------|------|---------|
| pytest | 100% 통과 | `pytest tests/ -v` |
| Raw→Staging 손실률 | < 5% | Level 2 SQL |
| Analytics 누락 | 0건 | Level 2 SQL |
| 파이프라인 이력 | RECORD_COUNT > 0 | Level 3 SQL |
| 가격 범위 | 0 < price < 10M | Level 4 확인 |
| 광고 필터 | 광고 ID 없음 | Level 4 확인 |

## 작업 절차

1. 검증 대상 파일/테이블 확인
2. Level 1(pytest) → Level 2(레이어 정합성) 순서로 진행
3. 실패 항목은 담당 에이전트에게 구체적 내용과 함께 전달
4. 수정 완료 후 재검증
5. 모든 항목 통과 시 orchestrator에게 완료 보고

## 보고서 형식

`_workspace/{phase}_qa_report.md`에 다음 형식으로 기록:

```markdown
## QA 검증 결과 — {날짜}

### pytest
- 결과: PASS / FAIL ({통과}/{전체})
- 실패 테스트: {목록}

### 레이어 정합성
- Raw 레코드 수: {N}
- Staging 적재: {N} (손실률 {%})
- Analytics 누락: {N}건

### 이상 항목
- {있으면 기록, 없으면 "없음"}

### 판정: PASS / FAIL
```
