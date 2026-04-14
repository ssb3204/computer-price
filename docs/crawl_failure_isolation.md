# 크롤링 실패 격리 설계 — try-except vs GitHub Actions 별도 job

## 문제 상황

3개 사이트(다나와·컴퓨존·견적왕) 크롤링이 단일 실행 흐름으로 묶여 있어,
특정 사이트의 일시 장애가 나머지 정상 사이트의 수집까지 차단하는 구조였다.
각 사이트는 서로 완전히 독립적임에도 1개 실패 시 파이프라인 전체가 중단됐다.

---

## 검토한 두 가지 방법

### 방법 A: GitHub Actions 사이트별 별도 job (병렬 크롤링)

```yaml
jobs:
  crawl-danawa:
    runs-on: ubuntu-latest
    steps: ...

  crawl-compuzone:
    runs-on: ubuntu-latest
    steps: ...

  crawl-pc-estimate:
    runs-on: ubuntu-latest
    steps: ...

  transform-and-analytics:
    needs: [crawl-danawa, crawl-compuzone, crawl-pc-estimate]
    steps: ...
```

**장점:**
- 3개 사이트 병렬 실행 → 크롤링 시간 단축 이론상 가능

**단점:**
| 문제 | 설명 |
|------|------|
| job 간 데이터 전달 불가 | Python 객체 공유 불가 → artifact 파일 경유 필요, 구조 복잡 |
| `needs` 의존성 재발 | 1개 job 실패 → transform job 미실행 (원래 문제와 동일) |
| job 기동 오버헤드 | checkout + setup-python + pip install ≈ 40~60초 × 3개 runner |
| GitHub Actions 한도 소모 | runner 수만큼 무료 사용량 소모 |

---

### 방법 B: 단일 job 내 try-except 격리 (현재 적용)

```python
for crawler in [DanawaCrawler, CompuzoneCrawler, PCEstimateCrawler]:
    try:
        raw_prices = crawler.crawl_raw()
        all_raw.extend(raw_prices)
    except (RequestException, ValueError, ...) as e:
        crawl_failures.append({"site_name": ..., "error": str(e)})
        # 다음 사이트로 계속 진행
```

**장점:**
- Python 객체(`all_raw`, `crawl_failures`) 그대로 다음 단계에 전달
- Snowflake 커넥션 1개
- 실패 사이트와 무관하게 transform 항상 실행
- 코드 한 곳에서 전체 흐름 제어

**종료 코드 전략:**
```
3개 모두 실패 → exit 1 (GitHub Actions 실패 처리, 알림 발송)
1~2개 실패   → exit 0 + PARTIAL 상태 (불필요한 재실행 방지)
전체 성공    → exit 0 + SUCCESS 상태
```

---

## 실측 데이터 기반 결론

**PIPELINE_STEP_RUNS 34회 실행 실측값:**

| 스텝 | 평균 소요시간 | 비고 |
|------|-------------|------|
| crawl | **44.2초** | 3개 사이트 순차 합계 |
| transform | 55.9초 | |
| analytics | 3.5초 | |
| load_raw | 3.3초 | |
| detect | 2.1초 | |
| quality | 1.8초 | |
| **전체 파이프라인** | **~111초** | |

**병렬화 시 시간 계산:**

```
방법 A (별도 job):
  병렬 크롤링: 44.2초 → 이론상 ~15초 (29초 단축)
  job 기동 오버헤드: 40~60초 × 3개 runner
  결과: 29초 단축 → 오버헤드에 완전히 상쇄, 오히려 느려질 수 있음

방법 B (try-except):
  job 기동 오버헤드: 없음
  전체 파이프라인: ~111초 (변화 없음)
```

**현재 규모(3개 사이트, 크롤링 44초)에서는 방법 B가 더 효율적.**

---

## 확장성 기준

| 크롤링 규모 | 적합한 방법 |
|------------|------------|
| 사이트 수십 개, 크롤링 수 분 이상 | **방법 A** — 병렬화 이득이 job 기동 오버헤드 초과 |
| 현재 (3개 사이트, 44초) | **방법 B** — 오버헤드 대비 병렬화 이득 없음 |

---

## 적용 결과

- 1개 사이트 장애 시 나머지 2개 사이트 데이터 정상 적재
- 부분 실패(PARTIAL) → exit 0 → GitHub Actions 불필요한 재실행 방지
- 실패 사이트명·시간·에러 Slack 즉시 알림
- PIPELINE_STEP_RUNS에 SUCCESS/PARTIAL/FAILED 이력 누적 → 대시보드에서 실패 패턴 추적 가능

---

*작성일: 2026-04-14*
*데이터 기준: PIPELINE_STEP_RUNS 34회 실행 실측값 (2026-04-06 ~ 2026-04-14)*
