# 기술 선택 및 변경 이유

## 아키텍처 전환

### Kafka/Consumer → Snowflake 배치 (PR #4)
- **이전 방식**: Kafka 브로커 + Consumer 서비스로 실시간 스트리밍
- **변경 이유**: 가격 데이터는 분/초 단위 실시간성이 불필요. 일 4회 배치로 충분하며 Kafka 운영 복잡도가 과도함
- **결과**: Docker 서비스 수 감소, 운영 단순화

### Airflow → GitHub Actions (2026-03-27)
- **이전 방식**: 로컬 Docker Compose에서 Airflow DAG로 매일 21:00/22:00 KST 실행
- **변경 이유**: PC가 항상 켜있지 않으면 스케줄 크롤링 누락 발생. 무료 CI/CD 인프라 활용
- **결과**: `.github/workflows/crawl.yml` + `run_pipeline.py`로 대체. 00:00/05:00/10:00/15:00 KST 하루 4회 실행
- **트레이드오프**: Airflow의 재시도/의존성 관리 기능 포기 → GitHub Actions timeout 30분 + exit code로 단순 처리

---

## 데이터 설계

### Snowflake 3-Layer 스키마 (Raw → Staging → Analytics)
- **Raw**: 원본 텍스트 그대로 보존 (가격도 문자열). 멱등성 보장을 위해 MERGE 사용
- **Staging**: 파싱/정규화된 데이터. DIM_SITES, DIM_CATEGORIES로 정규화. STG_LATEST_PRICES로 최신가격 빠른 조회
- **Analytics**: 집계 전용. PRODUCT_STATS에 ALL_TIME_LOW/HIGH 보관 → 변경감지에 활용
- **이유**: 각 레이어를 독립적으로 재처리 가능. Raw만 있으면 Staging 재생성 가능

### 변경 감지 방식 — LAG() 윈도우 함수
- **방식**: `LAG(PRICE) OVER (PARTITION BY PRODUCT_ID ORDER BY CRAWLED_AT)`로 직전 가격과 비교
- **이유**: Python 루프보다 Snowflake 내 SQL 처리가 효율적. 단일 INSERT INTO SELECT로 처리
- **기준**: 1% 미만 변동 무시, PRICE_DROP(-5% 이하), PRICE_SPIKE(+10% 이상), NEW_LOW/NEW_HIGH

### stock_status 컬럼 제거 (2026-03-27)
- **이유**: 크롤러에서 재고 상태를 수집하지 않아 항상 NULL. 미사용 컬럼 유지는 혼란 초래
- **처리**: Snowflake ALTER TABLE DROP COLUMN + 코드 전체 제거

---

## 리팩토링

### Phase 0–1: Airflow 제거 + Dead Code 정리 + app.py 모듈 분리 (2026-03-27)
- **Airflow 제거**: `src/airflow_dags/`, `scripts/snowflake_pipeline.py` 삭제. `docker-compose.yml`을 Dashboard 단일 서비스로 단순화
- **Dead Code 정리**: 미사용 모델 `Product`, `PriceChange` 제거. `PostgresSettings` 제거 (Airflow 전용이었음). `stock_status` 잔재 정리
- **app.py 분리**: 630줄 모놀리식 → `app.py`(55줄) + `callbacks.py`(270줄) + `helpers.py`(114줄) + 레이아웃 6개로 분리

### Phase 2: BaseCrawler 인터페이스 재설계 (2026-03-28)
- **이전 방식**: `BaseCrawler`에 `get_target_urls()`, `parse_page()` 추상 메서드 + `crawl()` → `RawPrice` 구체 메서드. 각 크롤러에 `crawl_raw()` → `RawCrawledPrice`가 별도 존재
- **문제**: 프로덕션(`run_pipeline.py`)은 `crawl_raw()`만 사용. `crawl()` 경로 전체(`RawPrice` 모델 포함)가 dead code. 3개 크롤러 모두 `crawl()`을 override하면서 추상 메서드 `parse_page()`, `get_target_urls()`는 "Not used" stub으로만 존재
- **변경**: `crawl_raw()`를 `BaseCrawler`의 유일한 추상 메서드로 승격. `crawl()`, `parse_page()`, `get_target_urls()` 및 `RawPrice` 전용 헬퍼 전부 제거. `RawPrice` 모델 자체도 삭제
- **유지**: `_fetch_with_retry()`, `_rate_limit()` (공용 인프라), `site_name` (추상 프로퍼티)
- **결과**: 총 -334줄 (danawa 314→152, compuzone 179→107, pc_estimate 167→100, base 81→58)
- **테스트**: `crawl()` 테스트를 `crawl_raw()` 테스트로 전환, `RawPrice` 테스트를 `RawCrawledPrice`로 전환. 19개 전체 통과

---

## 크롤링

### BeautifulSoup 선택 (Selenium 미사용)
- **이유**: 3개 사이트 모두 서버사이드 렌더링 또는 AJAX JSON 응답. JS 렌더링 불필요
- **컴퓨존/견적왕**: AJAX POST 요청으로 JSON 직접 수신 (EUC-KR 인코딩 처리 필요)
- **다나와**: HTML 파싱, `productItem*` 클래스만 선택 (광고 상품 `adReaderProductItem*` 제외)

### 크롤링 실패 처리
- **방식**: 사이트별 개별 try-except. 1개 사이트 실패해도 나머지 계속 진행
- **알림**: 실패 시 Slack Webhook으로 사이트명/시간/에러 전송
- **종료 코드**: 3개 사이트 전부 실패 시 exit 1, 일부 실패는 exit 0 (GitHub Actions 재실행 방지)
- **설계 선택: try-except vs GitHub Actions 별도 job**
  - 검토한 대안: 사이트별 GitHub Actions job 분리 (병렬 크롤링)
  - 기각 이유:
    1. job 간 Python 객체 공유 불가 → artifact 파일 경유 필요, 구조 복잡
    2. job 기동 오버헤드(checkout + setup-python + pip install) 약 40~60초 × 3개 job → 병렬화 이득 상쇄
    3. `needs` 의존성 설정 시 1개 job 실패 → transform job 미실행 (원래 문제 재발)
    4. GitHub Actions 무료 한도를 job 수만큼 소모
  - 실측 근거 (PIPELINE_STEP_RUNS 34회 기준):
    - crawl 평균 44.2초 (3개 사이트 순차 합계)
    - 병렬화 시 이론상 약 15초로 단축 가능하나, job 기동 오버헤드 40~60초에 상쇄됨
    - 크롤링 대상이 수십 개 사이트로 늘어 crawl 시간이 수 분 이상이 되는 규모라면 job 분리가 유리
    - 현재 3개 사이트 44초 수준에서는 단일 job + try-except가 더 효율적

---

## 대시보드

### Dash/Plotly 선택
- **이유**: Python 기반으로 Snowflake 연결 코드 재사용. React 빌드 과정 불필요
- **배포**: 로컬 Docker (localhost:8050). 외부 공개 불필요

### 알림 분리 (웹 vs Slack)
- **웹 대시보드 알림**: 가격 변동 이력 조회 (1% 이상 상승/하락, NEW_LOW/NEW_HIGH)
- **Slack 알림**: 크롤링 실패만 (운영 이슈 즉시 인지 목적)
- **이유**: 가격 변동은 참고용이므로 대시보드에서 확인. 크롤링 실패는 즉시 대응 필요

---

## Phase 7 — 파이프라인 옵저버빌리티 대시보드 + DQ 강화 (2026-04-12)

### 파이프라인 모니터링 대시보드 (`/pipeline` 신규)
- **문제**: `PIPELINE_RUNS` / `PIPELINE_STEP_RUNS` 테이블에 실행 이력이 쌓이지만 확인하려면 Snowflake 콘솔에서 직접 쿼리해야 함
- **변경**: Dash 대시보드에 `/pipeline` 페이지 추가
  - 요약 카드: 총 실행 횟수 / 성공률 / 마지막 실행 상태 + 시간
  - 실행 시간 추이 라인 차트
  - 스텝별 평균 소요시간 + 성공/실패 바 차트
  - 실행 이력 테이블 (행 클릭 시 해당 실행의 스텝 상세 드릴다운)
- **트레이드오프**: 대시보드 5분 자동 갱신 주기 — 실시간 모니터링이 아닌 운영 현황 파악 용도로 충분

### B4: 레이어 정합성 체크 추가
- **문제**: Raw→Staging 변환 중 손실이 발생해도 파이프라인이 조용히 통과. Analytics/LATEST_PRICES 누락 상품도 탐지 불가
- **변경**: `check_layer_consistency()` 함수 추가 — quality 스텝에서 3가지 체크 수행
  1. Raw vs Staging 건수 비교 → 손실률 > 10% 시 Slack 알림
  2. `STAGING.PRODUCTS` vs `ANALYTICS.PRODUCT_STATS` 누락 상품 수
  3. `STAGING.PRODUCTS` vs `STAGING.LATEST_PRICES` 누락 상품 수
- **설계 결정**: 이슈 발견 시 파이프라인 중단 대신 Slack WARNING + 계속 진행
  - 이유: 정합성 이슈가 있어도 나머지 데이터는 정상 처리 가능. 중단하면 모든 데이터를 못 씀
- **결과**: 실제 검증 — Raw 64건, Staging 62건, 손실률 3.1% (임계값 10% 이내 정상). 단위 테스트 17개 추가
- **손실 원인 분석**: `IS_PROCESSED=FALSE` 2건 조회 결과, `WesternDigital WD BLACK SN850X M.2 NVMe`가 다나와에서 RAM 카테고리로 잘못 분류되어 `validate_price(2,333,000, "RAM")` 실패 (RAM 상한 1,000,000원). 파이프라인이 의도대로 작동한 것으로 확인

### 대시보드 가격 추이 페이지 개선
- **상품 클릭 → 차트 연동 문제**: `dbc.Button` + 패턴매칭 콜백(`ALL`)으로 구현했으나 동적 테이블에서 `triggered_value`가 항상 None. 204 응답 반복
  - **원인**: Dash 패턴매칭은 정적 렌더링에 최적화. 동적으로 생성된 컴포넌트에서 신뢰성 낮음
  - **해결**: `dash_table.DataTable` + `active_cell` 콜백으로 교체. Dash 공식 셀 클릭 처리 방식
- **순환 의존성 해결**: `update_today_comparison`에 `Input("trend-search-input")`이 있어 클릭→검색→테이블 갱신→클릭 루프 발생. search Input 제거, 테이블은 카테고리 필터만 반응하도록 분리
- **차트 호버 개선**: 날짜·가격·사이트 외에 상품명 추가 (`hovertemplate` f-string)
- **비교 테이블 정리**: 변동2/3/4 컬럼 제거 (변동1만 유지)

---

## 개선 필요 사항

### IS_PROCESSED=FALSE 레코드 영구 누적 문제

- **문제**: `parse_korean_price()` 또는 `validate_price()` 실패 레코드가 `RAW.CRAWLED_PRICES`에 `IS_PROCESSED=FALSE`로 영구 잔류. 이후 모든 파이프라인 실행마다 `WHERE IS_PROCESSED=FALSE` 조건으로 반복 조회되지만 결과는 동일하게 실패
- **원인 구조**: MERGE 키가 `(SITE, CATEGORY, PRODUCT_NAME, CRAWLED_AT)` — 크롤링 A에서 실패한 레코드와 크롤링 B에서 성공한 레코드는 `CRAWLED_AT` 차이로 **별개 행**으로 존재. 성공 레코드(B)가 `IS_PROCESSED=TRUE`가 되어도, 실패 레코드(A)는 영구 `FALSE` 유지
- **실제 사례**: `WesternDigital WD BLACK SN850X M.2 NVMe`가 다나와에서 RAM 카테고리로 잘못 분류 → `validate_price(2,333,000, "RAM")` 실패 (RAM 상한 1,000,000원). 해당 레코드는 이후 크롤링과 무관하게 매 실행마다 무의미하게 재조회됨
- **개선 방향**:
  1. `RAW.CRAWLED_PRICES`에 `IS_INVALID BOOLEAN DEFAULT FALSE` 컬럼 추가
  2. `transform_staging()`에서 파싱/검증 실패 시 `IS_INVALID=TRUE`로 마킹
  3. transform 쿼리 조건을 `WHERE IS_PROCESSED=FALSE AND (IS_INVALID IS NULL OR IS_INVALID=FALSE)`로 변경
  4. 기존 누적된 실패 레코드는 `UPDATE SET IS_INVALID=TRUE WHERE IS_PROCESSED=FALSE AND CRAWLED_AT < CURRENT_DATE()`로 일괄 처리 가능
- **트레이드오프**: `IS_INVALID=TRUE` 레코드는 데이터 감사(audit) 목적으로 삭제하지 않고 보존. "처리 실패"(`FALSE`)와 "처리 불가"(`IS_INVALID=TRUE`)를 명시적으로 구분

---

## Phase 6 — 대시보드 개선 (2026-03-31)

### 카테고리 요약 페이지 삭제
- **이전 방식**: `/categories` 라우트에 별도 페이지 존재
- **변경 이유**: 대시보드 개요(overview) 페이지에 이미 카테고리별 요약 테이블이 포함되어 있어 완전한 중복
- **결과**: `categories.py` 레이아웃 파일 + 라우트/NavLink/콜백 전부 제거 (-42줄)

### 알림 페이지 날짜별 구분
- **이전 방식**: 알림 카드가 시간순 나열만 됨
- **변경 방식**: 날짜가 바뀔 때마다 "오늘"/"어제"/날짜 섹션 헤더 삽입, 시간은 HH:MM만 표시
- **이유**: 알림이 여러 날 쌓이면 언제 발생한 알림인지 맥락 파악이 어려움

### STG_PRODUCTS URL 업데이트 버그 수정
- **버그**: MERGE `WHEN MATCHED THEN UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP()` — URL 미갱신
- **증상**: 삼성전자 990 PRO 알림에서 1.05M원 가격 변동이지만 URL 클릭 시 384K 제품으로 이동
- **원인**: STG_PRODUCTS.URL이 최초 등록값으로 고정되어, 이후 크롤링에서 다른 URL이 수집돼도 반영 안 됨
- **수정**: `WHEN MATCHED THEN UPDATE SET URL = CASE WHEN s.NEW_URL != '' THEN s.NEW_URL ELSE t.URL END`

### 알림 이상치 필터 추가
- **문제**: 카테고리 랭킹 크롤링에서 같은 이름의 다른 용량 제품(예: SSD 2TB vs 500GB)이 같은 상품으로 매핑되면 단일 크롤 사이클에서 비정상적으로 큰 가격 변동 알림 발생
- **해결**: `detect_changes`에 `ABS(change_pct) <= 70` 조건 추가 — 단일 변동 70% 초과는 데이터 이상치로 간주하여 알림 미생성
- **근거**: 일반적인 가격 할인/인상은 50% 이내. 70%+ 단일 변동은 크롤링 데이터 품질 문제일 가능성이 높음
