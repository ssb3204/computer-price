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

---

## 대시보드

### Dash/Plotly 선택
- **이유**: Python 기반으로 Snowflake 연결 코드 재사용. React 빌드 과정 불필요
- **배포**: 로컬 Docker (localhost:8050). 외부 공개 불필요

### 알림 분리 (웹 vs Slack)
- **웹 대시보드 알림**: 가격 변동 이력 조회 (1% 이상 상승/하락, NEW_LOW/NEW_HIGH)
- **Slack 알림**: 크롤링 실패만 (운영 이슈 즉시 인지 목적)
- **이유**: 가격 변동은 참고용이므로 대시보드에서 확인. 크롤링 실패는 즉시 대응 필요
