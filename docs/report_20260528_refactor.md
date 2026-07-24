# 코드 개선 작업 보고서

**작성일:** 2026-05-28  
**대상 브랜치:** master  
**변경 파일:** 4개  
**변경 규모:** +228 / -365 라인 (순감 137 라인)

---

## 목차

1. [작업 배경](#1-작업-배경)
2. [변경 사항 1 — Private 함수 컨벤션 위반 수정](#2-변경-사항-1--private-함수-컨벤션-위반-수정)
3. [변경 사항 2 — Snowflake 타임존 버그 수정](#3-변경-사항-2--snowflake-타임존-버그-수정)
4. [변경 사항 3 — Watchlist 콜백 중복 코드 제거](#4-변경-사항-3--watchlist-콜백-중복-코드-제거)
5. [영향 범위 및 회귀 위험 분석](#5-영향-범위-및-회귀-위험-분석)
6. [검증 결과](#6-검증-결과)

---

## 1. 작업 배경

기존 코드 리뷰 과정에서 세 가지 문제가 식별되었다.

| 분류 | 문제 | 위험도 |
|------|------|--------|
| 버그 | Snowflake `CURRENT_DATE()` 타임존 불일치 | **HIGH** — 자정~오전 7시(UTC) 구간에 오늘 데이터 조회 실패 |
| 코드 품질 | `_send_slack_message` private 함수를 외부 모듈에서 직접 import | MEDIUM — 컨벤션 위반, 리팩토링 시 무통보 파손 가능성 |
| 유지보수성 | Watchlist 콜백 3사이트 × 4개 = 12개 완전 중복 | MEDIUM — 버그 수정 또는 사이트 추가 시 12곳 동시 수정 필요 |

---

## 2. 변경 사항 1 — Private 함수 컨벤션 위반 수정

### 2-1. 문제 발견

`src/pipeline/quality.py` 9번 라인에서 `slack.py`의 내부 헬퍼 함수를 직접 import하고 있었다.

```python
# src/pipeline/quality.py — 변경 전
from src.pipeline.slack import _send_slack_message  # ← 문제
```

### 2-2. 근본 원인

Python에서 `_` 접두사는 **모듈 내부 전용(module-private)** 을 의미하는 관례다.  
`_send_slack_message`는 최초 작성 시 `send_slack_failures` 내부에서만 쓰이는 헬퍼로 설계됐으나,  
이후 `quality.py`에서 이상 감지 알림을 직접 보내야 하는 기능이 추가되면서  
관례를 어기고 외부에서 import하는 코드가 생겼다.

**이 상태가 지속될 경우 발생하는 위험:**

- `slack.py` 내부를 리팩토링할 때 `_send_slack_message`를 이름 변경·삭제해도  
  IDE와 linter가 "외부 사용 중" 경고를 주지 않음  
  (`_` prefix는 외부 공개를 의도하지 않는다는 계약이므로)
- 추후 새 개발자가 `slack.py`의 공개 API(`send_slack_failures`)만 파악하고  
  `_send_slack_message`를 인지하지 못한 채 삭제할 경우 런타임 오류 발생

### 2-3. 변경 내용

**`src/pipeline/slack.py`**

```python
# 변경 전
def _send_slack_message(text: str) -> None:
    """Slack 메시지 전송 내부 헬퍼."""
    ...

def send_slack_failures(crawl_failures: list[dict]) -> int:
    ...
    _send_slack_message("\n".join(lines))  # 내부 호출

# 변경 후
def send_slack_message(text: str) -> None:   # ← _ 제거, 공개 API로 승격
    """Slack 메시지 전송."""
    ...

def send_slack_failures(crawl_failures: list[dict]) -> int:
    ...
    send_slack_message("\n".join(lines))     # 내부 호출도 일치
```

**`src/pipeline/quality.py`**

```python
# 변경 전
from src.pipeline.slack import _send_slack_message

# 변경 후
from src.pipeline.slack import send_slack_message
```

`quality.py` 내부 호출 2곳(`check_cross_site_prices`, `check_layer_consistency`) 동일하게 수정.

### 2-4. 설계 결정 근거

`send_slack_message`는 이제 두 모듈(`quality.py`, `slack.py`)에서 사용하므로  
**공개 API로 승격하는 것이 올바른 설계다.**  
`_sanitize_for_slack`은 여전히 `slack.py` 내부에서만 사용되므로 private 유지.

---

## 3. 변경 사항 2 — Snowflake 타임존 버그 수정

### 3-1. 문제 발견

아래 두 파일의 SQL 쿼리에서 `CURRENT_DATE()`를 날짜 필터로 사용하고 있었다.

```
src/pipeline/quality.py                         check_cross_site_prices (1곳)
                                                check_layer_consistency (2곳)
src/dashboard/data_access/snowflake_queries.py  get_today_crawl_comparison (1곳)
```

### 3-2. 근본 원인

**Snowflake 계정 기본 타임존이 UTC-7 (PDT/MST)** 이다.  
따라서 `CURRENT_DATE()`는 **서버 현지 시각 기준** 날짜를 반환한다.

반면, 크롤러는 `CRAWLED_AT`을 UTC 기준으로 저장한다.

```python
# src/pipeline/crawl.py
crawled_at = datetime.now(timezone.utc)  # ← UTC로 저장
```

**두 기준이 다를 때 발생하는 불일치:**

```
파이프라인 실행 시각: KST 10:00 = UTC 01:00
  CRAWLED_AT 저장값: 2026-05-28 01:00 UTC → ::DATE 변환 시 2026-05-28
  CURRENT_DATE()   : 2026-05-27 (UTC-7 기준 전날)
  결과             : WHERE 조건 불일치 → 10시 크롤링 데이터 조회 실패
```

파이프라인은 KST 00:00 / 05:00 / 10:00 / 15:00에 실행된다.  
UTC-7 자정(= KST 14:00) 이전에는 UTC 기준 날짜와 UTC-7 기준 날짜가 다르므로  
**KST 00:00, 05:00, 10:00 크롤링 결과가 당일 07:00(KST)까지 조회 불가** 상태가 된다.

이미 `get_summary_stats` 등 다른 쿼리는 올바르게 작성되어 있어 수정 기준이 명확히 존재했다.

```sql
-- get_summary_stats에서 이미 올바르게 사용 중인 패턴
CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE
```

### 3-3. 변경 내용

4곳 모두 동일한 패턴으로 수정.

```sql
-- 변경 전 (4곳 공통)
WHERE CRAWLED_AT::DATE = CURRENT_DATE()

-- 변경 후 (4곳 공통)
WHERE CRAWLED_AT::DATE = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE
```

`CRAWLED_AT`이 UTC로 저장되어 있으므로, 비교 기준도 명시적으로 UTC 날짜로 맞춘다.

### 3-4. 수정된 위치

| 파일 | 함수 | 수정 수 |
|------|------|---------|
| `src/pipeline/quality.py` | `check_cross_site_prices` | 1곳 |
| `src/pipeline/quality.py` | `check_layer_consistency` | 2곳 |
| `src/dashboard/data_access/snowflake_queries.py` | `get_today_crawl_comparison` | 1곳 |

---

## 4. 변경 사항 3 — Watchlist 콜백 중복 코드 제거

### 4-1. 문제 발견

`src/dashboard/callbacks.py`에서 다나와 / 견적왕 / 컴퓨존 3사이트에 대해  
각각 `do_search`, `load_list`, `handle_add`, `clear_search` 4개 콜백이  
거의 동일한 코드로 반복됐다. 총 12개 함수, 약 410 라인.

```
do_watch_search()            — 다나와 검색
load_watch_list()            — 다나와 목록 로드
handle_watch_add()           — 다나와 추가
clear_watch_search_on...()   — 다나와 검색 초기화

do_pcest_search()            — 견적왕 검색   ← 위와 95% 동일
load_pcest_list()            — 견적왕 목록 로드
handle_pcest_add()           — 견적왕 추가
clear_pcest_search_on...()   — 견적왕 검색 초기화

do_compuzone_search()        — 컴퓨존 검색  ← 위와 95% 동일
load_compuzone_list()        — 컴퓨존 목록 로드
handle_compuzone_add()       — 컴퓨존 추가
clear_compuzone_search...()  — 컴퓨존 검색 초기화
```

**이 구조의 유지보수 문제:**  
예를 들어 "추가 후 Slack 메시지 포맷을 변경"하려면 3곳을 동시에 수정해야 한다.  
한 곳을 빠뜨리면 사이트마다 동작이 달라지는 불일치 버그가 발생한다.

### 4-2. 사이트별 실질적 차이점 분석

리팩토링 전 3사이트의 실질적 차이점을 먼저 파악했다.  
공통점(95%)과 차이점(5%)을 명확히 구분한 뒤 설계 방향을 결정했다.

| 항목 | 다나와 | 견적왕 | 컴퓨존 |
|------|--------|--------|--------|
| Dash 컴포넌트 ID 접두사 | `watch` | `pcest` | `compuzone` |
| 상품코드 필드명 | `pcode` | `pd_no` | `product_no` |
| 검색 함수 인자 순서 | `(query, max_results, category)` | `(query, category, max_results)` | `(query, category, max_results)` |
| 상품명 보강(enrich) | 항상 실행 | 없음 | RAM/SSD 카테고리만 |
| 카테고리 미선택 시 기본값 | `None` | `"GPU"` | `"GPU"` |

### 4-3. 설계 결정 — 팩토리 패턴 선택 근거

Dash 콜백은 **함수 이름이 아닌 Output/Input 컴포넌트 ID로 구분**된다.  
따라서 팩토리 함수 내부에서 이름이 같은 중첩 함수를 정의해도  
각 호출에서 생성된 클로저가 서로 다른 설정 객체(`cfg`)를 캡처하므로 충돌이 없다.

```python
# Dash는 아래 두 콜백을 함수 이름이 아닌 Output ID로 구분한다
Output("watch-list-table", "children")      # 다나와 목록
Output("pcest-list-table", "children")      # 견적왕 목록
```

이 특성을 활용해 **설정 객체 + 팩토리 패턴**으로 중복을 제거했다.

### 4-4. 변경 내용

**Step 1 — 사이트 설정을 담는 dataclass 정의**

```python
@dataclass(frozen=True)
class _SiteWatchConfig:
    site_name: str      # DB 저장값 ("다나와" | "견적왕" | "컴퓨존")
    id_prefix: str      # Dash 컴포넌트 ID 접두사
    pcode_key: str      # 검색 결과 객체의 상품코드 속성명
    add_btn_type: str   # 추가 버튼 pattern-matching type
    del_btn_type: str   # 삭제 버튼 pattern-matching type
    search_fn: Callable # 정규화된 검색 함수: (query, category, max_results) → list
    enrich_fn: Callable # 상품명 보강 함수: (results, category) → list
```

`frozen=True`로 불변 인스턴스를 보장해 콜백 클로저에서 안전하게 참조되도록 했다.

**Step 2 — 크롤러별 검색 함수 시그니처 정규화**

3개 크롤러의 `search_products` 인자 순서가 달라 통일된 시그니처로 래핑했다.

```python
def _danawa_search(query, category, max_results):
    return danawa_search(query, max_results=max_results, category=category)
    # 이유: danawa_search의 2번째 인자가 max_results, 3번째가 category

def _pcest_search(query, category, max_results):
    return pcest_search(query, category=category or "GPU", max_results=max_results)
    # 이유: 카테고리 미선택 시 "GPU"가 기본값

def _compuzone_search(query, category, max_results):
    return compuzone_search(query, category=category or "GPU", max_results=max_results)

def _compuzone_enrich(results, category):
    return compuzone_enrich(results) if category in ("RAM", "SSD") else results
    # 이유: 컴퓨존 enrich는 RAM/SSD만 의미있음

def _noop_enrich(results, category):
    return results  # 견적왕은 enrich 없음
```

**Step 3 — 4개 콜백을 한 번에 등록하는 팩토리 함수**

```python
def _register_site_watchlist(app, cfg: _SiteWatchConfig):
    p = cfg.id_prefix

    @app.callback(...)  # do_search
    def do_search(...):
        results = cfg.search_fn(query, category, 10)
        results = cfg.enrich_fn(results, category)
        stored = [{cfg.pcode_key: getattr(r, cfg.pcode_key), ...} for r in results]
        ...

    @app.callback(...)  # load_list
    def load_list(...):
        df = get_watch_products(conn, site=cfg.site_name)
        return make_watchlist_table(df, del_btn_type=cfg.del_btn_type)

    @app.callback(...)  # handle_add
    def handle_add(...):
        add_watch_product(conn, pcode=product[cfg.pcode_key], site=cfg.site_name, ...)

    @app.callback(...)  # clear_search
    def clear_search(_):
        return html.Div(), [], ""
```

**Step 4 — 3회 호출**

```python
_register_site_watchlist(app, _SiteWatchConfig(
    site_name="다나와", id_prefix="watch", pcode_key="pcode",
    search_fn=_danawa_search, enrich_fn=_danawa_enrich, ...
))
_register_site_watchlist(app, _SiteWatchConfig(
    site_name="견적왕", id_prefix="pcest", pcode_key="pd_no",
    search_fn=_pcest_search, enrich_fn=_noop_enrich, ...
))
_register_site_watchlist(app, _SiteWatchConfig(
    site_name="컴퓨존", id_prefix="compuzone", pcode_key="product_no",
    search_fn=_compuzone_search, enrich_fn=_compuzone_enrich, ...
))
```

**변경하지 않은 부분 — 삭제 모달 콜백 2개**

`open_del_modal`과 `handle_del_confirm`은 3사이트의 삭제 버튼을 **동시에** Listen하고  
3개의 refresh trigger를 **모두 함께** 갱신하는 구조다.  
이를 사이트별로 분리하면 오히려 공유 상태 동기화 로직이 깨지므로 의도적으로 유지했다.

```python
# 3사이트 삭제 버튼을 모두 Listen — 사이트별 분리 불가
[Input({"type": "watch-del-btn", "index": ALL}, "n_clicks"),
 Input({"type": "pcest-del-btn", "index": ALL}, "n_clicks"),
 Input({"type": "compuzone-del-btn", "index": ALL}, "n_clicks")]
```

### 4-5. 효과

| 항목 | 변경 전 | 변경 후 |
|------|---------|---------|
| callbacks.py 전체 라인 수 | 947 라인 | 777 라인 (-170) |
| Watchlist 관련 코드 | 약 410 라인 | 약 180 라인 |
| 공통 버그 수정 필요 위치 | 3곳 | 1곳 (팩토리 내부) |
| 사이트 추가 시 필요 작업 | 4개 함수 복사 + 수정 | 래퍼 함수 2개 + config 1개 추가 |

---

## 5. 영향 범위 및 회귀 위험 분석

### 변경된 파일과 의존 관계

```
slack.py ──────────────────────────────────────────────────────────────
  변경: _send_slack_message → send_slack_message
  영향: quality.py (import 이번 작업에서 함께 수정 완료)
  비영향: run_pipeline.py (send_slack_failures만 사용 — 시그니처 변경 없음)

quality.py ─────────────────────────────────────────────────────────────
  변경: import 수정 + CURRENT_DATE() 3곳 수정
  영향: run_pipeline.py (check_cross_site_prices, check_layer_consistency 호출)
  함수 시그니처 변경 없음 → 호출부 수정 불필요

snowflake_queries.py ───────────────────────────────────────────────────
  변경: get_today_crawl_comparison의 WHERE 절 SQL
  영향: callbacks.py, benchmark 스크립트
  반환 타입/컬럼명 변경 없음 → 호출부 수정 불필요

callbacks.py ───────────────────────────────────────────────────────────
  변경: Watchlist 콜백 내부 구현 (Dash 컴포넌트 ID는 동일 유지)
  영향: 브라우저 UI (ID 동일 → 레이아웃 파일 수정 불필요)
```

### 회귀 위험 항목

| 항목 | 위험도 | 근거 |
|------|--------|------|
| Watchlist 검색·추가·삭제 동작 | LOW | Dash는 컴포넌트 ID 기반. ID 전부 동일 유지. 동작 로직도 동일 |
| 견적왕 기본 카테고리 "GPU" | LOW | `_pcest_search`에서 `category or "GPU"` 동작 그대로 재현 |
| 컴퓨존 RAM/SSD enrich 조건 | LOW | `_compuzone_enrich`에서 `category in ("RAM", "SSD")` 동일 조건 재현 |
| Slack 알림 발송 | LOW | `send_slack_message` 함수 내부 로직 무변경, 이름만 변경 |
| 타임존 수정 후 조회 범위 변경 | LOW | `CRAWLED_AT`이 UTC 저장이므로 UTC 기준 비교가 정확함 |

---

## 6. 검증 결과

### 문법 검사 (Python AST)

```bash
$ python -c "import ast; ast.parse(open('src/dashboard/callbacks.py', encoding='utf-8').read())"
→ 통과

$ python -c "import ast; ast.parse(open('src/pipeline/quality.py', encoding='utf-8').read())"
→ 통과

$ python -c "import ast; ast.parse(open('src/pipeline/slack.py', encoding='utf-8').read())"
→ 통과
```

### 잔여 버그 패턴 검사

```
CURRENT_DATE() 잔존 여부 검사:
  quality.py           → 0개  (정상)
  snowflake_queries.py → 0개  (정상)

_send_slack_message import 잔존 여부:
  quality.py           → 0개  (정상)
```

### 미검증 항목 (운영 환경 필요)

아래 항목은 Snowflake 접속이 필요하여 이번 작업 범위에서 확인하지 못했다.  
다음 배포 전 반드시 확인이 필요하다.

- [ ] Watchlist 검색 → 추가 → 삭제 E2E 플로우 (실제 브라우저 동작 확인)
- [ ] 파이프라인 실행 후 타임존 수정 효과 확인 (KST 00:00~07:00 구간)
- [ ] 단위 테스트 전체 실행: `python -m pytest tests/ -v -o "addopts="`

---

*작성: Claude Sonnet 4.6 | 2026-05-28*
