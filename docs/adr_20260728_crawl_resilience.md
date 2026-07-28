# ADR — 크롤링 단계 커넥션 수명 · 타임아웃 예산 · 실패 격리

- **상태**: 채택 (2026-07-28)
- **관련 브랜치**: `fix/crawl-connection-lifetime`
- **관련 문서**: [crawl_failure_isolation.md](crawl_failure_isolation.md), [pipeline_diagram.md](pipeline_diagram.md)

---

## 1. 맥락 — 무슨 일이 있었나

2026-07-28 08:39 KST 스케줄 실행이 exit 1로 실패했다. 직전 07-27 21:14 실행도
동일한 형태로 실패했고, 그 사이 실행들은 모두 43~47초에 정상 종료했다.

```
08:39:33  === 파이프라인 시작 ===
08:39:38  [크롤링] danawa: 1건                     ← 성공
08:39:38  (컴퓨존 워치리스트 조회 — 커넥션 마지막 사용)
   ...    ConnectTimeout × 12회
08:44:44  [크롤링] compuzone: 0건                  ← 실패를 정상 접수
08:44:44  [crawl] FAILED — (2006, "MySQL server has gone away
                             (SSLEOFError(8, 'EOF occurred in violation of protocol'))")
Error: Process completed with exit code 1.
```

### 1차 원인 — 외부 요인 (코드로 해결 불가)

컴퓨존이 GitHub Actions 러너(Azure 대역 IP)의 TCP SYN에 무응답이었다.
HTTP 4xx/5xx도 파싱 실패도 아닌 `ConnectTimeoutError`이므로 셀렉터·파서와 무관하다.
동일 러너에서 다나와는 정상이었고, 최근 12회 중 10회는 컴퓨존도 성공했다 — 간헐적 차단.

### 2차 원인 — 코드 (실제 exit 1의 원인)

무응답 자체보다 **무응답을 확인하는 데 5분이 걸린 것**이 문제였다.

| 단계 | 코드 | 소요 |
|---|---|---|
| ① 카테고리 목록 POST | `compuzone.py:331` | 31초 |
| ② 검색 fallback GET | `compuzone.py:354` | 30초 |
| ③ 상세페이지 정규식 | `compuzone.py:46,75` | 16초 |

대상당 약 77초 × 워치리스트 4건 = **5분 6초**. `requests`의 `timeout=30`은
connect와 read 공통값이라, 죽은 호스트에 연결을 시도하는 데만 30초를 썼다.

그동안 `crawl_all_sites()`는 커넥션 하나를 3개 크롤러 전체에 걸쳐 들고 있었다.
커넥션의 유일한 사용처는 각 크롤러 첫머리의 `_load_watch_products()`뿐이므로,
컴퓨존이 조회한 08:39:38 이후 5분간 소켓은 완전히 침묵했다. 경로 중간의 idle
timeout(러너 측 NAT로 추정, 정확한 값은 미확정 — 5분 6초 이하라는 것만 확인)이
TCP를 끊었고, 견적왕이 그 죽은 소켓에 쓰려다 `(2006)`으로 죽었다.

그 예외는 `crawl.py`의 except 목록
`(requests.RequestException, ValueError, TypeError, AttributeError, KeyError)`에
없어 그대로 전파됐고, `run_pipeline.py:42`가 크롤링 단계 실패로 처리해 `return 1` 했다.
**이미 수집을 마친 다나와 1건도 적재되지 못했다.**

### 왜 기존 격리 설계가 막지 못했나

[crawl_failure_isolation.md](crawl_failure_isolation.md)(2026-04-14)는 사이트별
try-except로 실패를 격리한다고 기록했고, 그 방향 자체는 옳았다. 다만 두 군데가 새고 있었다.

1. **예외 목록이 좁았다** — 나열되지 않은 예외는 격리를 통과한다
2. **커넥션이 공유 자원이었다** — 그 문서는 "커넥션 1개"를 장점으로 적었으나,
   한 사이트의 지연이 다른 사이트의 커넥션 상태를 망가뜨리는 **은닉된 결합**이었다

즉 "사이트별 실패는 격리했지만, 사이트별 **지연**은 격리하지 않았다."

---

## 2. 결정

### ⓐ MySQL 커넥션을 크롤러마다 열고 닫는다

```python
for crawler_cls in (DanawaCrawler, CompuzoneCrawler, PCEstimateCrawler):
    with get_connection(settings) as conn:
        crawler = crawler_cls(conn=conn)
        raw_prices = crawler.crawl_raw()
```

커넥션의 유일한 사용처가 `crawl_raw()` 첫머리의 워치리스트 조회이므로, 조회는 항상
"방금 연 커넥션"에서 일어난다. 앞 사이트가 몇 분을 끌든 뒤 사이트는 영향받지 않는다.

- 크롤러 클래스 튜플은 **함수 안에서** 해석한다. 모듈 전역에 두면 import 시점에 원본
  클래스가 고정돼 테스트의 `patch`가 무력화된다 (실제로 기존 테스트가 이를 잡아냈다).
- 크롤링 도중 커넥션이 죽어도 `close()`는 안전하다 — pymysql은 COM_QUIT 쓰기 실패를
  `try/except Exception: pass`로 삼킨다 (설치 버전 소스로 확인).

### ⓑ HTTP 타임아웃을 (connect, read)로 분리한다

```python
# src/crawlers/base.py
REQUEST_TIMEOUT = (5.0, 20.0)
```

무응답 호스트는 connect 단계에서 걸리므로 connect가 실패까지의 시간을 지배한다.
살아 있는 서버라면 TCP 연결은 1초 안에 끝나므로 5초로 충분하고, 응답 자체가 느린
경우(대용량 목록 페이지)는 read 20초가 받아준다. 크롤러 4개 모듈의 호출 지점
12곳을 모두 이 상수로 교체했다.

### ⓒ 사이트 단위 실패 격리를 예외 종류와 무관하게 만든다

```python
except Exception as e:
    crawl_failures.append({"site_name": ..., "error": f"{type(e).__name__}: {e}"})
    logger.exception(...)
```

예외 종류를 좁게 나열하면 목록 밖 예외가 격리를 통과한다는 것이 이번 장애로 증명됐다.
크롤러는 외부 사이트에 의존하므로 예상 밖 예외의 종류를 미리 다 열거할 수 없다.

전멸(3개 전부 실패) 시 exit 1 판단은 호출자인 `run_pipeline.py`가 그대로 유지한다.

---

## 3. 검토했으나 기각한 대안

### 커넥션 풀 (미리 3개 생성 후 대여·반납)

**기각.** 이 장애는 커넥션이 **모자라서**가 아니라 **쥐고 있던 것이 유휴 중 죽어서**
발생했다. 풀은 커넥션을 미리 만들어 놀려두는 장치이므로 같은 병에 더 취약하다.

| 근거 | 설명 |
|---|---|
| 병을 키운다 | 3개를 만들면 실제 쓰이는 1개 외 2개는 처음부터 끝까지 유휴 상태 |
| 문제를 푸는 건 풀이 아니다 | 풀에서 대여한 커넥션이 죽었으면 동일하게 `(2006)`. 이를 막는 건 체크아웃 시 `ping`/`pre_ping`이고, ping만 넣으면 풀은 불필요 |
| 동시성이 0 | `run_pipeline.py`는 단일 스레드 순차 실행 — 동시 필요 커넥션은 항상 1개 |
| 추상화 비용 | pymysql에 풀이 없어 DBUtils/SQLAlchemy 도입 필요. CLAUDE.md의 "새 추상화 계층을 만들지 않는다"와 충돌 |

이 워크로드에 맞는 처방은 "미리 만들어 오래 들고 있기"가 아니라 **"필요한 순간에만
열고 바로 닫기"**다. 커넥션 오픈 비용은 회당 수십 ms이고 배치는 4시간에 한 번 돈다.

> 풀이 실제로 이득일 수 있는 지점은 **API 프로세스**다. `users_repo`/`build_repo`/
> `watchlist_repo`가 요청마다 Oracle Cloud VM까지 TCP+TLS 핸드셰이크를 새로 한다.
> 다만 크롤링과는 별개 프로세스·별개 문제이므로 이 ADR의 범위 밖이며,
> 실제 응답시간 측정 후 판단한다.

### `mysql_client`에 ping 재연결만 추가

**부분 채택 보류.** ⓐ 없이 ping만 넣어도 이번 건은 살았겠지만, "5분간 유휴 커넥션을
들고 있는" 구조 자체는 남는다. ⓐ가 유휴 시간을 없애 문제를 원천 제거하므로 ping은
현재 불필요하다. 향후 크롤러가 중간에 DB를 쓰도록 바뀌면 재검토한다.

### 컴퓨존 IP 차단 우회

**범위 밖.** 러너 측 코드로 해결할 수 없다. 근본 해결은 크롤링을 Oracle Cloud VM에서
실행하는 구조 변경이며, 별도 논의 대상이다.

---

## 4. 결과

### 동일 장애 재발 시

| | 수정 전 | 수정 후 |
|---|---|---|
| 컴퓨존 실패 확인까지 | 5분 6초 | 약 1분 20초 |
| 견적왕 | 죽음 | 정상 실행 |
| 다나와 1건 | 유실 | 정상 적재 |
| 워크플로 | exit 1 | 성공 (컴퓨존만 Slack 알림) |

### 검증

- 전체 테스트 **183개 통과** (기존 167 + 신규 16), ruff 통과
- **변이 테스트**: `REQUEST_TIMEOUT`을 `30.0` / `(30.0, 20.0)`으로 바꾸면 각각 3개·2개
  테스트가 죽는다
- **실측**: 패킷을 드롭하는 주소로 연결 시도 시 실패까지 **21초 → 5.0초**
- 신규 테스트 파일
  - `tests/unit/test_crawl_connection_lifetime.py` — 커넥션 개폐 **순서**를 검증
    (`open → crawl → close`가 크롤러마다 반복). 커넥션 공유로 되돌리면 즉시 깨진다
  - `tests/unit/test_crawler_timeouts.py` — 공유 상수 사용 + 숫자 리터럴 재유입 차단
  - `tests/unit/test_crawl_site_isolation.py` — `OperationalError`/`RuntimeError`/`KeyError`
    각각에 대해 뒤 사이트 실행과 수집분 보존을 검증

### 남은 과제

- 실제 파이프라인 실행 검증 미완 (운영 DB 적재를 동반하므로 사용자 확인 후 진행)
- 컴퓨존 IP 차단의 근본 해결(크롤링 주체를 VM으로 이전) 미결
- API 프로세스의 커넥션 재사용 전략 미검토

---

## 5. 교훈

1. **실패를 격리했다고 지연까지 격리된 것은 아니다.** 공유 자원(커넥션)이 있으면
   한 컴포넌트의 느림이 다른 컴포넌트의 정확성을 깨뜨린다.
2. **좁은 except 목록은 격리를 새게 한다.** 외부 의존이 있는 코드에서 예외 종류를
   전부 열거하려는 시도는 실패한다.
3. **`timeout=N` 단일 값은 함정이다.** connect와 read는 성격이 다른 대기이고,
   장애 시 시간을 지배하는 쪽은 connect다.
4. **거절보다 무시가 위험하다.** 명시적 거절(4xx)은 즉시 끝나지만, 무응답은
   타임아웃을 꽉 채운다.

---

*작성일: 2026-07-28*
*근거: GitHub Actions run 30343258256 / 30305996818 로그, 로컬 실측*
