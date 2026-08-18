# 컴퓨터 가격 모니터링 시스템

## Language
- Respond in Korean for explanations and documentation unless code/commands
- Technical terms can remain in English

## 프로젝트 개요
컴퓨터 부품 가격 비교 사이트 3곳(다나와, 컴퓨존, 견적왕)을 크롤링하여 일일 가격을 수집하고,
사용자별 관심 상품(워치리스트)의 가격 변동을 추적하는 시스템. FastAPI 웹 UI로 제공.
부품 조합(builds)을 만들어 총액 추이를 보는 기능이 있으며, 조합은 공개 게시물이다
— 누구나 읽을 수 있고 수정은 작성자만 가능하다.

## 기술 스택
- Python 3.11+, BeautifulSoup (크롤링)
- Oracle Cloud VM 의 cron (오케스트레이션, 00:00/05:00/10:00/15:00 KST 4회/일 — 동작 중).
  코드는 VM 의 `~/price-pipeline`, 로그는 `~/crawl.log`. GitHub Actions 는 CI 전용이다
  (2026-08-18 이관 — 러너가 컴퓨존에 차단되고 3306 을 외부에 열어야 해서)
- DWH: MySQL 8.0 (Oracle Cloud VM 상시가동, 단일 DB `computer_price`, 3-Layer는 테이블 접두사 raw_/stg_/ans_ 방식)
- FastAPI + 정적 HTML (웹 UI: 로그인/회원가입/마이페이지/워치리스트)
- Docker Compose (1개 서비스: api)

## 데이터 흐름
크롤러 → run_pipeline.py → MySQL (raw_ → stg_ → 변경감지/알림 → ans_)
                                              ↓
                                          api ← MySQL

## 프로젝트 구조
src/
├── common/          # models.py, config.py, mysql_client.py
├── crawlers/        # base.py, danawa.py, compuzone.py, pc_estimate.py, parser_utils.py
├── pipeline/        # crawl, load_raw, transform, quality, detect, analytics, slack
└── api/             # main.py, security.py, users_*, watchlist_*, build_*, static/(웹 UI)
    └── static/      # index/signup/home/mypage/watchlist.html (조합 화면은 home.html 안)
run_pipeline.py      # 파이프라인 전체 실행 진입점

## 개발 규칙
- 한번에 전부 만들지 않음. 단계별로 나눠서 각 단계마다 테스트 후 진행
- 최하위(기초) 기능부터 구현/테스트 → 정상 확인 후 다음 단계로 진행
- 여러 기능이 있으면 가장 기초가 되는 부분을 먼저 만들고, 동작 확인 후 상위 기능으로 올라감
- 오류 발생 시 반드시 해결하고 나서 다음 단계로 넘어감
- 처음부터 큰 덩어리로 작업하지 않음 — 소분류로 나눠서 진행 현황을 명확히 파악
- 기능 설계를 먼저 완료한 후, 구현 단계를 사용자에게 보여주고 진행 여부를 확인받은 뒤에만 작업을 시작한다
- 사용자에게 권한 요청(진행 여부 확인) 또는 선택지를 제시하기 전에, 반드시 다음을 자체 검증한다:
  - 프롬프트의 요구사항이 모두 완료됐는지 체크리스트로 확인
  - 완료된 작업에 실수·누락·부작용이 없는지 재확인 (파일 오염, 의도치 않은 변경 포함)
  - 위 2가지가 모두 OK일 때만 사용자에게 물어본다
  - 자체 검증 없이 "완료됐습니다, 진행할까요?" 식으로 바로 묻지 않는다
- Git: feature branch → PR → code review → merge
- 커밋은 관련있는 것끼리 분리
- Co-Authored-By 추가하지 않음
- 테스트: python -m pytest tests/ -v -o "addopts=" (pytest-cov 미설치 시)

## Verification
- After implementing fixes, verify actual behavior (run integration tests, check DB state), not just unit tests
- Before schema changes, inspect the actual source data columns first
- 파이프라인 계층 전환/수정 후에는 행수·NULL 비율 정합 검증을 수행

## Scope Discipline
- When user asks 'should I X?', answer the question — do not execute X
- Confirm before running gh merge, force push, or destructive ops

## 기술적 주의사항
- 다나와 크롤러: productItem* = 실제상품, adReaderProductItem*/adPointProductItem* = 광고
- 컴퓨존 크롤러: 3사 공통으로 단일 경로다 — search_list.php GET(EUC-KR, li.li-obj 파싱)에서
  검색어+MediumDivNo로 조회 후 ProductNo 정확매칭. fallback 없음.
  브라우저에 보이는 div.prdbx는 AJAX 렌더링 후 DOM이라 직접 크롤링 불가 — 셀렉터 변경 금지.
  (구 3단계 fallback은 2026-07-29 제거: ①카테고리목록은 검색어를 안 써 추천순 100위 밖이면
   실패했고 검색 경로가 그 상위집합임을 실측 확인, ③상세페이지는 <title> 기반이라 상품명에
   용량·옵션이 덧붙어 같은 상품이 stg_products에서 둘로 갈라졌다)
- 견적왕 크롤러: 요청이 2단계다 (2026-07-30 확정).
  ① POST product_search.html (main_search=검색어) → 응답 HTML의 id="search_query" value가 토큰
  ② POST product_list_include_plist.php — search_query/search_cate/page 3개만 보낸다
  12개 필드를 하나씩 빼보며 실측한 결과, 이 3개 외 9개(action/search_word/view_type/timeid 등)는
  서버가 읽지 않는다. Referer 헤더와 세션 쿠키도 불필요하다.
  search_query가 없으면 0건, page가 없으면 1페이지 고정, search_cate가 없으면 다른 카테고리
  상품에 밀려 대상이 페이지 밖으로 나간다(삼성 RAM 40개 중 38개가 5페이지 밖으로 사라졌다).
  토큰은 세션이 아니라 검색어에 묶여 있다 — 검색어마다 새로 받고, 같은 검색어면
  페이지·대상 간에 재사용한다(_token_cache).
  구 depth/cate1/cate2 방식은 검색이 아니다. 서버가 search_word를 무시해 카테고리 목록을
  그대로 내려주므로, 없는 검색어를 넣어도 결과가 같다. 되돌리지 말 것.
- 요청 인코딩: 견적왕은 charset=euc-kr 사이트라 폼도 EUC-KR로 보내야 한다
  (pc_estimate.py의 _euc_kr_body). requests에 dict를 넘기면 UTF-8로 나가고 서버가 그 바이트를
  EUC-KR로 읽어 한글 검색어가 조용히 0건이 된다. 영문·숫자는 두 인코딩의 바이트가 같아
  우연히 통과하므로 "RTX 5080은 되는데 라이젠 7800X3D는 0건"인 형태로 나타난다 — 에러가
  안 나서 발견이 늦다. 다나와·컴퓨존 서버는 UTF-8 요청을 받아준다(컴퓨존은 응답만 EUC-KR).
- 브라우저에서 잘 되는 것은 크롤러가 된다는 근거가 아니다. 브라우저는 ①문서 charset을 보고
  폼을 대신 인코딩해주고 ②JS 렌더링 후 DOM을 보여준다. 둘 다 크롤러가 받는 것과 다르다.
  확인은 코드로 재현하거나 개발자도구 Network 탭의 Response로 한다 — Elements 탭이 아니다.
- fallback이 없으므로 검색 실패가 곧 그 상품의 수집 실패다. 3사 crawl_raw 모두 대상 미발견 시
  query와 사이트 고유 ID를 warning으로 남긴다. 총계 로그만으로는 워치리스트가 커졌을 때
  부분 실패를 알 수 없다.
- 크롤링 대상은 3사 모두 stg_watchlist에서 로드하고, 사이트 고유 ID(pcode/ProductNo/pd_no)로
  정확매칭한다. 이름 유사도 매칭이 아니다.
- crawled_at은 crawl_raw() 진입 시 한 번만 정해 그 회차 전체에 쓴다. 대상·페이지 루프 안에서
  now()를 부르면 같은 회차인데 상품마다 시각이 갈리고, stg_price_history 자연키가
  (product_id, crawled_at)이라 하위 시계열·일별 집계가 어긋난다.
- 크롤러 진단: python scripts/diagnose_compuzone.py [--all-paths] [--save-html]
  — 0건일 때 원인이 요청 계약/셀렉터/스캔 범위 중 무엇인지 구분해준다.
  단 로컬에서는 항상 통과한다 — 0건의 원인이 코드가 아니라 출발지 IP 차단이기 때문이다.
- 사이트마다 차단하는 IP 대역이 다르다(2026-08-18 실측). 컴퓨존은 GitHub Actions 러너를
  막고, 견적왕은 Oracle Cloud 대역을 막는다(토큰 요청부터 403). 가정용 회선은 셋 다 된다.
  어느 한 곳에서도 3사를 다 수집할 수 없다 — **0건이면 셀렉터·요청 계약보다 출발지 IP 를
  먼저 의심할 것.** 견적왕 크롤러는 일부러 그대로 두었다: 파이프라인이 막히지 않고
  (`실패: 1개 사이트` 로 기록 후 나머지 적재), 차단이 풀리면 자동으로 다시 수집된다
- Frozen dataclass로 모든 DTO 정의
- DB 접속: src/common/mysql_client.py의 get_connection 사용, 드라이버는 pymysql.
  접속 계정은 .env의 price_app만 사용 — root 금지. 새 추상화 계층(ABC/팩토리)을 만들지 않는다.
- 멱등성: INSERT ... ON DUPLICATE KEY UPDATE + 자연키 UNIQUE 제약으로 보장
- 증분 처리: "미처리 조인"(대상 테이블에 없는 raw 행만 조회) 방식. batch_id는 도입하지 않음
- 변경 감지: LAG() 윈도우 함수로 이전 가격 비교, ans_product_stats로 NEW_LOW/NEW_HIGH 판정
- 타임스탬프: DATETIME, 앱과 DB 세션 모두 UTC로 통일 저장(mysql_client가 연결 시 세션 타임존을 UTC로 고정)
- api 코드 변경 시 docker-compose가 ./src를 볼륨 마운트하므로 즉시 반영된다 (필요 시 docker compose restart api)
- 조합(builds): 읽기는 공개(/builds), 쓰기는 작성자만(/users/{uid}/builds).
  남의 조합 수정 시도는 403이 아니라 404로 응답한다 — 403은 "그 id가 존재한다"는
  사실을 흘린다
- 조합 총액 추이: 일별 마지막 값 + forward fill, 모든 부품이 가격을 가진 날부터 시작.
  집계는 src/api/build_trend.py의 순수 함수(DB 무관) — 규칙 변경 시 여기만 보면 된다
- stg_watchlist.is_active를 내리는 조건에 build_items도 포함해야 한다. 빼먹으면
  공개 조합에 담긴 상품의 크롤링이 멈춰 남의 화면에서 가격이 갱신되지 않는다
- 조합 화면은 별도 페이지가 아니라 home.html 안에서 사이드바로 전환한다(가격 추이·
  크롤링 대상 관리와 동일 방식). 차트도 그 화면의 renderChart와 같은 inline SVG로
  그린다 — 외부 차트 라이브러리를 쓰지 않는다

## Docker 서비스
| 서비스 | 포트 |
|---|---|
| API + 웹 UI | localhost:8001 |

## Environment
- Desktop files go to the actual visible Desktop (check OneDrive redirection on Windows)
- Confirm gh CLI and key deps are installed before scripting workflows

## 실행
docker compose up -d                    # api 시작
docker compose restart api              # 코드 변경 반영
docker compose logs -f api              # 로그 확인
python run_pipeline.py                  # 파이프라인 로컬 수동 실행
python -m pytest tests/ -v -o "addopts="  # 테스트 실행