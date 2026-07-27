# 컴퓨터 가격 모니터링 시스템

## Language
- Respond in Korean for explanations and documentation unless code/commands
- Technical terms can remain in English

## 프로젝트 개요
컴퓨터 부품 가격 비교 사이트 3곳(다나와, 컴퓨존, 견적왕)을 크롤링하여 일일 가격을 수집하고,
사용자별 관심 상품(워치리스트)의 가격 변동을 추적하는 시스템. FastAPI 웹 UI로 제공.

## 기술 스택
- Python 3.11+, BeautifulSoup (크롤링)
- GitHub Actions (오케스트레이션, 00:00/05:00/10:00/15:00 KST 4회/일 — 동작 중)
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
└── api/             # main.py, security.py, users_*, watchlist_*, static/(웹 UI)
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
- 컴퓨존 크롤러: crawl_single의 1차 경로만 product_list.php POST(EUC-KR, li.li-obj 파싱).
  검색(search_products)은 GET — 의도된 비대칭이니 통일하지 말 것.
  3단계 fallback: ① product_list.php POST → ② search_list.php GET → ③ 상세페이지 정규식.
  브라우저에 보이는 div.prdbx는 AJAX 셸이라 직접 크롤링 불가 — 셀렉터 변경 금지.
- Frozen dataclass로 모든 DTO 정의
- DB 접속: src/common/mysql_client.py의 get_connection 사용, 드라이버는 pymysql.
  접속 계정은 .env의 price_app만 사용 — root 금지. 새 추상화 계층(ABC/팩토리)을 만들지 않는다.
- 멱등성: INSERT ... ON DUPLICATE KEY UPDATE + 자연키 UNIQUE 제약으로 보장
- 증분 처리: "미처리 조인"(대상 테이블에 없는 raw 행만 조회) 방식. batch_id는 도입하지 않음
- 변경 감지: LAG() 윈도우 함수로 이전 가격 비교, ans_product_stats로 NEW_LOW/NEW_HIGH 판정
- 타임스탬프: DATETIME, 앱과 DB 세션 모두 UTC로 통일 저장(mysql_client가 연결 시 세션 타임존을 UTC로 고정)
- api 코드 변경 시 docker-compose가 ./src를 볼륨 마운트하므로 즉시 반영된다 (필요 시 docker compose restart api)

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