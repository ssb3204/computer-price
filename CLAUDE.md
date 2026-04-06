# 컴퓨터 가격 모니터링 시스템

## Language
- Respond in Korean for explanations and documentation unless code/commands
- Technical terms can remain in English

## 프로젝트 개요
컴퓨터 부품 가격 비교 사이트 3곳(다나와, 컴퓨존, 견적왕)을 크롤링하여 일일 가격을 수집하고, 가격 변동을 시각화하는 웹 대시보드. 로컬 Docker Compose로 운영.

## 기술 스택
- Python 3.11+, BeautifulSoup (크롤링)
- Apache Airflow (오케스트레이션, 매일 21:00/22:00 KST)
- Snowflake (DWH, 3-Layer: Raw → Staging → Analytics)
- PostgreSQL 16 (Airflow 메타DB 전용)
- Dash/Plotly (웹 대시보드)
- Docker Compose (3개 서비스)

## 데이터 흐름
```
크롤러 → Airflow DAG → Snowflake (Raw → Staging → 변경감지/알림 → Analytics)
                                                     ↓
                                              dashboard ← Snowflake
```

## 프로젝트 구조
```
src/
├── common/          # models, config, db, snowflake_client
├── crawlers/        # base.py, danawa.py, compuzone.py, pc_estimate.py
├── dashboard/       # Dash 앱, snowflake_queries
└── airflow_dags/    # snowflake_pipeline_dag.py (6단계 파이프라인)
```

## 개발 규칙
- 한번에 전부 만들지 않음. 단계별로 나눠서 각 단계마다 테스트 후 진행
- **최하위(기초) 기능부터 구현/테스트 → 정상 확인 후 다음 단계로 진행**
  - 여러 기능이 있으면 가장 기초가 되는 부분을 먼저 만들고, 동작 확인 후 상위 기능으로 올라감
  - 오류 발생 시 반드시 해결하고 나서 다음 단계로 넘어감
  - 처음부터 큰 덩어리로 작업하지 않음 — 소분류로 나눠서 진행 현황을 명확히 파악
- **기능 설계를 먼저 완료한 후, 구현 단계를 사용자에게 보여주고 진행 여부를 확인받은 뒤에만 작업을 시작한다**
- **사용자에게 권한 요청(진행 여부 확인) 또는 선택지를 제시하기 전에, 반드시 다음을 자체 검증한다:**
  1. 프롬프트의 요구사항이 모두 완료됐는지 체크리스트로 확인
  2. 완료된 작업에 실수·누락·부작용이 없는지 재확인 (파일 오염, 의도치 않은 변경 포함)
  3. 위 2가지가 모두 OK일 때만 사용자에게 물어본다
  - 자체 검증 없이 "완료됐습니다, 진행할까요?" 식으로 바로 묻지 않는다
- Git: feature branch → PR → code review → merge
- 커밋은 관련있는 것끼리 분리
- Co-Authored-By 추가하지 않음
- 테스트: `python -m pytest tests/ -v -o "addopts="` (pytest-cov 미설치 시)

## Verification
- After implementing fixes, verify actual behavior (run integration tests, check DB state), not just unit tests
- Before schema/dbt changes, inspect the actual source data columns first

## Scope Discipline
- When user asks 'should I X?', answer the question — do not execute X
- Confirm before running gh merge, force push, or destructive ops

## 기술적 주의사항
- 다나와 크롤러: productItem* = 실제상품, adReaderProductItem*/adPointProductItem* = 광고
- Frozen dataclass로 모든 DTO 정의
- Snowflake MERGE로 멱등성 보장
- Airflow 2.8은 SQLAlchemy <2.0 필요 → Dockerfile에서 직접 의존성 설치
- 변경 감지: LAG() 윈도우 함수로 이전 가격 비교, PRODUCT_STATS로 NEW_LOW/NEW_HIGH 판정

## Docker 서비스
| 서비스 | 포트 |
|--------|------|
| Dashboard | localhost:8050 |
| Airflow | localhost:8081 |
| PostgreSQL | localhost:5432 (Airflow 메타DB) |

## Environment
- Desktop files go to the actual visible Desktop (check OneDrive redirection on Windows)
- Confirm gh CLI and key deps (google-cloud-bigquery, etc.) are installed before scripting workflows

## 실행
```bash
docker compose up -d
```
