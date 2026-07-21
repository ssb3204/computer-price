\# 작업: Snowflake → MySQL 마이그레이션







\## 컨텍스트

\- 이 프로젝트는 PC부품 가격 추적 서비스. 현재 Snowflake 기반이며 크레딧 만료로 MySQL 8.0으로 이전한다.

\- 기존 Snowflake 데이터는 보존할 필요 없음 (크롤링 데이터라 재수집 가능).

\- 모델링 가이드라인 문서: docs/MYSQL_MODELING_GUIDE.md — 이 문서의 설계 결정을 따를 것.

&#x20; 핵심: 대리키 PK + 자연키 UNIQUE, 비식별 관계, RAW/STAGING/ANALYTICS 3계층,

&#x20; batch\_id 기반 증분 처리(Snowflake Stream/Task 대체), build\_item은 listing을 FK 참조.



\## 아키텍처 요구사항

\- DB: 로컬 MySQL 8.0 (localhost:3306), 드라이버는 pymysql 사용

\- 접속은 .env의 price\_app 계정만 사용. root 사용 금지.

\- CREATE DATABASE는 이미 완료됨. DDL에서 DB/USER 생성 구문은 제외할 것.

\- 기존 src/common/mysql\_client.py(untracked)를 그대로 활용한다. 새 추상화 계층을 만들지 않는다.

\- 전환 방식: 각 모듈에서 snowflake\_client import → mysql\_client import로 직접 교체 + SQL 방언 변환.

\- 접속 정보는 .env로 분리, 코드에 하드코딩 금지.





\## 진행 규칙 (중요)

\- 작업 전체를 feature/mysql-migration 브랜치에서 진행한다.

\- 아래 Phase를 순서대로 진행하고, 각 Phase 완료 시 멈춰서 검증 결과를 보고할 것.

&#x20; 내 승인 없이 다음 Phase로 넘어가지 말 것.

\- Phase 4 전까지 snowflake\_client.py 및 기존 Snowflake 코드를 삭제하지 말 것.

\- 각 Phase에서 수정한 파일 목록과 검증 방법(실행 명령)을 함께 보고할 것.



\## Phase 0: 조사

\- snowflake\_client를 import하는 모든 파일과 SQL 사용 지점을 표로 정리.

\- Snowflake 전용 문법(MERGE, QUALIFY, STREAM 참조 등) 사용 위치 목록화.



\## Phase 1: mysql\_client 검증

\- src/common/mysql\_client.py 리뷰. MySQLSettings가 .env(price\_app)를 읽도록 연결.

\- 접속 테스트 스크립트로 로컬 MySQL(computer\_price) 연결 검증.

\- 검증 통과 후 feature 브랜치에 커밋. 기존 서비스 코드는 아직 수정하지 않는다.



\## Phase 2: 스키마 적용

\- 가이드라인 문서 기반 MySQL DDL 생성 및 적용.

\- SHOW TABLES / DESCRIBE로 전 테이블 생성 확인 보고.



\## Phase 3: 모듈별 전환 (데이터 흐름 순서)

\- 3-1: 크롤러 적재 경로를 DBClient로 전환 → 실제 크롤링 1회 실행, RAW 적재 행수 확인

\- 3-2: transform 전환 (MERGE → INSERT ... ON DUPLICATE KEY UPDATE,

&#x20;      Stream 의존 로직 → batch\_id 증분) → RAW→STAGING 정합 검증 (행수, NULL 비율)

\- 3-3: 집계/서빙 코드 전환 → 화면/쿼리 결과 확인

\- 각 단계마다 검증 결과 보고 후 대기.



\## Phase 4: 철거

\- snowflake\_client.py, Snowflake 전용 SQL, requirements의 snowflake 의존성 제거.

\- grep으로 'snowflake' 참조 0건 확인 결과 보고.

