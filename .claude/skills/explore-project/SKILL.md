---
name: explore-project
description: "프로젝트 전체 구조·코드·MySQL 스키마·데이터 흐름·설계 결정을 세밀하게 탐색하여 project-deep-dive.md를 생성하는 스킬. 프로젝트 파악, 코드 분석, 구조 설명, 면접 준비, 프로젝트 정리 요청 시 반드시 이 스킬을 사용. 후속 작업(특정 모듈만 다시, 스키마 업데이트, 내용 보완)에도 반드시 이 스킬을 사용."
---

# 프로젝트 심층 탐색 스킬

프로젝트를 처음 보는 사람이 완전히 이해할 수 있도록, 전체 코드베이스를 탐색하여 구조화된 문서를 생성한다.

## 탐색 순서

추측 없이 실제 파일을 읽는 순서를 따른다:

1. **루트 구조 파악** — `CLAUDE.md`, `docker-compose.yml`, `requirements*.txt` 읽기
2. **공통 모델/설정** — `src/common/` 전체 (models.py, config.py, mysql_client.py)
3. **크롤러** — `src/crawlers/` 전체 (base.py → 각 사이트 크롤러 → parser_utils.py)
4. **파이프라인** — `src/pipeline/` 전체 (crawl → load_raw → transform → quality → detect → slack → observability)
5. **Airflow DAG** — `src/airflow_dags/` (DAG 구조, 태스크 의존성)
6. **대시보드** — `src/dashboard/` 전체 (app.py → layouts/ → callbacks.py → data_access/)
7. **테스트** — `tests/` 전체

## 산출물 구조 (`project-deep-dive.md`)

다음 섹션을 순서대로 작성한다:

### 1. 프로젝트 개요
- 목적, 기술 스택 요약, 시스템 경계

### 2. 전체 아키텍처 다이어그램
- 텍스트 기반 다이어그램으로 레이어 간 흐름 표현

### 3. 디렉토리 구조
- 각 디렉토리·파일의 역할을 한 줄 설명

### 4. 데이터 흐름 (엔드투엔드)
- 크롤러 실행부터 대시보드 표시까지 단계별 추적
- 각 단계의 입력/출력 데이터 형태 명시

### 5. MySQL 스키마
코드에서 직접 추출한 테이블별 스키마:
```
테이블명 (레이어)
├── 컬럼명: 타입 — 설명
├── ...
└── 키: MERGE 키 / PK 명시
```

### 6. 핵심 모듈 상세
각 모듈(크롤러, 파이프라인 단계, 대시보드 페이지)에 대해:
- 역할
- 핵심 함수/클래스와 동작 원리
- 주의사항 (예: 광고 필터 로직, Stream 소비 방식)

### 7. 핵심 설계 결정
각 결정에 대해:
- **결정**: 무엇을 선택했는가
- **이유**: 왜 이 선택을 했는가
- **트레이드오프**: 무엇을 포기했는가

설계 결정 예시 (코드에서 확인 후 작성):
- MERGE 멱등성 전략
- 미처리 조인 방식(증분 처리) 채택 이유
- frozen dataclass DTO
- LAG() 윈도우 함수 변경 감지
- Airflow 스케줄링 방식

### 8. Docker Compose 서비스 구성
- 각 서비스의 역할, 포트, 의존성

### 9. 환경 변수 및 설정
- `config.py` 기반 설정 구조

## 작성 원칙

- 추측하지 않는다 — 모든 내용은 실제 파일에서 확인한 사실이어야 한다
- 스키마는 SQL MERGE 쿼리, Python dataclass, CREATE TABLE 문에서 직접 추출한다
- 설계 결정의 "이유"는 코드 주석, CLAUDE.md, 코드 패턴에서 근거를 찾는다
- 불명확한 항목은 "(코드에서 확인 필요)" 또는 "(추정)" 표시를 남긴다

## 후속 실행 처리

`project-deep-dive.md`가 이미 존재하면:
- 부분 보완 요청 → 해당 섹션만 업데이트
- 전체 재탐색 요청 → 기존 파일을 `project-deep-dive_{YYYYMMDD}.md`로 보관 후 새로 생성
