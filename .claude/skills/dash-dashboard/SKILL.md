---
name: dash-dashboard
description: "Dash/Plotly 웹 대시보드 개발·수정 스킬. 새 페이지 추가, 차트 수정, 콜백 구현, Snowflake 쿼리 최적화, 레이아웃 변경 등 대시보드 관련 모든 작업 시 반드시 이 스킬을 사용. (localhost:8050)"
---

# Dash 대시보드 스킬

Dash/Plotly 기반 가격 모니터링 웹 대시보드를 개발·수정하는 지침.

## 프로젝트 구조

```
src/dashboard/
├── app.py                      — Dash 앱 인스턴스, 라우팅
├── callbacks.py                — 전체 콜백 등록
├── helpers.py                  — 공용 헬퍼 함수
├── layouts/
│   ├── overview.py             — 개요 페이지
│   ├── prices.py               — 가격 현황 페이지
│   ├── trends.py               — 트렌드 페이지
│   ├── alerts.py               — 알림 페이지
│   ├── stats.py                — 통계 페이지
│   ├── watchlist.py            — 관심 목록 페이지
│   └── pipeline.py             — 파이프라인 모니터링 페이지
└── data_access/
    └── snowflake_queries.py    — Snowflake 쿼리 함수
```

## 핵심 패턴

### 1. 레이아웃 파일 구조

각 페이지는 `layouts/` 하위에 별도 파일로 분리한다. 파일 상단에 레이아웃 함수, 하단에 콜백 등록 함수를 둔다.

새 페이지 추가 시:
1. `layouts/{name}.py` 생성
2. `app.py`에 라우팅 추가
3. `callbacks.py`에 콜백 임포트 추가

### 2. Snowflake 쿼리 원칙

- Analytics 레이어만 조회 (`ANALYTICS.*` 또는 `STAGING.*` 뷰)
- Raw/Staging 직접 조회 금지 — 대시보드 성능과 레이어 분리 원칙
- 쿼리 함수는 `data_access/snowflake_queries.py`에 집중

### 3. 콜백 순수성

콜백은 입력 → 출력 변환만 담당한다:
- 사이드이펙트(파일 쓰기, 전역 상태 변경) 없이 작성
- 무거운 Snowflake 쿼리는 `@dash.callback` 내부가 아닌 `data_access/` 함수로 분리

### 4. Plotly 차트

- 가격 트렌드: `go.Scatter` (line)
- 사이트 비교: `go.Bar`
- 변동 알림 표시: `go.Indicator` 또는 배경색 변경
- 공통 레이아웃 설정은 `helpers.py`에서 관리

## 작업 절차

1. 관련 레이아웃 파일과 `snowflake_queries.py` 읽기
2. 설계 제시 → 사용자 확인 → 구현
3. 레이아웃 → 쿼리 → 콜백 순서로 단계별 구현
4. 각 단계 완료 후 Docker 환경에서 실제 동작 확인 (localhost:8050)

## 디버깅 체크리스트

대시보드 오류 시:
- [ ] Docker 서비스 실행 여부 확인 (`docker compose ps`)
- [ ] Snowflake 연결 환경변수 확인
- [ ] 콜백 ID 충돌 여부 확인 (중복 Output ID)
- [ ] 쿼리 대상 테이블이 Analytics 레이어인지 확인
- [ ] 빈 데이터 핸들링 (쿼리 결과 0건 시 차트 동작)
