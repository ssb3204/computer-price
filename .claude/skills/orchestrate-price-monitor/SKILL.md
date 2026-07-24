---
name: orchestrate-price-monitor
description: "컴퓨터 가격 모니터링 시스템의 에이전트 팀을 조율하는 오케스트레이터. 크롤러 수정/추가, MySQL 파이프라인 변경, Dash 대시보드 개발, 새 기능 추가, 버그 수정, 테스트 실행, 프로젝트 구조 파악, 면접 대비 문서화, 기술 개념 Q&A 등 이 프로젝트의 모든 작업 시 반드시 이 스킬을 사용. 후속 작업(수정, 보완, 다시 실행, 업데이트, 이전 결과 개선, 크롤러만 다시, 파이프라인만, 개념 추가, 섹션 보완)에도 반드시 이 스킬을 사용."
---

# 가격 모니터링 오케스트레이터

컴퓨터 가격 모니터링 시스템의 개발 작업을 분석하고 적절한 에이전트에 라우팅하는 통합 스킬.

## 실행 모드: 하이브리드

| Phase | 모드 | 이유 |
|-------|------|------|
| 단일 레이어 작업 | 서브 에이전트 | 독립적, 팀 통신 불필요 |
| 크로스-레이어 작업 | 에이전트 팀 | 레이어 간 데이터 의존성, 협업 필요 |

## 에이전트 구성

| 에이전트 | 역할 | 스킬 | 담당 경로 |
|---------|------|------|---------|
| crawler-agent | 크롤러 개발·수정 | crawl-price-sites | src/crawlers/ |
| pipeline-agent | MySQL/Airflow 파이프라인 | mysql-pipeline | src/pipeline/, src/airflow_dags/ |
| dashboard-agent | Dash 대시보드 | dash-dashboard | src/dashboard/ |
| qa-agent | 통합 테스트·검증 | qa-integration | tests/ |
| explorer-agent | 프로젝트 전체 탐색·문서화 | explore-project | 전체 |
| concept-tutor-agent | 기술 개념 Q&A 생성 | concept-interview-prep | 전체 |

## 워크플로우

### Phase 0: 컨텍스트 확인 (후속 작업 지원)

1. `_workspace/` 디렉토리 존재 여부 확인
2. 실행 모드 결정:
   - **`_workspace/` 미존재** → 초기 실행, Phase 1로 진행
   - **`_workspace/` 존재 + 부분 수정 요청** → 해당 에이전트만 재호출, 기존 산출물 중 수정 대상만 덮어씀
   - **`_workspace/` 존재 + 새 작업** → 기존 `_workspace/`를 `_workspace_{YYYYMMDD_HHMMSS}/`로 이동 후 새 실행

### Phase 1: 작업 분석

1. 사용자 요청에서 영향받는 레이어 파악:
   - 크롤러만 → crawler-agent
   - 파이프라인(SQL/DAG)만 → pipeline-agent
   - 대시보드만 → dashboard-agent
   - 2개 이상 레이어 → 에이전트 팀 구성
2. CLAUDE.md 개발 규칙 확인: 설계 먼저 제시, 사용자 확인 후 구현
3. `_workspace/` 생성 (신규 실행 시)

### Phase 2: 단일 레이어 작업 — 서브 에이전트 모드

**실행 모드:** 서브 에이전트

해당 전문 에이전트를 직접 호출한다:

```
Agent(
  subagent_type: "crawler-agent" | "pipeline-agent" | "dashboard-agent",
  model: "opus",
  prompt: "
    [스킬 로드] .claude/skills/{skill-name}/SKILL.md 를 읽고 지침을 따른다.
    [작업] {사용자 요청 상세}
    [산출물 경로] _workspace/02_{agent}_{artifact}.md 에 결과 저장
    [규칙] CLAUDE.md 개발 규칙 준수 (설계 먼저, 사용자 확인 후 구현)
  "
)
```

작업 완료 후 qa-agent를 서브에이전트로 호출하여 검증:
```
Agent(
  subagent_type: "qa-agent",
  model: "opus",
  prompt: "
    [스킬 로드] .claude/skills/qa-integration/SKILL.md 를 읽고 지침을 따른다.
    [검증 대상] {수정된 파일 경로}
    [산출물 경로] _workspace/03_qa_report.md 에 결과 저장
  "
)
```

### Phase 3: 크로스-레이어 작업 — 에이전트 팀 모드

**실행 모드:** 에이전트 팀

1. 팀 구성:
   ```
   TeamCreate(
     team_name: "price-monitor-team",
     members: [
       { name: "crawler", agent_type: "crawler-agent", model: "opus",
         prompt: ".claude/skills/crawl-price-sites/SKILL.md 를 읽고 담당 작업 수행" },
       { name: "pipeline", agent_type: "pipeline-agent", model: "opus",
         prompt: ".claude/skills/mysql-pipeline/SKILL.md 를 읽고 담당 작업 수행" },
       { name: "dashboard", agent_type: "dashboard-agent", model: "opus",
         prompt: ".claude/skills/dash-dashboard/SKILL.md 를 읽고 담당 작업 수행" },
       { name: "qa", agent_type: "qa-agent", model: "opus",
         prompt: ".claude/skills/qa-integration/SKILL.md 를 읽고 각 모듈 완성 직후 점진적 검증 수행" }
     ]
   )
   ```

2. 작업 등록:
   ```
   TaskCreate(tasks: [
     { title: "크롤러 수정", assignee: "crawler",
       description: "{크롤러 관련 상세 작업}" },
     { title: "파이프라인 수정", assignee: "pipeline",
       description: "{파이프라인 관련 상세 작업}",
       depends_on: ["크롤러 수정"] },
     { title: "대시보드 수정", assignee: "dashboard",
       description: "{대시보드 관련 상세 작업}",
       depends_on: ["파이프라인 수정"] },
     { title: "통합 검증", assignee: "qa",
       description: "전체 레이어 정합성 검증",
       depends_on: ["크롤러 수정", "파이프라인 수정", "대시보드 수정"] }
   ])
   ```

3. 팀원 자체 조율 대기 — 팀원이 유휴 상태가 되면 자동 알림 수신

4. 모든 작업 완료 후:
   - TaskGet으로 전체 완료 확인
   - qa 팀원의 검증 리포트 Read
   - SendMessage로 팀원에게 종료 요청
   - TeamDelete로 팀 정리

### Phase 4: 결과 보고

1. 수정된 파일 목록 요약
2. QA 검증 결과 요약
3. 사용자에게 최종 보고 (한국어)
4. `_workspace/` 보존

## 데이터 흐름

```
사용자 요청
    ↓
Phase 1: 레이어 파악
    ↓
단일 레이어? ──Yes──→ 서브 에이전트 (Phase 2)
    ↓ No                    ↓
에이전트 팀 (Phase 3)   qa 검증
    ↓                       ↓
결과 통합            결과 보고 (Phase 4)
    ↓
결과 보고 (Phase 4)
```

## 에러 핸들링

| 상황 | 전략 |
|------|------|
| 서브 에이전트 실패 | 1회 재시도. 재실패 시 사용자에게 에러 내용 보고 후 진행 여부 확인 |
| 팀원 1명 실패 | SendMessage로 상태 확인 → 재시작 또는 작업 재할당 |
| QA 검증 실패 | 해당 에이전트에게 실패 내용 전달 → 수정 → 재검증 |
| 설계 확인 필요 | 구현 전 사용자에게 설계 제시 후 승인 대기 |

## 테스트 시나리오

### 정상 흐름 (단일 레이어)
1. 사용자: "다나와 크롤러에서 가격 파싱이 안 돼"
2. Phase 1: 크롤러 레이어 단일 영향 파악
3. Phase 2: crawler-agent 서브에이전트 호출
4. crawler-agent가 `src/crawlers/danawa.py` 분석·수정
5. qa-agent가 수정 파일 검증
6. 결과 보고

### 에러 흐름 (크로스-레이어)
1. 사용자: "새 부품 카테고리(모니터) 추가해줘"
2. Phase 1: 크롤러 + 파이프라인 + 대시보드 전체 영향 파악
3. Phase 3: 에이전트 팀 구성
4. pipeline-agent가 DTO 변경으로 실패
5. SendMessage로 상태 확인 → 스키마 재확인 후 재시도
6. 재시도 성공 → 나머지 팀원 계속 진행
7. 최종 통합 검증 후 보고
