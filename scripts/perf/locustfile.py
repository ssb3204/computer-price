"""부하 측정용 Locust 시나리오.

시나리오는 반드시 `--tags` 로 **하나만** 골라 실행한다. 태그를 안 주면 locust 가
세 task 를 무작위로 섞어 돌리는데, 커넥션 점유율이 다른 경로가 섞이면
점유시간 평균이 뭉개져 최적 풀 크기가 나오지 않는다.

| 태그 | 경로 | 점유시간 | 성격 |
|------|------|---------|------|
| s0 | `/health` | 2.5ms | DB 없음 — 측정 환경 천장 대조군 |
| s1 | `/builds` | 52ms (RTT 1회) | 점유율 ≈100% — **풀 크기 스윕 주력** |
| s2 | `/builds/{id}/price-trend` | 167ms (RTT 3회) | 점유시간 긴 케이스 |

`/crawl/search` 는 절대 넣지 않는다 — 다나와·컴퓨존·견적왕을 실시간 호출하므로
동시 부하를 걸면 우리가 그 3사를 공격하는 셈이다(GitHub Actions IP 차단 이력 있음).
로그인·회원가입도 제외한다 — bcrypt(cost 12)가 요청당 수백 ms CPU 를 쓰는데 이건
GIL 에 묶이는 CPU 작업이라 DB 대기와 성격이 정반대고, 쓰기라 실데이터를 오염시킨다.

기본 HttpUser(requests) 대신 FastHttpUser(geventhttpclient)를 쓴다.
전자는 요청당 CPU 가 커서 동시 80 부근에서 Locust 자신이 병목이 된다.

실행:
    # 스모크 — 파이프 작동 확인 (계단 없음)
    PYTHONUTF8=1 locust -f scripts/perf/locustfile.py --host http://localhost:8001 \
        --tags s1 --headless -u 2 -r 2 -t 20s

    # 계단식 — RPS 곡선과 꺾임 위치 (단일 프로세스로 충분)
    PYTHONUTF8=1 PERF_SHAPE=1 PERF_STAGE_USERS=1,2,5,8,10,13,16,20,30,40 \
    locust -f scripts/perf/locustfile.py --host http://localhost:8001 \
        --tags s1 --headless --csv scripts/perf/out/s1

    # 분산 — 정확한 p95 (단일 프로세스는 지연을 과소보고한다, RESULTS.md 1단계 참고)
    #   master 는 --expect-workers 4, worker 4개는 --worker 로 따로 띄운다

환경변수:
    PERF_SHAPE=1        계단 활성. 없으면 -u/-r/-t 로 수동 지정
    PERF_STAGE_SEC      계단 1칸 길이(초), 기본 30
    PERF_STAGE_USERS    계단 동시 사용자 목록, 기본 1,5,10,20,40,80,160
    PERF_BUILD_ID       s2 가 쓸 조합 id, 기본 401

주의: LoadTestShape 가 정의돼 있으면 -u/-r/-t 를 덮어쓴다. 그래서 PERF_SHAPE 로
      클래스 정의 자체를 켜고 끈다 — 스모크와 계단을 같은 파일로 쓰기 위함.
주의: PYTHONUTF8=1 없이 실행하면 locust 가 pyproject.toml(한글 주석)을 cp949 로
      읽다 죽는다.
"""

import os

from locust import FastHttpUser, LoadTestShape, constant, tag, task

# 계단 1칸 길이(초). 짧으면 램프업 과도상태가 평균에 섞이고, 길면 총 시간이 늘어난다.
STAGE_SEC = int(os.getenv("PERF_STAGE_SEC", "30"))

# 동시 사용자 계단. 꺾임이 예상되는 값 주변을 촘촘하게 잡아야 위치가 특정된다.
# DB 경로에서는 상한을 2~4 × 풀크기로 두면 충분하다 — 그 이상은 큐만 깊어지고 시간만 쓴다.
STAGE_USERS = tuple(int(u) for u in os.getenv("PERF_STAGE_USERS", "1,5,10,20,40,80,160").split(","))

# s2 대상 조합. 401 = 항목 4개, 442 = 항목 2개 (둘 다 RTT 3회라 N+1 은 없다)
BUILD_ID = int(os.getenv("PERF_BUILD_ID", "401"))


class PerfUser(FastHttpUser):
    """읽기 전용 경로만 두드린다. 태그로 시나리오를 하나 골라 쓴다."""

    # 생각시간 0 = 포화점을 최단 시간에 드러낸다. 실사용 흉내가 목적이 아니다.
    wait_time = constant(0)

    @tag("s0")
    @task
    def health(self) -> None:
        """DB 를 타지 않는 경로. 측정 환경의 상한을 잡는 대조군."""
        self.client.get("/health", name="s0 /health")

    @tag("s1")
    @task
    def list_builds(self) -> None:
        """쿼리 1회, 인증 없음, 응답 466B. 점유율이 가장 높아 풀 크기 효과가 선명하다."""
        self.client.get("/builds", name="s1 /builds")

    @tag("s2")
    @task
    def price_trend(self) -> None:
        """쿼리 다수 + 총액 추이 집계. 점유시간이 길어 같은 풀로 처리량이 낮아야 한다."""
        self.client.get(f"/builds/{BUILD_ID}/price-trend", name="s2 /builds/{id}/price-trend")


if os.getenv("PERF_SHAPE") == "1":

    class Staircase(LoadTestShape):
        """동시 사용자를 계단식으로 올린다. RPS 가 평평해지는 높이가 처리량 천장,
        평평해지기 시작하는 동시 사용자 수가 실측된 유효 풀 크기다."""

        def tick(self) -> tuple[int, int] | None:
            elapsed = self.get_run_time()
            for step, users in enumerate(STAGE_USERS, start=1):
                if elapsed < STAGE_SEC * step:
                    # spawn_rate 를 users 와 같게 두면 계단 전환이 1초 안에 끝나
                    # 과도구간이 짧아진다. locust 는 부족한 만큼만 추가 생성한다.
                    return users, users
            return None  # 모든 계단 소진 → 테스트 종료
