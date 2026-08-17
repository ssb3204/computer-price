"""Locust 계단식 결과를 계단별로 집계하고 2구간 모델 예측과 비교한다.

사용:
    python scripts/perf/analyze.py <stats_history.csv> [풀크기] [점유시간ms] [최소응답ms]

모델 — 처리량은 두 제약의 최솟값이다.

    RPS = min( users / R0 ,  P / H )
                 ^^^^^^^^^^   ^^^^^
                 동시성 제약    풀 제약(천장)

    R0 = 무부하 응답시간(50ms). 동시 사용자가 적으면 각자 순차로 돌아 이 값이 지배한다.
    H  = 포화 시 커넥션 점유시간(55ms). 순수 RTT(40.7ms)보다 큰 이유는 고동시성에서
         스레드가 커넥션을 쥔 채 CPU/GIL 을 기다리기 때문이다 — DB 가 느려진 게 아니다.

각 계단의 앞 2개 스냅샷(램프업 과도구간)은 버린다.
"""

import csv
import statistics as st
import sys


def main() -> None:
    path = sys.argv[1]
    pool = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    hold = (float(sys.argv[3]) if len(sys.argv) > 3 else 55.0) / 1000
    base = (float(sys.argv[4]) if len(sys.argv) > 4 else 50.0) / 1000

    rows = [r for r in csv.DictReader(open(path, encoding="utf-8")) if r["Name"] == "Aggregated"]
    by_users: dict[int, list[dict[str, str]]] = {}
    for r in rows:
        users = int(r["User Count"])
        if users:
            by_users.setdefault(users, []).append(r)

    ceiling = pool / hold
    print(f"P={pool}, 점유시간 H={hold * 1000:.0f}ms, 무부하응답 R0={base * 1000:.0f}ms")
    print(f"→ 예측 천장 = P/H = {ceiling:.0f} RPS")
    print(f"{'users':>6} {'RPS':>7} {'예측':>7} {'오차':>7} {'p50':>6} {'p95':>6} {'커넥션수요':>10} {'fail/s':>7}")
    for users in sorted(by_users):
        snaps = by_users[users][2:] or by_users[users]
        col = lambda k: [float(s[k]) for s in snaps if s[k] not in ("", "N/A")]  # noqa: E731
        rps = st.mean(col("Requests/s"))
        pred = min(users / base, ceiling)
        # 실측 RPS 로 역산한 동시 커넥션 수요. P 에 닿으면 풀이 병목이라는 뜻이다.
        demand = rps * hold
        flag = " ←포화" if demand >= pool * 0.95 else ""
        print(
            f"{users:>6} {rps:>7.1f} {pred:>7.0f} {rps / pred - 1:>+6.0%} "
            f"{st.mean(col('50%')):>6.0f} {st.mean(col('95%')):>6.0f} "
            f"{demand:>10.1f} {st.mean(col('Failures/s')):>7.2f}{flag}"
        )


if __name__ == "__main__":
    main()
