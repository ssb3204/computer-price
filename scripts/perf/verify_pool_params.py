"""PooledDB 파라미터 분리 동작과 ping 비용을 실측한다.

컨테이너 안에서 실행한다(호스트에는 DBUtils 가 없다):
    docker compose exec -T api python - < scripts/perf/verify_pool_params.py

확인 대상 두 가지.

1) mincached / maxcached / maxconnections 를 분리하면 위험만 줄고 처리량은 유지되는가
   - 기동 시 열리는 수     = mincached
   - 부하 중 최대          = maxconnections
   - 부하 후 유휴로 남는 수 = maxcached  (초과분은 반납 시 close 된다)

2) ping=1(대여 시 생존 검사)이 대여당 왕복 1회를 추가하는가

부하를 걸고 커넥션을 여러 개 여는 스크립트이므로 import 만으로 실행되지 않게
main() 가드 안에 둔다. 만든 풀은 finally 에서 반드시 닫는다.
"""

import threading
import time

from dbutils.pooled_db import PooledDB
from pymysql.connections import Connection

from src.common.config import MySQLSettings
from src.common.mysql_client import create_connection


def live(cur, base: int = 0) -> int:
    """현재 MySQL 커넥션 수(모니터 자신 제외). base 를 주면 기준선 대비 증가분."""
    cur.execute("SHOW STATUS LIKE 'Threads_connected'")
    return int(cur.fetchone()[1]) - 1 - base


def wait_until(cur, base: int, target: int, timeout: float = 20.0) -> int:
    """커넥션 수가 target 에 닿을 때까지 기다린다.

    고정 sleep 을 쓰면 안 된다. 풀 확장이 커넥션당 약 350ms 씩 **직렬**이라
    2 → 20 은 18 × 350ms ≈ 6.3초가 걸리는데, 짧게 잡으면 확장 도중의 값을
    최댓값으로 착각한다(2초면 8개에서 멈춘 것으로 보인다).
    """
    deadline = time.monotonic() + timeout
    n = live(cur, base)
    while n < target and time.monotonic() < deadline:
        time.sleep(0.2)
        n = live(cur, base)
    return n


def check_param_separation(settings: MySQLSettings, mon: Connection, base: int) -> None:
    """mincached / maxcached / maxconnections 가 각각 다른 시점을 통제하는지 확인한다."""
    print("=" * 62)
    print("[1] mincached=2, maxcached=5, maxconnections=20 분리 동작")
    print("=" * 62)

    cur = mon.cursor()
    t0 = time.perf_counter()
    pool = PooledDB(
        creator=lambda: create_connection(settings),
        mincached=2,
        maxcached=5,
        maxconnections=20,
        blocking=True,
        ping=0,
        reset=False,
    )
    try:
        init_sec = time.perf_counter() - t0
        print(f"기동 시간          {init_sec:5.2f}s   (mincached=20 이면 20×350ms≈7s)")
        print(f"기동 직후 커넥션   {live(cur, base):5d}     (기대 2 = mincached)")

        hold = threading.Event()

        def worker() -> None:
            conn = pool.connection()
            c = conn.cursor()
            c.execute("SELECT 1")
            c.fetchall()
            # 전원이 동시에 쥐고 있도록 붙잡아 둔다. 측정이 끝나면 hold.set() 으로
            # 즉시 풀리므로 이 값은 상한일 뿐이다.
            hold.wait(30.0)
            conn.close()

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        try:
            peak = wait_until(cur, base, target=20)
            print(f"동시 20 대여 중    {peak:5d}     (기대 20 = maxconnections)")
        finally:
            hold.set()
            for t in threads:
                t.join()

        time.sleep(0.5)
        print(f"전원 반납 후 유휴  {live(cur, base):5d}     (기대 5 = maxcached, 초과 15개는 close)")
    finally:
        pool.close()


def bench(settings: MySQLSettings, ping: int, n: int = 20) -> float:
    """대여 → 쿼리 1회 → 반납 을 n 회. 매번 idle_cache 를 거치므로 ping 이 걸린다."""
    p = PooledDB(
        creator=lambda: create_connection(settings),
        mincached=1,
        maxcached=1,
        maxconnections=1,
        blocking=True,
        ping=ping,
        reset=False,
    )
    try:
        samples = []
        for _ in range(n):
            t = time.perf_counter()
            c = p.connection()
            cur = c.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            c.close()
            samples.append((time.perf_counter() - t) * 1000)
        samples.sort()
        return samples[len(samples) // 2]
    finally:
        p.close()


def check_ping_cost(settings: MySQLSettings) -> None:
    """ping=1 이 대여마다 왕복 1회를 더하는지 확인한다."""
    print()
    print("=" * 62)
    print("[2] ping=0 vs ping=1 대여 비용")
    print("=" * 62)

    p0 = bench(settings, ping=0)
    p1 = bench(settings, ping=1)
    print(f"ping=0  대여+쿼리  {p0:6.1f}ms")
    print(f"ping=1  대여+쿼리  {p1:6.1f}ms")
    print(f"ping 추가 비용     {p1 - p0:+6.1f}ms  ({(p1 / p0 - 1) * 100:+.0f}%)")


def main() -> None:
    settings = MySQLSettings()
    mon = create_connection(settings)
    try:
        base = live(mon.cursor())  # 실행 중인 API 의 풀(20개)이 이미 잡혀 있다
        print(f"기준선(API 풀 등) = {base}\n")

        check_param_separation(settings, mon, base)
        check_ping_cost(settings)
    finally:
        mon.close()


if __name__ == "__main__":
    main()
