"""PooledDB 파라미터 분리 동작과 ping 비용을 실측한다.

컨테이너 안에서 실행한다(호스트에는 DBUtils 가 없다):
    docker compose exec -T api python - < scripts/perf/verify_pool_params.py

확인 대상 두 가지.

1) mincached / maxcached / maxconnections 를 분리하면 위험만 줄고 처리량은 유지되는가
   - 기동 시 열리는 수     = mincached
   - 부하 중 최대          = maxconnections
   - 부하 후 유휴로 남는 수 = maxcached  (초과분은 반납 시 close 된다)

2) ping=1(대여 시 생존 검사)이 대여당 왕복 1회를 추가하는가
"""

import threading
import time

from dbutils.pooled_db import PooledDB

from src.common.config import MySQLSettings
from src.common.mysql_client import create_connection

SETTINGS = MySQLSettings()
_mon = create_connection(SETTINGS)
_mcur = _mon.cursor()


def live() -> int:
    """현재 MySQL 커넥션 수(모니터 자신 제외)."""
    _mcur.execute("SHOW STATUS LIKE 'Threads_connected'")
    return int(_mcur.fetchone()[1]) - 1


BASE = live()  # 실행 중인 API 의 풀(20개)이 이미 잡혀 있다
print(f"기준선(API 풀 등) = {BASE}\n")

print("=" * 62)
print("[1] mincached=2, maxcached=5, maxconnections=20 분리 동작")
print("=" * 62)

t0 = time.perf_counter()
pool = PooledDB(
    creator=lambda: create_connection(SETTINGS),
    mincached=2,
    maxcached=5,
    maxconnections=20,
    blocking=True,
    ping=0,
    reset=False,
)
init_sec = time.perf_counter() - t0
print(f"기동 시간          {init_sec:5.2f}s   (mincached=20 이면 20×350ms≈7s)")
print(f"기동 직후 커넥션   {live() - BASE:5d}     (기대 2 = mincached)")

hold = threading.Event()
peak = [0]


def worker() -> None:
    conn = pool.connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    hold.wait(3.0)  # 전원이 동시에 쥐고 있도록 붙잡아 둔다
    conn.close()


threads = [threading.Thread(target=worker) for _ in range(20)]
for t in threads:
    t.start()
time.sleep(2.0)  # 20개가 모두 커넥션을 확보할 시간
peak[0] = live() - BASE
print(f"동시 20 대여 중    {peak[0]:5d}     (기대 20 = maxconnections)")

hold.set()
for t in threads:
    t.join()
time.sleep(0.5)
print(f"전원 반납 후 유휴  {live() - BASE:5d}     (기대 5 = maxcached, 초과 15개는 close)")

print()
print("=" * 62)
print("[2] ping=0 vs ping=1 대여 비용")
print("=" * 62)


def bench(ping: int, n: int = 20) -> float:
    """대여 → 쿼리 1회 → 반납 을 n 회. 매번 idle_cache 를 거치므로 ping 이 걸린다."""
    p = PooledDB(
        creator=lambda: create_connection(SETTINGS),
        mincached=1,
        maxcached=1,
        maxconnections=1,
        blocking=True,
        ping=ping,
        reset=False,
    )
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


p0 = bench(0)
p1 = bench(1)
print(f"ping=0  대여+쿼리  {p0:6.1f}ms")
print(f"ping=1  대여+쿼리  {p1:6.1f}ms")
print(f"ping 추가 비용     {p1 - p0:+6.1f}ms  ({(p1 / p0 - 1) * 100:+.0f}%)")

_mon.close()
