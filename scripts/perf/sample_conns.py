"""부하 중 MySQL 커넥션 수를 표본 추출한다.

커넥션 1개를 잡고 유지하며 주기적으로 조회하므로, 표본 추출 자체가 만드는
커넥션은 1개로 고정된다(그 1개는 출력에서 빼서 보고한다).

사용:
    python scripts/perf/sample_conns.py [샘플수] [간격초]
"""

import sys
import time

from src.common.config import MySQLSettings
from src.common.mysql_client import create_connection

SELF = 1  # 이 스크립트가 쓰는 커넥션


def main() -> None:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    gap = float(sys.argv[2]) if len(sys.argv) > 2 else 2.0

    conn = create_connection(MySQLSettings())
    cur = conn.cursor()
    peak = 0
    print(f"{'t(s)':>6} {'api 커넥션':>11}  (표본추출용 1개 제외)")
    t0 = time.perf_counter()
    for _ in range(n):
        cur.execute("SHOW STATUS LIKE 'Threads_connected'")
        live = int(cur.fetchone()[1]) - SELF
        peak = max(peak, live)
        print(f"{time.perf_counter() - t0:>6.1f} {live:>11}")
        time.sleep(gap)
    conn.close()
    print(f"\n최대 동시 커넥션 = {peak}")


if __name__ == "__main__":
    main()
