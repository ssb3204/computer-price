"""MySQL connection factory (로컬 MySQL 8.0+).

기존 Snowflake 코드는 `cur.fetchall()` 결과를 `row[0]`, `row[1]`처럼 위치
인덱싱으로 사용하므로, DictCursor가 아닌 기본 튜플 커서를 사용한다.

API 프로세스는 시작 시 init_pool()로 커넥션 풀을 켠다. 크롤러/파이프라인은
init_pool()을 호출하지 않으므로 기존과 동일하게 매번 새 커넥션을 연다
(장시간 유휴 커넥션이 끊긴 장애 이력 때문에 의도적으로 유지).
"""

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

import pymysql
from pymysql.connections import Connection

from src.common.config import MySQLSettings

logger = logging.getLogger(__name__)

_pool = None


def create_connection(settings: MySQLSettings) -> Connection:
    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        charset="utf8mb4",
        autocommit=True,
        # crawled_at(앱이 채우는 UTC naive)과 DEFAULT CURRENT_TIMESTAMP 컬럼
        # (created_at/updated_at/loaded_at/failed_at)의 시각 기준을 통일한다.
        # 서버 SYSTEM 타임존(KST)에 의존하지 않도록 세션 타임존을 UTC로 고정.
        init_command="SET time_zone = '+00:00'",
    )


def init_pool(settings: MySQLSettings, size: int | None = None) -> None:
    """API 프로세스에서만 호출한다. 호출 전에는 기존 동작(매번 새 커넥션)."""
    global _pool
    if _pool is not None:
        return

    from dbutils.pooled_db import PooledDB

    # 20 은 부하 측정으로 정한 값이다. anyio threadpool 40 × 커넥션 점유율 50% ≈ 20 이고,
    # 워커 1개의 CPU 천장 378 RPS × 점유시간 55ms ≈ 21 로 두 계산이 같은 값에 수렴한다.
    # 20 을 넘겨도 처리량·지연이 개선되지 않는다(P=40 에서 +3%, 변동 범위).
    # 근거: scripts/perf/RESULTS.md
    size = size or int(os.getenv("DB_POOL_SIZE", "20"))

    # 기동 시 미리 여는 수. PooledDB 가 새 커넥션을 조건 락 안에서 순차로 만들기 때문에
    # size 만큼 열면 기동이 size × 350ms 걸린다(20 이면 7초, 그동안 요청을 받지 못한다).
    # 5 면 1.75초이고, 부족한 15개는 첫 부하에서 자란다. maxcached=size 라 한 번 자란
    # 뒤에는 닫히지 않으므로 램프업 비용을 1회만 낸다.
    # 부하 측정 시에는 DB_POOL_MIN_CACHED 를 DB_POOL_SIZE 와 같게 맞춰야 한다 —
    # 확장이 커넥션당 350ms 직렬이라 초기 계단이 부분 워밍 상태로 오염된다.
    min_cached = min(int(os.getenv("DB_POOL_MIN_CACHED", "5")), size)

    _pool = PooledDB(
        creator=lambda: create_connection(settings),
        mincached=min_cached,
        maxcached=size,
        maxconnections=size,
        blocking=True,  # 풀 소진 시 예외 대신 대기
        # 대여 시 생존 검사를 하지 않는다. ping=1 은 대여마다 왕복 1회를 더해
        # 실측 50.1ms → 100.0ms (+49.9ms, 정확히 2배)가 된다. 유휴 커넥션 staleness 는
        # 검사가 아니라 수명 관리나 예외 기반 재시도로 다뤄야 한다(미구현).
        ping=0,
        reset=False,  # 반납 시 rollback 안 함 (autocommit이라 불필요)
    )
    logger.info("MySQL connection pool initialized (size=%d, mincached=%d)", size, min_cached)


@contextmanager
def get_connection(settings: MySQLSettings) -> Iterator[Connection]:
    conn = _pool.connection() if _pool is not None else create_connection(settings)
    try:
        yield conn
    finally:
        conn.close()  # 풀이 있으면 반납, 없으면 실제 종료
