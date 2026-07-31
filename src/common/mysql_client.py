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

    size = size or int(os.getenv("DB_POOL_SIZE", "10"))
    _pool = PooledDB(
        creator=lambda: create_connection(settings),
        mincached=size,
        maxcached=size,
        maxconnections=size,
        blocking=True,  # 풀 소진 시 예외 대신 대기
        ping=0,  # 대여 시 ping 안 함 (RTT 41ms 추가 방지)
        reset=False,  # 반납 시 rollback 안 함 (autocommit이라 불필요)
    )
    logger.info("MySQL connection pool initialized (size=%d)", size)


@contextmanager
def get_connection(settings: MySQLSettings) -> Iterator[Connection]:
    conn = _pool.connection() if _pool is not None else create_connection(settings)
    try:
        yield conn
    finally:
        conn.close()  # 풀이 있으면 반납, 없으면 실제 종료
