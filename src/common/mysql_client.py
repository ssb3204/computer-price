"""MySQL connection factory (로컬 MySQL 8.0+).

기존 Snowflake 코드는 `cur.fetchall()` 결과를 `row[0]`, `row[1]`처럼 위치
인덱싱으로 사용하므로, DictCursor가 아닌 기본 튜플 커서를 사용한다.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import pymysql
from pymysql.connections import Connection

from src.common.config import MySQLSettings

logger = logging.getLogger(__name__)


def create_connection(settings: MySQLSettings) -> Connection:
    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        charset="utf8mb4",
        autocommit=True,
    )


@contextmanager
def get_connection(settings: MySQLSettings) -> Iterator[Connection]:
    conn = create_connection(settings)
    try:
        yield conn
    finally:
        conn.close()
