"""Snowflake connection and batch operations."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.common.config import SnowflakeSettings

logger = logging.getLogger(__name__)


def create_connection(settings: SnowflakeSettings) -> SnowflakeConnection:
    return snowflake.connector.connect(
        account=settings.account,
        user=settings.user,
        password=settings.password,
        warehouse=settings.warehouse,
        database=settings.database,
        schema=settings.schema_name,
    )


@contextmanager
def get_connection(settings: SnowflakeSettings) -> Iterator[SnowflakeConnection]:
    conn = create_connection(settings)
    try:
        yield conn
    finally:
        conn.close()


def batch_insert(
    conn: SnowflakeConnection,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"

    cursor = conn.cursor()
    try:
        cursor.executemany(sql, rows)
        return len(rows)
    finally:
        cursor.close()
