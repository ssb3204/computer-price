"""Snowflake connection factory."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager

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
