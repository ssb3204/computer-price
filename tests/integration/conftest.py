"""Integration test fixtures — real Snowflake connection required."""

import pytest
from dotenv import load_dotenv

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection

load_dotenv()

TEST_PREFIX = "IT_TEST_"


def _cleanup(settings: SnowflakeSettings) -> None:
    """IT_TEST_ 로 시작하는 테스트 데이터 전부 삭제 (FK 순서 준수)."""
    with get_connection(settings) as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT PRODUCT_ID FROM STAGING.STG_PRODUCTS WHERE NAME LIKE %s",
            (TEST_PREFIX + "%",),
        )
        test_pids = [row[0] for row in cur.fetchall()]

        if test_pids:
            placeholders = ", ".join(["%s"] * len(test_pids))
            # Analytics (PRODUCT_STATS has FK → STG_PRODUCTS)
            cur.execute(
                f"DELETE FROM ANALYTICS.PRODUCT_STATS WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                f"DELETE FROM ANALYTICS.DAILY_SUMMARY WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                f"DELETE FROM ANALYTICS.WEEKLY_SUMMARY WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            # Staging children
            cur.execute(
                f"DELETE FROM STAGING.STG_ALERTS WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                f"DELETE FROM STAGING.STG_DAILY_PRICES WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                f"DELETE FROM STAGING.STG_LATEST_PRICES WHERE PRODUCT_ID IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                "DELETE FROM STAGING.STG_PRODUCTS WHERE NAME LIKE %s",
                (TEST_PREFIX + "%",),
            )

        cur.execute(
            "DELETE FROM RAW.RAW_CRAWLED_PRICES WHERE PRODUCT_NAME LIKE %s",
            (TEST_PREFIX + "%",),
        )
        cur.close()


@pytest.fixture(scope="session")
def snowflake_settings():
    return SnowflakeSettings()


@pytest.fixture(scope="session")
def snowflake_conn(snowflake_settings):
    with get_connection(snowflake_settings) as conn:
        yield conn


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_data(snowflake_settings):
    _cleanup(snowflake_settings)  # 세션 시작 전 1회
    yield
    _cleanup(snowflake_settings)  # 세션 종료 후 1회
