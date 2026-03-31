"""Integration test: aggregate_analytics() — Analytics 집계 검증."""

import pytest

from run_pipeline import aggregate_analytics, load_raw, transform_staging
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_load_raw import _make_raw


def _setup_staging(settings):
    """Raw 적재 + Staging 변환까지 공통 준비."""
    load_raw(settings, [_make_raw(f"{TEST_PREFIX}MOBO_001", "200,000원")])
    transform_staging(settings)


@pytest.mark.integration
def test_aggregate_analytics_creates_product_stats(snowflake_settings, snowflake_conn):
    """aggregate_analytics() 후 PRODUCT_STATS에 row가 생성되는지 확인."""
    _setup_staging(snowflake_settings)

    aggregate_analytics(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM ANALYTICS.PRODUCT_STATS ps
        JOIN STAGING.STG_PRODUCTS p ON ps.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_aggregate_analytics_creates_daily_summary(snowflake_settings, snowflake_conn):
    """aggregate_analytics() 후 DAILY_SUMMARY에 row가 생성되는지 확인."""
    _setup_staging(snowflake_settings)

    aggregate_analytics(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM ANALYTICS.DAILY_SUMMARY ds
        JOIN STAGING.STG_PRODUCTS p ON ds.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_aggregate_analytics_correct_values(snowflake_settings, snowflake_conn):
    """PRODUCT_STATS의 ALL_TIME_LOW/HIGH가 실제 가격과 일치하는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}PSU_001", "150,000원")])
    transform_staging(snowflake_settings)

    aggregate_analytics(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT ps.ALL_TIME_LOW, ps.ALL_TIME_HIGH
        FROM ANALYTICS.PRODUCT_STATS ps
        JOIN STAGING.STG_PRODUCTS p ON ps.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME = %s
    """, (f"{TEST_PREFIX}PSU_001",))
    row = cur.fetchone()
    cur.close()

    assert row is not None
    assert row[0] == 150000  # ALL_TIME_LOW
    assert row[1] == 150000  # ALL_TIME_HIGH
