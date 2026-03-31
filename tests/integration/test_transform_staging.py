"""Integration test: transform_staging() — Staging 변환 검증."""

import pytest

from run_pipeline import load_raw, transform_staging
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_load_raw import _make_raw


@pytest.mark.integration
def test_transform_staging_creates_product(snowflake_settings, snowflake_conn):
    """transform_staging() 후 STG_PRODUCTS에 상품이 생성되는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}GPU_001", "500,000원")])

    count = transform_staging(snowflake_settings)

    assert count >= 1
    cur = snowflake_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM STAGING.STG_PRODUCTS WHERE NAME LIKE %s",
        (TEST_PREFIX + "%",),
    )
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_creates_daily_price(snowflake_settings, snowflake_conn):
    """transform_staging() 후 STG_DAILY_PRICES에 가격 row가 생성되는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}RAM_001", "80,000원")])
    transform_staging(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM STAGING.STG_DAILY_PRICES dp
        JOIN STAGING.STG_PRODUCTS p ON dp.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_marks_raw_processed(snowflake_settings, snowflake_conn):
    """transform_staging() 후 RAW IS_PROCESSED = TRUE로 표시되는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}SSD_001", "120,000원")])
    transform_staging(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute(
        "SELECT IS_PROCESSED FROM RAW.RAW_CRAWLED_PRICES WHERE PRODUCT_NAME LIKE %s",
        (TEST_PREFIX + "%",),
    )
    rows = cur.fetchall()
    assert all(row[0] is True for row in rows)
    cur.close()


@pytest.mark.integration
def test_transform_staging_filters_anomaly_price(snowflake_settings, snowflake_conn):
    """이상치 가격(1원)은 Staging에 올라가지 않는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}ANOMALY_001", "1원")])

    count = transform_staging(snowflake_settings)

    assert count == 0
    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM STAGING.STG_DAILY_PRICES dp
        JOIN STAGING.STG_PRODUCTS p ON dp.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME = %s
    """, (f"{TEST_PREFIX}ANOMALY_001",))
    assert cur.fetchone()[0] == 0
    cur.close()
