"""Integration test: transform_staging() — Staging 변환 검증."""

import pytest

from run_pipeline import load_raw, transform_staging
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_load_raw import _make_raw


@pytest.mark.integration
def test_transform_staging_creates_product(snowflake_settings, snowflake_conn):
    """transform_staging() 후 PRODUCTS에 상품이 생성되는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}GPU_001", "500,000원")])

    count = transform_staging(snowflake_settings)

    assert count >= 1
    cur = snowflake_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM STAGING.PRODUCTS WHERE PRODUCT_NAME LIKE %s",
        (TEST_PREFIX + "%",),
    )
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_creates_daily_price(snowflake_settings, snowflake_conn):
    """transform_staging() 후 PRICE_HISTORY에 가격 row가 생성되는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}RAM_001", "80,000원")])
    transform_staging(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM STAGING.PRICE_HISTORY dp
        JOIN STAGING.PRODUCTS p ON dp.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_consumes_stream(snowflake_settings, snowflake_conn):
    """transform_staging() 후 Stream이 비어있는지 확인 (Stream 소비 검증)."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}SSD_001", "120,000원")])
    transform_staging(snowflake_settings)

    cur = snowflake_conn.cursor()
    # SELECT만으로는 Stream이 소비되지 않음 — 0건이면 정상 소비된 것
    cur.execute("SELECT COUNT(*) FROM RAW.CRAWLED_PRICES_STREAM")
    assert cur.fetchone()[0] == 0, "transform 후 Stream에 미소비 레코드가 없어야 함"
    cur.close()


@pytest.mark.integration
def test_transform_staging_filters_anomaly_price(snowflake_settings, snowflake_conn):
    """이상치 가격(1원)은 Staging에 올라가지 않는지 확인."""
    load_raw(snowflake_settings, [_make_raw(f"{TEST_PREFIX}ANOMALY_001", "1원")])

    count = transform_staging(snowflake_settings)

    assert count == 0
    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM STAGING.PRICE_HISTORY dp
        JOIN STAGING.PRODUCTS p ON dp.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME = %s
    """, (f"{TEST_PREFIX}ANOMALY_001",))
    assert cur.fetchone()[0] == 0
    cur.close()
