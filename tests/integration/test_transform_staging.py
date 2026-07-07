"""Integration test: transform_staging() — Staging 변환 검증."""

import pytest

from run_pipeline import load_raw, transform_staging
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_load_raw import _make_raw


@pytest.mark.integration
def test_transform_staging_creates_product(mysql_settings, mysql_conn):
    """transform_staging() 후 stg_products에 상품이 생성되는지 확인."""
    load_raw(mysql_settings, [_make_raw(f"{TEST_PREFIX}GPU_001", "500,000원")])

    count = transform_staging(mysql_settings)

    assert count >= 1
    cur = mysql_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM `stg_products` WHERE `product_name` LIKE %s",
        (TEST_PREFIX + "%",),
    )
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_creates_daily_price(mysql_settings, mysql_conn):
    """transform_staging() 후 stg_price_history에 가격 row가 생성되는지 확인."""
    load_raw(mysql_settings, [_make_raw(f"{TEST_PREFIX}RAM_001", "80,000원")])
    transform_staging(mysql_settings)

    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM `stg_price_history` dp
        JOIN `stg_products` p ON dp.`product_id` = p.`product_id`
        WHERE p.`product_name` LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_transform_staging_no_reprocess(mysql_settings, mysql_conn):
    """transform_staging() 후 같은 raw 행을 다시 실행해도 재처리되지 않는지 확인.

    구 Snowflake Stream의 consume-once를 '미처리 조인'(stg_price_history/
    raw_transform_failures에 없는 raw 행만 대상)으로 재현했으므로, 동일 raw 데이터에
    대해 두 번째 transform_staging() 호출은 0건을 반환해야 한다.
    """
    load_raw(mysql_settings, [_make_raw(f"{TEST_PREFIX}SSD_001", "120,000원")])
    first_count = transform_staging(mysql_settings)
    assert first_count >= 1

    second_count = transform_staging(mysql_settings)
    assert second_count == 0, "미처리 조인 대상에서 이미 처리된 raw 행이 재조회되면 안 됨"


@pytest.mark.integration
def test_transform_staging_filters_anomaly_price(mysql_settings, mysql_conn):
    """이상치 가격(1원)은 Staging에 올라가지 않는지 확인."""
    load_raw(mysql_settings, [_make_raw(f"{TEST_PREFIX}ANOMALY_001", "1원")])

    count = transform_staging(mysql_settings)

    assert count == 0
    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM `stg_price_history` dp
        JOIN `stg_products` p ON dp.`product_id` = p.`product_id`
        WHERE p.`product_name` = %s
    """, (f"{TEST_PREFIX}ANOMALY_001",))
    assert cur.fetchone()[0] == 0
    cur.close()
