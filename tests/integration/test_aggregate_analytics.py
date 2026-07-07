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
def test_aggregate_analytics_creates_product_stats(mysql_settings, mysql_conn):
    """aggregate_analytics() 후 ans_product_stats에 row가 생성되는지 확인."""
    _setup_staging(mysql_settings)

    aggregate_analytics(mysql_settings)

    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM `ans_product_stats` ps
        JOIN `stg_products` p ON ps.`product_id` = p.`product_id`
        WHERE p.`product_name` LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_aggregate_analytics_creates_daily_summary(mysql_settings, mysql_conn):
    """aggregate_analytics() 후 ans_daily_price_stats에 row가 생성되는지 확인."""
    _setup_staging(mysql_settings)

    aggregate_analytics(mysql_settings)

    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM `ans_daily_price_stats` ds
        JOIN `stg_products` p ON ds.`product_id` = p.`product_id`
        WHERE p.`product_name` LIKE %s
    """, (TEST_PREFIX + "%",))
    assert cur.fetchone()[0] >= 1
    cur.close()


@pytest.mark.integration
def test_aggregate_analytics_correct_values(mysql_settings, mysql_conn):
    """ans_product_stats의 min_price_ever/max_price_ever가 실제 가격과 일치하는지 확인."""
    load_raw(mysql_settings, [_make_raw(f"{TEST_PREFIX}PSU_001", "150,000원")])
    transform_staging(mysql_settings)

    aggregate_analytics(mysql_settings)

    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT ps.`min_price_ever`, ps.`max_price_ever`
        FROM `ans_product_stats` ps
        JOIN `stg_products` p ON ps.`product_id` = p.`product_id`
        WHERE p.`product_name` = %s
    """, (f"{TEST_PREFIX}PSU_001",))
    row = cur.fetchone()
    cur.close()

    assert row is not None
    assert row[0] == 150000  # min_price_ever
    assert row[1] == 150000  # max_price_ever
