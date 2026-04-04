"""Integration test: detect_changes() — 가격 변동 감지 검증."""

from datetime import datetime, timezone

import pytest

from run_pipeline import detect_changes, load_raw, transform_staging
from src.common.models import RawCrawledPrice
from tests.integration.conftest import TEST_PREFIX


def _make_raw_at(name: str, price: str, crawled_at: datetime) -> RawCrawledPrice:
    return RawCrawledPrice(
        site="danawa",
        category="CPU",
        product_name=name,
        price_text=price,
        brand="테스트브랜드",
        url="https://example.com",
        crawled_at=crawled_at,
    )


T1 = datetime(2000, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2000, 1, 2, tzinfo=timezone.utc)


@pytest.mark.integration
def test_detect_changes_price_drop(snowflake_settings, snowflake_conn):
    """가격이 5% 이상 하락하면 PRICE_DROP 알림이 생성되는지 확인."""
    name = f"{TEST_PREFIX}ALERT_DROP"

    # T1: 100,000원 적재
    load_raw(snowflake_settings, [_make_raw_at(name, "100,000원", T1)])
    transform_staging(snowflake_settings)

    # T2: 50,000원으로 하락 (-50%)
    load_raw(snowflake_settings, [_make_raw_at(name, "50,000원", T2)])
    transform_staging(snowflake_settings)

    alert_count = detect_changes(snowflake_settings)

    assert alert_count >= 1
    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT a.ALERT_TYPE FROM STAGING.PRICE_ALERTS a
        JOIN STAGING.PRODUCTS p ON a.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME = %s
    """, (name,))
    rows = cur.fetchall()
    cur.close()

    alert_types = [r[0] for r in rows]
    assert "PRICE_DROP" in alert_types


@pytest.mark.integration
def test_detect_changes_price_spike(snowflake_settings, snowflake_conn):
    """가격이 10% 이상 상승하면 PRICE_SPIKE 알림이 생성되는지 확인."""
    name = f"{TEST_PREFIX}ALERT_SPIKE"

    # T1: 100,000원 적재
    load_raw(snowflake_settings, [_make_raw_at(name, "100,000원", T1)])
    transform_staging(snowflake_settings)

    # T2: 160,000원으로 상승 (+60%, MAX_CHANGE_PCT=70% 범위 내)
    load_raw(snowflake_settings, [_make_raw_at(name, "160,000원", T2)])
    transform_staging(snowflake_settings)

    detect_changes(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT a.ALERT_TYPE FROM STAGING.PRICE_ALERTS a
        JOIN STAGING.PRODUCTS p ON a.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME = %s
    """, (name,))
    rows = cur.fetchall()
    cur.close()

    alert_types = [r[0] for r in rows]
    assert "PRICE_SPIKE" in alert_types


@pytest.mark.integration
def test_detect_changes_no_alert_small_change(snowflake_settings, snowflake_conn):
    """가격 변동이 1% 미만이면 알림이 생성되지 않는지 확인."""
    name = f"{TEST_PREFIX}ALERT_SMALL"

    # T1: 100,000원
    load_raw(snowflake_settings, [_make_raw_at(name, "100,000원", T1)])
    transform_staging(snowflake_settings)

    # T2: 100,500원 (+0.5%, 임계값 미만)
    load_raw(snowflake_settings, [_make_raw_at(name, "100,500원", T2)])
    transform_staging(snowflake_settings)

    detect_changes(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM STAGING.PRICE_ALERTS a
        JOIN STAGING.PRODUCTS p ON a.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME = %s
    """, (name,))
    assert cur.fetchone()[0] == 0
    cur.close()
