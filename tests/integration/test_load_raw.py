"""Integration test: load_raw() — raw_crawled_prices 적재 검증."""

from datetime import datetime, timezone

import pytest

from run_pipeline import load_raw
from src.common.models import RawCrawledPrice
from tests.integration.conftest import TEST_PREFIX


def _make_raw(name: str, price: str = "100,000원") -> RawCrawledPrice:
    return RawCrawledPrice(
        site="danawa",
        category="CPU",
        product_name=name,
        price_text=price,
        brand="테스트브랜드",
        url="https://example.com",
        crawled_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
    )


@pytest.mark.integration
def test_load_raw_inserts_rows(mysql_settings, mysql_conn):
    """load_raw()가 raw_crawled_prices에 데이터를 적재하는지 확인."""
    raw = [_make_raw(f"{TEST_PREFIX}CPU_001"), _make_raw(f"{TEST_PREFIX}CPU_002")]

    count = load_raw(mysql_settings, raw)

    assert count == 2
    cur = mysql_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM `raw_crawled_prices` WHERE `product_name` IN (%s, %s)",
        (f"{TEST_PREFIX}CPU_001", f"{TEST_PREFIX}CPU_002"),
    )
    assert cur.fetchone()[0] == 2
    cur.close()


@pytest.mark.integration
def test_load_raw_idempotent(mysql_settings, mysql_conn):
    """동일 데이터 두 번 적재 시 중복 삽입 안 됨 (자연키 UNIQUE + ON DUPLICATE KEY UPDATE 멱등성)."""
    raw = [_make_raw(f"{TEST_PREFIX}CPU_DUPE")]

    load_raw(mysql_settings, raw)
    load_raw(mysql_settings, raw)  # 동일 데이터 재실행

    cur = mysql_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM `raw_crawled_prices` WHERE `product_name` = %s",
        (f"{TEST_PREFIX}CPU_DUPE",),
    )
    assert cur.fetchone()[0] == 1
    cur.close()


@pytest.mark.integration
def test_load_raw_empty(mysql_settings):
    """빈 리스트 전달 시 0 반환."""
    count = load_raw(mysql_settings, [])
    assert count == 0
