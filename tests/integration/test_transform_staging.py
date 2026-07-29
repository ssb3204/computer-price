"""Integration test: transform_staging() — Staging 변환 검증."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from run_pipeline import load_raw, transform_staging
from src.common.models import RawCrawledPrice
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_load_raw import _make_raw


def _make_raw_at(name: str, crawled_at: datetime) -> RawCrawledPrice:
    """_make_raw와 동일하되 crawled_at만 지정 (raw 자연키를 달리하기 위함)."""
    return replace(_make_raw(name), crawled_at=crawled_at)


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
def test_transform_staging_maps_case_variant_name(mysql_settings, mysql_conn):
    """대소문자만 다른 상품명도 product_id 매핑에 성공해야 한다.

    stg_products의 UNIQUE(site, product_name)는 utf8mb4_0900_ai_ci(대소문자 무시)라
    두 변형이 한 행으로 합쳐진다. 반면 product_map은 파이썬 dict라 대소문자를 구분하므로,
    합쳐지지 않은 쪽 이름으로 조회하면 None이 되어 해당 raw 행이 조용히 버려진다
    (stg_price_history에도 raw_transform_failures에도 남지 않아 매 실행 재조회됨).
    """
    name_a = f"{TEST_PREFIX}GPU_CASE_Nitro"
    name_b = f"{TEST_PREFIX}GPU_CASE_NITRO"  # 대소문자만 다름
    # raw의 UNIQUE도 ai_ci이므로 crawled_at을 달리해야 두 행이 모두 적재된다
    # (사이트가 회차 사이에 표기를 바꾼 상황을 재현).
    load_raw(mysql_settings, [
        _make_raw_at(name_a, datetime(2000, 1, 1, tzinfo=UTC)),
        _make_raw_at(name_b, datetime(2000, 1, 2, tzinfo=UTC)),
    ])

    count = transform_staging(mysql_settings)

    assert count == 2, "대소문자 변형 때문에 raw 행이 버려지면 안 됨"
    cur = mysql_conn.cursor()
    # 두 변형은 ai_ci 때문에 stg_products 한 행으로 합쳐지므로, 그 상품의 이력이 2건이어야 한다
    cur.execute("""
        SELECT COUNT(*) FROM `stg_price_history` ph
        JOIN `stg_products` p ON p.`product_id` = ph.`product_id`
        WHERE p.`product_name` IN (%s, %s)
    """, (name_a, name_b))
    assert cur.fetchone()[0] == 2

    # 버려진 행이 실패 기록으로도 남지 않는(= 영구 재조회) 상태가 아닌지 확인
    cur.execute(
        "SELECT COUNT(*) FROM `raw_transform_failures` WHERE `product_name` IN (%s, %s)",
        (name_a, name_b),
    )
    assert cur.fetchone()[0] == 0
    cur.close()


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
