"""Integration test: 전체 파이프라인 end-to-end 검증.

Raw → Staging → Analytics 전 구간이 한 번에 정상 동작하는지 확인.
"""

from datetime import UTC, datetime

import pytest

from run_pipeline import (
    aggregate_analytics,
    detect_changes,
    load_raw,
    transform_staging,
)
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_detect_changes import _make_raw_at

T1 = datetime(2000, 2, 1, tzinfo=UTC)
T2 = datetime(2000, 2, 2, tzinfo=UTC)


@pytest.mark.integration
def test_full_pipeline_data_flows_through_all_layers(mysql_settings, mysql_conn):
    """전체 파이프라인 실행 후 각 Layer에 데이터가 존재하는지 확인."""
    name = f"{TEST_PREFIX}E2E_CPU"

    # Step 1+2: 크롤링 데이터 → Raw 적재
    raw_count = load_raw(
        mysql_settings,
        [_make_raw_at(name, "300,000원", T1)],
    )
    assert raw_count == 1, "Raw 적재 실패"

    # Step 3: Staging 변환
    stg_count = transform_staging(mysql_settings)
    assert stg_count >= 1, "Staging 변환 실패"

    # Step 6: Analytics 집계
    aggregate_analytics(mysql_settings)

    cur = mysql_conn.cursor()

    # RAW 확인
    cur.execute(
        "SELECT COUNT(*) FROM `raw_crawled_prices` WHERE `product_name` = %s",
        (name,),
    )
    assert cur.fetchone()[0] == 1, "RAW 레이어 데이터 없음"

    # STAGING 확인
    cur.execute(
        "SELECT COUNT(*) FROM `stg_products` WHERE `product_name` = %s", (name,)
    )
    assert cur.fetchone()[0] == 1, "stg_products 데이터 없음"

    cur.execute("""
        SELECT COUNT(*) FROM `stg_price_history` dp
        JOIN `stg_products` p ON dp.`product_id` = p.`product_id`
        WHERE p.`product_name` = %s
    """, (name,))
    assert cur.fetchone()[0] >= 1, "stg_price_history 데이터 없음"

    # ANALYTICS 확인
    cur.execute("""
        SELECT COUNT(*) FROM `ans_daily_price_stats` ds
        JOIN `stg_products` p ON ds.`product_id` = p.`product_id`
        WHERE p.`product_name` = %s
    """, (name,))
    assert cur.fetchone()[0] == 1, "ans_daily_price_stats 데이터 없음"

    cur.close()


@pytest.mark.integration
def test_full_pipeline_price_change_detected(mysql_settings, mysql_conn):
    """두 번 실행 후 가격 변동이 감지되는지 end-to-end 확인."""
    name = f"{TEST_PREFIX}E2E_SPIKE"

    # 1차 실행: 100,000원
    load_raw(mysql_settings, [_make_raw_at(name, "100,000원", T1)])
    transform_staging(mysql_settings)
    aggregate_analytics(mysql_settings)

    # 2차 실행: 160,000원 (+60%, MAX_CHANGE_PCT=70% 범위 내)
    load_raw(mysql_settings, [_make_raw_at(name, "160,000원", T2)])
    transform_staging(mysql_settings)
    detect_changes(mysql_settings)
    aggregate_analytics(mysql_settings)

    cur = mysql_conn.cursor()
    cur.execute("""
        SELECT a.`alert_type`, ps.`min_price_ever`, ps.`max_price_ever`
        FROM `stg_price_alerts` a
        JOIN `stg_products` p ON a.`product_id` = p.`product_id`
        JOIN (
            SELECT `product_id`, MIN(`min_price`) AS `min_price_ever`, MAX(`max_price`) AS `max_price_ever`
            FROM `ans_daily_price_stats` GROUP BY `product_id`
        ) ps ON ps.`product_id` = p.`product_id`
        WHERE p.`product_name` = %s
    """, (name,))
    row = cur.fetchone()
    cur.close()

    assert row is not None, "알림이 생성되지 않음"
    # NEW_HIGH가 PRICE_SPIKE보다 우선순위 높음 (detect_changes CASE 순서)
    assert row[0] in ("NEW_HIGH", "PRICE_SPIKE"), f"예상치 못한 alert_type: {row[0]}"
    assert row[1] == 100000  # min_price_ever
    assert row[2] == 160000  # max_price_ever
