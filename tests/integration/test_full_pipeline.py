"""Integration test: 전체 파이프라인 end-to-end 검증.

Raw → Staging → Analytics 전 구간이 한 번에 정상 동작하는지 확인.
"""

from datetime import datetime, timezone

import pytest

from run_pipeline import (
    aggregate_analytics,
    detect_changes,
    load_raw,
    transform_staging,
)
from tests.integration.conftest import TEST_PREFIX
from tests.integration.test_detect_changes import _make_raw_at

T1 = datetime(2000, 2, 1, tzinfo=timezone.utc)
T2 = datetime(2000, 2, 2, tzinfo=timezone.utc)


@pytest.mark.integration
def test_full_pipeline_data_flows_through_all_layers(snowflake_settings, snowflake_conn):
    """전체 파이프라인 실행 후 각 Layer에 데이터가 존재하는지 확인."""
    name = f"{TEST_PREFIX}E2E_CPU"

    # Step 1+2: 크롤링 데이터 → Raw 적재
    raw_count = load_raw(
        snowflake_settings,
        [_make_raw_at(name, "300,000원", T1)],
    )
    assert raw_count == 1, "Raw 적재 실패"

    # Step 3: Staging 변환
    stg_count = transform_staging(snowflake_settings)
    assert stg_count >= 1, "Staging 변환 실패"

    # Step 6: Analytics 집계
    aggregate_analytics(snowflake_settings)

    cur = snowflake_conn.cursor()

    # RAW 확인
    cur.execute(
        "SELECT COUNT(*) FROM RAW.RAW_CRAWLED_PRICES WHERE PRODUCT_NAME = %s",
        (name,),
    )
    assert cur.fetchone()[0] == 1, "RAW 레이어 데이터 없음"

    # STAGING 확인
    cur.execute(
        "SELECT COUNT(*) FROM STAGING.STG_PRODUCTS WHERE NAME = %s", (name,)
    )
    assert cur.fetchone()[0] == 1, "STG_PRODUCTS 데이터 없음"

    cur.execute("""
        SELECT COUNT(*) FROM STAGING.STG_DAILY_PRICES dp
        JOIN STAGING.STG_PRODUCTS p ON dp.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME = %s
    """, (name,))
    assert cur.fetchone()[0] >= 1, "STG_DAILY_PRICES 데이터 없음"

    # ANALYTICS 확인
    cur.execute("""
        SELECT COUNT(*) FROM ANALYTICS.PRODUCT_STATS ps
        JOIN STAGING.STG_PRODUCTS p ON ps.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME = %s
    """, (name,))
    assert cur.fetchone()[0] == 1, "PRODUCT_STATS 데이터 없음"

    cur.close()


@pytest.mark.integration
def test_full_pipeline_price_change_detected(snowflake_settings, snowflake_conn):
    """두 번 실행 후 가격 변동이 감지되는지 end-to-end 확인."""
    name = f"{TEST_PREFIX}E2E_SPIKE"

    # 1차 실행: 100,000원
    load_raw(snowflake_settings, [_make_raw_at(name, "100,000원", T1)])
    transform_staging(snowflake_settings)
    aggregate_analytics(snowflake_settings)

    # 2차 실행: 200,000원 (+100%)
    load_raw(snowflake_settings, [_make_raw_at(name, "200,000원", T2)])
    transform_staging(snowflake_settings)
    detect_changes(snowflake_settings)
    aggregate_analytics(snowflake_settings)

    cur = snowflake_conn.cursor()
    cur.execute("""
        SELECT a.ALERT_TYPE, ps.ALL_TIME_LOW, ps.ALL_TIME_HIGH
        FROM STAGING.STG_ALERTS a
        JOIN STAGING.STG_PRODUCTS p ON a.PRODUCT_ID = p.PRODUCT_ID
        JOIN ANALYTICS.PRODUCT_STATS ps ON ps.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.NAME = %s
    """, (name,))
    row = cur.fetchone()
    cur.close()

    assert row is not None, "알림이 생성되지 않음"
    # NEW_HIGH가 PRICE_SPIKE보다 우선순위 높음 (detect_changes CASE 순서)
    assert row[0] in ("NEW_HIGH", "PRICE_SPIKE"), f"예상치 못한 alert_type: {row[0]}"
    assert row[1] == 100000  # ALL_TIME_LOW
    assert row[2] == 200000  # ALL_TIME_HIGH
