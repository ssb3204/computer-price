"""Integration test: quality.py 의 '오늘' 범위가 자정 경계에서 정확히 끊기는지 검증.

`DATE(crawled_at) = UTC_DATE()` 를 반열린 구간 `[UTC_DATE(), UTC_DATE() + 1일)` 로
바꿨으므로, 경계 시각에 수집된 행이 어제/오늘/내일로 정확히 갈리는지 실제 MySQL 로
확인한다. 앱·DB 세션이 모두 UTC 라 기준일은 UTC_DATE() 다.

경계 4개:
    어제 23:59:59 → 제외
    오늘 00:00:00 → 포함 (하한 닫힘)
    오늘 23:59:59 → 포함
    내일 00:00:00 → 제외 (상한 열림)

건수는 절대값이 아니라 삽입 전후 차이로 본다 — 같은 DB 에 다른 테스트·실제 크롤링
데이터가 함께 있어서 오늘 건수의 절대값을 고정할 수 없다.
"""

from datetime import datetime, time, timedelta
from unittest.mock import patch

import pytest

from src.pipeline.quality import check_cross_site_prices, check_layer_consistency
from tests.integration.conftest import TEST_PREFIX


def _boundaries(conn) -> dict[str, datetime]:
    """DB 의 UTC_DATE() 기준 경계 시각 4개 (호스트 시계가 아니라 DB 시계를 쓴다)."""
    cur = conn.cursor()
    cur.execute("SELECT UTC_DATE()")
    today_start = datetime.combine(cur.fetchone()[0], time.min)
    cur.close()
    one_day = timedelta(days=1)
    one_second = timedelta(seconds=1)
    return {
        "yesterday_end": today_start - one_second,
        "today_start":   today_start,
        "today_end":     today_start + one_day - one_second,
        "tomorrow_start": today_start + one_day,
    }


def _insert_raw(conn, name: str, crawled_at: datetime) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO `raw_crawled_prices` "
        "(`site`, `category`, `product_name`, `price_text`, `brand`, `url`, `crawled_at`) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        ("danawa", "CPU", name, "100,000원", None, "https://example.com", crawled_at),
    )
    cur.close()


def _insert_staging(conn, site: str, name: str, price: int, crawled_at: datetime) -> None:
    """stg_products + stg_price_history 에 한 쌍을 넣는다."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO `stg_products` (`site`, `category`, `product_name`, `url`) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE `product_id` = LAST_INSERT_ID(`product_id`)",
        (site, "CPU", name, ""),
    )
    product_id = cur.lastrowid
    cur.execute(
        "INSERT INTO `stg_price_history` (`product_id`, `price`, `crawled_at`) "
        "VALUES (%s, %s, %s)",
        (product_id, price, crawled_at),
    )
    cur.close()


@pytest.mark.integration
def test_layer_consistency_counts_only_today_rows(mysql_settings, mysql_conn):
    """경계 4건 중 오늘 00:00:00 / 23:59:59 두 건만 raw/staging 건수에 잡힌다."""
    boundaries = _boundaries(mysql_conn)

    with patch("src.pipeline.quality.send_slack_message"):
        before = check_layer_consistency(mysql_settings)

    for label, crawled_at in boundaries.items():
        _insert_raw(mysql_conn, f"{TEST_PREFIX}BOUND_RAW_{label}", crawled_at)
        _insert_staging(
            mysql_conn, "다나와", f"{TEST_PREFIX}BOUND_STG_{label}", 100_000, crawled_at,
        )

    with patch("src.pipeline.quality.send_slack_message"):
        after = check_layer_consistency(mysql_settings)

    assert after.raw_count - before.raw_count == 2
    assert after.staging_count - before.staging_count == 2


@pytest.mark.integration
def test_cross_site_includes_midnight_boundary_rows(mysql_settings, mysql_conn):
    """오늘 00:00:00 / 23:59:59 에 수집된 가격은 교차검증 대상에 들어간다."""
    boundaries = _boundaries(mysql_conn)
    name = f"{TEST_PREFIX}BOUND_CROSS_TODAY"
    _insert_staging(mysql_conn, "다나와", name, 1_000_000, boundaries["today_start"])
    _insert_staging(mysql_conn, "컴퓨존", name, 1_500_000, boundaries["today_end"])  # +50%

    with patch("src.pipeline.quality.send_slack_message") as mock_slack:
        check_cross_site_prices(mysql_settings)

    assert mock_slack.called, "오늘 경계 행이 빠져 이상치가 감지되지 않았다"
    assert name in mock_slack.call_args[0][0]


@pytest.mark.integration
def test_cross_site_excludes_rows_outside_today(mysql_settings, mysql_conn):
    """어제 23:59:59 / 내일 00:00:00 에 수집된 가격은 교차검증 대상에서 빠진다.

    두 경계를 각각 독립된 상품 쌍으로 넣는다 — 한 쌍을 두 경계에 걸쳐 두면 한쪽
    경계만 잘못돼도 사이트가 1개로 줄어 이상치가 안 잡히고, 그래서 테스트가
    통과해 버린다.
    """
    boundaries = _boundaries(mysql_conn)
    yesterday_name = f"{TEST_PREFIX}BOUND_CROSS_YESTERDAY"
    tomorrow_name = f"{TEST_PREFIX}BOUND_CROSS_TOMORROW"
    for name, crawled_at in (
        (yesterday_name, boundaries["yesterday_end"]),
        (tomorrow_name, boundaries["tomorrow_start"]),
    ):
        _insert_staging(mysql_conn, "다나와", name, 1_000_000, crawled_at)
        _insert_staging(mysql_conn, "컴퓨존", name, 1_500_000, crawled_at)  # +50%

    with patch("src.pipeline.quality.send_slack_message") as mock_slack:
        check_cross_site_prices(mysql_settings)

    message = mock_slack.call_args[0][0] if mock_slack.called else ""
    assert yesterday_name not in message
    assert tomorrow_name not in message
