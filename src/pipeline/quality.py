"""Step 3.5: 데이터 품질 교차 검증 — 동일 제품의 사이트 간 가격 편차 감지."""

import logging
from collections import defaultdict

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.pipeline.slack import _send_slack_message

logger = logging.getLogger(__name__)


def _find_cross_site_anomalies(
    rows: list[tuple[str, str, int]],
    threshold: float = 20.0,
) -> list[tuple[str, str, int, int, float]]:
    """동일 제품이 여러 사이트에 있을 때 가격 편차를 확인한다 (순수 함수).

    최저가 대비 threshold% 이상 비싼 사이트를 이상치로 판정한다.

    Args:
        rows: [(product_name, site, price), ...]
        threshold: 이상치 판정 편차 기준 (%, 기본 20%)

    Returns:
        [(product_name, site, price, min_price, deviation_pct), ...]
    """
    price_by_product: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for name, site, price in rows:
        price_by_product[name].append((site, price))

    anomalies = []
    for name, entries in price_by_product.items():
        if len(entries) < 2:
            continue
        min_price = min(p for _, p in entries)
        if min_price == 0:
            continue
        for site, price in entries:
            deviation = (price - min_price) / min_price * 100
            if deviation >= threshold:
                anomalies.append((name, site, price, min_price, deviation))
    return anomalies


def check_cross_site_prices(settings: SnowflakeSettings) -> int:
    """교차 검증: 동일 제품이 여러 사이트에 존재할 때 오늘 가격 편차 확인.

    이상치 발견 시 Slack WARNING을 보내지만 파이프라인은 계속 진행한다.
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute("""
            SELECT p.PRODUCT_NAME, p.SITE, ph.PRICE
            FROM STAGING.PRICE_HISTORY ph
            JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = ph.PRODUCT_ID
            WHERE ph.CRAWLED_AT::DATE = CURRENT_DATE()
        """)
        rows = cur.fetchall()
        cur.close()

    anomalies = _find_cross_site_anomalies(rows)

    if anomalies:
        lines = [f"*⚠️ [교차검증] 가격 이상 감지 — {len(anomalies)}건*"]
        for name, site, price, min_price, deviation in anomalies:
            lines.append(
                f"• {name[:35]} | {site}: {price:,}원 "
                f"(최저가 {min_price:,}원 대비 {deviation:.1f}%)"
            )
        _send_slack_message("\n".join(lines))
        logger.warning("[교차검증] 이상 %d건 감지", len(anomalies))
    else:
        logger.info("[교차검증] 이상 없음")

    return len(anomalies)
