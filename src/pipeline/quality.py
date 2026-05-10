"""Step 3.5: 데이터 품질 검증 — 사이트 간 가격 편차 + 레이어 정합성 체크."""

import logging
from collections import defaultdict
from dataclasses import dataclass

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


@dataclass
class LayerConsistencyResult:
    """레이어 정합성 체크 결과."""
    raw_count: int           # 오늘 Raw 수집 건수
    staging_count: int       # 오늘 Staging 변환 건수
    drop_count: int          # Raw → Staging 손실 건수
    drop_rate: float         # 손실률 (%)
    missing_analytics: int   # ANALYTICS.PRODUCT_STATS 미집계 상품 수

    @property
    def total_issues(self) -> int:
        """임계값 초과 항목 수 (0이면 정상)."""
        issues = 0
        if self.drop_rate > 10.0:
            issues += 1
        issues += self.missing_analytics
        return issues


def check_layer_consistency(settings: SnowflakeSettings) -> LayerConsistencyResult:
    """레이어 정합성 체크: Raw→Staging 손실률, Analytics 누락 상품.

    임계값 초과 시 Slack WARNING을 보내지만 파이프라인은 계속 진행한다.

    체크 항목:
        1. Raw → Staging 손실률 > 10%: 파싱/이상치 제외로 과도한 드롭 발생
        2. ANALYTICS.PRODUCT_STATS 누락: analytics 스텝 미실행 또는 버그
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE DATABASE COMPUTER_PRICE")

        # 1. 오늘 Raw vs Staging 건수
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM RAW.CRAWLED_PRICES
                 WHERE CRAWLED_AT::DATE = CURRENT_DATE()) AS raw_count,
                (SELECT COUNT(*) FROM STAGING.PRICE_HISTORY
                 WHERE CRAWLED_AT::DATE = CURRENT_DATE()) AS staging_count
        """)
        row = cur.fetchone()
        raw_count, staging_count = int(row[0]), int(row[1])

        # 2. Analytics 미집계 상품 (PRODUCTS에 있지만 PRODUCT_STATS에 없음)
        cur.execute("""
            SELECT COUNT(*)
            FROM STAGING.PRODUCTS p
            WHERE NOT EXISTS (
                SELECT 1 FROM ANALYTICS.PRODUCT_STATS ps
                WHERE ps.PRODUCT_ID = p.PRODUCT_ID
            )
        """)
        missing_analytics = int(cur.fetchone()[0])
        cur.close()

    drop_count = raw_count - staging_count
    drop_rate = drop_count / raw_count * 100 if raw_count > 0 else 0.0

    result = LayerConsistencyResult(
        raw_count=raw_count,
        staging_count=staging_count,
        drop_count=drop_count,
        drop_rate=drop_rate,
        missing_analytics=missing_analytics,
    )

    # 이슈 로깅 및 Slack 알림
    issues: list[str] = []
    if drop_rate > 10.0:
        issues.append(f"Raw→Staging 손실률 {drop_rate:.1f}% ({drop_count}건 누락, Raw={raw_count}건)")
    if missing_analytics > 0:
        issues.append(f"Analytics 미집계 상품 {missing_analytics}개")

    if issues:
        lines = [f"*⚠️ [레이어 정합성] 이슈 {len(issues)}건*"] + [f"• {i}" for i in issues]
        _send_slack_message("\n".join(lines))
        logger.warning("[레이어 정합성] %s", " | ".join(issues))
    else:
        logger.info(
            "[레이어 정합성] 정상 — Raw=%d → Staging=%d (손실률 %.1f%%), Analytics 누락=%d",
            raw_count, staging_count, drop_rate, missing_analytics,
        )

    return result
