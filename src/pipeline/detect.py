"""Step 4: 변경 감지 — 가격 변동을 탐지해 STAGING.PRICE_ALERTS에 기록."""

import logging

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection

logger = logging.getLogger(__name__)


def detect_changes(settings: SnowflakeSettings) -> int:
    """LAG() 윈도우 함수로 직전 가격 대비 변동을 탐지, PRICE_ALERTS에 INSERT."""
    MIN_CHANGE_PCT = 1.0
    MAX_CHANGE_PCT = 70.0   # 70% 초과 단일 변동은 데이터 이상치로 간주
    PRICE_DROP_PCT = -5.0
    PRICE_SPIKE_PCT = 10.0

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO STAGING.PRICE_ALERTS
                (PRODUCT_ID, DAILY_PRICE_ID, ALERT_TYPE, OLD_PRICE, NEW_PRICE, CHANGE_PCT)
            WITH ranked AS (
                SELECT
                    PRODUCT_ID,
                    ID AS DAILY_PRICE_ID,
                    PRICE,
                    CRAWLED_AT,
                    LAG(PRICE) OVER (
                        PARTITION BY PRODUCT_ID ORDER BY CRAWLED_AT
                    ) AS prev_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY PRODUCT_ID ORDER BY CRAWLED_AT DESC
                    ) AS rn
                FROM STAGING.PRICE_HISTORY
            ),
            candidates AS (
                SELECT
                    r.PRODUCT_ID,
                    r.DAILY_PRICE_ID,
                    r.PRICE AS new_price,
                    r.prev_price AS old_price,
                    CASE WHEN r.prev_price > 0
                         THEN ROUND((r.PRICE - r.prev_price) / r.prev_price * 100, 4)
                         ELSE NULL
                    END AS change_pct,
                    ps.MIN_PRICE_EVER,
                    ps.MAX_PRICE_EVER
                FROM ranked r
                LEFT JOIN ANALYTICS.PRODUCT_STATS ps ON r.PRODUCT_ID = ps.PRODUCT_ID
                WHERE r.rn = 1
                  AND r.prev_price IS NOT NULL
                  AND r.PRICE != r.prev_price
                  AND ABS((r.PRICE - r.prev_price) / r.prev_price * 100) >= %s
                  AND ABS((r.PRICE - r.prev_price) / r.prev_price * 100) <= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM STAGING.PRICE_ALERTS a
                      WHERE a.DAILY_PRICE_ID = r.DAILY_PRICE_ID
                  )
            )
            SELECT
                PRODUCT_ID, DAILY_PRICE_ID,
                CASE
                    WHEN MIN_PRICE_EVER IS NOT NULL AND new_price < MIN_PRICE_EVER THEN 'NEW_LOW'
                    WHEN MAX_PRICE_EVER IS NOT NULL AND new_price > MAX_PRICE_EVER THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                END AS alert_type,
                old_price, new_price, change_pct
            FROM candidates
            WHERE CASE
                    WHEN MIN_PRICE_EVER IS NOT NULL AND new_price < MIN_PRICE_EVER THEN 'NEW_LOW'
                    WHEN MAX_PRICE_EVER IS NOT NULL AND new_price > MAX_PRICE_EVER THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                  END IS NOT NULL
        """, (MIN_CHANGE_PCT, MAX_CHANGE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT))
        alert_count = cur.rowcount
        cur.close()

    logger.info("[Alert] %d건 알림 생성", alert_count)
    return alert_count
