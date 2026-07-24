"""Step 4: 변경 감지 — 가격 변동을 탐지해 stg_price_alerts에 기록."""

import logging

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)


def detect_changes(settings: MySQLSettings) -> int:
    """LAG() 윈도우 함수로 직전 가격 대비 변동을 탐지, stg_price_alerts에 INSERT."""
    MIN_CHANGE_PCT = 1.0
    MAX_CHANGE_PCT = 70.0   # 70% 초과 단일 변동은 데이터 이상치로 간주
    PRICE_DROP_PCT = -5.0
    PRICE_SPIKE_PCT = 10.0

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO `stg_price_alerts`
                (`product_id`, `daily_price_id`, `alert_type`, `old_price`, `new_price`, `change_pct`)
            WITH ranked AS (
                SELECT
                    `product_id`,
                    `id` AS daily_price_id,
                    `price`,
                    `crawled_at`,
                    LAG(`price`) OVER (
                        PARTITION BY `product_id` ORDER BY `crawled_at`
                    ) AS prev_price,
                    ROW_NUMBER() OVER (
                        PARTITION BY `product_id` ORDER BY `crawled_at` DESC
                    ) AS rn
                FROM `stg_price_history`
            ),
            product_stats AS (
                -- 전체기간 최저/최고가를 ans_daily_price_stats(일별)에서 즉석 집계.
                -- 실체 테이블(ans_product_stats)을 따로 안 두는 이유: §benchmark 20260724
                -- (LAG() 스캔이 이미 지배적 비용이라 즉석 집계 오버헤드가 미미함)
                SELECT `product_id`, MIN(`min_price`) AS `min_price_ever`, MAX(`max_price`) AS `max_price_ever`
                FROM `ans_daily_price_stats` GROUP BY `product_id`
            ),
            candidates AS (
                SELECT
                    r.`product_id`,
                    r.daily_price_id,
                    r.`price` AS new_price,
                    r.prev_price AS old_price,
                    CASE WHEN r.prev_price > 0
                         THEN ROUND((r.`price` - r.prev_price) / r.prev_price * 100, 4)
                         ELSE NULL
                    END AS change_pct,
                    ps.`min_price_ever`,
                    ps.`max_price_ever`
                FROM ranked r
                LEFT JOIN product_stats ps ON r.`product_id` = ps.`product_id`
                WHERE r.rn = 1
                  AND r.prev_price IS NOT NULL
                  AND r.`price` != r.prev_price
                  AND ABS((r.`price` - r.prev_price) / r.prev_price * 100) >= %s
                  AND ABS((r.`price` - r.prev_price) / r.prev_price * 100) <= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM `stg_price_alerts` a
                      WHERE a.`daily_price_id` = r.daily_price_id
                  )
            )
            SELECT
                `product_id`, daily_price_id,
                CASE
                    WHEN `min_price_ever` IS NOT NULL AND new_price < `min_price_ever` THEN 'NEW_LOW'
                    WHEN `max_price_ever` IS NOT NULL AND new_price > `max_price_ever` THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                END AS alert_type,
                old_price, new_price, change_pct
            FROM candidates
            WHERE CASE
                    WHEN `min_price_ever` IS NOT NULL AND new_price < `min_price_ever` THEN 'NEW_LOW'
                    WHEN `max_price_ever` IS NOT NULL AND new_price > `max_price_ever` THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                  END IS NOT NULL
        """, (MIN_CHANGE_PCT, MAX_CHANGE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT))
        alert_count = cur.rowcount
        cur.close()

    logger.info("[Alert] %d건 알림 생성", alert_count)
    return alert_count
