"""Step 6: Analytics 집계 — ans_* 집계 테이블 갱신."""

import logging

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)


def aggregate_analytics(settings: MySQLSettings) -> None:
    """ans_daily_price_stats, ans_weekly_price_stats, ans_product_stats를 stg_price_history로 갱신."""
    with get_connection(settings) as conn:
        cur = conn.cursor()
        # ans_daily_price_stats — CRAWLED_AT::DATE → DATE(crawled_at)
        cur.execute("""
            INSERT INTO `ans_daily_price_stats`
                (`product_id`, `price_date`, `min_price`, `max_price`, `avg_price`, `record_count`)
            SELECT `product_id`, DATE(`crawled_at`) AS `price_date`,
                MIN(`price`), MAX(`price`), AVG(`price`), COUNT(*)
            FROM `stg_price_history` GROUP BY `product_id`, DATE(`crawled_at`)
            ON DUPLICATE KEY UPDATE
                `min_price` = VALUES(`min_price`), `max_price` = VALUES(`max_price`),
                `avg_price` = VALUES(`avg_price`), `record_count` = VALUES(`record_count`)
        """)
        # ans_weekly_price_stats — DATE_TRUNC('WEEK', ...) → 월요일 시작(WEEKDAY 방식, §7-3 확정)
        cur.execute("""
            INSERT INTO `ans_weekly_price_stats`
                (`product_id`, `week_start`, `min_price`, `max_price`, `avg_price`, `record_count`)
            SELECT `product_id`,
                DATE_SUB(DATE(`crawled_at`), INTERVAL WEEKDAY(`crawled_at`) DAY) AS `week_start`,
                MIN(`price`), MAX(`price`), AVG(`price`), COUNT(*)
            FROM `stg_price_history`
            GROUP BY `product_id`, DATE_SUB(DATE(`crawled_at`), INTERVAL WEEKDAY(`crawled_at`) DAY)
            ON DUPLICATE KEY UPDATE
                `min_price` = VALUES(`min_price`), `max_price` = VALUES(`max_price`),
                `avg_price` = VALUES(`avg_price`), `record_count` = VALUES(`record_count`)
        """)
        # ans_product_stats — product_id가 PK이므로 별도 UNIQUE 불필요
        cur.execute("""
            INSERT INTO `ans_product_stats`
                (`product_id`, `avg_price`, `min_price_ever`, `max_price_ever`,
                 `first_crawled_at`, `last_crawled_at`, `total_records`)
            SELECT `product_id`, AVG(`price`), MIN(`price`), MAX(`price`),
                MIN(`crawled_at`), MAX(`crawled_at`), COUNT(*)
            FROM `stg_price_history` GROUP BY `product_id`
            ON DUPLICATE KEY UPDATE
                `avg_price` = VALUES(`avg_price`), `min_price_ever` = VALUES(`min_price_ever`),
                `max_price_ever` = VALUES(`max_price_ever`), `first_crawled_at` = VALUES(`first_crawled_at`),
                `last_crawled_at` = VALUES(`last_crawled_at`), `total_records` = VALUES(`total_records`),
                `updated_at` = CURRENT_TIMESTAMP
        """)
        cur.close()

    logger.info("[Analytics] 집계 완료")
