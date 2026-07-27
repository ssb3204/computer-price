"""Step 6: Analytics 집계 — ans_* 집계 테이블 갱신."""

import logging

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)


def aggregate_analytics(settings: MySQLSettings) -> None:
    """ans_daily_price_stats를 stg_price_history로 갱신한다.

    주별/월별/전체기간 통계는 이 함수가 만들지 않는다 — 소비하는 쪽(detect.py,
    quality.py)이 ans_daily_price_stats를 즉석 GROUP BY 해서 구한다.
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()
        # ans_daily_price_stats — CRAWLED_AT::DATE → DATE(crawled_at)
        cur.execute("""
            INSERT INTO `ans_daily_price_stats`
                (`product_id`, `price_date`, `min_price`, `max_price`, `avg_price`,
                 `record_count`, `first_crawled_at`, `last_crawled_at`)
            SELECT `product_id`, DATE(`crawled_at`) AS `price_date`,
                MIN(`price`), MAX(`price`), AVG(`price`), COUNT(*),
                MIN(`crawled_at`), MAX(`crawled_at`)
            FROM `stg_price_history` GROUP BY `product_id`, DATE(`crawled_at`)
            ON DUPLICATE KEY UPDATE
                `min_price` = VALUES(`min_price`), `max_price` = VALUES(`max_price`),
                `avg_price` = VALUES(`avg_price`), `record_count` = VALUES(`record_count`),
                `first_crawled_at` = VALUES(`first_crawled_at`),
                `last_crawled_at` = VALUES(`last_crawled_at`)
        """)
        cur.close()

    logger.info("[Analytics] 집계 완료")
