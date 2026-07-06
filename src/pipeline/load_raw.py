"""Step 2: Raw 적재 — 크롤링 결과를 raw_crawled_prices에 저장."""

import logging
from datetime import timezone

from src.common.config import MySQLSettings
from src.common.models import RawCrawledPrice
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)


def load_raw(settings: MySQLSettings, all_raw: list[RawCrawledPrice]) -> int:
    """Raw 크롤링 데이터를 raw_crawled_prices에 적재.

    자연키 UNIQUE(site, category, product_name, crawled_at)로 중복을 무시한다
    (구 Snowflake MERGE의 WHEN NOT MATCHED 동작을 ON DUPLICATE KEY UPDATE no-op로 대체).
    crawled_at은 UTC naive DATETIME으로 저장한다.
    """
    if not all_raw:
        logger.warning("[Raw] 적재할 데이터 없음")
        return 0

    rows = [
        (
            rp.site, rp.category, rp.product_name, rp.price_text,
            rp.brand, rp.url,
            rp.crawled_at.astimezone(timezone.utc).replace(tzinfo=None),
        )
        for rp in all_raw
    ]

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO `raw_crawled_prices` "
            "(`site`, `category`, `product_name`, `price_text`, `brand`, `url`, `crawled_at`) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE `id` = `id`",
            rows,
        )
        count = cur.rowcount
        cur.close()

    logger.info("[Raw] %d건 적재 완료", count)
    return count
