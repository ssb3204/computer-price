"""Step 2: Raw 적재 — 크롤링 결과를 RAW.CRAWLED_PRICES에 저장."""

import logging

from src.common.config import SnowflakeSettings
from src.common.models import RawCrawledPrice
from src.common.snowflake_client import get_connection

logger = logging.getLogger(__name__)


def load_raw(settings: SnowflakeSettings, all_raw: list[RawCrawledPrice]) -> int:
    """Raw 크롤링 데이터를 Snowflake RAW 레이어에 적재. 중복은 MERGE로 방지."""
    if not all_raw:
        logger.warning("[Raw] 적재할 데이터 없음")
        return 0

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE SCHEMA RAW")
        cur.execute("""
            CREATE TEMPORARY TABLE TEMP_RAW_LOAD (
                SITE STRING, CATEGORY STRING, PRODUCT_NAME STRING,
                PRICE_TEXT STRING, BRAND STRING, URL STRING,
                CRAWLED_AT STRING
            )
        """)
        rows = [
            (rp.site, rp.category, rp.product_name, rp.price_text,
             rp.brand, rp.url, rp.crawled_at.isoformat())
            for rp in all_raw
        ]
        cur.executemany(
            "INSERT INTO TEMP_RAW_LOAD VALUES (%s, %s, %s, %s, %s, %s, %s)",
            rows,
        )
        cur.execute("""
            MERGE INTO CRAWLED_PRICES t
            USING TEMP_RAW_LOAD s
            ON t.SITE = s.SITE AND t.CATEGORY = s.CATEGORY
               AND t.PRODUCT_NAME = s.PRODUCT_NAME AND t.CRAWLED_AT = s.CRAWLED_AT
            WHEN NOT MATCHED THEN INSERT
                (SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, CRAWLED_AT)
                VALUES (s.SITE, s.CATEGORY, s.PRODUCT_NAME, s.PRICE_TEXT,
                        s.BRAND, s.URL, s.CRAWLED_AT)
        """)
        count = cur.rowcount
        cur.close()

    logger.info("[Raw] %d건 적재 완료", count)
    return count
