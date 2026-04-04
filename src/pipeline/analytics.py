"""Step 6: Analytics 집계 — ANALYTICS 레이어 집계 테이블 갱신."""

import logging

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection

logger = logging.getLogger(__name__)


def aggregate_analytics(settings: SnowflakeSettings) -> None:
    """DAILY_PRICE_STATS, WEEKLY_PRICE_STATS, PRODUCT_STATS를 STAGING 데이터로 갱신."""
    with get_connection(settings) as conn:
        cur = conn.cursor()
        # DAILY_PRICE_STATS
        cur.execute("""
            MERGE INTO ANALYTICS.DAILY_PRICE_STATS t
            USING (
                SELECT PRODUCT_ID, CRAWLED_AT::DATE AS PRICE_DATE,
                    MIN(PRICE) AS MIN_PRICE, MAX(PRICE) AS MAX_PRICE,
                    AVG(PRICE) AS AVG_PRICE, COUNT(*) AS RECORD_COUNT
                FROM STAGING.PRICE_HISTORY GROUP BY PRODUCT_ID, CRAWLED_AT::DATE
            ) s ON t.PRODUCT_ID = s.PRODUCT_ID AND t.PRICE_DATE = s.PRICE_DATE
            WHEN NOT MATCHED THEN INSERT
                (PRODUCT_ID, PRICE_DATE, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT)
                VALUES (s.PRODUCT_ID, s.PRICE_DATE, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT)
            WHEN MATCHED THEN UPDATE SET
                MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
                AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT
        """)
        # WEEKLY_PRICE_STATS
        cur.execute("""
            MERGE INTO ANALYTICS.WEEKLY_PRICE_STATS t
            USING (
                SELECT PRODUCT_ID, DATE_TRUNC('WEEK', CRAWLED_AT)::DATE AS WEEK_START,
                    MIN(PRICE) AS MIN_PRICE, MAX(PRICE) AS MAX_PRICE,
                    AVG(PRICE) AS AVG_PRICE, COUNT(*) AS RECORD_COUNT
                FROM STAGING.PRICE_HISTORY GROUP BY PRODUCT_ID, DATE_TRUNC('WEEK', CRAWLED_AT)
            ) s ON t.PRODUCT_ID = s.PRODUCT_ID AND t.WEEK_START = s.WEEK_START
            WHEN NOT MATCHED THEN INSERT
                (PRODUCT_ID, WEEK_START, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT)
                VALUES (s.PRODUCT_ID, s.WEEK_START, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT)
            WHEN MATCHED THEN UPDATE SET
                MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
                AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT
        """)
        # PRODUCT_STATS
        cur.execute("""
            MERGE INTO ANALYTICS.PRODUCT_STATS t
            USING (
                SELECT PRODUCT_ID, AVG(PRICE) AS AVG_PRICE,
                    MIN(PRICE) AS MIN_PRICE_EVER, MAX(PRICE) AS MAX_PRICE_EVER,
                    MIN(CRAWLED_AT) AS FIRST_CRAWLED_AT, MAX(CRAWLED_AT) AS LAST_CRAWLED_AT,
                    COUNT(*) AS TOTAL_RECORDS
                FROM STAGING.PRICE_HISTORY GROUP BY PRODUCT_ID
            ) s ON t.PRODUCT_ID = s.PRODUCT_ID
            WHEN NOT MATCHED THEN INSERT
                (PRODUCT_ID, AVG_PRICE, MIN_PRICE_EVER, MAX_PRICE_EVER,
                 FIRST_CRAWLED_AT, LAST_CRAWLED_AT, TOTAL_RECORDS)
                VALUES (s.PRODUCT_ID, s.AVG_PRICE, s.MIN_PRICE_EVER, s.MAX_PRICE_EVER,
                        s.FIRST_CRAWLED_AT, s.LAST_CRAWLED_AT, s.TOTAL_RECORDS)
            WHEN MATCHED THEN UPDATE SET
                AVG_PRICE = s.AVG_PRICE, MIN_PRICE_EVER = s.MIN_PRICE_EVER,
                MAX_PRICE_EVER = s.MAX_PRICE_EVER, FIRST_CRAWLED_AT = s.FIRST_CRAWLED_AT,
                LAST_CRAWLED_AT = s.LAST_CRAWLED_AT, TOTAL_RECORDS = s.TOTAL_RECORDS,
                UPDATED_AT = CURRENT_TIMESTAMP()
        """)
        cur.close()

    logger.info("[Analytics] 집계 완료")
