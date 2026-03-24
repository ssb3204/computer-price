"""Snowflake 3-Layer 파이프라인 DAG.

크롤링 → Raw 적재 → Staging 변환 → 변경 감지/알림 → Analytics 집계
매일 21:00, 22:00 KST (12:00, 13:00 UTC) 실행.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "computer_price",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="snowflake_price_pipeline",
    default_args=default_args,
    description="크롤링 → Raw → Staging → 변경감지 → Analytics (매일 21:00, 22:00 KST)",
    schedule_interval="0 12,13 * * *",  # 21:00, 22:00 KST = 12:00, 13:00 UTC
    start_date=datetime(2026, 3, 22),
    catchup=False,
    max_active_runs=1,
    tags=["crawl", "snowflake", "pipeline"],
) as dag:

    def _crawl_all_sites(**context):
        """Step 1: 3개 사이트 크롤링 → Raw 데이터 수집."""
        import json
        import logging

        from src.common.models import RawCrawledPrice
        from src.crawlers.compuzone import CompuzoneCrawler
        from src.crawlers.danawa import DanawaCrawler
        from src.crawlers.pc_estimate import PCEstimateCrawler

        logger = logging.getLogger(__name__)
        all_raw: list[RawCrawledPrice] = []

        for CrawlerClass in [DanawaCrawler, CompuzoneCrawler, PCEstimateCrawler]:
            crawler = CrawlerClass()
            try:
                raw_prices = crawler.crawl_raw()
                all_raw.extend(raw_prices)
                logger.info("[크롤링] %s: %d건", crawler.site_name, len(raw_prices))
            except Exception:
                logger.exception("[크롤링] %s 실패", crawler.site_name)

        logger.info("[크롤링] 총 %d건 수집", len(all_raw))

        # XCom으로 다음 태스크에 전달 (직렬화)
        serialized = [
            {
                "site": rp.site,
                "category": rp.category,
                "product_name": rp.product_name,
                "price_text": rp.price_text,
                "brand": rp.brand,
                "url": rp.url,
                "stock_status": rp.stock_status,
                "crawled_at": rp.crawled_at.isoformat(),
            }
            for rp in all_raw
        ]
        context["ti"].xcom_push(key="raw_prices", value=serialized)
        return len(all_raw)

    def _load_raw(**context):
        """Step 2: Raw 레이어에 원본 데이터 적재."""
        import logging
        from datetime import datetime, timezone

        from src.common.config import SnowflakeSettings
        from src.common.snowflake_client import get_connection

        logger = logging.getLogger(__name__)
        raw_data = context["ti"].xcom_pull(task_ids="crawl_all_sites", key="raw_prices")

        if not raw_data:
            logger.warning("[Raw] 적재할 데이터 없음")
            return 0

        settings = SnowflakeSettings()
        with get_connection(settings) as conn:
            cur = conn.cursor()
            cur.execute("USE DATABASE COMPUTER_PRICE")
            cur.execute("USE SCHEMA RAW")

            count = 0
            for rp in raw_data:
                cur.execute(
                    """INSERT INTO RAW_CRAWLED_PRICES
                       (SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, STOCK_STATUS, CRAWLED_AT)
                       SELECT %s, %s, %s, %s, %s, %s, %s, %s
                       WHERE NOT EXISTS (
                           SELECT 1 FROM RAW_CRAWLED_PRICES
                           WHERE SITE = %s AND CATEGORY = %s
                             AND PRODUCT_NAME = %s AND CRAWLED_AT = %s
                       )""",
                    (
                        rp["site"], rp["category"], rp["product_name"], rp["price_text"],
                        rp["brand"], rp["url"], rp["stock_status"], rp["crawled_at"],
                        rp["site"], rp["category"], rp["product_name"], rp["crawled_at"],
                    ),
                )
                count += 1

            cur.close()

        logger.info("[Raw] %d건 적재 완료", count)
        return count

    def _transform_staging(**context):
        """Step 3: Raw → Staging 변환."""
        import logging
        import re

        from src.common.config import SnowflakeSettings
        from src.common.snowflake_client import get_connection
        from src.crawlers.parser_utils import parse_korean_price

        logger = logging.getLogger(__name__)

        SITES = {
            "danawa": ("https://shop.danawa.com", "다나와"),
            "compuzone": ("https://www.compuzone.co.kr", "컴퓨존"),
            "pc_estimate": ("https://kjwwang.com", "견적왕"),
        }
        CATEGORIES = ["CPU", "GPU", "RAM", "SSD"]

        settings = SnowflakeSettings()
        with get_connection(settings) as conn:
            cur = conn.cursor()
            cur.execute("USE DATABASE COMPUTER_PRICE")
            cur.execute("USE SCHEMA STAGING")

            # Ensure DIM tables
            for name, (base_url, display_name) in SITES.items():
                cur.execute(
                    "MERGE INTO DIM_SITES t USING (SELECT %s AS NAME, %s AS BASE_URL, %s AS DISPLAY_NAME) s "
                    "ON t.NAME = s.NAME "
                    "WHEN NOT MATCHED THEN INSERT (NAME, DISPLAY_NAME, BASE_URL) "
                    "VALUES (s.NAME, s.DISPLAY_NAME, s.BASE_URL)",
                    (name, base_url, display_name),
                )
            cur.execute("SELECT SITE_ID, NAME FROM DIM_SITES")
            site_map = {row[1]: row[0] for row in cur.fetchall()}

            for cat in CATEGORIES:
                cur.execute(
                    "MERGE INTO DIM_CATEGORIES t USING (SELECT %s AS NAME) s "
                    "ON t.NAME = s.NAME "
                    "WHEN NOT MATCHED THEN INSERT (NAME) VALUES (s.NAME)",
                    (cat,),
                )
            cur.execute("SELECT CATEGORY_ID, NAME FROM DIM_CATEGORIES")
            cat_map = {row[1]: row[0] for row in cur.fetchall()}

            # Process unprocessed raw rows
            cur.execute("USE SCHEMA RAW")
            cur.execute(
                "SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, "
                "STOCK_STATUS, CRAWLED_AT FROM RAW_CRAWLED_PRICES WHERE IS_PROCESSED = FALSE"
            )
            raw_rows = cur.fetchall()

            cur.execute("USE SCHEMA STAGING")
            processed = 0

            for row in raw_rows:
                raw_id, site, category, product_name, price_text, brand, url, stock_status, crawled_at = row

                price = parse_korean_price(price_text)
                if price is None:
                    continue

                site_id = site_map.get(site)
                cat_id = cat_map.get(category)
                if site_id is None or cat_id is None:
                    continue

                cleaned_name = re.sub(r"\s+", " ", product_name.strip())
                stock = "in_stock"

                # MERGE product
                cur.execute(
                    "MERGE INTO STG_PRODUCTS t "
                    "USING (SELECT %s AS SITE_ID, %s AS NAME) s "
                    "ON t.SITE_ID = s.SITE_ID AND t.NAME = s.NAME "
                    "WHEN NOT MATCHED THEN INSERT (SITE_ID, CATEGORY_ID, NAME, BRAND, URL) "
                    "VALUES (%s, %s, %s, %s, %s) "
                    "WHEN MATCHED THEN UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP()",
                    (site_id, cleaned_name, site_id, cat_id, cleaned_name, brand, url),
                )

                cur.execute(
                    "SELECT PRODUCT_ID FROM STG_PRODUCTS WHERE SITE_ID = %s AND NAME = %s",
                    (site_id, cleaned_name),
                )
                product_row = cur.fetchone()
                if not product_row:
                    continue
                product_id = product_row[0]

                # Insert daily price
                cur.execute(
                    "INSERT INTO STG_DAILY_PRICES (PRODUCT_ID, RAW_ID, PRICE, STOCK_STATUS, CRAWLED_AT) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (product_id, raw_id, price, stock, crawled_at),
                )

                # Upsert latest price
                cur.execute(
                    "MERGE INTO STG_LATEST_PRICES t "
                    "USING (SELECT %s AS PRODUCT_ID, %s AS PRICE, %s AS STOCK_STATUS, %s AS CRAWLED_AT) s "
                    "ON t.PRODUCT_ID = s.PRODUCT_ID "
                    "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRICE, STOCK_STATUS, CRAWLED_AT) "
                    "VALUES (s.PRODUCT_ID, s.PRICE, s.STOCK_STATUS, s.CRAWLED_AT) "
                    "WHEN MATCHED AND t.CRAWLED_AT <= s.CRAWLED_AT THEN UPDATE SET "
                    "PRICE = s.PRICE, STOCK_STATUS = s.STOCK_STATUS, "
                    "CRAWLED_AT = s.CRAWLED_AT, UPDATED_AT = CURRENT_TIMESTAMP()",
                    (product_id, price, stock, crawled_at),
                )

                # Mark raw as processed
                cur.execute("USE SCHEMA RAW")
                cur.execute(
                    "UPDATE RAW_CRAWLED_PRICES SET IS_PROCESSED = TRUE, "
                    "PROCESSED_AT = CURRENT_TIMESTAMP() WHERE ID = %s",
                    (raw_id,),
                )
                cur.execute("USE SCHEMA STAGING")
                processed += 1

            cur.close()

        logger.info("[Staging] %d건 변환 완료", processed)
        return processed

    def _detect_changes_and_alert(**context):
        """Step 4: 가격 변경 감지 → STG_ALERTS 생성.

        transform_staging 이후, aggregate_analytics 이전에 실행.
        PRODUCT_STATS가 아직 갱신 전이므로 ALL_TIME_LOW/HIGH 비교가 정확함.
        """
        import logging

        from src.common.config import SnowflakeSettings
        from src.common.snowflake_client import get_connection

        logger = logging.getLogger(__name__)
        settings = SnowflakeSettings()

        PRICE_DROP_PCT = -5.0
        PRICE_SPIKE_PCT = 10.0

        with get_connection(settings) as conn:
            cur = conn.cursor()
            cur.execute("USE DATABASE COMPUTER_PRICE")

            cur.execute(f"""
                INSERT INTO STAGING.STG_ALERTS
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
                    FROM STAGING.STG_DAILY_PRICES
                ),
                candidates AS (
                    SELECT
                        r.PRODUCT_ID,
                        r.DAILY_PRICE_ID,
                        r.PRICE AS new_price,
                        r.prev_price AS old_price,
                        CASE WHEN r.prev_price > 0
                             THEN ROUND((r.PRICE - r.prev_price)
                                        / r.prev_price * 100, 4)
                             ELSE NULL
                        END AS change_pct,
                        ps.ALL_TIME_LOW,
                        ps.ALL_TIME_HIGH
                    FROM ranked r
                    LEFT JOIN ANALYTICS.PRODUCT_STATS ps
                        ON r.PRODUCT_ID = ps.PRODUCT_ID
                    WHERE r.rn = 1
                      AND r.prev_price IS NOT NULL
                      AND r.PRICE != r.prev_price
                      AND NOT EXISTS (
                          SELECT 1 FROM STAGING.STG_ALERTS a
                          WHERE a.DAILY_PRICE_ID = r.DAILY_PRICE_ID
                      )
                )
                SELECT
                    PRODUCT_ID,
                    DAILY_PRICE_ID,
                    CASE
                        WHEN ALL_TIME_LOW IS NOT NULL AND new_price < ALL_TIME_LOW
                            THEN 'NEW_LOW'
                        WHEN ALL_TIME_HIGH IS NOT NULL AND new_price > ALL_TIME_HIGH
                            THEN 'NEW_HIGH'
                        WHEN change_pct <= {PRICE_DROP_PCT}
                            THEN 'PRICE_DROP'
                        WHEN change_pct >= {PRICE_SPIKE_PCT}
                            THEN 'PRICE_SPIKE'
                    END AS alert_type,
                    old_price,
                    new_price,
                    change_pct
                FROM candidates
                WHERE CASE
                        WHEN ALL_TIME_LOW IS NOT NULL AND new_price < ALL_TIME_LOW
                            THEN 'NEW_LOW'
                        WHEN ALL_TIME_HIGH IS NOT NULL AND new_price > ALL_TIME_HIGH
                            THEN 'NEW_HIGH'
                        WHEN change_pct <= {PRICE_DROP_PCT}
                            THEN 'PRICE_DROP'
                        WHEN change_pct >= {PRICE_SPIKE_PCT}
                            THEN 'PRICE_SPIKE'
                    END IS NOT NULL
            """)

            alert_count = cur.rowcount
            logger.info("[Alert] %d건 알림 생성", alert_count)

            cur.execute("SELECT COUNT(*) FROM STAGING.STG_ALERTS")
            total = cur.fetchone()[0]
            logger.info("[Alert] STG_ALERTS 총 %d건", total)

            cur.close()

        return alert_count

    def _aggregate_analytics(**context):
        """Step 5: Staging → Analytics 집계."""
        import logging

        from src.common.config import SnowflakeSettings
        from src.common.snowflake_client import get_connection

        logger = logging.getLogger(__name__)
        settings = SnowflakeSettings()

        with get_connection(settings) as conn:
            cur = conn.cursor()
            cur.execute("USE DATABASE COMPUTER_PRICE")

            # Daily summary
            cur.execute("""
                MERGE INTO ANALYTICS.DAILY_SUMMARY t
                USING (
                    SELECT PRODUCT_ID, CRAWLED_AT::DATE AS PRICE_DATE,
                        MIN(PRICE) AS MIN_PRICE, MAX(PRICE) AS MAX_PRICE,
                        AVG(PRICE) AS AVG_PRICE, COUNT(*) AS RECORD_COUNT,
                        MAX_BY(STOCK_STATUS, CRAWLED_AT) AS STOCK_STATUS
                    FROM STAGING.STG_DAILY_PRICES GROUP BY PRODUCT_ID, CRAWLED_AT::DATE
                ) s ON t.PRODUCT_ID = s.PRODUCT_ID AND t.PRICE_DATE = s.PRICE_DATE
                WHEN NOT MATCHED THEN INSERT
                    (PRODUCT_ID, PRICE_DATE, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT, STOCK_STATUS)
                    VALUES (s.PRODUCT_ID, s.PRICE_DATE, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT, s.STOCK_STATUS)
                WHEN MATCHED THEN UPDATE SET
                    MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
                    AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT, STOCK_STATUS = s.STOCK_STATUS
            """)

            # Weekly summary
            cur.execute("""
                MERGE INTO ANALYTICS.WEEKLY_SUMMARY t
                USING (
                    SELECT PRODUCT_ID, DATE_TRUNC('WEEK', CRAWLED_AT)::DATE AS WEEK_START,
                        MIN(PRICE) AS MIN_PRICE, MAX(PRICE) AS MAX_PRICE,
                        AVG(PRICE) AS AVG_PRICE, COUNT(*) AS RECORD_COUNT
                    FROM STAGING.STG_DAILY_PRICES GROUP BY PRODUCT_ID, DATE_TRUNC('WEEK', CRAWLED_AT)
                ) s ON t.PRODUCT_ID = s.PRODUCT_ID AND t.WEEK_START = s.WEEK_START
                WHEN NOT MATCHED THEN INSERT
                    (PRODUCT_ID, WEEK_START, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT)
                    VALUES (s.PRODUCT_ID, s.WEEK_START, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT)
                WHEN MATCHED THEN UPDATE SET
                    MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
                    AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT
            """)

            # Product stats
            cur.execute("""
                MERGE INTO ANALYTICS.PRODUCT_STATS t
                USING (
                    SELECT PRODUCT_ID, AVG(PRICE) AS OVERALL_AVG,
                        MIN(PRICE) AS ALL_TIME_LOW, MAX(PRICE) AS ALL_TIME_HIGH,
                        MIN(CRAWLED_AT) AS FIRST_SEEN, MAX(CRAWLED_AT) AS LAST_SEEN,
                        COUNT(*) AS TOTAL_RECORDS
                    FROM STAGING.STG_DAILY_PRICES GROUP BY PRODUCT_ID
                ) s ON t.PRODUCT_ID = s.PRODUCT_ID
                WHEN NOT MATCHED THEN INSERT
                    (PRODUCT_ID, OVERALL_AVG, ALL_TIME_LOW, ALL_TIME_HIGH, FIRST_SEEN, LAST_SEEN, TOTAL_RECORDS)
                    VALUES (s.PRODUCT_ID, s.OVERALL_AVG, s.ALL_TIME_LOW, s.ALL_TIME_HIGH, s.FIRST_SEEN, s.LAST_SEEN, s.TOTAL_RECORDS)
                WHEN MATCHED THEN UPDATE SET
                    OVERALL_AVG = s.OVERALL_AVG, ALL_TIME_LOW = s.ALL_TIME_LOW,
                    ALL_TIME_HIGH = s.ALL_TIME_HIGH, FIRST_SEEN = s.FIRST_SEEN,
                    LAST_SEEN = s.LAST_SEEN, TOTAL_RECORDS = s.TOTAL_RECORDS,
                    UPDATED_AT = CURRENT_TIMESTAMP()
            """)

            # 결과 로그
            for schema, table in [
                ("RAW", "RAW_CRAWLED_PRICES"),
                ("STAGING", "STG_PRODUCTS"),
                ("STAGING", "STG_DAILY_PRICES"),
                ("ANALYTICS", "DAILY_SUMMARY"),
                ("ANALYTICS", "WEEKLY_SUMMARY"),
                ("ANALYTICS", "PRODUCT_STATS"),
            ]:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                logger.info("[Analytics] %s.%s: %d건", schema, table, cur.fetchone()[0])

            cur.close()

    # ── Task 정의 ──

    crawl_task = PythonOperator(
        task_id="crawl_all_sites",
        python_callable=_crawl_all_sites,
    )

    raw_task = PythonOperator(
        task_id="load_raw",
        python_callable=_load_raw,
    )

    staging_task = PythonOperator(
        task_id="transform_staging",
        python_callable=_transform_staging,
    )

    detect_task = PythonOperator(
        task_id="detect_changes_and_alert",
        python_callable=_detect_changes_and_alert,
    )

    analytics_task = PythonOperator(
        task_id="aggregate_analytics",
        python_callable=_aggregate_analytics,
    )

    crawl_task >> raw_task >> staging_task >> detect_task >> analytics_task
