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
        from datetime import datetime, timezone

        import requests

        from src.common.models import RawCrawledPrice
        from src.crawlers.compuzone import CompuzoneCrawler
        from src.crawlers.danawa import DanawaCrawler
        from src.crawlers.pc_estimate import PCEstimateCrawler

        logger = logging.getLogger(__name__)
        all_raw: list[RawCrawledPrice] = []
        crawl_failures: list[dict] = []

        for CrawlerClass in [DanawaCrawler, CompuzoneCrawler, PCEstimateCrawler]:
            crawler = CrawlerClass()
            try:
                raw_prices = crawler.crawl_raw()
                all_raw.extend(raw_prices)
                logger.info("[크롤링] %s: %d건", crawler.site_name, len(raw_prices))
            except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError) as e:
                failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                crawl_failures.append({
                    "site_name": crawler.site_name,
                    "error": f"{type(e).__name__}: {e}",
                    "failed_at": failed_at,
                })
                logger.exception("[크롤링] %s 실패", crawler.site_name)

        logger.info("[크롤링] 총 %d건 수집", len(all_raw))
        context["ti"].xcom_push(key="crawl_failures", value=crawl_failures)

        # XCom으로 다음 태스크에 전달 (직렬화)
        serialized = [
            {
                "site": rp.site,
                "category": rp.category,
                "product_name": rp.product_name,
                "price_text": rp.price_text,
                "brand": rp.brand,
                "url": rp.url,
                "crawled_at": rp.crawled_at.isoformat(),
            }
            for rp in all_raw
        ]
        context["ti"].xcom_push(key="raw_prices", value=serialized)
        return len(all_raw)

    def _load_raw(**context):
        """Step 2: Raw 레이어에 원본 데이터 적재 (batch)."""
        import logging

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
            cur.execute("USE SCHEMA RAW")

            # Temp table에 batch insert 후 MERGE로 중복 제거
            cur.execute("""
                CREATE TEMPORARY TABLE TEMP_RAW_LOAD (
                    SITE STRING, CATEGORY STRING, PRODUCT_NAME STRING,
                    PRICE_TEXT STRING, BRAND STRING, URL STRING,
                    CRAWLED_AT STRING
                )
            """)

            rows = [
                (rp["site"], rp["category"], rp["product_name"], rp["price_text"],
                 rp["brand"], rp["url"], rp["crawled_at"])
                for rp in raw_data
            ]
            cur.executemany(
                "INSERT INTO TEMP_RAW_LOAD VALUES (%s, %s, %s, %s, %s, %s, %s)",
                rows,
            )

            cur.execute("""
                MERGE INTO RAW_CRAWLED_PRICES t
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

    def _transform_staging(**context):
        """Step 3: Raw → Staging 변환 (batch)."""
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

            # Fetch unprocessed raw rows
            cur.execute("USE SCHEMA RAW")
            cur.execute(
                "SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, "
                "CRAWLED_AT FROM RAW_CRAWLED_PRICES WHERE IS_PROCESSED = FALSE"
            )
            raw_rows = cur.fetchall()
            cur.execute("USE SCHEMA STAGING")

            if not raw_rows:
                logger.info("[Staging] 변환할 데이터 없음")
                cur.close()
                return 0

            # Python에서 가격 파싱 → 유효한 행만 수집
            parsed = []
            for row in raw_rows:
                raw_id, site, category, product_name, price_text, brand, url, crawled_at = row
                price = parse_korean_price(price_text)
                if price is None:
                    continue
                site_id = site_map.get(site)
                cat_id = cat_map.get(category)
                if site_id is None or cat_id is None:
                    continue
                cleaned_name = re.sub(r"\s+", " ", product_name.strip())
                parsed.append((raw_id, site_id, cat_id, cleaned_name, brand, url, price, crawled_at))

            if not parsed:
                logger.info("[Staging] 유효한 데이터 없음")
                cur.close()
                return 0

            # 1) Batch MERGE products
            cur.executemany(
                "MERGE INTO STG_PRODUCTS t "
                "USING (SELECT %s AS SITE_ID, %s AS NAME) s "
                "ON t.SITE_ID = s.SITE_ID AND t.NAME = s.NAME "
                "WHEN NOT MATCHED THEN INSERT (SITE_ID, CATEGORY_ID, NAME, BRAND, URL) "
                "VALUES (%s, %s, %s, %s, %s) "
                "WHEN MATCHED THEN UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP()",
                [(site_id, name, site_id, cat_id, name, brand, url)
                 for _, site_id, cat_id, name, brand, url, _, _ in parsed],
            )

            # 2) Fetch product_id mapping (single query)
            cur.execute("SELECT PRODUCT_ID, SITE_ID, NAME FROM STG_PRODUCTS")
            product_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

            # 3) Batch INSERT daily prices
            daily_rows = []
            processed_raw_ids = []
            for raw_id, site_id, cat_id, name, brand, url, price, crawled_at in parsed:
                product_id = product_map.get((site_id, name))
                if product_id is None:
                    continue
                daily_rows.append((product_id, raw_id, price, crawled_at))
                processed_raw_ids.append(raw_id)

            if daily_rows:
                cur.executemany(
                    "INSERT INTO STG_DAILY_PRICES (PRODUCT_ID, RAW_ID, PRICE, CRAWLED_AT) "
                    "VALUES (%s, %s, %s, %s)",
                    daily_rows,
                )

            # 4) Batch MERGE latest prices
            if daily_rows:
                cur.executemany(
                    "MERGE INTO STG_LATEST_PRICES t "
                    "USING (SELECT %s AS PRODUCT_ID, %s AS PRICE, %s AS CRAWLED_AT) s "
                    "ON t.PRODUCT_ID = s.PRODUCT_ID "
                    "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRICE, CRAWLED_AT) "
                    "VALUES (s.PRODUCT_ID, s.PRICE, s.CRAWLED_AT) "
                    "WHEN MATCHED AND t.CRAWLED_AT <= s.CRAWLED_AT THEN UPDATE SET "
                    "PRICE = s.PRICE, "
                    "CRAWLED_AT = s.CRAWLED_AT, UPDATED_AT = CURRENT_TIMESTAMP()",
                    [(pid, price, cat) for pid, _, price, cat in daily_rows],
                )

            # 5) Batch mark raw as processed
            if processed_raw_ids:
                cur.execute("USE SCHEMA RAW")
                placeholders = ", ".join(["%s"] * len(processed_raw_ids))
                cur.execute(
                    f"UPDATE RAW_CRAWLED_PRICES SET IS_PROCESSED = TRUE, "  # noqa: S608
                    f"PROCESSED_AT = CURRENT_TIMESTAMP() WHERE ID IN ({placeholders})",
                    processed_raw_ids,
                )

            cur.close()

        logger.info("[Staging] %d건 변환 완료", len(daily_rows))
        return len(daily_rows)

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

        MIN_CHANGE_PCT = 1.0   # 최소 1% 변동 시에만 알림
        PRICE_DROP_PCT = -5.0
        PRICE_SPIKE_PCT = 10.0

        with get_connection(settings) as conn:
            cur = conn.cursor()
            cur.execute("""
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
                      AND ABS((r.PRICE - r.prev_price) / r.prev_price * 100) >= %s
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
                        WHEN change_pct <= %s
                            THEN 'PRICE_DROP'
                        WHEN change_pct >= %s
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
                        WHEN change_pct <= %s
                            THEN 'PRICE_DROP'
                        WHEN change_pct >= %s
                            THEN 'PRICE_SPIKE'
                    END IS NOT NULL
            """, (
                MIN_CHANGE_PCT,
                PRICE_DROP_PCT, PRICE_SPIKE_PCT,
                PRICE_DROP_PCT, PRICE_SPIKE_PCT,
            ))

            alert_count = cur.rowcount
            logger.info("[Alert] %d건 알림 생성", alert_count)

            cur.execute("SELECT COUNT(*) FROM STAGING.STG_ALERTS")
            total = cur.fetchone()[0]
            logger.info("[Alert] STG_ALERTS 총 %d건", total)

            cur.close()

        return alert_count

    def _send_slack_alerts(**context):
        """Step 4.5: 크롤링 실패 시 Slack으로 전송."""
        import json
        import logging
        import os
        import urllib.error
        import urllib.request

        logger = logging.getLogger(__name__)

        webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
        if not webhook_url:
            logger.info("[Slack] SLACK_WEBHOOK_URL 미설정 — 건너뜀")
            return 0

        crawl_failures = context["ti"].xcom_pull(task_ids="crawl_all_sites", key="crawl_failures") or []
        if not crawl_failures:
            logger.info("[Slack] 크롤링 실패 없음 — 건너뜀")
            return 0

        lines = [f"*⚠️ 크롤링 실패 — {len(crawl_failures)}개 사이트*\n"]
        for failure in crawl_failures:
            lines.append(
                f"🔴 *{failure['site_name']}* — {failure['failed_at']}\n"
                f"    `{failure['error']}`"
            )

        payload = {"text": "\n".join(lines)}
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info("[Slack] 전송 완료 (status=%d)", resp.status)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            logger.exception("[Slack] 전송 실패")

        return len(crawl_failures)

    def _aggregate_analytics(**context):
        """Step 5: Staging → Analytics 집계."""
        import logging

        from src.common.config import SnowflakeSettings
        from src.common.snowflake_client import get_connection

        logger = logging.getLogger(__name__)
        settings = SnowflakeSettings()

        with get_connection(settings) as conn:
            cur = conn.cursor()
            # Daily summary
            cur.execute("""
                MERGE INTO ANALYTICS.DAILY_SUMMARY t
                USING (
                    SELECT PRODUCT_ID, CRAWLED_AT::DATE AS PRICE_DATE,
                        MIN(PRICE) AS MIN_PRICE, MAX(PRICE) AS MAX_PRICE,
                        AVG(PRICE) AS AVG_PRICE, COUNT(*) AS RECORD_COUNT
                    FROM STAGING.STG_DAILY_PRICES GROUP BY PRODUCT_ID, CRAWLED_AT::DATE
                ) s ON t.PRODUCT_ID = s.PRODUCT_ID AND t.PRICE_DATE = s.PRICE_DATE
                WHEN NOT MATCHED THEN INSERT
                    (PRODUCT_ID, PRICE_DATE, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT)
                    VALUES (s.PRODUCT_ID, s.PRICE_DATE, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT)
                WHEN MATCHED THEN UPDATE SET
                    MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
                    AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT
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

            # 결과 로그 — 허용 목록 기반 (외부 입력 없음)
            ALLOWED_TABLES = {
                ("RAW", "RAW_CRAWLED_PRICES"),
                ("STAGING", "STG_PRODUCTS"),
                ("STAGING", "STG_DAILY_PRICES"),
                ("ANALYTICS", "DAILY_SUMMARY"),
                ("ANALYTICS", "WEEKLY_SUMMARY"),
                ("ANALYTICS", "PRODUCT_STATS"),
            }
            for schema, table in ALLOWED_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")  # noqa: S608
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

    slack_task = PythonOperator(
        task_id="send_slack_alerts",
        python_callable=_send_slack_alerts,
    )

    analytics_task = PythonOperator(
        task_id="aggregate_analytics",
        python_callable=_aggregate_analytics,
    )

    crawl_task >> raw_task >> staging_task >> detect_task >> slack_task >> analytics_task
