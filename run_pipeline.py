"""Standalone pipeline runner — Airflow 없이 전체 파이프라인 실행.

GitHub Actions 또는 로컬에서 직접 실행:
    python run_pipeline.py

환경변수 필요:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE (기본값: COMPUTE_WH)
    SNOWFLAKE_DATABASE  (기본값: COMPUTER_PRICE)
    SLACK_WEBHOOK_URL   (선택, 크롤링 실패 시 알림)
"""

import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

import requests

from src.common.config import SnowflakeSettings
from src.common.models import RawCrawledPrice
from src.common.snowflake_client import get_connection
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.danawa import DanawaCrawler
from src.crawlers.parser_utils import CATEGORIES, parse_korean_price, validate_price
from src.crawlers.pc_estimate import PCEstimateCrawler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Step 1: 크롤링 ──────────────────────────────────────────────────────────


def crawl_all_sites() -> tuple[list[RawCrawledPrice], list[dict]]:
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

    logger.info("[크롤링] 총 %d건 수집 (실패: %d개 사이트)", len(all_raw), len(crawl_failures))
    return all_raw, crawl_failures


# ── Step 2: Raw 적재 ─────────────────────────────────────────────────────────


def load_raw(settings: SnowflakeSettings, all_raw: list[RawCrawledPrice]) -> int:
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


# ── Step 3: Staging 변환 ─────────────────────────────────────────────────────


def transform_staging(settings: SnowflakeSettings) -> int:
    SITES = {
        "danawa": ("https://shop.danawa.com", "다나와"),
        "compuzone": ("https://www.compuzone.co.kr", "컴퓨존"),
        "pc_estimate": ("https://kjwwang.com", "견적왕"),
    }

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE SCHEMA STAGING")

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

        parsed = []
        anomaly_count = 0
        for row in raw_rows:
            raw_id, site, category, product_name, price_text, brand, url, crawled_at = row
            price = parse_korean_price(price_text)
            if price is None:
                continue
            if not validate_price(price, category):
                logger.warning(
                    "[Staging] 이상치 가격 제외 — site=%s category=%s name=%s price=%d",
                    site, category, product_name[:40], price,
                )
                anomaly_count += 1
                continue
            site_id = site_map.get(site)
            cat_id = cat_map.get(category)
            if site_id is None or cat_id is None:
                continue
            cleaned_name = re.sub(r"\s+", " ", product_name.strip())
            parsed.append((raw_id, site_id, cat_id, cleaned_name, brand, url, price, crawled_at))

        if anomaly_count:
            logger.warning("[Staging] 이상치 총 %d건 제외", anomaly_count)

        if not parsed:
            logger.info("[Staging] 유효한 데이터 없음")
            cur.close()
            return 0

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

        cur.execute("SELECT PRODUCT_ID, SITE_ID, NAME FROM STG_PRODUCTS")
        product_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

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
                "MERGE INTO STG_DAILY_PRICES t "
                "USING (SELECT %s AS PRODUCT_ID, %s AS RAW_ID, %s AS PRICE, %s AS CRAWLED_AT) s "
                "ON t.PRODUCT_ID = s.PRODUCT_ID AND t.CRAWLED_AT = s.CRAWLED_AT "
                "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, RAW_ID, PRICE, CRAWLED_AT) "
                "VALUES (s.PRODUCT_ID, s.RAW_ID, s.PRICE, s.CRAWLED_AT)",
                daily_rows,
            )
            cur.executemany(
                "MERGE INTO STG_LATEST_PRICES t "
                "USING (SELECT %s AS PRODUCT_ID, %s AS PRICE, %s AS CRAWLED_AT) s "
                "ON t.PRODUCT_ID = s.PRODUCT_ID "
                "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRICE, CRAWLED_AT) "
                "VALUES (s.PRODUCT_ID, s.PRICE, s.CRAWLED_AT) "
                "WHEN MATCHED AND t.CRAWLED_AT <= s.CRAWLED_AT THEN UPDATE SET "
                "PRICE = s.PRICE, CRAWLED_AT = s.CRAWLED_AT, UPDATED_AT = CURRENT_TIMESTAMP()",
                [(pid, price, cat) for pid, _, price, cat in daily_rows],
            )

        if processed_raw_ids:
            cur.execute("USE SCHEMA RAW")
            placeholders = ", ".join(["%s"] * len(processed_raw_ids))
            cur.execute(
                f"UPDATE RAW_CRAWLED_PRICES SET IS_PROCESSED = TRUE, "  # noqa: S608
                f"PROCESSED_AT = CURRENT_TIMESTAMP() WHERE ID IN ({placeholders})",
                processed_raw_ids,
            )

        cur.close()

    logger.info("[Staging] %d건 변환 완료", len(daily_rows) if daily_rows else 0)
    return len(daily_rows) if daily_rows else 0


# ── Step 4: 변경 감지 ────────────────────────────────────────────────────────


def detect_changes(settings: SnowflakeSettings) -> int:
    MIN_CHANGE_PCT = 1.0
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
                         THEN ROUND((r.PRICE - r.prev_price) / r.prev_price * 100, 4)
                         ELSE NULL
                    END AS change_pct,
                    ps.ALL_TIME_LOW,
                    ps.ALL_TIME_HIGH
                FROM ranked r
                LEFT JOIN ANALYTICS.PRODUCT_STATS ps ON r.PRODUCT_ID = ps.PRODUCT_ID
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
                PRODUCT_ID, DAILY_PRICE_ID,
                CASE
                    WHEN ALL_TIME_LOW IS NOT NULL AND new_price < ALL_TIME_LOW THEN 'NEW_LOW'
                    WHEN ALL_TIME_HIGH IS NOT NULL AND new_price > ALL_TIME_HIGH THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                END AS alert_type,
                old_price, new_price, change_pct
            FROM candidates
            WHERE CASE
                    WHEN ALL_TIME_LOW IS NOT NULL AND new_price < ALL_TIME_LOW THEN 'NEW_LOW'
                    WHEN ALL_TIME_HIGH IS NOT NULL AND new_price > ALL_TIME_HIGH THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                  END IS NOT NULL
        """, (MIN_CHANGE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT))
        alert_count = cur.rowcount
        cur.close()

    logger.info("[Alert] %d건 알림 생성", alert_count)
    return alert_count


# ── Step 5: Slack 전송 (크롤링 실패만) ──────────────────────────────────────


def send_slack_failures(crawl_failures: list[dict]) -> int:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.info("[Slack] SLACK_WEBHOOK_URL 미설정 — 건너뜀")
        return 0
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


# ── Step 6: Analytics 집계 ───────────────────────────────────────────────────


def aggregate_analytics(settings: SnowflakeSettings) -> None:
    with get_connection(settings) as conn:
        cur = conn.cursor()
        # DAILY_SUMMARY
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
        # WEEKLY_SUMMARY
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
        # PRODUCT_STATS
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
        cur.close()

    logger.info("[Analytics] 집계 완료")


# ── 메인 ─────────────────────────────────────────────────────────────────────


def main() -> int:
    logger.info("=== 파이프라인 시작 ===")
    settings = SnowflakeSettings()

    # Step 1: 크롤링
    all_raw, crawl_failures = crawl_all_sites()

    # Step 2: Raw 적재
    if all_raw:
        load_raw(settings, all_raw)

    # Step 3: Staging 변환
    transform_staging(settings)

    # Step 4: 변경 감지
    detect_changes(settings)

    # Step 5: Slack (크롤링 실패만)
    send_slack_failures(crawl_failures)

    # Step 6: Analytics 집계
    aggregate_analytics(settings)

    logger.info("=== 파이프라인 완료 ===")

    # 모든 사이트가 실패하면 비정상 종료
    if len(crawl_failures) == 3:
        logger.error("모든 사이트 크롤링 실패 — exit 1")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
