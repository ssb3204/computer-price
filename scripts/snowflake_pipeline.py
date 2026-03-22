"""Snowflake 3-Layer 파이프라인: 크롤링 → Raw → Staging → Analytics.

사용법:
  PYTHONPATH=. python scripts/snowflake_pipeline.py
"""

import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from src.common.config import SnowflakeSettings
from src.common.models import RawCrawledPrice
from src.common.snowflake_client import get_connection
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.danawa import DanawaCrawler
from src.crawlers.pc_estimate import PCEstimateCrawler
from src.crawlers.parser_utils import parse_korean_price

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── DIM 초기 데이터 ──

SITES = {
    "danawa": ("https://shop.danawa.com", "다나와"),
    "compuzone": ("https://www.compuzone.co.kr", "컴퓨존"),
    "pc_estimate": ("https://kjwwang.com", "견적왕"),
}

CATEGORIES = ["CPU", "GPU", "RAM", "SSD"]

STOCK_STATUS_MAP = {
    "품절": "out_of_stock",
    "재고없음": "out_of_stock",
    "재고있음": "in_stock",
    "판매중": "in_stock",
}


def normalize_stock_status(raw: str | None) -> str:
    if raw is None:
        return "in_stock"  # 가격이 있는 상품은 판매중
    return STOCK_STATUS_MAP.get(raw.strip(), "in_stock")


def clean_product_name(raw: str) -> str:
    """상품명 정제: trim, 연속 공백 제거."""
    import re
    text = raw.strip()
    text = re.sub(r"\s+", " ", text)
    return text


# ── Step 1: 크롤링 ──

def crawl_all() -> list[RawCrawledPrice]:
    """3개 사이트에서 원본 데이터 수집."""
    all_raw: list[RawCrawledPrice] = []

    crawlers = [DanawaCrawler(), CompuzoneCrawler(), PCEstimateCrawler()]
    for crawler in crawlers:
        try:
            raw_prices = crawler.crawl_raw()
            all_raw.extend(raw_prices)
            logger.info("[크롤링] %s: %d건 수집", crawler.site_name, len(raw_prices))
        except Exception:
            logger.exception("[크롤링] %s 실패", crawler.site_name)

    logger.info("[크롤링] 총 %d건 수집 완료", len(all_raw))
    return all_raw


# ── Step 2: Raw 적재 ──

def load_raw(cursor, raw_prices: list[RawCrawledPrice]) -> list[int]:
    """Raw 레이어에 원본 데이터 적재. 반환: 삽입된 row ID 목록."""
    cursor.execute("USE DATABASE COMPUTER_PRICE")
    cursor.execute("USE SCHEMA RAW")

    inserted_ids = []
    for rp in raw_prices:
        cursor.execute(
            """INSERT INTO RAW_CRAWLED_PRICES
               (SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, STOCK_STATUS, CRAWLED_AT)
               SELECT %s, %s, %s, %s, %s, %s, %s, %s
               WHERE NOT EXISTS (
                   SELECT 1 FROM RAW_CRAWLED_PRICES
                   WHERE SITE = %s AND CATEGORY = %s
                     AND PRODUCT_NAME = %s AND CRAWLED_AT = %s
               )""",
            (
                rp.site, rp.category, rp.product_name, rp.price_text,
                rp.brand, rp.url, rp.stock_status, rp.crawled_at.isoformat(),
                rp.site, rp.category, rp.product_name, rp.crawled_at.isoformat(),
            ),
        )
        # 방금 삽입된 ID 가져오기
        cursor.execute(
            """SELECT ID FROM RAW_CRAWLED_PRICES
               WHERE SITE = %s AND CATEGORY = %s
                 AND PRODUCT_NAME = %s AND CRAWLED_AT = %s""",
            (rp.site, rp.category, rp.product_name, rp.crawled_at.isoformat()),
        )
        row = cursor.fetchone()
        if row:
            inserted_ids.append(row[0])

    logger.info("[Raw] %d건 적재 완료", len(inserted_ids))
    return inserted_ids


# ── Step 3: Staging 변환 ──

def ensure_dim_tables(cursor) -> tuple[dict[str, int], dict[str, int]]:
    """DIM 테이블 초기화 및 ID 매핑 반환."""
    cursor.execute("USE DATABASE COMPUTER_PRICE")
    cursor.execute("USE SCHEMA STAGING")

    # Sites
    for name, (base_url, display_name) in SITES.items():
        cursor.execute(
            "MERGE INTO DIM_SITES t USING (SELECT %s AS NAME, %s AS BASE_URL, %s AS DISPLAY_NAME) s "
            "ON t.NAME = s.NAME "
            "WHEN NOT MATCHED THEN INSERT (NAME, DISPLAY_NAME, BASE_URL) VALUES (s.NAME, s.DISPLAY_NAME, s.BASE_URL)",
            (name, base_url, display_name),
        )
    cursor.execute("SELECT SITE_ID, NAME FROM DIM_SITES")
    site_map = {row[1]: row[0] for row in cursor.fetchall()}

    # Categories
    for cat in CATEGORIES:
        cursor.execute(
            "MERGE INTO DIM_CATEGORIES t USING (SELECT %s AS NAME) s "
            "ON t.NAME = s.NAME "
            "WHEN NOT MATCHED THEN INSERT (NAME) VALUES (s.NAME)",
            (cat,),
        )
    cursor.execute("SELECT CATEGORY_ID, NAME FROM DIM_CATEGORIES")
    cat_map = {row[1]: row[0] for row in cursor.fetchall()}

    return site_map, cat_map


def transform_raw_to_staging(cursor) -> int:
    """미처리 Raw 데이터를 Staging으로 변환."""
    cursor.execute("USE DATABASE COMPUTER_PRICE")

    site_map, cat_map = ensure_dim_tables(cursor)

    # 미처리 raw 데이터 조회
    cursor.execute("USE SCHEMA RAW")
    cursor.execute(
        "SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, "
        "STOCK_STATUS, CRAWLED_AT FROM RAW_CRAWLED_PRICES WHERE IS_PROCESSED = FALSE"
    )
    raw_rows = cursor.fetchall()

    if not raw_rows:
        logger.info("[Staging] 처리할 미처리 Raw 데이터 없음")
        return 0

    cursor.execute("USE SCHEMA STAGING")
    processed = 0

    for row in raw_rows:
        raw_id, site, category, product_name, price_text, brand, url, stock_status, crawled_at = row

        # 가격 파싱
        price = parse_korean_price(price_text)
        if price is None:
            logger.warning("[Staging] 가격 파싱 실패: %s (raw_id=%d)", price_text, raw_id)
            continue

        site_id = site_map.get(site)
        cat_id = cat_map.get(category)
        if site_id is None or cat_id is None:
            logger.warning("[Staging] 매핑 실패: site=%s, category=%s", site, category)
            continue

        cleaned_name = clean_product_name(product_name)
        stock = normalize_stock_status(stock_status)

        # MERGE product
        cursor.execute(
            "MERGE INTO STG_PRODUCTS t "
            "USING (SELECT %s AS SITE_ID, %s AS NAME) s "
            "ON t.SITE_ID = s.SITE_ID AND t.NAME = s.NAME "
            "WHEN NOT MATCHED THEN INSERT (SITE_ID, CATEGORY_ID, NAME, BRAND, URL) "
            "VALUES (%s, %s, %s, %s, %s) "
            "WHEN MATCHED THEN UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP()",
            (site_id, cleaned_name, site_id, cat_id, cleaned_name, brand, url),
        )

        # Get product_id
        cursor.execute(
            "SELECT PRODUCT_ID FROM STG_PRODUCTS WHERE SITE_ID = %s AND NAME = %s",
            (site_id, cleaned_name),
        )
        product_row = cursor.fetchone()
        if not product_row:
            continue
        product_id = product_row[0]

        # Insert daily price
        cursor.execute(
            "INSERT INTO STG_DAILY_PRICES (PRODUCT_ID, RAW_ID, PRICE, STOCK_STATUS, CRAWLED_AT) "
            "VALUES (%s, %s, %s, %s, %s)",
            (product_id, raw_id, price, stock, crawled_at),
        )

        # Upsert latest price
        cursor.execute(
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
        cursor.execute("USE SCHEMA RAW")
        cursor.execute(
            "UPDATE RAW_CRAWLED_PRICES SET IS_PROCESSED = TRUE, PROCESSED_AT = CURRENT_TIMESTAMP() "
            "WHERE ID = %s",
            (raw_id,),
        )
        cursor.execute("USE SCHEMA STAGING")
        processed += 1

    logger.info("[Staging] %d건 변환 완료", processed)
    return processed


# ── Step 4: Analytics 집계 ──

def aggregate_analytics(cursor) -> None:
    """Staging → Analytics 집계."""
    cursor.execute("USE DATABASE COMPUTER_PRICE")

    # Daily summary
    cursor.execute("""
        MERGE INTO ANALYTICS.DAILY_SUMMARY t
        USING (
            SELECT
                PRODUCT_ID,
                CRAWLED_AT::DATE AS PRICE_DATE,
                MIN(PRICE) AS MIN_PRICE,
                MAX(PRICE) AS MAX_PRICE,
                AVG(PRICE) AS AVG_PRICE,
                COUNT(*) AS RECORD_COUNT,
                MAX_BY(STOCK_STATUS, CRAWLED_AT) AS STOCK_STATUS
            FROM STAGING.STG_DAILY_PRICES
            GROUP BY PRODUCT_ID, CRAWLED_AT::DATE
        ) s
        ON t.PRODUCT_ID = s.PRODUCT_ID AND t.PRICE_DATE = s.PRICE_DATE
        WHEN NOT MATCHED THEN INSERT
            (PRODUCT_ID, PRICE_DATE, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT, STOCK_STATUS)
            VALUES (s.PRODUCT_ID, s.PRICE_DATE, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT, s.STOCK_STATUS)
        WHEN MATCHED THEN UPDATE SET
            MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
            AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT,
            STOCK_STATUS = s.STOCK_STATUS
    """)
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.DAILY_SUMMARY")
    logger.info("[Analytics] DAILY_SUMMARY: %d건", cursor.fetchone()[0])

    # Weekly summary
    cursor.execute("""
        MERGE INTO ANALYTICS.WEEKLY_SUMMARY t
        USING (
            SELECT
                PRODUCT_ID,
                DATE_TRUNC('WEEK', CRAWLED_AT)::DATE AS WEEK_START,
                MIN(PRICE) AS MIN_PRICE,
                MAX(PRICE) AS MAX_PRICE,
                AVG(PRICE) AS AVG_PRICE,
                COUNT(*) AS RECORD_COUNT
            FROM STAGING.STG_DAILY_PRICES
            GROUP BY PRODUCT_ID, DATE_TRUNC('WEEK', CRAWLED_AT)
        ) s
        ON t.PRODUCT_ID = s.PRODUCT_ID AND t.WEEK_START = s.WEEK_START
        WHEN NOT MATCHED THEN INSERT
            (PRODUCT_ID, WEEK_START, MIN_PRICE, MAX_PRICE, AVG_PRICE, RECORD_COUNT)
            VALUES (s.PRODUCT_ID, s.WEEK_START, s.MIN_PRICE, s.MAX_PRICE, s.AVG_PRICE, s.RECORD_COUNT)
        WHEN MATCHED THEN UPDATE SET
            MIN_PRICE = s.MIN_PRICE, MAX_PRICE = s.MAX_PRICE,
            AVG_PRICE = s.AVG_PRICE, RECORD_COUNT = s.RECORD_COUNT
    """)
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.WEEKLY_SUMMARY")
    logger.info("[Analytics] WEEKLY_SUMMARY: %d건", cursor.fetchone()[0])

    # Product stats
    cursor.execute("""
        MERGE INTO ANALYTICS.PRODUCT_STATS t
        USING (
            SELECT
                PRODUCT_ID,
                AVG(PRICE) AS OVERALL_AVG,
                MIN(PRICE) AS ALL_TIME_LOW,
                MAX(PRICE) AS ALL_TIME_HIGH,
                MIN(CRAWLED_AT) AS FIRST_SEEN,
                MAX(CRAWLED_AT) AS LAST_SEEN,
                COUNT(*) AS TOTAL_RECORDS
            FROM STAGING.STG_DAILY_PRICES
            GROUP BY PRODUCT_ID
        ) s
        ON t.PRODUCT_ID = s.PRODUCT_ID
        WHEN NOT MATCHED THEN INSERT
            (PRODUCT_ID, OVERALL_AVG, ALL_TIME_LOW, ALL_TIME_HIGH, FIRST_SEEN, LAST_SEEN, TOTAL_RECORDS)
            VALUES (s.PRODUCT_ID, s.OVERALL_AVG, s.ALL_TIME_LOW, s.ALL_TIME_HIGH, s.FIRST_SEEN, s.LAST_SEEN, s.TOTAL_RECORDS)
        WHEN MATCHED THEN UPDATE SET
            OVERALL_AVG = s.OVERALL_AVG, ALL_TIME_LOW = s.ALL_TIME_LOW,
            ALL_TIME_HIGH = s.ALL_TIME_HIGH, FIRST_SEEN = s.FIRST_SEEN,
            LAST_SEEN = s.LAST_SEEN, TOTAL_RECORDS = s.TOTAL_RECORDS,
            UPDATED_AT = CURRENT_TIMESTAMP()
    """)
    cursor.execute("SELECT COUNT(*) FROM ANALYTICS.PRODUCT_STATS")
    logger.info("[Analytics] PRODUCT_STATS: %d건", cursor.fetchone()[0])


# ── Main ──

def main() -> None:
    settings = SnowflakeSettings()

    # Step 1: 크롤링
    logger.info("=" * 50)
    logger.info("STEP 1: 크롤링")
    logger.info("=" * 50)
    raw_prices = crawl_all()
    if not raw_prices:
        logger.error("수집된 데이터 없음. 종료.")
        sys.exit(1)

    with get_connection(settings) as conn:
        cursor = conn.cursor()

        # Step 2: Raw 적재
        logger.info("=" * 50)
        logger.info("STEP 2: Raw 적재")
        logger.info("=" * 50)
        load_raw(cursor, raw_prices)

        # Step 3: Staging 변환
        logger.info("=" * 50)
        logger.info("STEP 3: Raw → Staging 변환")
        logger.info("=" * 50)
        transform_raw_to_staging(cursor)

        # Step 4: Analytics 집계
        logger.info("=" * 50)
        logger.info("STEP 4: Staging → Analytics 집계")
        logger.info("=" * 50)
        aggregate_analytics(cursor)

        # 최종 확인
        logger.info("=" * 50)
        logger.info("최종 결과")
        logger.info("=" * 50)
        for schema, table in [
            ("RAW", "RAW_CRAWLED_PRICES"),
            ("STAGING", "DIM_SITES"),
            ("STAGING", "DIM_CATEGORIES"),
            ("STAGING", "STG_PRODUCTS"),
            ("STAGING", "STG_DAILY_PRICES"),
            ("STAGING", "STG_LATEST_PRICES"),
            ("ANALYTICS", "DAILY_SUMMARY"),
            ("ANALYTICS", "WEEKLY_SUMMARY"),
            ("ANALYTICS", "PRODUCT_STATS"),
        ]:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count = cursor.fetchone()[0]
            logger.info("  %s.%s: %d건", schema, table, count)

        cursor.close()

    logger.info("파이프라인 완료!")


if __name__ == "__main__":
    main()
