"""Step 3: Staging 변환 — Raw 데이터를 정제해 STAGING.PRODUCTS / PRICE_HISTORY에 적재."""

import logging
import re

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.crawlers.parser_utils import parse_korean_price, validate_price

logger = logging.getLogger(__name__)

# 크롤러 내부 site_name → Snowflake 표시명 매핑
_SITE_DISPLAY_MAP = {
    "danawa":      "다나와",
    "compuzone":   "컴퓨존",
    "pc_estimate": "견적왕",
}


def transform_staging(settings: SnowflakeSettings) -> int:
    """RAW.CRAWLED_PRICES_STREAM을 소비해 STAGING.PRODUCTS / PRICE_HISTORY에 변환 적재.

    Stream 소비 방식:
        CREATE TEMPORARY TABLE AS SELECT FROM stream (DML) → Stream offset 이동
        → 소비된 레코드는 다음 파이프라인 실행에서 Stream에 나타나지 않음
        → 실패 레코드도 소비되어 영구 재조회 문제 없음
    실패 레코드는 RAW.TRANSFORM_FAILURES에 원인과 함께 기록 (감사용).
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE SCHEMA RAW")

        # Stream 소비: CREATE TABLE AS SELECT (DML) → Stream offset 이동
        # APPEND_ONLY Stream이므로 METADATA$ACTION은 항상 'INSERT'
        cur.execute("""
            CREATE OR REPLACE TEMPORARY TABLE TEMP_STREAM_DATA AS
            SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, CRAWLED_AT
            FROM CRAWLED_PRICES_STREAM
            WHERE METADATA$ACTION = 'INSERT'
        """)
        cur.execute(
            "SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, "
            "CRAWLED_AT FROM TEMP_STREAM_DATA"
        )
        raw_rows = cur.fetchall()
        cur.execute("USE SCHEMA STAGING")

        if not raw_rows:
            logger.info("[Staging] 변환할 데이터 없음")
            cur.close()
            return 0

        parsed = []
        failures = []  # (raw_id, site, category, name, price_text, crawled_at, reason)
        anomaly_count = 0
        for row in raw_rows:
            raw_id, site, category, product_name, price_text, brand, url, crawled_at = row
            price = parse_korean_price(price_text)
            if price is None:
                failures.append((raw_id, site, category, product_name, price_text, crawled_at, "가격 파싱 실패"))
                continue
            if not validate_price(price, category):
                logger.warning(
                    "[Staging] 이상치 가격 제외 — site=%s category=%s name=%s price=%d",
                    site, category, product_name[:40], price,
                )
                failures.append((raw_id, site, category, product_name, price_text, crawled_at, f"카테고리 범위 초과: {price}원"))
                anomaly_count += 1
                continue
            site_display = _SITE_DISPLAY_MAP.get(site)
            if site_display is None:
                failures.append((raw_id, site, category, product_name, price_text, crawled_at, f"알 수 없는 사이트: {site}"))
                continue
            cleaned_name = re.sub(r"\s+", " ", product_name.strip())
            parsed.append((raw_id, site_display, category, cleaned_name, brand, url, price, crawled_at))

        if anomaly_count:
            logger.warning("[Staging] 이상치 총 %d건 제외", anomaly_count)

        # 실패 레코드를 감사 테이블에 기록 (IS_PROCESSED=FALSE 역할 대체)
        # Stream에서 이미 소비됐으므로 다음 실행에서 재조회되지 않음
        if failures:
            cur.execute("USE SCHEMA RAW")
            cur.executemany(
                "INSERT INTO TRANSFORM_FAILURES "
                "(CRAWLED_PRICES_ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, CRAWLED_AT, REJECT_REASON) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                failures,
            )
            logger.info("[Staging] 변환 실패 %d건 → TRANSFORM_FAILURES 기록", len(failures))
            cur.execute("USE SCHEMA STAGING")

        if not parsed:
            logger.info("[Staging] 유효한 데이터 없음")
            cur.close()
            return 0

        cur.executemany(
            "MERGE INTO PRODUCTS t "
            "USING (SELECT %s AS SITE, %s AS PRODUCT_NAME, %s AS NEW_URL) s "
            "ON t.SITE = s.SITE AND t.PRODUCT_NAME = s.PRODUCT_NAME "
            "WHEN NOT MATCHED THEN INSERT (SITE, CATEGORY, PRODUCT_NAME, BRAND, URL) "
            "VALUES (%s, %s, %s, %s, s.NEW_URL) "
            "WHEN MATCHED THEN UPDATE SET "
            "URL = CASE WHEN s.NEW_URL != '' THEN s.NEW_URL ELSE t.URL END, "
            "UPDATED_AT = CURRENT_TIMESTAMP()",
            [(site_display, name, url or '', site_display, category, name, brand)
             for _, site_display, category, name, brand, url, _, _ in parsed],
        )

        cur.execute("SELECT PRODUCT_ID, SITE, PRODUCT_NAME FROM PRODUCTS")
        product_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

        daily_rows = []
        for raw_id, site_display, category, name, brand, url, price, crawled_at in parsed:
            product_id = product_map.get((site_display, name))
            if product_id is None:
                continue
            daily_rows.append((product_id, raw_id, price, crawled_at))

        if daily_rows:
            cur.executemany(
                "MERGE INTO PRICE_HISTORY t "
                "USING (SELECT %s AS PRODUCT_ID, %s AS RAW_ID, %s AS PRICE, %s AS CRAWLED_AT) s "
                "ON t.PRODUCT_ID = s.PRODUCT_ID AND t.CRAWLED_AT = s.CRAWLED_AT "
                "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, RAW_ID, PRICE, CRAWLED_AT) "
                "VALUES (s.PRODUCT_ID, s.RAW_ID, s.PRICE, s.CRAWLED_AT)",
                daily_rows,
            )

        cur.close()

    logger.info("[Staging] %d건 변환 완료", len(daily_rows) if daily_rows else 0)
    return len(daily_rows) if daily_rows else 0
