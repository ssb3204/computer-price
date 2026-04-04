"""Step 3: Staging 변환 — Raw 데이터를 정제해 STAGING 레이어에 적재."""

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
    """RAW.CRAWLED_PRICES(미처리)를 STAGING.PRODUCTS / PRICE_HISTORY / LATEST_PRICES에 변환 적재."""
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE SCHEMA RAW")
        cur.execute(
            "SELECT ID, SITE, CATEGORY, PRODUCT_NAME, PRICE_TEXT, BRAND, URL, "
            "CRAWLED_AT FROM CRAWLED_PRICES WHERE IS_PROCESSED = FALSE"
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
            site_display = _SITE_DISPLAY_MAP.get(site)
            if site_display is None:
                continue
            cleaned_name = re.sub(r"\s+", " ", product_name.strip())
            parsed.append((raw_id, site_display, category, cleaned_name, brand, url, price, crawled_at))

        if anomaly_count:
            logger.warning("[Staging] 이상치 총 %d건 제외", anomaly_count)

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
        processed_raw_ids = []
        for raw_id, site_display, category, name, brand, url, price, crawled_at in parsed:
            product_id = product_map.get((site_display, name))
            if product_id is None:
                continue
            daily_rows.append((product_id, raw_id, price, crawled_at))
            processed_raw_ids.append(raw_id)

        if daily_rows:
            cur.executemany(
                "MERGE INTO PRICE_HISTORY t "
                "USING (SELECT %s AS PRODUCT_ID, %s AS RAW_ID, %s AS PRICE, %s AS CRAWLED_AT) s "
                "ON t.PRODUCT_ID = s.PRODUCT_ID AND t.CRAWLED_AT = s.CRAWLED_AT "
                "WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, RAW_ID, PRICE, CRAWLED_AT) "
                "VALUES (s.PRODUCT_ID, s.RAW_ID, s.PRICE, s.CRAWLED_AT)",
                daily_rows,
            )
            cur.executemany(
                "MERGE INTO LATEST_PRICES t "
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
                f"UPDATE CRAWLED_PRICES SET IS_PROCESSED = TRUE, "  # noqa: S608
                f"PROCESSED_AT = CURRENT_TIMESTAMP() WHERE ID IN ({placeholders})",
                processed_raw_ids,
            )

        cur.close()

    logger.info("[Staging] %d건 변환 완료", len(daily_rows) if daily_rows else 0)
    return len(daily_rows) if daily_rows else 0
