"""Step 3: Staging 변환 — Raw 데이터를 정제해 stg_products / stg_price_history에 적재."""

import logging
import re

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection
from src.crawlers.parser_utils import parse_korean_price, validate_price

logger = logging.getLogger(__name__)

# 크롤러 내부 site_name → 표시명 매핑
_SITE_DISPLAY_MAP = {
    "danawa":      "다나와",
    "compuzone":   "컴퓨존",
    "kjwwang":     "견적왕",
}


def transform_staging(settings: MySQLSettings) -> int:
    """미처리 raw_crawled_prices를 정제해 stg_products / stg_price_history에 적재.

    증분 처리(구 Snowflake Stream 대체):
        stg_price_history.raw_id에 아직 없고, raw_transform_failures에도 없는
        raw_crawled_prices 행만 '미처리'로 간주해 처리한다(미처리 조인).
        → 성공 레코드는 price_history.raw_id로, 실패 레코드는 transform_failures로
          '소비'되어 다음 실행에서 재조회되지 않는다(Stream의 consume-once 재현).
    실패 레코드는 raw_transform_failures에 원인과 함께 기록(감사용).
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()

        # 미처리 조인: price_history에도 transform_failures에도 없는 raw 행만
        cur.execute("""
            SELECT r.`id`, r.`site`, r.`category`, r.`product_name`,
                   r.`price_text`, r.`url`, r.`crawled_at`
            FROM `raw_crawled_prices` r
            LEFT JOIN `stg_price_history` h ON h.`raw_id` = r.`id`
            WHERE h.`raw_id` IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM `raw_transform_failures` f
                  WHERE f.`crawled_prices_id` = r.`id`
              )
        """)
        raw_rows = cur.fetchall()

        if not raw_rows:
            logger.info("[Staging] 변환할 데이터 없음")
            cur.close()
            return 0

        parsed = []
        failures = []  # (raw_id, site, category, name, price_text, crawled_at, reason)
        anomaly_count = 0
        for row in raw_rows:
            raw_id, site, category, product_name, price_text, url, crawled_at = row
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
            parsed.append((raw_id, site_display, category, cleaned_name, url, price, crawled_at))

        if anomaly_count:
            logger.warning("[Staging] 이상치 총 %d건 제외", anomaly_count)

        # 실패 레코드를 감사 테이블에 기록 → 다음 실행에서 미처리 조인으로 재조회되지 않음
        if failures:
            cur.executemany(
                "INSERT INTO `raw_transform_failures` "
                "(`crawled_prices_id`, `site`, `category`, `product_name`, `price_text`, `crawled_at`, `reject_reason`) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                failures,
            )
            logger.info("[Staging] 변환 실패 %d건 → raw_transform_failures 기록", len(failures))

        if not parsed:
            logger.info("[Staging] 유효한 데이터 없음")
            cur.close()
            return 0

        # stg_products UPSERT (자연키 site+product_name, 구 MERGE 대체)
        cur.executemany(
            "INSERT INTO `stg_products` (`site`, `category`, `product_name`, `url`) "
            "VALUES (%s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "`url` = IF(VALUES(`url`) <> '', VALUES(`url`), `url`), "
            "`updated_at` = CURRENT_TIMESTAMP",
            [(site_display, category, name, url or '')
             for _, site_display, category, name, url, _, _ in parsed],
        )

        # product_id 매핑 조회
        cur.execute("SELECT `product_id`, `site`, `product_name` FROM `stg_products`")
        product_map = {(row[1], row[2]): row[0] for row in cur.fetchall()}

        daily_rows = []
        for raw_id, site_display, category, name, url, price, crawled_at in parsed:
            product_id = product_map.get((site_display, name))
            if product_id is None:
                continue
            daily_rows.append((product_id, raw_id, price, crawled_at))

        if daily_rows:
            # stg_price_history UPSERT (자연키 product_id+crawled_at, WHEN NOT MATCHED → no-op)
            cur.executemany(
                "INSERT INTO `stg_price_history` (`product_id`, `raw_id`, `price`, `crawled_at`) "
                "VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE `id` = `id`",
                daily_rows,
            )

        cur.close()

    logger.info("[Staging] %d건 변환 완료", len(daily_rows) if daily_rows else 0)
    return len(daily_rows) if daily_rows else 0
