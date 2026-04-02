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
from collections import defaultdict
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

# 크롤러 내부 site_name → Snowflake에 저장되는 표시명 매핑
_SITE_DISPLAY_MAP = {
    "danawa":      "다나와",
    "compuzone":   "컴퓨존",
    "pc_estimate": "견적왕",
}


# ── Step 1: 크롤링 ──────────────────────────────────────────────────────────


def crawl_all_sites(settings: SnowflakeSettings) -> tuple[list[RawCrawledPrice], list[dict]]:
    all_raw: list[RawCrawledPrice] = []
    crawl_failures: list[dict] = []

    with get_connection(settings) as conn:
        crawlers = [
            DanawaCrawler(conn=conn),
            CompuzoneCrawler(),
            PCEstimateCrawler(),
        ]
        for crawler in crawlers:
            try:
                raw_prices = crawler.crawl_raw()
                all_raw.extend(raw_prices)
                logger.info("[크롤링] %s: %d건", crawler.site_name, len(raw_prices))
                if len(raw_prices) == 0:
                    failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    crawl_failures.append({
                        "site_name": crawler.site_name,
                        "error": "크롤링 결과 0건 — 페이지 구조 변경 의심",
                        "failed_at": failed_at,
                    })
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


# ── Step 3: Staging 변환 ─────────────────────────────────────────────────────


def transform_staging(settings: SnowflakeSettings) -> int:
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


# ── Step 4: 변경 감지 ────────────────────────────────────────────────────────


def detect_changes(settings: SnowflakeSettings) -> int:
    MIN_CHANGE_PCT = 1.0
    MAX_CHANGE_PCT = 70.0   # 70% 초과 단일 변동은 데이터 이상치로 간주
    PRICE_DROP_PCT = -5.0
    PRICE_SPIKE_PCT = 10.0

    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO STAGING.PRICE_ALERTS
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
                FROM STAGING.PRICE_HISTORY
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
                    ps.MIN_PRICE_EVER,
                    ps.MAX_PRICE_EVER
                FROM ranked r
                LEFT JOIN ANALYTICS.PRODUCT_STATS ps ON r.PRODUCT_ID = ps.PRODUCT_ID
                WHERE r.rn = 1
                  AND r.prev_price IS NOT NULL
                  AND r.PRICE != r.prev_price
                  AND ABS((r.PRICE - r.prev_price) / r.prev_price * 100) >= %s
                  AND ABS((r.PRICE - r.prev_price) / r.prev_price * 100) <= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM STAGING.PRICE_ALERTS a
                      WHERE a.DAILY_PRICE_ID = r.DAILY_PRICE_ID
                  )
            )
            SELECT
                PRODUCT_ID, DAILY_PRICE_ID,
                CASE
                    WHEN MIN_PRICE_EVER IS NOT NULL AND new_price < MIN_PRICE_EVER THEN 'NEW_LOW'
                    WHEN MAX_PRICE_EVER IS NOT NULL AND new_price > MAX_PRICE_EVER THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                END AS alert_type,
                old_price, new_price, change_pct
            FROM candidates
            WHERE CASE
                    WHEN MIN_PRICE_EVER IS NOT NULL AND new_price < MIN_PRICE_EVER THEN 'NEW_LOW'
                    WHEN MAX_PRICE_EVER IS NOT NULL AND new_price > MAX_PRICE_EVER THEN 'NEW_HIGH'
                    WHEN change_pct <= %s THEN 'PRICE_DROP'
                    WHEN change_pct >= %s THEN 'PRICE_SPIKE'
                  END IS NOT NULL
        """, (MIN_CHANGE_PCT, MAX_CHANGE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT, PRICE_DROP_PCT, PRICE_SPIKE_PCT))
        alert_count = cur.rowcount
        cur.close()

    logger.info("[Alert] %d건 알림 생성", alert_count)
    return alert_count


# ── Step 5: Slack 전송 ───────────────────────────────────────────────────────


def _send_slack_message(text: str) -> None:
    """Slack 메시지 전송 내부 헬퍼."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.info("[Slack] SLACK_WEBHOOK_URL 미설정 — 건너뜀")
        return
    payload = {"text": text}
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


def send_slack_failures(crawl_failures: list[dict]) -> int:
    if not crawl_failures:
        logger.info("[Slack] 크롤링 실패 없음 — 건너뜀")
        return 0

    lines = [f"*🔴 크롤링 실패 — {len(crawl_failures)}개 사이트*"]
    for failure in crawl_failures:
        lines.append(
            f"• *{failure['site_name']}* — {failure['failed_at']}\n"
            f"    `{failure['error']}`"
        )
    _send_slack_message("\n".join(lines))
    return len(crawl_failures)


# ── Step 6: Analytics 집계 ───────────────────────────────────────────────────


def aggregate_analytics(settings: SnowflakeSettings) -> None:
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


# ── Step 7: 데이터 품질 교차 검증 ────────────────────────────────────────────


def _find_cross_site_anomalies(
    rows: list[tuple[str, str, int]],
    threshold: float = 20.0,
) -> list[tuple[str, str, int, int, float]]:
    """동일 제품이 여러 사이트에 있을 때 가격 편차를 확인한다 (순수 함수).

    최저가 대비 threshold% 이상 비싼 사이트를 이상치로 판정한다.

    Args:
        rows: [(product_name, site, price), ...]
        threshold: 이상치 판정 편차 기준 (%, 기본 20%)

    Returns:
        [(product_name, site, price, min_price, deviation_pct), ...]
    """
    price_by_product: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for name, site, price in rows:
        price_by_product[name].append((site, price))

    anomalies = []
    for name, entries in price_by_product.items():
        if len(entries) < 2:
            continue
        min_price = min(p for _, p in entries)
        if min_price == 0:
            continue
        for site, price in entries:
            deviation = (price - min_price) / min_price * 100
            if deviation >= threshold:
                anomalies.append((name, site, price, min_price, deviation))
    return anomalies


def check_cross_site_prices(settings: SnowflakeSettings) -> int:
    """교차 검증: 동일 제품이 여러 사이트에 존재할 때 오늘 가격 편차 확인.

    이상치 발견 시 Slack WARNING을 보내지만 파이프라인은 계속 진행한다.
    """
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute("""
            SELECT p.PRODUCT_NAME, p.SITE, ph.PRICE
            FROM STAGING.PRICE_HISTORY ph
            JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = ph.PRODUCT_ID
            WHERE ph.CRAWLED_AT::DATE = CURRENT_DATE()
        """)
        rows = cur.fetchall()
        cur.close()

    anomalies = _find_cross_site_anomalies(rows)

    if anomalies:
        lines = [f"*⚠️ [교차검증] 가격 이상 감지 — {len(anomalies)}건*"]
        for name, site, price, min_price, deviation in anomalies:
            lines.append(
                f"• {name[:35]} | {site}: {price:,}원 "
                f"(최저가 {min_price:,}원 대비 {deviation:.1f}%)"
            )
        _send_slack_message("\n".join(lines))
        logger.warning("[교차검증] 이상 %d건 감지", len(anomalies))
    else:
        logger.info("[교차검증] 이상 없음")

    return len(anomalies)


# ── 메인 ─────────────────────────────────────────────────────────────────────


def main() -> int:
    logger.info("=== 파이프라인 시작 ===")
    settings = SnowflakeSettings()

    # Step 1: 크롤링
    all_raw, crawl_failures = crawl_all_sites(settings)

    # Step 2: Raw 적재
    if all_raw:
        load_raw(settings, all_raw)

    # Step 3: Staging 변환
    transform_staging(settings)

    # Step 3.5: 교차 검증 (WARNING만, 파이프라인 계속)
    check_cross_site_prices(settings)

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
