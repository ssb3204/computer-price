"""Snowflake 쿼리 — 새 3-Layer 스키마 기반."""

import pandas as pd
from snowflake.connector import SnowflakeConnection


def get_latest_prices_all(conn: SnowflakeConnection) -> pd.DataFrame:
    """WATCHLIST 활성 상품의 최신 가격 목록 (대시보드 메인 테이블)."""
    sql = f"""
        SELECT
            p.PRODUCT_ID,
            p.SITE,
            p.CATEGORY,
            p.PRODUCT_NAME,
            p.BRAND,
            lp.PRICE,
            lp.CRAWLED_AT,
            p.URL
        FROM STAGING.LATEST_PRICES lp
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = lp.PRODUCT_ID
        WHERE {_WATCHLIST_EXISTS}
        ORDER BY p.CATEGORY, lp.PRICE ASC
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_summary_stats(conn: SnowflakeConnection) -> dict:
    """대시보드 상단 요약 통계."""
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute("""
            SELECT
                (SELECT COUNT(*)               FROM STAGING.PRODUCTS)          AS total_products,
                (SELECT COUNT(DISTINCT CATEGORY) FROM STAGING.PRODUCTS)        AS total_categories,
                (SELECT COUNT(DISTINCT SITE)     FROM STAGING.PRODUCTS)        AS total_sites,
                (SELECT COUNT(*)               FROM STAGING.PRICE_HISTORY
                 WHERE CRAWLED_AT::DATE = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE) AS today_records
        """)
        row = cur.fetchone()
        return {
            "total_products":   row[0],
            "total_categories": row[1],
            "total_sites":      row[2],
            "today_records":    row[3],
        }
    finally:
        cur.close()


def get_product_stats(conn: SnowflakeConnection) -> pd.DataFrame:
    """WATCHLIST 활성 상품의 전체 기간 통계.

    ANALYTICS.PRODUCT_STATS에 아직 집계되지 않은 신규 상품도 포함하기 위해
    STAGING.PRODUCTS를 기준으로 LEFT JOIN한다.
    """
    sql = f"""
        SELECT
            p.PRODUCT_ID,
            p.SITE,
            p.CATEGORY,
            p.PRODUCT_NAME,
            p.URL,
            COALESCE(ps.AVG_PRICE,      lp.PRICE) AS AVG_PRICE,
            COALESCE(ps.MIN_PRICE_EVER, lp.PRICE) AS MIN_PRICE_EVER,
            COALESCE(ps.MAX_PRICE_EVER, lp.PRICE) AS MAX_PRICE_EVER,
            ps.FIRST_CRAWLED_AT,
            ps.LAST_CRAWLED_AT,
            COALESCE(ps.TOTAL_RECORDS, 1)         AS TOTAL_RECORDS
        FROM STAGING.PRODUCTS p
        LEFT JOIN ANALYTICS.PRODUCT_STATS ps ON ps.PRODUCT_ID = p.PRODUCT_ID
        LEFT JOIN STAGING.LATEST_PRICES   lp ON lp.PRODUCT_ID = p.PRODUCT_ID
        WHERE {_WATCHLIST_EXISTS}
        ORDER BY p.CATEGORY, p.PRODUCT_NAME
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_price_trend(
    conn: SnowflakeConnection,
    category: str | None = None,
    search: str | None = None,
    days: int | None = None,
) -> pd.DataFrame:
    """검색 키워드 매칭 상품의 사이트별 최저가 추이 (라인 차트용).

    같은 사이트에 여러 매칭 상품이 있으면 크롤 시점별 최저가만 반환.
    days=None이면 전체 기간 조회.
    """
    if not search:
        return pd.DataFrame()

    conditions: list[str] = []
    params: list = []

    if days:
        conditions.append("dp.CRAWLED_AT >= DATEADD(day, -%s, CURRENT_TIMESTAMP())")
        params.append(days)

    if category and category != "ALL":
        conditions.append("p.CATEGORY = %s")
        params.append(category)

    conditions.append("p.PRODUCT_NAME ILIKE %s")
    params.append(f"%{search}%")

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            p.SITE      AS site,
            dp.CRAWLED_AT AS crawled_at,
            MIN(dp.PRICE) AS price
        FROM STAGING.PRICE_HISTORY dp
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = dp.PRODUCT_ID
        WHERE {where}
        GROUP BY p.SITE, dp.CRAWLED_AT
        ORDER BY dp.CRAWLED_AT
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


_WATCHLIST_EXISTS = """EXISTS (
            SELECT 1 FROM STAGING.WATCHLIST w
            WHERE w.IS_ACTIVE = TRUE
              AND p.SITE = w.SITE
              AND (
                  (w.SITE = '다나와' AND p.URL ILIKE '%pcode='     || w.PCODE || '%')
               OR (w.SITE = '컴퓨존' AND p.URL ILIKE '%ProductNo=' || w.PCODE || '%')
               OR (w.SITE = '견적왕' AND p.URL ILIKE '%pd_no='    || w.PCODE || '%')
              )
        )"""


def get_today_crawl_comparison(
    conn: SnowflakeConnection,
    category: str | None = None,
    search: str | None = None,
) -> pd.DataFrame:
    """오늘 크롤링 4회(1~4차) 가격 비교 — WATCHLIST 활성 상품만."""
    conditions = ["dp.CRAWLED_AT::DATE = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::DATE", _WATCHLIST_EXISTS]
    params: list = []

    if category and category != "ALL":
        conditions.append("p.CATEGORY = %s")
        params.append(category)
    if search:
        conditions.append("p.PRODUCT_NAME ILIKE %s")
        params.append(f"%{search}%")

    where = " AND ".join(conditions)

    sql = f"""
        WITH daily AS (
            SELECT
                dp.PRODUCT_ID,
                dp.PRICE,
                dp.CRAWLED_AT,
                ROW_NUMBER() OVER (
                    PARTITION BY dp.PRODUCT_ID
                    ORDER BY dp.CRAWLED_AT
                ) AS rn
            FROM STAGING.PRICE_HISTORY dp
            JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = dp.PRODUCT_ID
            WHERE {where}
        )
        SELECT
            p.SITE          AS site,
            p.CATEGORY      AS category,
            p.PRODUCT_NAME  AS product_name,
            d1.PRICE        AS price_1st,
            d2.PRICE        AS price_2nd,
            d3.PRICE        AS price_3rd,
            d4.PRICE        AS price_4th
        FROM daily d1
        LEFT JOIN daily d2 ON d1.PRODUCT_ID = d2.PRODUCT_ID AND d2.rn = 2
        LEFT JOIN daily d3 ON d1.PRODUCT_ID = d3.PRODUCT_ID AND d3.rn = 3
        LEFT JOIN daily d4 ON d1.PRODUCT_ID = d4.PRODUCT_ID AND d4.rn = 4
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = d1.PRODUCT_ID
        WHERE d1.rn = 1
        ORDER BY p.CATEGORY, p.PRODUCT_NAME
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_alerts(
    conn: SnowflakeConnection,
    alert_type: str | None = None,
    category: str | None = None,
    days: int | None = None,
) -> pd.DataFrame:
    """알림 목록 조회 (필터: 유형, 카테고리, 기간). 1% 미만 변동 제외."""
    conditions: list[str] = ["ABS(a.CHANGE_PCT) >= 1.0"]
    params: list = []

    if alert_type and alert_type != "ALL":
        conditions.append("a.ALERT_TYPE = %s")
        params.append(alert_type)

    if category and category != "ALL":
        conditions.append("p.CATEGORY = %s")
        params.append(category)

    if days:
        conditions.append("a.CREATED_AT >= DATEADD(day, -%s, CURRENT_TIMESTAMP())")
        params.append(days)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            a.ALERT_ID,
            a.ALERT_TYPE,
            p.SITE,
            p.CATEGORY,
            p.PRODUCT_NAME,
            p.URL,
            a.OLD_PRICE,
            a.NEW_PRICE,
            a.CHANGE_PCT,
            a.CREATED_AT
        FROM STAGING.PRICE_ALERTS a
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = a.PRODUCT_ID
        {where}
        ORDER BY a.CREATED_AT DESC
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_watchlist_product_ids(conn: SnowflakeConnection) -> set[int]:
    """WATCHLIST 활성 상품의 PRODUCT_ID 집합을 반환 (URL의 pcode/ProductNo/pd_no로 매칭)."""
    sql = """
        SELECT DISTINCT p.PRODUCT_ID
        FROM STAGING.WATCHLIST w
        JOIN STAGING.PRODUCTS p
          ON p.SITE = w.SITE
          AND (
              (w.SITE = '다나와' AND p.URL ILIKE '%pcode='     || w.PCODE || '%')
           OR (w.SITE = '컴퓨존' AND p.URL ILIKE '%ProductNo=' || w.PCODE || '%')
           OR (w.SITE = '견적왕' AND p.URL ILIKE '%pd_no='    || w.PCODE || '%')
          )
        WHERE w.IS_ACTIVE = TRUE
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
        return {row[0] for row in cur.fetchall()}
    finally:
        cur.close()


def get_watch_products(conn: SnowflakeConnection, site: str | None = None) -> pd.DataFrame:
    """크롤링 대상 제품 목록 조회 (활성/비활성 모두). site 지정 시 해당 사이트만 반환."""
    site_filter = "AND w.SITE = %s" if site else ""
    sql = f"""
        SELECT
            w.ID, w.SITE, w.QUERY, w.PCODE, w.PRODUCT_NAME, w.CATEGORY, w.BRAND,
            w.ADDED_AT, w.IS_ACTIVE,
            lp.PRICE,
            lp.CRAWLED_AT AS LAST_CRAWLED_AT
        FROM STAGING.WATCHLIST w
        LEFT JOIN STAGING.PRODUCTS p
          ON p.SITE = w.SITE
          AND (
              (w.SITE = '다나와' AND p.URL ILIKE '%%pcode='     || w.PCODE || '%%')
           OR (w.SITE = '컴퓨존' AND p.URL ILIKE '%%ProductNo=' || w.PCODE || '%%')
           OR (w.SITE = '견적왕' AND p.URL ILIKE '%%pd_no='    || w.PCODE || '%%')
          )
        LEFT JOIN STAGING.LATEST_PRICES lp ON lp.PRODUCT_ID = p.PRODUCT_ID
        WHERE 1=1 {site_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY w.ID ORDER BY lp.CRAWLED_AT DESC NULLS LAST) = 1
        ORDER BY w.IS_ACTIVE DESC, w.ADDED_AT DESC
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql, (site,) if site else ())
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def add_watch_product(
    conn: SnowflakeConnection,
    query: str,
    pcode: str,
    product_name: str | None,
    category: str,
    brand: str | None = None,
    site: str = "다나와",
) -> None:
    """크롤링 대상 제품 추가. (site, pcode) 중복이면 IS_ACTIVE=TRUE로 복원."""
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute("""
            MERGE INTO STAGING.WATCHLIST t
            USING (SELECT %s AS SITE, %s AS PCODE) s
              ON t.SITE = s.SITE AND t.PCODE = s.PCODE
            WHEN MATCHED THEN
                UPDATE SET IS_ACTIVE = TRUE, QUERY = %s, PRODUCT_NAME = %s,
                           CATEGORY = %s, BRAND = %s
            WHEN NOT MATCHED THEN
                INSERT (SITE, QUERY, PCODE, PRODUCT_NAME, CATEGORY, BRAND)
                VALUES (%s, %s, %s, %s, %s, %s)
        """, (site, pcode, query, product_name, category, brand,
              site, query, pcode, product_name, category, brand))
    finally:
        cur.close()


def remove_watch_product(conn: SnowflakeConnection, watch_id: int) -> None:
    """크롤링 대상 제품 비활성화 (soft delete)."""
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(
            "UPDATE STAGING.WATCHLIST SET IS_ACTIVE = FALSE WHERE ID = %s",
            (watch_id,)
        )
    finally:
        cur.close()


def get_category_price_summary(conn: SnowflakeConnection) -> pd.DataFrame:
    """카테고리별 가격 요약."""
    sql = """
        SELECT
            p.CATEGORY,
            COUNT(*) AS PRODUCT_COUNT,
            MIN(lp.PRICE) AS MIN_PRICE,
            MAX(lp.PRICE) AS MAX_PRICE,
            ROUND(AVG(lp.PRICE)) AS AVG_PRICE
        FROM STAGING.LATEST_PRICES lp
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = lp.PRODUCT_ID
        GROUP BY p.CATEGORY
        ORDER BY p.CATEGORY
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()
