"""Snowflake 쿼리 — 새 3-Layer 스키마 기반."""

import pandas as pd
from snowflake.connector import SnowflakeConnection


def get_latest_prices_all(conn: SnowflakeConnection) -> pd.DataFrame:
    """전체 상품 최신 가격 목록 (대시보드 메인 테이블)."""
    sql = """
        SELECT
            p.PRODUCT_ID,
            s.DISPLAY_NAME AS SITE,
            c.NAME AS CATEGORY,
            p.NAME AS PRODUCT_NAME,
            p.BRAND,
            lp.PRICE,
            lp.CRAWLED_AT,
            p.URL
        FROM STAGING.STG_LATEST_PRICES lp
        JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = lp.PRODUCT_ID
        JOIN STAGING.DIM_SITES s ON s.SITE_ID = p.SITE_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
        ORDER BY c.NAME, lp.PRICE ASC
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
        cur.execute("SELECT COUNT(*) FROM STAGING.STG_PRODUCTS")
        total_products = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT CATEGORY_ID) FROM STAGING.STG_PRODUCTS")
        total_categories = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT SITE_ID) FROM STAGING.STG_PRODUCTS")
        total_sites = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM STAGING.STG_DAILY_PRICES
            WHERE CRAWLED_AT::DATE = CURRENT_DATE()
        """)
        today_records = cur.fetchone()[0]

        return {
            "total_products": total_products,
            "total_categories": total_categories,
            "total_sites": total_sites,
            "today_records": today_records,
        }
    finally:
        cur.close()


def get_product_stats(conn: SnowflakeConnection) -> pd.DataFrame:
    """상품별 전체 기간 통계."""
    sql = """
        SELECT
            ps.PRODUCT_ID,
            s.DISPLAY_NAME AS SITE,
            c.NAME AS CATEGORY,
            p.NAME AS PRODUCT_NAME,
            p.URL,
            ps.OVERALL_AVG,
            ps.ALL_TIME_LOW,
            ps.ALL_TIME_HIGH,
            ps.FIRST_SEEN,
            ps.LAST_SEEN,
            ps.TOTAL_RECORDS
        FROM ANALYTICS.PRODUCT_STATS ps
        JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = ps.PRODUCT_ID
        JOIN STAGING.DIM_SITES s ON s.SITE_ID = p.SITE_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
        ORDER BY c.NAME, p.NAME
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
        conditions.append("c.NAME = %s")
        params.append(category)

    conditions.append("p.NAME ILIKE %s")
    params.append(f"%{search}%")

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            s.DISPLAY_NAME AS site,
            dp.CRAWLED_AT   AS crawled_at,
            MIN(dp.PRICE)   AS price
        FROM STAGING.STG_DAILY_PRICES dp
        JOIN STAGING.STG_PRODUCTS   p ON p.PRODUCT_ID  = dp.PRODUCT_ID
        JOIN STAGING.DIM_SITES      s ON s.SITE_ID     = p.SITE_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
        WHERE {where}
        GROUP BY s.DISPLAY_NAME, dp.CRAWLED_AT
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


def get_today_crawl_comparison(
    conn: SnowflakeConnection,
    category: str | None = None,
    search: str | None = None,
) -> pd.DataFrame:
    """오늘 크롤링 4회(1~4차) 가격 비교."""
    conditions = ["dp.CRAWLED_AT::DATE = CURRENT_DATE()"]
    params: list = []

    if category and category != "ALL":
        conditions.append("c.NAME = %s")
        params.append(category)
    if search:
        conditions.append("p.NAME ILIKE %s")
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
            FROM STAGING.STG_DAILY_PRICES dp
            JOIN STAGING.STG_PRODUCTS   p ON p.PRODUCT_ID  = dp.PRODUCT_ID
            JOIN STAGING.DIM_SITES      s ON s.SITE_ID     = p.SITE_ID
            JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
            WHERE {where}
        )
        SELECT
            s.DISPLAY_NAME  AS site,
            c.NAME          AS category,
            p.NAME          AS product_name,
            d1.PRICE        AS price_1st,
            d2.PRICE        AS price_2nd,
            d3.PRICE        AS price_3rd,
            d4.PRICE        AS price_4th
        FROM daily d1
        LEFT JOIN daily d2 ON d1.PRODUCT_ID = d2.PRODUCT_ID AND d2.rn = 2
        LEFT JOIN daily d3 ON d1.PRODUCT_ID = d3.PRODUCT_ID AND d3.rn = 3
        LEFT JOIN daily d4 ON d1.PRODUCT_ID = d4.PRODUCT_ID AND d4.rn = 4
        JOIN STAGING.STG_PRODUCTS   p ON p.PRODUCT_ID  = d1.PRODUCT_ID
        JOIN STAGING.DIM_SITES      s ON s.SITE_ID     = p.SITE_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
        WHERE d1.rn = 1
        ORDER BY c.NAME, p.NAME
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
        conditions.append("c.NAME = %s")
        params.append(category)

    if days:
        conditions.append("a.CREATED_AT >= DATEADD(day, -%s, CURRENT_TIMESTAMP())")
        params.append(days)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            a.ALERT_ID,
            a.ALERT_TYPE,
            s.DISPLAY_NAME AS SITE,
            c.NAME AS CATEGORY,
            p.NAME AS PRODUCT_NAME,
            p.URL,
            a.OLD_PRICE,
            a.NEW_PRICE,
            a.CHANGE_PCT,
            a.CREATED_AT
        FROM STAGING.STG_ALERTS a
        JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = a.PRODUCT_ID
        JOIN STAGING.DIM_SITES s ON s.SITE_ID = p.SITE_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
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


def get_category_price_summary(conn: SnowflakeConnection) -> pd.DataFrame:
    """카테고리별 가격 요약."""
    sql = """
        SELECT
            c.NAME AS CATEGORY,
            COUNT(*) AS PRODUCT_COUNT,
            MIN(lp.PRICE) AS MIN_PRICE,
            MAX(lp.PRICE) AS MAX_PRICE,
            ROUND(AVG(lp.PRICE)) AS AVG_PRICE
        FROM STAGING.STG_LATEST_PRICES lp
        JOIN STAGING.STG_PRODUCTS p ON p.PRODUCT_ID = lp.PRODUCT_ID
        JOIN STAGING.DIM_CATEGORIES c ON c.CATEGORY_ID = p.CATEGORY_ID
        GROUP BY c.NAME
        ORDER BY c.NAME
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()
