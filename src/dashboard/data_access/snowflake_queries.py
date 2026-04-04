"""Snowflake 쿼리 — 새 3-Layer 스키마 기반."""

import pandas as pd
from snowflake.connector import SnowflakeConnection


def get_latest_prices_all(conn: SnowflakeConnection) -> pd.DataFrame:
    """전체 상품 최신 가격 목록 (대시보드 메인 테이블)."""
    sql = """
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
                 WHERE CRAWLED_AT::DATE = CURRENT_DATE())                       AS today_records
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
    """상품별 전체 기간 통계."""
    sql = """
        SELECT
            ps.PRODUCT_ID,
            p.SITE,
            p.CATEGORY,
            p.PRODUCT_NAME,
            p.URL,
            ps.AVG_PRICE,
            ps.MIN_PRICE_EVER,
            ps.MAX_PRICE_EVER,
            ps.FIRST_CRAWLED_AT,
            ps.LAST_CRAWLED_AT,
            ps.TOTAL_RECORDS
        FROM ANALYTICS.PRODUCT_STATS ps
        JOIN STAGING.PRODUCTS p ON p.PRODUCT_ID = ps.PRODUCT_ID
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


def get_today_crawl_comparison(
    conn: SnowflakeConnection,
    category: str | None = None,
    search: str | None = None,
) -> pd.DataFrame:
    """오늘 크롤링 4회(1~4차) 가격 비교."""
    conditions = ["dp.CRAWLED_AT::DATE = CURRENT_DATE()"]
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


def get_watch_products(conn: SnowflakeConnection) -> pd.DataFrame:
    """사용자 크롤링 대상 제품 목록 조회."""
    sql = """
        SELECT ID, QUERY, PCODE, PRODUCT_NAME, CATEGORY, BRAND, ADDED_AT
        FROM STAGING.WATCHLIST
        WHERE IS_ACTIVE = TRUE
        ORDER BY ADDED_AT DESC
    """
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute(sql)
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
) -> None:
    """크롤링 대상 제품 추가. pcode 중복이면 IS_ACTIVE=TRUE로 복원."""
    cur = conn.cursor()
    try:
        cur.execute("USE DATABASE COMPUTER_PRICE")
        cur.execute("""
            MERGE INTO STAGING.WATCHLIST t
            USING (SELECT %s AS PCODE) s ON t.PCODE = s.PCODE
            WHEN MATCHED THEN
                UPDATE SET IS_ACTIVE = TRUE, QUERY = %s, PRODUCT_NAME = %s,
                           CATEGORY = %s, BRAND = %s
            WHEN NOT MATCHED THEN
                INSERT (QUERY, PCODE, PRODUCT_NAME, CATEGORY, BRAND)
                VALUES (%s, %s, %s, %s, %s)
        """, (pcode, query, product_name, category, brand,
              query, pcode, product_name, category, brand))
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
