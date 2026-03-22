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
            lp.STOCK_STATUS,
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
