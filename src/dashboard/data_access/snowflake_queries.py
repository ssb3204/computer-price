"""Snowflake read queries for dashboard trend charts."""

import pandas as pd
from snowflake.connector import SnowflakeConnection


def get_weekly_price_trends(conn: SnowflakeConnection, product_id: str, days: int = 30) -> pd.DataFrame:
    sql = """
        SELECT crawled_at::DATE as date, site, new_price as price
        FROM RAW.PRICE_CHANGES
        WHERE product_id = %s
          AND crawled_at >= DATEADD(day, -%s, CURRENT_TIMESTAMP())
        ORDER BY date ASC, site
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (product_id, days))
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()


def get_price_range_90d(conn: SnowflakeConnection, product_id: str) -> dict:
    sql = """
        SELECT MIN(new_price) as min_price, MAX(new_price) as max_price
        FROM RAW.PRICE_CHANGES
        WHERE product_id = %s
          AND crawled_at >= DATEADD(day, -90, CURRENT_TIMESTAMP())
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (product_id,))
        row = cursor.fetchone()
        return {"min_price": row[0], "max_price": row[1]} if row else {"min_price": None, "max_price": None}
    finally:
        cursor.close()


def get_cross_site_comparison(conn: SnowflakeConnection, category: str) -> pd.DataFrame:
    sql = """
        SELECT pc.product_id, pc.product_name, pc.site, pc.new_price as price, pc.url
        FROM RAW.PRICE_CHANGES pc
        INNER JOIN (
            SELECT product_id, site, MAX(crawled_at) as latest
            FROM RAW.PRICE_CHANGES
            WHERE category = %s
            GROUP BY product_id, site
        ) latest ON pc.product_id = latest.product_id
                 AND pc.site = latest.site
                 AND pc.crawled_at = latest.latest
        WHERE pc.category = %s
        ORDER BY pc.product_name, pc.site
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (category, category))
        columns = [desc[0].lower() for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()
