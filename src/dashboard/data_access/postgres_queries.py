"""PostgreSQL read queries for dashboard."""

import uuid as _uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

MAX_HISTORY_DAYS = 365


def _validate_uuid(value: str) -> str:
    try:
        _uuid.UUID(value)
    except ValueError:
        raise ValueError(f"Invalid UUID: {value}") from None
    return value


def get_product_list(session: Session, category: str | None = None) -> pd.DataFrame:
    if category:
        sql = "SELECT product_id, name, category, brand FROM products WHERE category = :category ORDER BY name"
        params = {"category": category}
    else:
        sql = "SELECT product_id, name, category, brand FROM products ORDER BY name"
        params = {}
    return pd.read_sql(text(sql), session.connection(), params=params)


def get_latest_prices(session: Session, product_id: str) -> pd.DataFrame:
    _validate_uuid(product_id)
    sql = """
        SELECT lp.site, lp.price, lp.url, lp.crawled_at, p.name
        FROM latest_prices lp
        JOIN products p ON p.product_id = lp.product_id
        WHERE lp.product_id = :product_id
        ORDER BY lp.price ASC
    """
    return pd.read_sql(text(sql), session.connection(), params={"product_id": product_id})


def get_recent_alerts(session: Session, limit: int = 20) -> pd.DataFrame:
    sql = """
        SELECT a.alert_id, a.alert_type, a.site, a.old_price, a.new_price,
               a.change_pct, a.created_at, a.is_read, p.name as product_name, p.category
        FROM alerts a
        JOIN products p ON p.product_id = a.product_id
        ORDER BY a.created_at DESC
        LIMIT :limit
    """
    return pd.read_sql(text(sql), session.connection(), params={"limit": limit})


def get_unread_alert_count(session: Session) -> int:
    result = session.execute(text("SELECT COUNT(*) FROM alerts WHERE is_read = FALSE"))
    return result.scalar_one()


def get_summary_stats(session: Session) -> dict:
    result = session.execute(text("""
        SELECT
            (SELECT COUNT(*) FROM products) AS total_products,
            COUNT(*) FILTER (WHERE is_read = FALSE) AS active_alerts,
            COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE) AS changes_today,
            COUNT(*) FILTER (
                WHERE alert_type = 'NEW_LOW'
                  AND created_at >= CURRENT_DATE - INTERVAL '7 days'
            ) AS new_lows
        FROM alerts
    """))
    row = result.fetchone()
    return {
        "total_products": row.total_products,
        "active_alerts": row.active_alerts,
        "changes_today": row.changes_today,
        "new_lows": row.new_lows,
    }


def get_price_history(session: Session, product_id: str, days: int = 30) -> pd.DataFrame:
    _validate_uuid(product_id)
    if not 1 <= days <= MAX_HISTORY_DAYS:
        raise ValueError(f"days must be between 1 and {MAX_HISTORY_DAYS}")

    sql = """
        SELECT site, price, crawled_at
        FROM price_history
        WHERE product_id = :product_id
          AND crawled_at >= NOW() - MAKE_INTERVAL(days => :days)
        ORDER BY crawled_at ASC, site
    """
    return pd.read_sql(
        text(sql), session.connection(),
        params={"product_id": product_id, "days": days},
    )
