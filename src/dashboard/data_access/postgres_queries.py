"""PostgreSQL read queries for dashboard."""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session


def get_product_list(session: Session, category: str | None = None) -> pd.DataFrame:
    sql = "SELECT product_id, name, category, brand FROM products"
    params: dict = {}
    if category:
        sql += " WHERE category = :category"
        params["category"] = category
    sql += " ORDER BY name"
    return pd.read_sql(text(sql), session.bind, params=params)


def get_latest_prices(session: Session, product_id: str) -> pd.DataFrame:
    sql = """
        SELECT lp.site, lp.price, lp.url, lp.crawled_at, p.name
        FROM latest_prices lp
        JOIN products p ON p.product_id = lp.product_id
        WHERE lp.product_id = :product_id
        ORDER BY lp.price ASC
    """
    return pd.read_sql(text(sql), session.bind, params={"product_id": product_id})


def get_recent_alerts(session: Session, limit: int = 20) -> pd.DataFrame:
    sql = """
        SELECT a.alert_id, a.alert_type, a.site, a.old_price, a.new_price,
               a.change_pct, a.created_at, a.is_read, p.name as product_name, p.category
        FROM alerts a
        JOIN products p ON p.product_id = a.product_id
        ORDER BY a.created_at DESC
        LIMIT :limit
    """
    return pd.read_sql(text(sql), session.bind, params={"limit": limit})


def get_unread_alert_count(session: Session) -> int:
    result = session.execute(text("SELECT COUNT(*) FROM alerts WHERE is_read = FALSE"))
    return result.scalar_one()


def get_summary_stats(session: Session) -> dict:
    total_products = session.execute(text("SELECT COUNT(*) FROM products")).scalar_one()
    active_alerts = session.execute(
        text("SELECT COUNT(*) FROM alerts WHERE is_read = FALSE")
    ).scalar_one()
    changes_today = session.execute(text("""
        SELECT COUNT(*) FROM alerts
        WHERE created_at >= CURRENT_DATE
    """)).scalar_one()
    new_lows = session.execute(text("""
        SELECT COUNT(*) FROM alerts
        WHERE alert_type = 'NEW_LOW' AND created_at >= CURRENT_DATE - INTERVAL '7 days'
    """)).scalar_one()

    return {
        "total_products": total_products,
        "active_alerts": active_alerts,
        "changes_today": changes_today,
        "new_lows": new_lows,
    }
