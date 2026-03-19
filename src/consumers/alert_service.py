"""Kafka consumer that generates alerts for significant price changes."""

import logging
import os
import uuid
from datetime import datetime, timezone

from sqlalchemy import text

from src.common.config import load_settings
from src.common.db import create_db_engine, create_session_factory, get_session
from src.common.kafka_client import create_consumer
from src.common.logging_config import setup_logging
from src.common.serialization import deserialize_price_change

logger = logging.getLogger(__name__)

PRICE_DROP_THRESHOLD_PCT = float(os.environ.get("ALERT_PRICE_DROP_PCT", "5.0"))
PRICE_SPIKE_THRESHOLD_PCT = float(os.environ.get("ALERT_PRICE_SPIKE_PCT", "10.0"))

INSERT_ALERT_SQL = text("""
    INSERT INTO alerts (alert_id, product_id, alert_type, site, old_price, new_price, change_pct, created_at)
    VALUES (:alert_id, :product_id, :alert_type, :site, :old_price, :new_price, :change_pct, :created_at)
""")

SELECT_PRICE_RANGE_SQL = text("""
    SELECT MIN(price), MAX(price) FROM latest_prices WHERE product_id = :product_id
""")


def _classify_alert(
    new_price: int,
    old_price: int | None,
    change_pct: float | None,
    min_price: int | None,
    max_price: int | None,
) -> str | None:
    if min_price is not None and new_price < min_price:
        return "NEW_LOW"
    if max_price is not None and new_price > max_price:
        return "NEW_HIGH"
    if change_pct is not None and change_pct <= -PRICE_DROP_THRESHOLD_PCT:
        return "PRICE_DROP"
    if change_pct is not None and change_pct >= PRICE_SPIKE_THRESHOLD_PCT:
        return "PRICE_SPIKE"
    return None


def run() -> None:
    setup_logging("alert-service")
    settings = load_settings()

    engine = create_db_engine(settings.postgres)
    session_factory = create_session_factory(engine)

    consumer = create_consumer(settings.kafka, "alert-service", ["price-changes"])

    logger.info("Alert service started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            change = deserialize_price_change(msg.value())
            change_pct_float = float(change.change_pct) if change.change_pct else None

            with get_session(session_factory) as session:
                row = session.execute(SELECT_PRICE_RANGE_SQL, {
                    "product_id": change.product_id,
                }).fetchone()

                min_price = row[0] if row else None
                max_price = row[1] if row else None

                alert_type = _classify_alert(
                    change.new_price, change.old_price, change_pct_float, min_price, max_price
                )

                if alert_type:
                    session.execute(INSERT_ALERT_SQL, {
                        "alert_id": str(uuid.uuid4()),
                        "product_id": change.product_id,
                        "alert_type": alert_type,
                        "site": change.site,
                        "old_price": change.old_price,
                        "new_price": change.new_price,
                        "change_pct": change_pct_float,
                        "created_at": datetime.now(timezone.utc),
                    })
                    logger.info(
                        "Alert created: %s for %s (%s -> %s)",
                        alert_type, change.product_name, change.old_price, change.new_price,
                    )

            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutting down alert service")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
