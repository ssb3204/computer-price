"""Kafka consumer that detects price changes by comparing against last-known prices."""

import logging
import uuid
from decimal import Decimal

from sqlalchemy import text

from src.common.config import load_settings
from src.common.db import create_db_engine, create_session_factory, get_session
from src.common.kafka_client import create_consumer, create_producer
from src.common.logging_config import setup_logging
from src.common.models import PriceChange
from src.common.serialization import deserialize_raw_price, serialize

logger = logging.getLogger(__name__)

UPSERT_PRODUCT_SQL = text("""
    INSERT INTO products (name, category, brand, normalized_name, created_at, updated_at)
    VALUES (:name, :category, :brand, :normalized_name, NOW(), NOW())
    ON CONFLICT (normalized_name) DO UPDATE SET updated_at = NOW()
    RETURNING product_id
""")

SELECT_LATEST_PRICE_FOR_UPDATE_SQL = text("""
    SELECT price FROM latest_prices
    WHERE product_id = :product_id AND site = :site
    FOR UPDATE
""")

UPSERT_LATEST_PRICE_SQL = text("""
    INSERT INTO latest_prices (product_id, site, price, url, crawled_at)
    VALUES (:product_id, :site, :price, :url, :crawled_at)
    ON CONFLICT (product_id, site) DO UPDATE
    SET price = EXCLUDED.price, url = EXCLUDED.url, crawled_at = EXCLUDED.crawled_at
""")

INSERT_PRICE_HISTORY_SQL = text("""
    INSERT INTO price_history (product_id, site, price, url, crawled_at)
    VALUES (:product_id, :site, :price, :url, :crawled_at)
""")


def _normalize(name: str) -> str:
    from src.crawlers.parser_utils import normalize_product_name
    return normalize_product_name(name)


def run() -> None:
    setup_logging("change-detector")
    settings = load_settings()

    engine = create_db_engine(settings.postgres)
    session_factory = create_session_factory(engine)

    consumer = create_consumer(settings.kafka, "change-detector", ["raw-prices"])
    producer = create_producer(settings.kafka)

    logger.info("Change detector started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            raw_price = deserialize_raw_price(msg.value())

            with get_session(session_factory) as session:
                normalized = _normalize(raw_price.product_name)
                result = session.execute(UPSERT_PRODUCT_SQL, {
                    "name": raw_price.product_name,
                    "category": raw_price.category,
                    "brand": raw_price.brand,
                    "normalized_name": normalized,
                })
                product_id = result.scalar_one()

                # Lock row to prevent race condition between SELECT and UPSERT
                row = session.execute(SELECT_LATEST_PRICE_FOR_UPDATE_SQL, {
                    "product_id": product_id,
                    "site": raw_price.site,
                }).fetchone()

                old_price = row[0] if row else None
                is_new = old_price is None
                is_changed = old_price is not None and old_price != raw_price.price

                if is_new or is_changed:
                    change_amount = (raw_price.price - old_price) if old_price else None
                    change_pct = (
                        Decimal(str(change_amount)) / Decimal(str(old_price)) * 100
                        if old_price and change_amount is not None
                        else None
                    )

                    change = PriceChange(
                        change_id=str(uuid.uuid4()),
                        product_id=str(product_id),
                        product_name=raw_price.product_name,
                        category=raw_price.category,
                        site=raw_price.site,
                        old_price=old_price,
                        new_price=raw_price.price,
                        change_amount=change_amount,
                        change_pct=change_pct,
                        url=raw_price.url,
                        crawled_at=raw_price.crawled_at,
                    )

                    producer.produce(
                        topic="price-changes",
                        key=f"{product_id}:{raw_price.site}".encode(),
                        value=serialize(change),
                    )
                    producer.flush()

                upsert_params = {
                    "product_id": product_id,
                    "site": raw_price.site,
                    "price": raw_price.price,
                    "url": raw_price.url,
                    "crawled_at": raw_price.crawled_at,
                }
                session.execute(UPSERT_LATEST_PRICE_SQL, upsert_params)
                session.execute(INSERT_PRICE_HISTORY_SQL, upsert_params)

            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutting down change detector")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()
