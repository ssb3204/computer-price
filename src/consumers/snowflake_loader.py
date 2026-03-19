"""Kafka consumer that batch-loads price changes into Snowflake."""

import logging
import time

from src.common.config import load_settings
from src.common.kafka_client import create_consumer
from src.common.logging_config import setup_logging
from src.common.serialization import deserialize_price_change
from src.common.snowflake_client import batch_insert, get_connection

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
FLUSH_INTERVAL_SECONDS = 60

PRICE_CHANGES_COLUMNS = [
    "change_id", "product_id", "product_name", "category", "site",
    "old_price", "new_price", "change_amount", "change_pct",
    "url", "crawled_at",
]


def run() -> None:
    setup_logging("snowflake-loader")
    settings = load_settings()

    consumer = create_consumer(settings.kafka, "snowflake-loader", ["price-changes"])

    logger.info("Snowflake loader started")
    batch: list[tuple] = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                change = deserialize_price_change(msg.value())
                batch.append((
                    change.change_id,
                    change.product_id,
                    change.product_name,
                    change.category,
                    change.site,
                    change.old_price,
                    change.new_price,
                    change.change_amount,
                    str(change.change_pct) if change.change_pct else None,
                    change.url,
                    change.crawled_at.isoformat(),
                ))
            elif msg is not None and msg.error():
                logger.error("Consumer error: %s", msg.error())

            should_flush = (
                len(batch) >= BATCH_SIZE
                or (batch and time.time() - last_flush >= FLUSH_INTERVAL_SECONDS)
            )

            if should_flush:
                _flush_batch(settings, batch)
                consumer.commit()
                batch = []
                last_flush = time.time()

    except KeyboardInterrupt:
        if batch:
            _flush_batch(settings, batch)
            consumer.commit()
        logger.info("Shutting down snowflake loader")
    finally:
        consumer.close()


def _flush_batch(settings, batch: list[tuple]) -> None:
    try:
        with get_connection(settings.snowflake) as conn:
            count = batch_insert(conn, "RAW.PRICE_CHANGES", PRICE_CHANGES_COLUMNS, batch)
            logger.info("Loaded %d records into Snowflake", count)
    except Exception:
        logger.exception("Failed to load batch into Snowflake (%d records)", len(batch))


if __name__ == "__main__":
    run()
