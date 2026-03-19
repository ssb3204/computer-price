"""Kafka producer and consumer factory."""

import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

from src.common.config import KafkaSettings

logger = logging.getLogger(__name__)


def create_producer(settings: KafkaSettings) -> Producer:
    return Producer({
        "bootstrap.servers": settings.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
    })


def create_consumer(
    settings: KafkaSettings,
    group_id: str,
    topics: list[str],
    auto_commit: bool = False,
) -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": auto_commit,
    })
    consumer.subscribe(topics)
    return consumer


def check_kafka_health(settings: KafkaSettings) -> bool:
    try:
        admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        return len(metadata.brokers) > 0
    except Exception:
        logger.exception("Kafka health check failed")
        return False
