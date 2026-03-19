"""Tests for Kafka message serialization."""

from src.common.serialization import serialize, deserialize_raw_price, deserialize_price_change


def test_raw_price_roundtrip(sample_raw_price):
    data = serialize(sample_raw_price)
    restored = deserialize_raw_price(data)

    assert restored.product_name == sample_raw_price.product_name
    assert restored.price == sample_raw_price.price
    assert restored.site == sample_raw_price.site
    assert restored.crawled_at == sample_raw_price.crawled_at


def test_price_change_roundtrip(sample_price_change):
    data = serialize(sample_price_change)
    restored = deserialize_price_change(data)

    assert restored.change_id == sample_price_change.change_id
    assert restored.old_price == sample_price_change.old_price
    assert restored.new_price == sample_price_change.new_price
    assert restored.change_pct == sample_price_change.change_pct


def test_serialize_produces_utf8_bytes(sample_raw_price):
    data = serialize(sample_raw_price)
    assert isinstance(data, bytes)
    text = data.decode("utf-8")
    assert "라이젠" in text
