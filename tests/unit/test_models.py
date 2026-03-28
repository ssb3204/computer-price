"""Tests for immutable data models."""

import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from src.common.models import RawCrawledPrice


@pytest.fixture
def sample_raw_crawled_price():
    return RawCrawledPrice(
        site="danawa",
        category="CPU",
        product_name="AMD 라이젠 7 7800X3D",
        price_text="450,000원",
        brand="AMD",
        url="https://shop.danawa.com/product/12345",
        crawled_at=datetime(2026, 3, 19, 6, 0, 0, tzinfo=timezone.utc),
    )


def test_raw_crawled_price_is_frozen(sample_raw_crawled_price):
    with pytest.raises(FrozenInstanceError):
        sample_raw_crawled_price.price_text = "999"


def test_raw_crawled_price_fields(sample_raw_crawled_price):
    assert sample_raw_crawled_price.product_name == "AMD 라이젠 7 7800X3D"
    assert sample_raw_crawled_price.price_text == "450,000원"
    assert sample_raw_crawled_price.site == "danawa"
    assert sample_raw_crawled_price.category == "CPU"
