"""Tests for immutable data models."""

import pytest
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from src.common.models import RawPrice, Product


def test_raw_price_is_frozen(sample_raw_price):
    with pytest.raises(FrozenInstanceError):
        sample_raw_price.price = 999


def test_raw_price_fields(sample_raw_price):
    assert sample_raw_price.product_name == "AMD 라이젠 7 7800X3D"
    assert sample_raw_price.price == 450000
    assert sample_raw_price.site == "danawa"
    assert sample_raw_price.category == "CPU"


def test_product_frozen():
    p = Product(
        product_id="test",
        name="Test Product",
        category="CPU",
        brand="AMD",
        model_number="7800X3D",
        normalized_name="test product",
    )
    with pytest.raises(FrozenInstanceError):
        p.name = "changed"
