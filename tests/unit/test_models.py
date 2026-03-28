"""Tests for immutable data models."""

import pytest
from dataclasses import FrozenInstanceError

from src.common.models import RawPrice


def test_raw_price_is_frozen(sample_raw_price):
    with pytest.raises(FrozenInstanceError):
        sample_raw_price.price = 999


def test_raw_price_fields(sample_raw_price):
    assert sample_raw_price.product_name == "AMD 라이젠 7 7800X3D"
    assert sample_raw_price.price == 450000
    assert sample_raw_price.site == "danawa"
    assert sample_raw_price.category == "CPU"
