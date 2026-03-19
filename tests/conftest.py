"""Shared test fixtures."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal

from src.common.models import RawPrice, PriceChange, Alert


@pytest.fixture
def sample_raw_price():
    return RawPrice(
        product_name="AMD 라이젠 7 7800X3D",
        category="CPU",
        brand="AMD",
        site="danawa",
        price=450000,
        url="https://shop.danawa.com/product/12345",
        crawled_at=datetime(2026, 3, 19, 6, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_price_change():
    return PriceChange(
        change_id="test-change-001",
        product_id="test-product-001",
        product_name="AMD 라이젠 7 7800X3D",
        category="CPU",
        site="danawa",
        old_price=470000,
        new_price=450000,
        change_amount=-20000,
        change_pct=Decimal("-4.2553"),
        url="https://shop.danawa.com/product/12345",
        crawled_at=datetime(2026, 3, 19, 6, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture
def sample_alert():
    return Alert(
        alert_id="test-alert-001",
        product_id="test-product-001",
        alert_type="PRICE_DROP",
        site="danawa",
        old_price=470000,
        new_price=450000,
        change_pct=Decimal("-4.2553"),
        created_at=datetime(2026, 3, 19, 6, 0, 0, tzinfo=timezone.utc),
    )
