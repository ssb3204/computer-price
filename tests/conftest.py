"""Shared test fixtures."""

import pytest
from datetime import datetime, timezone
from decimal import Decimal

from src.common.models import Alert


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
