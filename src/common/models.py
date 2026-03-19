"""Immutable data transfer objects shared across all services."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Product:
    product_id: str
    name: str
    category: str  # 'CPU', 'GPU', 'RAM', 'SSD', 'Mainboard'
    brand: str | None
    model_number: str | None
    normalized_name: str


@dataclass(frozen=True)
class RawPrice:
    product_name: str
    category: str
    brand: str | None
    site: str  # 'danawa' | 'compuzone' | 'pc_estimate'
    price: int  # KRW
    url: str
    crawled_at: datetime


@dataclass(frozen=True)
class PriceChange:
    change_id: str
    product_id: str
    product_name: str
    category: str
    site: str
    old_price: int | None
    new_price: int
    change_amount: int | None
    change_pct: Decimal | None
    url: str
    crawled_at: datetime


@dataclass(frozen=True)
class Alert:
    alert_id: str
    product_id: str
    alert_type: str  # 'NEW_LOW' | 'NEW_HIGH' | 'PRICE_DROP' | 'PRICE_SPIKE'
    site: str
    old_price: int | None
    new_price: int
    change_pct: Decimal | None
    created_at: datetime
