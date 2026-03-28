"""Immutable data transfer objects shared across all services."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


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
class RawCrawledPrice:
    """Layer 1 (Raw): 크롤러가 수집한 원본 데이터. 가공 없음."""
    site: str             # 'danawa' | 'compuzone' | 'pc_estimate'
    category: str         # 원본 카테고리 문자열
    product_name: str     # 상품명 원본 (특수문자, 공백 그대로)
    price_text: str       # 가격 원본 텍스트 ("1,234,500원" 등)
    brand: str | None
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
