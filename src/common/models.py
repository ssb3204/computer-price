"""Immutable data transfer objects shared across all services."""

from dataclasses import dataclass
from datetime import datetime


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
