"""워치리스트 API 요청/응답 스키마 (Pydantic)."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class WatchlistItemAdd(BaseModel):
    """검색 결과에서 체크박스로 고른 상품 1개. /crawl/search 응답을 그대로 재사용."""

    site: str
    query: str = Field(min_length=1)
    pcode: str = Field(min_length=1)
    product_name: str
    category: str
    brand: str | None = None


class WatchlistAddRequest(BaseModel):
    """선택 완료 시 여러 개를 한 번에 담는다."""

    items: list[WatchlistItemAdd] = Field(min_length=1)


class WatchlistItemPublic(BaseModel):
    watchlist_id: int
    site: str
    pcode: str
    product_name: str | None
    category: str
    brand: str | None
    added_at: datetime


class PricePointPublic(BaseModel):
    price: int
    crawled_at: datetime
