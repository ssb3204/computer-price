"""부품 조합 API 요청/응답 스키마 (Pydantic)."""
from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class BuildCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class BuildRenameRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class BuildItemAddRequest(BaseModel):
    """내 워치리스트에 담긴 항목의 id."""

    watchlist_id: int


class BuildSummaryPublic(BaseModel):
    build_id: int
    name: str
    author_id: int
    author: str
    item_count: int
    created_at: datetime
    updated_at: datetime


class BuildItemPublic(BaseModel):
    watchlist_id: int
    site: str
    pcode: str
    product_name: str | None
    category: str
    brand: str | None
    added_at: datetime


class BuildDetailPublic(BaseModel):
    """조합 상세 = 요약 + 부품 목록. 누구나 볼 수 있다."""

    build: BuildSummaryPublic
    items: list[BuildItemPublic]


class TrendPointPublic(BaseModel):
    date: date
    total: int
    prices: dict[int, int]  # watchlist_id -> 그날 적용된 가격


class BuildTrendPublic(BaseModel):
    """조합 총액 추이.

    points 가 비어 있을 수 있다 — 부품이 없거나, 아직 가격 이력이 없는 부품이
    섞여 있으면 총액을 만들 수 없다. 그 이유를 unavailable_reason 으로 알린다.
    """

    build_id: int
    name: str
    points: list[TrendPointPublic]
    unavailable_reason: str | None = None
