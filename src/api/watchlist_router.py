"""크롤링 대상 검색/워치리스트 라우터 (FastAPI).

검색은 크롤러의 search_products() 를 그대로 재사용한다. 사이트별로 반환 필드명이
다르므로(pcode/pd_no/product_no) 여기서 "pcode" 하나로 정규화해 응답한다
(stg_watchlist 저장 시에도 전부 pcode 컬럼 하나로 통일되는 것과 동일한 관례).

엔드포인트:
  GET    /crawl/search                              실시간 상품 검색 (?site=&query=&category=)
  POST   /users/{user_id}/watchlist                  선택한 상품들을 이 사용자 워치리스트에 담기
  GET    /users/{user_id}/watchlist                  이 사용자가 담은 워치리스트 조회
  DELETE /users/{user_id}/watchlist/{id}              워치리스트에서 빼기 (마지막 사용자면 is_active=0)
  GET    /users/{user_id}/watchlist/{id}/price-history  이 항목의 시간별 가격 이력
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from src.api import users_repo, watchlist_repo
from src.api.watchlist_schemas import PricePointPublic, WatchlistAddRequest, WatchlistItemPublic
from src.common.config import MySQLSettings
from src.crawlers.compuzone import search_products as compuzone_search
from src.crawlers.danawa import search_products as danawa_search
from src.crawlers.pc_estimate import search_products as pcest_search
from src.pipeline.crawl import crawl_and_load_single

logger = logging.getLogger(__name__)

crawl_router = APIRouter(prefix="/crawl", tags=["crawl"])
user_watchlist_router = APIRouter(prefix="/users", tags=["watchlist"])


def get_settings() -> MySQLSettings:
    return MySQLSettings()


@dataclass(frozen=True)
class _SiteConfig:
    pcode_key: str
    search_fn: Callable[[str, str, int], list]


def _danawa(query: str, category: str, max_results: int) -> list:
    return danawa_search(query, max_results=max_results, category=category)


def _pcest(query: str, category: str, max_results: int) -> list:
    return pcest_search(query, category=category, max_results=max_results)


def _compuzone(query: str, category: str, max_results: int) -> list:
    return compuzone_search(query, category=category, max_results=max_results)


_SITE_CONFIG: dict[str, _SiteConfig] = {
    "다나와": _SiteConfig(pcode_key="pcode", search_fn=_danawa),
    "견적왕": _SiteConfig(pcode_key="pd_no", search_fn=_pcest),
    "컴퓨존": _SiteConfig(pcode_key="product_no", search_fn=_compuzone),
}

_VALID_CATEGORIES = {"CPU", "GPU", "RAM", "SSD"}


@crawl_router.get("/search")
def search_products_endpoint(
    site: str,
    query: str,
    category: str,
    max_results: int = 10,
) -> list[dict]:
    """실시간 상품 검색. 사이트에 직접 요청을 보내므로 몇 초 걸릴 수 있다.

    반환 필드는 사이트 무관하게 pcode/product_name/url 로 통일한다
    (다나와=pcode, 견적왕=pd_no, 컴퓨존=product_no 를 pcode 로 정규화).
    """
    cfg = _SITE_CONFIG.get(site)
    if cfg is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"지원하지 않는 사이트입니다: {site}",
        )
    if category.upper() not in _VALID_CATEGORIES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"지원하지 않는 카테고리입니다: {category}",
        )
    if not query.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="검색어를 입력해주세요.",
        )

    try:
        results = cfg.search_fn(query, category.upper(), max_results)
    except Exception:
        logger.exception("[검색] 실패: site=%s query=%s category=%s", site, query, category)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="검색 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.",
        )

    return [
        {
            "pcode": getattr(r, cfg.pcode_key),
            "product_name": r.product_name,
            "url": r.url,
        }
        for r in results
    ]


def _to_public(item: watchlist_repo.UserWatchlistItem) -> WatchlistItemPublic:
    return WatchlistItemPublic(
        watchlist_id=item.watchlist_id,
        site=item.site,
        pcode=item.pcode,
        product_name=item.product_name,
        category=item.category,
        brand=item.brand,
        added_at=item.added_at,
    )


def _trigger_immediate_crawl(
    settings: MySQLSettings, site: str, query: str, pcode: str, category: str, brand: str | None
) -> None:
    """담기 직후 백그라운드에서 1회 즉시 크롤링.

    실패해도 워치리스트 담기 자체는 이미 성공한 상태 — 다음 정규 스케줄에서 재시도된다.
    """
    try:
        crawl_and_load_single(settings, site, query, pcode, category, brand)
    except Exception:
        logger.exception("[즉시 크롤링] 실패: site=%s pcode=%s", site, pcode)


@user_watchlist_router.post(
    "/{user_id}/watchlist",
    response_model=list[WatchlistItemPublic],
    status_code=status.HTTP_201_CREATED,
)
def add_to_watchlist(
    user_id: int,
    payload: WatchlistAddRequest,
    background_tasks: BackgroundTasks,
    settings: MySQLSettings = Depends(get_settings),
) -> list[WatchlistItemPublic]:
    """체크박스로 고른 상품들을 한 번에 이 사용자 워치리스트에 담는다.

    이미 전역에 있는 상품(pcode 동일)이면 stg_watchlist는 upsert만 하고,
    이 사용자가 이미 담았던 항목이면 user_watchlist는 조용히 무시한다(멱등성).
    """
    user = users_repo.get_active_by_id(settings, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )

    for item in payload.items:
        watchlist_id = watchlist_repo.upsert_watchlist_item(
            settings, item.site, item.query, item.pcode, item.product_name, item.category, item.brand
        )
        watchlist_repo.link_user_watchlist(settings, user_id, watchlist_id)
        background_tasks.add_task(
            _trigger_immediate_crawl, settings, item.site, item.query, item.pcode, item.category, item.brand
        )

    return [_to_public(item) for item in watchlist_repo.get_user_watchlist(settings, user_id)]


@user_watchlist_router.get("/{user_id}/watchlist", response_model=list[WatchlistItemPublic])
def get_watchlist(
    user_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> list[WatchlistItemPublic]:
    """이 사용자가 담은 워치리스트 전체 조회."""
    user = users_repo.get_active_by_id(settings, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )
    return [_to_public(item) for item in watchlist_repo.get_user_watchlist(settings, user_id)]


@user_watchlist_router.delete("/{user_id}/watchlist/{watchlist_id}", status_code=status.HTTP_204_NO_CONTENT)
def remove_from_watchlist(
    user_id: int,
    watchlist_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> None:
    """워치리스트에서 빼기. 이 상품을 담은 사용자가 아무도 안 남으면 크롤링도 중단(is_active=0)."""
    affected = watchlist_repo.unlink_user_watchlist(settings, user_id, watchlist_id)
    if affected == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="워치리스트에서 해당 항목을 찾을 수 없습니다.",
        )
    watchlist_repo.deactivate_if_orphaned(settings, watchlist_id)
    return None


@user_watchlist_router.get(
    "/{user_id}/watchlist/{watchlist_id}/price-history",
    response_model=list[PricePointPublic],
)
def get_price_history(
    user_id: int,
    watchlist_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> list[PricePointPublic]:
    """이 워치리스트 항목의 시간별 가격 이력. 본인이 담은 항목만 조회 가능."""
    if not watchlist_repo.is_watchlist_owner(settings, user_id, watchlist_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="워치리스트에서 해당 항목을 찾을 수 없습니다.",
        )
    points = watchlist_repo.get_price_history(settings, watchlist_id)
    return [PricePointPublic(price=p.price, crawled_at=p.crawled_at) for p in points]
