"""부품 조합 라우터 (FastAPI).

조합은 공개 게시물이라 읽기와 쓰기의 경로·권한이 다르다.

  읽기 (누구나)
    GET    /builds                              공개 조합 목록 (최신순)
    GET    /builds/{build_id}                   조합 상세 (부품 목록 포함)
    GET    /builds/{build_id}/price-trend       총액 추이 (?days=30)

  쓰기 (작성자만)
    POST   /users/{user_id}/builds                          조합 생성
    PATCH  /users/{user_id}/builds/{build_id}               이름 변경
    DELETE /users/{user_id}/builds/{build_id}               조합 삭제
    POST   /users/{user_id}/builds/{build_id}/items         부품 추가
    DELETE /users/{user_id}/builds/{build_id}/items/{wid}   부품 제거
    GET    /users/{user_id}/builds                          내 조합 목록

쓰기 경로가 /users/{user_id}/ 아래인 것은 기존 워치리스트 라우터 관례를 따른 것이다.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status

from src.api import build_repo, users_repo, watchlist_repo
from src.api.build_schemas import (
    BuildCreateRequest,
    BuildDetailPublic,
    BuildItemAddRequest,
    BuildItemPublic,
    BuildRenameRequest,
    BuildSummaryPublic,
    BuildTrendPublic,
    TrendPointPublic,
)
from src.api.build_trend import build_daily_totals, take_last_days
from src.common.config import MySQLSettings

logger = logging.getLogger(__name__)

public_build_router = APIRouter(prefix="/builds", tags=["builds"])
user_build_router = APIRouter(prefix="/users", tags=["builds"])

MAX_PAGE_SIZE = 100


def get_settings() -> MySQLSettings:
    return MySQLSettings()


# ── 공통 검증 ────────────────────────────────────────────────────────────────


def _require_user(settings: MySQLSettings, user_id: int) -> None:
    if users_repo.get_active_by_id(settings, user_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="사용자를 찾을 수 없습니다.",
        )


def _require_owned_build(settings: MySQLSettings, user_id: int, build_id: int) -> None:
    """작성자 본인인지 확인. 남의 조합이면 404로 응답한다.

    403(권한 없음)이 아니라 404(없음)를 쓰는 이유: 403을 주면 "그 id의 조합이
    존재한다"는 사실이 새어나간다. 조합 자체는 공개지만, 수정 경로에서까지
    존재 여부를 알려줄 이유는 없다.
    """
    if not build_repo.is_build_owner(settings, user_id, build_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="조합을 찾을 수 없습니다.",
        )


# ── 읽기 (공개) ──────────────────────────────────────────────────────────────


@public_build_router.get("", response_model=list[BuildSummaryPublic])
def list_builds(
    limit: int = Query(default=20, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0),
    settings: MySQLSettings = Depends(get_settings),
) -> list[BuildSummaryPublic]:
    """공개 조합 목록. 누구나 볼 수 있다."""
    return [_to_summary(b) for b in build_repo.list_public_builds(settings, limit, offset)]


@public_build_router.get("/{build_id}", response_model=BuildDetailPublic)
def get_build_detail(
    build_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> BuildDetailPublic:
    """조합 상세. 작성자가 아니어도 부품 목록까지 그대로 보인다."""
    build = build_repo.get_build(settings, build_id)
    if build is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="조합을 찾을 수 없습니다.",
        )
    items = build_repo.get_build_items(settings, build_id)
    return BuildDetailPublic(
        build=_to_summary(build),
        items=[_to_item(i) for i in items],
    )


@public_build_router.get("/{build_id}/price-trend", response_model=BuildTrendPublic)
def get_build_price_trend(
    build_id: int,
    days: int | None = Query(default=None, ge=1, le=3650),
    settings: MySQLSettings = Depends(get_settings),
) -> BuildTrendPublic:
    """조합 총액의 일별 추이. days 를 주면 최근 N일치만 반환한다.

    집계 규칙은 build_trend 모듈 참고 (일별 마지막 값 + forward fill,
    모든 부품이 값을 갖는 날부터 시작).
    """
    build = build_repo.get_build(settings, build_id)
    if build is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="조합을 찾을 수 없습니다.",
        )

    part_prices = build_repo.get_daily_part_prices(settings, build_id)
    points = take_last_days(build_daily_totals(part_prices), days)

    return BuildTrendPublic(
        build_id=build.build_id,
        name=build.name,
        points=[
            TrendPointPublic(date=p.date, total=p.total, prices=p.prices) for p in points
        ],
        unavailable_reason=None if points else _unavailable_reason(part_prices),
    )


def _unavailable_reason(part_prices: dict[int, list]) -> str:
    """추이를 못 그리는 이유를 화면에 그대로 띄울 문구로 만든다."""
    if not part_prices:
        return "부품이 없습니다. 워치리스트에서 부품을 추가해 주세요."
    if any(not points for points in part_prices.values()):
        return "아직 가격이 수집되지 않은 부품이 있습니다. 다음 크롤링 이후 표시됩니다."
    return "표시할 가격 이력이 없습니다."


# ── 쓰기 (작성자만) ──────────────────────────────────────────────────────────


@user_build_router.get("/{user_id}/builds", response_model=list[BuildSummaryPublic])
def list_my_builds(
    user_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> list[BuildSummaryPublic]:
    _require_user(settings, user_id)
    return [_to_summary(b) for b in build_repo.list_user_builds(settings, user_id)]


@user_build_router.post(
    "/{user_id}/builds",
    response_model=BuildSummaryPublic,
    status_code=status.HTTP_201_CREATED,
)
def create_build(
    user_id: int,
    payload: BuildCreateRequest,
    settings: MySQLSettings = Depends(get_settings),
) -> BuildSummaryPublic:
    _require_user(settings, user_id)
    build_id = build_repo.create_build(settings, user_id, payload.name)
    if build_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="같은 이름의 조합이 이미 있습니다.",
        )
    return _to_summary(build_repo.get_build(settings, build_id))


@user_build_router.patch(
    "/{user_id}/builds/{build_id}", response_model=BuildSummaryPublic
)
def rename_build(
    user_id: int,
    build_id: int,
    payload: BuildRenameRequest,
    settings: MySQLSettings = Depends(get_settings),
) -> BuildSummaryPublic:
    _require_owned_build(settings, user_id, build_id)
    if not build_repo.rename_build(settings, build_id, payload.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="같은 이름의 조합이 이미 있습니다.",
        )
    return _to_summary(build_repo.get_build(settings, build_id))


@user_build_router.delete(
    "/{user_id}/builds/{build_id}", status_code=status.HTTP_204_NO_CONTENT
)
def delete_build(
    user_id: int,
    build_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> None:
    _require_owned_build(settings, user_id, build_id)
    build_repo.delete_build(settings, build_id)
    return None


@user_build_router.post(
    "/{user_id}/builds/{build_id}/items",
    response_model=list[BuildItemPublic],
    status_code=status.HTTP_201_CREATED,
)
def add_build_item(
    user_id: int,
    build_id: int,
    payload: BuildItemAddRequest,
    settings: MySQLSettings = Depends(get_settings),
) -> list[BuildItemPublic]:
    """조합에 부품 추가. 내 워치리스트에 담긴 상품만 넣을 수 있다."""
    _require_owned_build(settings, user_id, build_id)
    if not watchlist_repo.is_watchlist_owner(settings, user_id, payload.watchlist_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="내 워치리스트에서 해당 상품을 찾을 수 없습니다.",
        )
    build_repo.add_build_item(settings, build_id, payload.watchlist_id)
    return [_to_item(i) for i in build_repo.get_build_items(settings, build_id)]


@user_build_router.delete(
    "/{user_id}/builds/{build_id}/items/{watchlist_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def remove_build_item(
    user_id: int,
    build_id: int,
    watchlist_id: int,
    settings: MySQLSettings = Depends(get_settings),
) -> None:
    """조합에서 부품 제거.

    제거 후 이 상품을 참조하는 곳(워치리스트/다른 조합)이 아무것도 없으면
    크롤링도 중단한다 — 워치리스트에서 뺄 때와 같은 처리다.
    """
    _require_owned_build(settings, user_id, build_id)
    if not build_repo.remove_build_item(settings, build_id, watchlist_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="조합에서 해당 부품을 찾을 수 없습니다.",
        )
    watchlist_repo.deactivate_if_orphaned(settings, watchlist_id)
    return None


# ── 변환 ─────────────────────────────────────────────────────────────────────


def _to_summary(b: build_repo.BuildSummary) -> BuildSummaryPublic:
    return BuildSummaryPublic(
        build_id=b.build_id, name=b.name, author_id=b.author_id, author=b.author,
        item_count=b.item_count, created_at=b.created_at, updated_at=b.updated_at,
    )


def _to_item(i: build_repo.BuildItem) -> BuildItemPublic:
    return BuildItemPublic(
        watchlist_id=i.watchlist_id, site=i.site, pcode=i.pcode,
        product_name=i.product_name, category=i.category, brand=i.brand,
        added_at=i.added_at,
    )
