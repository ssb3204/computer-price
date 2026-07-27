"""부품 조합 데이터 접근 계층 (repository).

builds(조합 = 공개 게시물)와 build_items(조합-상품 연결) 두 테이블을 다룬다.
쓰기 함수는 기존 mysql_client.get_connection() 을 그대로 사용한다
(autocommit=True, 커서는 기본 튜플 커서).

읽기와 쓰기의 권한이 다르다:
  - 읽기(get_build, list_public_builds, get_build_items)는 누구나 가능하다.
    조합은 공개 게시물이므로 user_id 로 거르지 않는다.
  - 쓰기는 라우터에서 is_build_owner() 로 작성자를 확인한 뒤 호출한다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

from src.api.watchlist_repo import WATCHLIST_PRODUCT_JOIN
from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BuildSummary:
    """공개 목록에 쓰는 조합 요약."""
    build_id: int
    name: str
    author_id: int
    author: str          # nickname 우선, 없으면 username
    item_count: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class BuildItem:
    """조합에 담긴 부품 한 개."""
    watchlist_id: int
    site: str
    pcode: str
    product_name: str | None
    category: str
    brand: str | None
    added_at: datetime


# 작성자 표시명: nickname 이 비어 있으면 username 으로 폴백한다.
_AUTHOR_EXPR = "COALESCE(NULLIF(u.nickname, ''), u.username)"


def create_build(settings: MySQLSettings, user_id: int, name: str) -> int | None:
    """조합 생성. 같은 사용자가 같은 이름을 이미 쓰고 있으면 None.

    UNIQUE(user_id, name) 위반을 예외로 터뜨리지 않고 None 으로 알린다
    (라우터가 409 로 변환).
    """
    sql = "INSERT IGNORE INTO builds (user_id, name) VALUES (%s, %s)"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, name))
            if cur.rowcount == 0:
                return None
            return cur.lastrowid


def delete_build(settings: MySQLSettings, build_id: int) -> bool:
    """조합 삭제. build_items 는 FK ON DELETE CASCADE 로 함께 지워진다.

    Returns: 실제로 지웠으면 True.
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM builds WHERE id = %s", (build_id,))
            return cur.rowcount > 0


def rename_build(settings: MySQLSettings, build_id: int, name: str) -> bool:
    """조합 이름 변경. 같은 사용자의 다른 조합과 이름이 겹치면 False."""
    sql = "UPDATE IGNORE builds SET name = %s WHERE id = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (name, build_id))
            return cur.rowcount > 0


def is_build_owner(settings: MySQLSettings, user_id: int, build_id: int) -> bool:
    """이 조합의 작성자인지 확인. 쓰기 작업 전에 라우터가 호출한다."""
    sql = "SELECT 1 FROM builds WHERE id = %s AND user_id = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (build_id, user_id))
            return cur.fetchone() is not None


def get_build(settings: MySQLSettings, build_id: int) -> BuildSummary | None:
    """조합 요약 조회 (공개 — 소유자 무관).

    탈퇴한 회원(users.deleted_at IS NOT NULL)의 조합은 없는 것으로 취급한다.
    """
    sql = f"""
        SELECT b.id, b.name, b.user_id, {_AUTHOR_EXPR} AS author,
               (SELECT COUNT(*) FROM build_items bi WHERE bi.build_id = b.id) AS item_count,
               b.created_at, b.updated_at
        FROM builds b
        JOIN users u ON u.id = b.user_id
        WHERE b.id = %s AND u.deleted_at IS NULL
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (build_id,))
            row = cur.fetchone()
    return _to_summary(row) if row else None


def list_public_builds(
    settings: MySQLSettings, limit: int = 20, offset: int = 0
) -> list[BuildSummary]:
    """공개 조합 목록 (최신순). 탈퇴 회원의 조합은 제외한다."""
    sql = f"""
        SELECT b.id, b.name, b.user_id, {_AUTHOR_EXPR} AS author,
               (SELECT COUNT(*) FROM build_items bi WHERE bi.build_id = b.id) AS item_count,
               b.created_at, b.updated_at
        FROM builds b
        JOIN users u ON u.id = b.user_id
        WHERE u.deleted_at IS NULL
        ORDER BY b.created_at DESC, b.id DESC
        LIMIT %s OFFSET %s
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit, offset))
            rows = cur.fetchall()
    return [_to_summary(r) for r in rows]


def list_user_builds(settings: MySQLSettings, user_id: int) -> list[BuildSummary]:
    """특정 사용자가 만든 조합 목록 (최신순)."""
    sql = f"""
        SELECT b.id, b.name, b.user_id, {_AUTHOR_EXPR} AS author,
               (SELECT COUNT(*) FROM build_items bi WHERE bi.build_id = b.id) AS item_count,
               b.created_at, b.updated_at
        FROM builds b
        JOIN users u ON u.id = b.user_id
        WHERE b.user_id = %s AND u.deleted_at IS NULL
        ORDER BY b.created_at DESC, b.id DESC
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            rows = cur.fetchall()
    return [_to_summary(r) for r in rows]


def get_build_items(settings: MySQLSettings, build_id: int) -> list[BuildItem]:
    """조합에 담긴 부품 목록 (공개 — 소유자 무관).

    부품은 전역 stg_watchlist 를 참조하므로, 담은 사람이 아니어도
    상품 정보가 그대로 보인다.
    """
    sql = """
        SELECT w.id, w.site, w.pcode, w.product_name, w.category, w.brand, bi.added_at
        FROM build_items bi
        JOIN stg_watchlist w ON w.id = bi.watchlist_id
        WHERE bi.build_id = %s
        ORDER BY w.category ASC, bi.added_at ASC
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (build_id,))
            rows = cur.fetchall()
    return [
        BuildItem(
            watchlist_id=r[0], site=r[1], pcode=r[2],
            product_name=r[3], category=r[4], brand=r[5], added_at=r[6],
        )
        for r in rows
    ]


def add_build_item(settings: MySQLSettings, build_id: int, watchlist_id: int) -> bool:
    """조합에 부품 추가. 이미 담겨 있으면 조용히 무시(멱등성).

    Returns: 실제로 추가했으면 True.
    """
    sql = "INSERT IGNORE INTO build_items (build_id, watchlist_id) VALUES (%s, %s)"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (build_id, watchlist_id))
            return cur.rowcount > 0


def remove_build_item(settings: MySQLSettings, build_id: int, watchlist_id: int) -> bool:
    """조합에서 부품 제거.

    Returns: 실제로 제거했으면 True.
    """
    sql = "DELETE FROM build_items WHERE build_id = %s AND watchlist_id = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (build_id, watchlist_id))
            return cur.rowcount > 0


def get_daily_part_prices(
    settings: MySQLSettings, build_id: int
) -> dict[int, list[tuple[date, int]]]:
    """조합에 담긴 부품들의 '일별 마지막 가격'을 부품별로 모아 반환한다.

    같은 날 여러 번 크롤링되므로(4회/일) ROW_NUMBER 로 그날 마지막 행만 남긴다.
    crawled_at 이 같은 값이 들어오는 극단적 경우까지 순서를 확정하려고 id 도
    정렬 키에 넣었다.

    가격 이력이 하나도 없는 부품은 빈 리스트로 담아 반환한다 — 호출부
    (build_trend.build_daily_totals)가 "총액을 만들 수 없는 조합"으로 판정하는
    데 이 정보가 필요하다.

    반환: {watchlist_id: [(날짜, 가격), ...]}  (날짜 오름차순)
    """
    sql = f"""
        SELECT watchlist_id, d, price FROM (
            SELECT bi.watchlist_id,
                   DATE(ph.crawled_at) AS d,
                   ph.price,
                   ROW_NUMBER() OVER (
                       PARTITION BY bi.watchlist_id, DATE(ph.crawled_at)
                       ORDER BY ph.crawled_at DESC, ph.id DESC
                   ) AS rn
            FROM build_items bi
            JOIN stg_watchlist w ON w.id = bi.watchlist_id
            {WATCHLIST_PRODUCT_JOIN}
            JOIN stg_price_history ph ON ph.product_id = p.product_id
            WHERE bi.build_id = %s
        ) t
        WHERE t.rn = 1
        ORDER BY t.watchlist_id ASC, t.d ASC
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT watchlist_id FROM build_items WHERE build_id = %s", (build_id,))
            result: dict[int, list[tuple[date, int]]] = {r[0]: [] for r in cur.fetchall()}
            if not result:
                return {}
            cur.execute(sql, (build_id,))
            for watchlist_id, day, price in cur.fetchall():
                result[watchlist_id].append((day, int(price)))
    return result


def _to_summary(row: tuple) -> BuildSummary:
    return BuildSummary(
        build_id=row[0], name=row[1], author_id=row[2], author=row[3],
        item_count=row[4], created_at=row[5], updated_at=row[6],
    )
