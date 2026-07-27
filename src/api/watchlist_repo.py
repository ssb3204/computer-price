"""워치리스트 데이터 접근 계층 (repository).

stg_watchlist(전역, pcode UNIQUE — 실제 크롤링 대상 마스터)와
user_watchlist(사용자-상품 연결 테이블) 두 테이블을 함께 다룬다.
쓰기 함수는 기존 mysql_client.get_connection() 을 그대로 사용한다
(autocommit=True, 커서는 기본 튜플 커서).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

logger = logging.getLogger(__name__)

# stg_watchlist ↔ stg_products 조인 조건.
# 두 테이블 사이에 FK가 없고 URL 안의 상품 ID 문자열로만 연결되는 기존 스키마
# 관례를 그대로 따른다(사이트마다 파라미터 이름이 다름). build_repo 도 같은
# 경로로 가격을 찾으므로 여기서 한 번만 정의해 공유한다.
# 주의: pymysql 에 파라미터를 넘기므로 LIKE 의 % 는 %% 로 이스케이프해야 한다.
WATCHLIST_PRODUCT_JOIN = """
        JOIN stg_products p
          ON p.site = w.site
          AND (
              (w.site = '다나와' AND p.url LIKE CONCAT('%%pcode=', w.pcode, '%%'))
           OR (w.site = '컴퓨존' AND p.url LIKE CONCAT('%%ProductNo=', w.pcode, '%%'))
           OR (w.site = '견적왕' AND p.url LIKE CONCAT('%%pd_no=', w.pcode, '%%'))
          )
"""


@dataclass(frozen=True)
class PricePoint:
    price: int
    crawled_at: datetime


@dataclass(frozen=True)
class UserWatchlistItem:
    watchlist_id: int
    site: str
    pcode: str
    product_name: str | None
    category: str
    brand: str | None
    added_at: datetime


def upsert_watchlist_item(
    settings: MySQLSettings,
    site: str,
    query: str,
    pcode: str,
    product_name: str | None,
    category: str,
    brand: str | None,
) -> int:
    """stg_watchlist upsert (pcode UNIQUE). 결과 row의 id를 반환한다.

    ON DUPLICATE KEY UPDATE 절에서 `id = LAST_INSERT_ID(id)` 트릭을 써서,
    새로 INSERT 되든 기존 row가 UPDATE 되든 cur.lastrowid로 항상 정확한
    id를 얻을 수 있게 한다(트릭 없이는 UPDATE 경로에서 lastrowid가 0이 됨).
    """
    sql = """
        INSERT INTO stg_watchlist (site, query, pcode, product_name, category, brand)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            is_active = 1,
            query = VALUES(query),
            product_name = VALUES(product_name),
            category = VALUES(category),
            brand = VALUES(brand)
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (site, query, pcode, product_name, category, brand))
            return cur.lastrowid


def link_user_watchlist(settings: MySQLSettings, user_id: int, watchlist_id: int) -> bool:
    """user_watchlist에 연결 추가. 이미 담았으면 조용히 무시(멱등성).

    Returns: 새로 연결됐으면 True, 이미 있었으면 False.
    """
    sql = "INSERT IGNORE INTO user_watchlist (user_id, watchlist_id) VALUES (%s, %s)"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            affected = cur.execute(sql, (user_id, watchlist_id))
    return affected == 1


def get_user_watchlist(settings: MySQLSettings, user_id: int) -> list[UserWatchlistItem]:
    """이 사용자가 담은 워치리스트 항목 전체 조회. is_active 무관하게 보여준다
    (본인이 담은 건 계속 자기 목록에 남아있어야 하므로 — 설계 근거는
    mysql/ddl/user_watchlist.sql 참고)."""
    sql = """
        SELECT w.id, w.site, w.pcode, w.product_name, w.category, w.brand, uw.added_at
        FROM user_watchlist uw
        JOIN stg_watchlist w ON w.id = uw.watchlist_id
        WHERE uw.user_id = %s
        ORDER BY uw.added_at DESC
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id,))
            rows = cur.fetchall()
    return [
        UserWatchlistItem(
            watchlist_id=row[0],
            site=row[1],
            pcode=row[2],
            product_name=row[3],
            category=row[4],
            brand=row[5],
            added_at=row[6],
        )
        for row in rows
    ]


def unlink_user_watchlist(settings: MySQLSettings, user_id: int, watchlist_id: int) -> int:
    """user_watchlist 연결 제거. 영향받은 row 수 반환(0이면 원래 없었음)."""
    sql = "DELETE FROM user_watchlist WHERE user_id = %s AND watchlist_id = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            return cur.execute(sql, (user_id, watchlist_id))


def deactivate_if_orphaned(settings: MySQLSettings, watchlist_id: int) -> bool:
    """이 상품을 참조하는 곳이 하나도 없으면 stg_watchlist.is_active를 0으로.

    참조자는 두 종류다:
      1. user_watchlist — 누군가의 워치리스트에 담겨 있음
      2. build_items    — 누군가의 공개 조합에 부품으로 들어가 있음

    2번을 빼먹으면, 조합에 담긴 상품을 작성자가 워치리스트에서 빼는 순간
    크롤링이 멈춰서 남들이 보고 있는 공개 조합의 가격이 갱신되지 않는다.

    하나라도 참조가 남아 있으면 아무것도 하지 않는다(계속 크롤링 유지).
    Returns: 실제로 비활성화했으면 True.
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM user_watchlist WHERE watchlist_id = %s)
                  + (SELECT COUNT(*) FROM build_items    WHERE watchlist_id = %s)
                """,
                (watchlist_id, watchlist_id),
            )
            (remaining,) = cur.fetchone()
            if remaining > 0:
                return False
            cur.execute(
                "UPDATE stg_watchlist SET is_active = 0 WHERE id = %s",
                (watchlist_id,),
            )
    return True


def is_watchlist_owner(settings: MySQLSettings, user_id: int, watchlist_id: int) -> bool:
    """이 사용자가 실제로 담은 항목인지 확인(가격 이력 조회 전 소유권 체크)."""
    sql = "SELECT 1 FROM user_watchlist WHERE user_id = %s AND watchlist_id = %s"
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, watchlist_id))
            return cur.fetchone() is not None


def get_price_history(settings: MySQLSettings, watchlist_id: int) -> list[PricePoint]:
    """이 워치리스트 항목의 시간별 가격 이력 조회 (오래된 순).

    stg_watchlist(pcode) -> stg_products(url에 pcode 포함 여부로 매칭) -> stg_price_history
    조인 조건은 WATCHLIST_PRODUCT_JOIN 참고.
    """
    sql = f"""
        SELECT ph.price, ph.crawled_at
        FROM stg_watchlist w
        {WATCHLIST_PRODUCT_JOIN}
        JOIN stg_price_history ph ON ph.product_id = p.product_id
        WHERE w.id = %s
        ORDER BY ph.crawled_at ASC
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (watchlist_id,))
            rows = cur.fetchall()
    return [PricePoint(price=row[0], crawled_at=row[1]) for row in rows]
