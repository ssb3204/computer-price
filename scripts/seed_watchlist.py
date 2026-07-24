"""stg_watchlist 초기 시드 (MySQL 마이그레이션용).

구 Snowflake watchlist 데이터는 폐기 대상이므로, MySQL computer_price DB의
stg_watchlist를 초기 크롤링 대상으로 채운다.

- 다나와: 알려진 pcode로 직접 시드
- 컴퓨존/견적왕: 각 사이트 search_products로 실제 상품 ID를 해석해 시드
  (사이트별 ID 체계가 달라 검색으로 해석해야 함)

멱등: pcode UNIQUE 기준 ON DUPLICATE KEY UPDATE.

실행: python scripts/seed_watchlist.py
"""

import logging

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection
from src.crawlers import compuzone, pc_estimate

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# 다나와: (query, pcode, category, brand)
DANAWA_SEED = [
    ("라이젠 7800X3D", "19627934", "CPU", "AMD"),
    ("RTX 5070",       "77379452", "GPU", "NVIDIA"),
    ("RTX 5070 Ti",    "76464143", "GPU", "NVIDIA"),
    ("RX 9070 XT",     "77381483", "GPU", "AMD"),
]

# 컴퓨존/견적왕: 검색으로 ID 해석할 (query, category) 목록
SEARCH_SEED = [
    ("RTX 5070", "GPU"),
    ("라이젠 7800X3D", "CPU"),
]


def _resolve_rows() -> list[tuple]:
    """시드할 (query, pcode, category, brand, site) 행 목록을 만든다."""
    rows: list[tuple] = [
        (q, p, c, b, "다나와") for (q, p, c, b) in DANAWA_SEED
    ]

    for q, cat in SEARCH_SEED:
        try:
            res = compuzone.search_products(q, cat, max_results=1)
            if res:
                rows.append((q, res[0].product_no, cat, None, "컴퓨존"))
                logger.info("[컴퓨존] '%s' → product_no=%s", q, res[0].product_no)
        except Exception:
            logger.exception("[컴퓨존] '%s' 검색 실패", q)

    for q, cat in SEARCH_SEED:
        try:
            res = pc_estimate.search_products(q, cat, max_results=1)
            if res:
                rows.append((q, res[0].pd_no, cat, None, "견적왕"))
                logger.info("[견적왕] '%s' → pd_no=%s", q, res[0].pd_no)
        except Exception:
            logger.exception("[견적왕] '%s' 검색 실패", q)

    return rows


def seed_watchlist(settings: MySQLSettings) -> None:
    rows = _resolve_rows()
    with get_connection(settings) as conn:
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO `stg_watchlist` (`query`, `pcode`, `category`, `brand`, `site`) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE `query` = VALUES(`query`)",
            rows,
        )
        cur.execute("SELECT `site`, COUNT(*) FROM `stg_watchlist` GROUP BY `site`")
        for site, cnt in cur.fetchall():
            logger.info("[stg_watchlist] %s: %d건", site, cnt)
        cur.close()


if __name__ == "__main__":
    seed_watchlist(MySQLSettings())
