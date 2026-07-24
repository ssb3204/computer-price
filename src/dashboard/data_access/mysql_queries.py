"""MySQL 쿼리 — 새 3-Layer 스키마(테이블 접두사 raw_/stg_/ans_) 기반.

mysql_client.get_connection이 연결 시 세션 타임존을 UTC(+00:00)로 고정하므로
`crawled_at`(앱이 채우는 UTC naive)과 `created_at`/`updated_at`/`loaded_at` 등
DEFAULT CURRENT_TIMESTAMP 컬럼(DB가 채움)이 모두 같은 UTC 기준이다.
NOW()/UTC_TIMESTAMP()/UTC_DATE() 어느 쪽을 써도 이 연결 안에서는 동일하다.
"""

import pandas as pd
from pymysql.connections import Connection


def get_latest_prices_all(conn: Connection) -> pd.DataFrame:
    """WATCHLIST 활성 상품의 최신 가격 목록 (대시보드 메인 테이블)."""
    sql = f"""
        SELECT
            p.`product_id`,
            p.`site`,
            p.`category`,
            p.`product_name`,
            lp.`price`,
            lp.`crawled_at`,
            p.`url`
        FROM `stg_latest_prices` lp
        JOIN `stg_products` p ON p.`product_id` = lp.`product_id`
        WHERE {_WATCHLIST_EXISTS}
        ORDER BY p.`category`, lp.`price` ASC
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, ())
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_summary_stats(conn: Connection) -> dict:
    """대시보드 상단 요약 통계."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                (SELECT COUNT(*)                  FROM `stg_products`)      AS total_products,
                (SELECT COUNT(DISTINCT `category`) FROM `stg_products`)      AS total_categories,
                (SELECT COUNT(DISTINCT `site`)     FROM `stg_products`)      AS total_sites,
                (SELECT COUNT(*)                  FROM `stg_price_history`
                 WHERE DATE(`crawled_at`) = UTC_DATE())                     AS today_records
        """)
        row = cur.fetchone()
        return {
            "total_products":   row[0],
            "total_categories": row[1],
            "total_sites":      row[2],
            "today_records":    row[3],
        }
    finally:
        cur.close()


def get_product_stats(conn: Connection) -> pd.DataFrame:
    """WATCHLIST 활성 상품의 전체 기간 통계.

    ans_daily_price_stats(일별)를 상품 단위로 즉석 GROUP BY 해서 전체기간 통계를 구한다
    (실체 테이블을 따로 두지 않음 — §benchmark 20260724). 아직 집계되지 않은
    신규 상품도 포함하기 위해 stg_products를 기준으로 LEFT JOIN한다.
    """
    sql = f"""
        SELECT
            p.`product_id`,
            p.`site`,
            p.`category`,
            p.`product_name`,
            p.`url`,
            COALESCE(ps.`avg_price`,      lp.`price`) AS avg_price,
            COALESCE(ps.`min_price_ever`, lp.`price`) AS min_price_ever,
            COALESCE(ps.`max_price_ever`, lp.`price`) AS max_price_ever,
            ps.`first_crawled_at`,
            ps.`last_crawled_at`,
            COALESCE(ps.`total_records`, 1)            AS total_records
        FROM `stg_products` p
        LEFT JOIN (
            SELECT `product_id`,
                SUM(`avg_price` * `record_count`) / SUM(`record_count`) AS `avg_price`,
                MIN(`min_price`)        AS `min_price_ever`,
                MAX(`max_price`)        AS `max_price_ever`,
                MIN(`first_crawled_at`) AS `first_crawled_at`,
                MAX(`last_crawled_at`)  AS `last_crawled_at`,
                SUM(`record_count`)     AS `total_records`
            FROM `ans_daily_price_stats`
            GROUP BY `product_id`
        ) ps ON ps.`product_id` = p.`product_id`
        LEFT JOIN `stg_latest_prices` lp ON lp.`product_id` = p.`product_id`
        WHERE {_WATCHLIST_EXISTS}
        ORDER BY p.`category`, p.`product_name`
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, ())
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_price_trend(
    conn: Connection,
    category: str | None = None,
    search: str | None = None,
    days: int | None = None,
) -> pd.DataFrame:
    """검색 키워드 매칭 상품의 사이트별 최저가 추이 (라인 차트용).

    같은 사이트에 여러 매칭 상품이 있으면 크롤 시점별 최저가만 반환.
    days=None이면 전체 기간 조회.
    """
    if not search:
        return pd.DataFrame()

    conditions: list[str] = []
    params: list = []

    if days:
        conditions.append("dp.`crawled_at` >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)")
        params.append(days)

    if category and category != "ALL":
        conditions.append("p.`category` = %s")
        params.append(category)

    conditions.append("p.`product_name` LIKE %s")
    params.append(f"%{search}%")

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            p.`site`       AS site,
            dp.`crawled_at` AS crawled_at,
            MIN(dp.`price`) AS price
        FROM `stg_price_history` dp
        JOIN `stg_products` p ON p.`product_id` = dp.`product_id`
        WHERE {where}
        GROUP BY p.`site`, dp.`crawled_at`
        ORDER BY dp.`crawled_at`
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


## pymysql은 cur.execute(sql, args)에서 args가 None이 아니면 Python '%' 포맷팅으로
## bind를 수행한다. 아래 CONCAT 리터럴의 '%'는 데이터 자리표시자가 아니므로,
## args를 전달하는 모든 호출부에서 깨지지 않도록 '%%'로 이스케이프해 둔다.
_WATCHLIST_EXISTS = """EXISTS (
            SELECT 1 FROM `stg_watchlist` w
            WHERE w.`is_active` = 1
              AND p.`site` = w.`site`
              AND (
                  (w.`site` = '다나와' AND p.`url` LIKE CONCAT('%%pcode=', w.`pcode`, '%%'))
               OR (w.`site` = '컴퓨존' AND p.`url` LIKE CONCAT('%%ProductNo=', w.`pcode`, '%%'))
               OR (w.`site` = '견적왕' AND p.`url` LIKE CONCAT('%%pd_no=', w.`pcode`, '%%'))
              )
        )"""


def get_today_crawl_comparison(
    conn: Connection,
    category: str | None = None,
    search: str | None = None,
) -> pd.DataFrame:
    """오늘 크롤링 4회(1~4차) 가격 비교 — WATCHLIST 활성 상품만."""
    conditions = ["DATE(dp.`crawled_at`) = UTC_DATE()", _WATCHLIST_EXISTS]
    params: list = []

    if category and category != "ALL":
        conditions.append("p.`category` = %s")
        params.append(category)
    if search:
        conditions.append("p.`product_name` LIKE %s")
        params.append(f"%{search}%")

    where = " AND ".join(conditions)

    sql = f"""
        WITH daily AS (
            SELECT
                dp.`product_id`,
                dp.`price`,
                dp.`crawled_at`,
                ROW_NUMBER() OVER (
                    PARTITION BY dp.`product_id`
                    ORDER BY dp.`crawled_at`
                ) AS rn
            FROM `stg_price_history` dp
            JOIN `stg_products` p ON p.`product_id` = dp.`product_id`
            WHERE {where}
        )
        SELECT
            p.`site`          AS site,
            p.`category`      AS category,
            p.`product_name`  AS product_name,
            d1.`price`        AS price_1st,
            d2.`price`        AS price_2nd,
            d3.`price`        AS price_3rd,
            d4.`price`        AS price_4th
        FROM daily d1
        LEFT JOIN daily d2 ON d1.`product_id` = d2.`product_id` AND d2.rn = 2
        LEFT JOIN daily d3 ON d1.`product_id` = d3.`product_id` AND d3.rn = 3
        LEFT JOIN daily d4 ON d1.`product_id` = d4.`product_id` AND d4.rn = 4
        JOIN `stg_products` p ON p.`product_id` = d1.`product_id`
        WHERE d1.rn = 1
        ORDER BY p.`category`, p.`product_name`
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_alerts(
    conn: Connection,
    alert_type: str | None = None,
    category: str | None = None,
    days: int | None = None,
) -> pd.DataFrame:
    """알림 목록 조회 (필터: 유형, 카테고리, 기간). 1% 미만 변동 제외."""
    conditions: list[str] = ["ABS(a.`change_pct`) >= 1.0"]
    params: list = []

    if alert_type and alert_type != "ALL":
        conditions.append("a.`alert_type` = %s")
        params.append(alert_type)

    if category and category != "ALL":
        conditions.append("p.`category` = %s")
        params.append(category)

    if days:
        # 세션 타임존이 UTC로 고정돼 있어 NOW()는 UTC_TIMESTAMP()와 동일
        conditions.append("a.`created_at` >= DATE_SUB(NOW(), INTERVAL %s DAY)")
        params.append(days)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT
            a.`alert_id`,
            a.`alert_type`,
            p.`site`,
            p.`category`,
            p.`product_name`,
            p.`url`,
            a.`old_price`,
            a.`new_price`,
            a.`change_pct`,
            a.`created_at`
        FROM `stg_price_alerts` a
        JOIN `stg_products` p ON p.`product_id` = a.`product_id`
        {where}
        ORDER BY a.`created_at` DESC
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def get_watchlist_product_ids(conn: Connection) -> set[int]:
    """WATCHLIST 활성 상품의 product_id 집합을 반환 (URL의 pcode/ProductNo/pd_no로 매칭)."""
    sql = """
        SELECT DISTINCT p.`product_id`
        FROM `stg_watchlist` w
        JOIN `stg_products` p
          ON p.`site` = w.`site`
          AND (
              (w.`site` = '다나와' AND p.`url` LIKE CONCAT('%%pcode=', w.`pcode`, '%%'))
           OR (w.`site` = '컴퓨존' AND p.`url` LIKE CONCAT('%%ProductNo=', w.`pcode`, '%%'))
           OR (w.`site` = '견적왕' AND p.`url` LIKE CONCAT('%%pd_no=', w.`pcode`, '%%'))
          )
        WHERE w.`is_active` = 1
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, ())
        return {row[0] for row in cur.fetchall()}
    finally:
        cur.close()


def get_watch_products(conn: Connection, site: str | None = None) -> pd.DataFrame:
    """크롤링 대상 제품 목록 조회 (활성/비활성 모두). site 지정 시 해당 사이트만 반환.

    QUALIFY ROW_NUMBER()... = 1 → 서브쿼리 + ROW_NUMBER 필터로 재작성(§4).
    NULLS LAST → ORDER BY (crawled_at IS NULL), crawled_at DESC로 대체.
    """
    site_filter = "AND w.`site` = %s" if site else ""
    sql = f"""
        SELECT `id`, `site`, `query`, `pcode`, `product_name`, `category`, `brand`,
               `added_at`, `is_active`, `price`, `last_crawled_at`
        FROM (
            SELECT
                w.`id`, w.`site`, w.`query`, w.`pcode`, w.`product_name`, w.`category`, w.`brand`,
                w.`added_at`, w.`is_active`,
                lp.`price`,
                lp.`crawled_at` AS last_crawled_at,
                ROW_NUMBER() OVER (
                    PARTITION BY w.`id`
                    ORDER BY (lp.`crawled_at` IS NULL), lp.`crawled_at` DESC
                ) AS rn
            FROM `stg_watchlist` w
            LEFT JOIN `stg_products` p
              ON p.`site` = w.`site`
              AND (
                  (w.`site` = '다나와' AND p.`url` LIKE CONCAT('%%pcode=', w.`pcode`, '%%'))
               OR (w.`site` = '컴퓨존' AND p.`url` LIKE CONCAT('%%ProductNo=', w.`pcode`, '%%'))
               OR (w.`site` = '견적왕' AND p.`url` LIKE CONCAT('%%pd_no=', w.`pcode`, '%%'))
              )
            LEFT JOIN `stg_latest_prices` lp ON lp.`product_id` = p.`product_id`
            WHERE 1=1 {site_filter}
        ) ranked
        WHERE rn = 1
        ORDER BY `is_active` DESC, `added_at` DESC
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (site,) if site else ())
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def add_watch_product(
    conn: Connection,
    query: str,
    pcode: str,
    product_name: str | None,
    category: str,
    brand: str | None = None,
    site: str = "다나와",
) -> None:
    """크롤링 대상 제품 추가. (site, pcode) 중복이면 is_active=1로 복원.

    stg_watchlist.pcode는 UNIQUE(사이트 무관 공용 키, §6.6) — ON DUPLICATE KEY UPDATE로
    MERGE의 WHEN MATCHED/NOT MATCHED를 재현한다.
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO `stg_watchlist`
                (`site`, `query`, `pcode`, `product_name`, `category`, `brand`)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `is_active` = 1, `query` = VALUES(`query`),
                `product_name` = VALUES(`product_name`),
                `category` = VALUES(`category`), `brand` = VALUES(`brand`)
        """, (site, query, pcode, product_name, category, brand))
    finally:
        cur.close()


def remove_watch_product(conn: Connection, watch_id: int) -> None:
    """크롤링 대상 제품 비활성화 (soft delete)."""
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE `stg_watchlist` SET `is_active` = 0 WHERE `id` = %s",
            (watch_id,)
        )
    finally:
        cur.close()


def get_category_price_summary(conn: Connection) -> pd.DataFrame:
    """카테고리별 가격 요약."""
    sql = """
        SELECT
            p.`category`,
            COUNT(*) AS product_count,
            MIN(lp.`price`) AS min_price,
            MAX(lp.`price`) AS max_price,
            ROUND(AVG(lp.`price`)) AS avg_price
        FROM `stg_latest_prices` lp
        JOIN `stg_products` p ON p.`product_id` = lp.`product_id`
        GROUP BY p.`category`
        ORDER BY p.`category`
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()
