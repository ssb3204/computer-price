"""Integration test fixtures — 실제 로컬 MySQL 접속 필요."""

import pytest
from dotenv import load_dotenv

from src.common.config import MySQLSettings
from src.common.mysql_client import get_connection

load_dotenv()

TEST_PREFIX = "IT_TEST_"


def _cleanup(settings: MySQLSettings) -> None:
    """IT_TEST_ 로 시작하는 테스트 데이터 전부 삭제 (FK 순서 준수)."""
    with get_connection(settings) as conn:
        cur = conn.cursor()

        cur.execute(
            "SELECT `product_id` FROM `stg_products` WHERE `product_name` LIKE %s",
            (TEST_PREFIX + "%",),
        )
        test_pids = [row[0] for row in cur.fetchall()]

        if test_pids:
            placeholders = ", ".join(["%s"] * len(test_pids))
            # Analytics
            cur.execute(
                f"DELETE FROM `ans_daily_price_stats` WHERE `product_id` IN ({placeholders})",
                test_pids,
            )
            # Staging children
            cur.execute(
                f"DELETE FROM `stg_price_alerts` WHERE `product_id` IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                f"DELETE FROM `stg_price_history` WHERE `product_id` IN ({placeholders})",
                test_pids,
            )
            cur.execute(
                "DELETE FROM `stg_products` WHERE `product_name` LIKE %s",
                (TEST_PREFIX + "%",),
            )

        cur.execute(
            "DELETE FROM `raw_crawled_prices` WHERE `product_name` LIKE %s",
            (TEST_PREFIX + "%",),
        )
        cur.execute(
            "DELETE FROM `raw_transform_failures` WHERE `product_name` LIKE %s",
            (TEST_PREFIX + "%",),
        )

        # 조합/워치리스트/사용자 (FK 역순: 자식 → 부모)
        # build_items 는 builds 삭제 시 ON DELETE CASCADE 로 지워지지만,
        # stg_watchlist 만 참조하는 행이 남을 수 있어 먼저 명시적으로 지운다.
        like = TEST_PREFIX + "%"
        cur.execute(
            """
            DELETE FROM `build_items`
            WHERE `build_id` IN (
                    SELECT `id` FROM `builds` WHERE `user_id` IN (
                        SELECT `id` FROM `users` WHERE `username` LIKE %s))
               OR `watchlist_id` IN (
                    SELECT `id` FROM `stg_watchlist` WHERE `query` LIKE %s)
            """,
            (like, like),
        )
        cur.execute(
            "DELETE FROM `builds` WHERE `user_id` IN "
            "(SELECT `id` FROM `users` WHERE `username` LIKE %s)",
            (like,),
        )
        cur.execute(
            """
            DELETE FROM `user_watchlist`
            WHERE `user_id` IN (SELECT `id` FROM `users` WHERE `username` LIKE %s)
               OR `watchlist_id` IN (SELECT `id` FROM `stg_watchlist` WHERE `query` LIKE %s)
            """,
            (like, like),
        )
        cur.execute("DELETE FROM `stg_watchlist` WHERE `query` LIKE %s", (like,))
        cur.execute("DELETE FROM `users` WHERE `username` LIKE %s", (like,))
        cur.close()


@pytest.fixture(scope="session")
def mysql_settings():
    return MySQLSettings()


@pytest.fixture(scope="session")
def mysql_conn(mysql_settings):
    with get_connection(mysql_settings) as conn:
        yield conn


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_data(mysql_settings):
    _cleanup(mysql_settings)  # 세션 시작 전 1회
    yield
    _cleanup(mysql_settings)  # 세션 종료 후 1회
