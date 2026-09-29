"""Integration test: transform_staging() 증분 창과 전체 점검 모드.

일반 실행은 최근 INCREMENTAL_WINDOW_DAYS 일 이내 수집분만 변환한다. 창 밖으로 밀려난
미처리 행은 영영 처리되지 않으므로, 창 없이 도는 전체 점검(full_scan=True)이 회수한다.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from run_pipeline import load_raw, transform_staging
from src.common.models import RawCrawledPrice
from src.pipeline.transform import INCREMENTAL_WINDOW_DAYS
from tests.integration.conftest import RECENT_T1, TEST_PREFIX
from tests.integration.test_load_raw import _make_raw

# 창 밖 시각. 창 기본값(7일)에 상대적으로 잡으면 상수를 늘려도 테스트 데이터가 같이
# 밀려나 통과해 버리므로, 절대값 10일로 고정해 기본값까지 함께 고정한다.
OUTSIDE_WINDOW_DAYS = 10
assert INCREMENTAL_WINDOW_DAYS < OUTSIDE_WINDOW_DAYS, (
    f"증분 창({INCREMENTAL_WINDOW_DAYS}일)이 테스트 데이터({OUTSIDE_WINDOW_DAYS}일 전)를 "
    "덮는다 — 창을 늘렸다면 이 테스트의 시각도 창 밖으로 옮겨야 한다"
)
OUTSIDE_WINDOW = datetime.now(UTC).replace(microsecond=0) - timedelta(days=OUTSIDE_WINDOW_DAYS)


def _make_raw_at(name: str, crawled_at: datetime) -> RawCrawledPrice:
    return replace(_make_raw(name), crawled_at=crawled_at)


def _raw_id(conn, name: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT `id` FROM `raw_crawled_prices` WHERE `product_name` = %s", (name,))
    row = cur.fetchone()
    cur.close()
    assert row is not None, f"raw 적재 실패: {name}"
    return row[0]


def _is_staged(conn, raw_id: int) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM `stg_price_history` WHERE `raw_id` = %s", (raw_id,))
    count = cur.fetchone()[0]
    cur.close()
    return count > 0


def _failure_count(conn, name: str) -> int:
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM `raw_transform_failures` WHERE `product_name` = %s", (name,)
    )
    count = cur.fetchone()[0]
    cur.close()
    return count


@pytest.mark.integration
def test_row_outside_window_skipped_in_normal_run(mysql_settings, mysql_conn):
    """창 밖(10일 전) 미처리 raw 행은 일반 실행에서 변환되지 않는다."""
    name = f"{TEST_PREFIX}WINDOW_OLD"
    load_raw(mysql_settings, [_make_raw_at(name, OUTSIDE_WINDOW)])
    raw_id = _raw_id(mysql_conn, name)

    transform_staging(mysql_settings)

    assert not _is_staged(mysql_conn, raw_id)
    # 실패로도 기록되면 안 된다 — 기록되는 순간 NOT EXISTS 에 걸려
    # 전체 점검에서도 영영 회수할 수 없게 된다.
    assert _failure_count(mysql_conn, name) == 0


@pytest.mark.integration
def test_row_outside_window_processed_in_full_scan(mysql_settings, mysql_conn):
    """같은 행이 전체 점검 모드에서는 변환된다."""
    name = f"{TEST_PREFIX}WINDOW_OLD_FULL"
    load_raw(mysql_settings, [_make_raw_at(name, OUTSIDE_WINDOW)])
    raw_id = _raw_id(mysql_conn, name)

    transform_staging(mysql_settings)
    assert not _is_staged(mysql_conn, raw_id), "일반 실행이 창 밖 행을 건드렸다"

    transform_staging(mysql_settings, full_scan=True)

    assert _is_staged(mysql_conn, raw_id), "전체 점검이 창 밖 행을 회수하지 못했다"


@pytest.mark.integration
def test_row_inside_window_processed_in_normal_run(mysql_settings, mysql_conn):
    """창 안(2일 전) 미처리 raw 행은 일반 실행에서 변환된다."""
    name = f"{TEST_PREFIX}WINDOW_RECENT"
    load_raw(mysql_settings, [_make_raw_at(name, RECENT_T1)])
    raw_id = _raw_id(mysql_conn, name)

    transform_staging(mysql_settings)

    assert _is_staged(mysql_conn, raw_id)
