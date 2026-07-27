"""Integration test: 부품 조합 repository.

실제 MySQL 상대로 제약(UNIQUE/FK/CASCADE)과 공개 조회 규칙을 검증한다.
SQL 이 의도대로 동작하는지가 핵심이라 mock 이 아닌 실 DB 로 확인한다.
"""

from datetime import date, datetime

import pytest

from src.api import build_repo, watchlist_repo
from src.api.build_trend import build_daily_totals
from src.common.mysql_client import get_connection
from tests.integration.conftest import TEST_PREFIX

# ── 헬퍼 ─────────────────────────────────────────────────────────────────────


def _make_user(settings, username: str, nickname: str | None) -> int:
    """테스트용 사용자 생성. username 은 TEST_PREFIX 로 시작해야 정리된다."""
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash, nickname) VALUES (%s, %s, %s)",
                (username, "dummy-hash", nickname),
            )
            return cur.lastrowid


def _make_watch_item(settings, pcode: str, category: str = "CPU") -> int:
    """테스트용 stg_watchlist 항목 생성. query 가 TEST_PREFIX 여야 정리된다."""
    return watchlist_repo.upsert_watchlist_item(
        settings,
        site="다나와",
        query=f"{TEST_PREFIX}{pcode}",
        pcode=f"{TEST_PREFIX}{pcode}",
        product_name=f"{TEST_PREFIX}상품 {pcode}",
        category=category,
        brand="테스트브랜드",
    )


def _set_deleted(settings, user_id: int) -> None:
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET deleted_at = NOW() WHERE id = %s", (user_id,))


@pytest.fixture(scope="session")
def author(mysql_settings):
    """세션 내내 재사용하는 작성자.

    함수 스코프로 두면 테스트마다 같은 username 을 다시 INSERT 해서
    users.uq_users_username 에 걸린다.
    """
    return _make_user(mysql_settings, f"{TEST_PREFIX}author", f"{TEST_PREFIX}닉네임")


# ── 조합 생성/삭제 ───────────────────────────────────────────────────────────


@pytest.mark.integration
def test_create_build_returns_id(mysql_settings, author):
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}게이밍용")

    assert build_id is not None
    summary = build_repo.get_build(mysql_settings, build_id)
    assert summary is not None
    assert summary.name == f"{TEST_PREFIX}게이밍용"
    assert summary.author_id == author
    assert summary.item_count == 0


@pytest.mark.integration
def test_duplicate_name_for_same_user_returns_none(mysql_settings, author):
    """UNIQUE(user_id, name) — 같은 사람이 같은 이름을 두 번 쓸 수 없다."""
    name = f"{TEST_PREFIX}중복이름"
    first = build_repo.create_build(mysql_settings, author, name)
    second = build_repo.create_build(mysql_settings, author, name)

    assert first is not None
    assert second is None, "중복 이름인데 생성됐다"


@pytest.mark.integration
def test_same_name_allowed_for_different_users(mysql_settings, author):
    """이름 제약은 사용자별이다 — 다른 사람은 같은 이름을 쓸 수 있다."""
    other = _make_user(mysql_settings, f"{TEST_PREFIX}other", None)
    name = f"{TEST_PREFIX}공용이름"  # author 는 세션 공유라 다른 테스트와 이름이 겹치면 안 된다

    assert build_repo.create_build(mysql_settings, author, name) is not None
    assert build_repo.create_build(mysql_settings, other, name) is not None


@pytest.mark.integration
def test_delete_build_cascades_items(mysql_settings, author):
    """조합을 지우면 build_items 도 FK CASCADE 로 함께 지워진다."""
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}삭제대상")
    wid = _make_watch_item(mysql_settings, "cascade1")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    assert build_repo.delete_build(mysql_settings, build_id) is True

    with get_connection(mysql_settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM build_items WHERE build_id = %s", (build_id,))
            assert cur.fetchone()[0] == 0, "CASCADE 로 안 지워졌다"


# ── 소유권 ───────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_is_build_owner_distinguishes_users(mysql_settings, author):
    other = _make_user(mysql_settings, f"{TEST_PREFIX}stranger", None)
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}내조합")

    assert build_repo.is_build_owner(mysql_settings, author, build_id) is True
    assert build_repo.is_build_owner(mysql_settings, other, build_id) is False


# ── 공개 조회 ────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_other_user_can_read_build(mysql_settings, author):
    """조합은 공개 게시물 — 작성자가 아니어도 내용을 볼 수 있다."""
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}공개조합")
    wid = _make_watch_item(mysql_settings, "public1", category="GPU")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    # 조회에 user_id 를 넘기지 않는다 = 누구나 같은 결과를 본다
    summary = build_repo.get_build(mysql_settings, build_id)
    items = build_repo.get_build_items(mysql_settings, build_id)

    assert summary is not None
    assert summary.item_count == 1
    assert len(items) == 1
    assert items[0].category == "GPU"
    assert items[0].product_name == f"{TEST_PREFIX}상품 public1"


@pytest.mark.integration
def test_author_falls_back_to_username_when_no_nickname(mysql_settings):
    """nickname 이 없으면 username 으로 표시한다."""
    uid = _make_user(mysql_settings, f"{TEST_PREFIX}nonick", None)
    build_id = build_repo.create_build(mysql_settings, uid, f"{TEST_PREFIX}닉없음")

    summary = build_repo.get_build(mysql_settings, build_id)
    assert summary.author == f"{TEST_PREFIX}nonick"


@pytest.mark.integration
def test_deleted_user_build_is_hidden(mysql_settings):
    """탈퇴 회원의 조합은 공개 목록·상세에서 사라진다."""
    uid = _make_user(mysql_settings, f"{TEST_PREFIX}quitter", None)
    build_id = build_repo.create_build(mysql_settings, uid, f"{TEST_PREFIX}탈퇴자조합")
    assert build_repo.get_build(mysql_settings, build_id) is not None

    _set_deleted(mysql_settings, uid)

    assert build_repo.get_build(mysql_settings, build_id) is None
    listed = build_repo.list_public_builds(mysql_settings, limit=100)
    assert all(b.build_id != build_id for b in listed)


@pytest.mark.integration
def test_list_public_builds_is_newest_first(mysql_settings, author):
    ids = [
        build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}조합{i}")
        for i in range(3)
    ]
    listed = build_repo.list_public_builds(mysql_settings, limit=100)
    mine = [b.build_id for b in listed if b.build_id in ids]

    assert mine == sorted(ids, reverse=True), "최신순이 아니다"


# ── 부품 추가/제거 ───────────────────────────────────────────────────────────


@pytest.mark.integration
def test_add_build_item_is_idempotent(mysql_settings, author):
    """같은 부품을 두 번 담아도 한 번만 들어간다(UNIQUE)."""
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}멱등")
    wid = _make_watch_item(mysql_settings, "idem1")

    assert build_repo.add_build_item(mysql_settings, build_id, wid) is True
    assert build_repo.add_build_item(mysql_settings, build_id, wid) is False
    assert len(build_repo.get_build_items(mysql_settings, build_id)) == 1


@pytest.mark.integration
def test_remove_build_item(mysql_settings, author):
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}제거")
    wid = _make_watch_item(mysql_settings, "rm1")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    assert build_repo.remove_build_item(mysql_settings, build_id, wid) is True
    assert build_repo.remove_build_item(mysql_settings, build_id, wid) is False
    assert build_repo.get_build_items(mysql_settings, build_id) == []


# ── 크롤링 유지 (공개 조합이 조용히 죽지 않게) ──────────────────────────────


@pytest.mark.integration
def test_item_in_build_keeps_crawling_alive(mysql_settings, author):
    """조합에 담긴 상품은 워치리스트에서 빠져도 크롤링을 계속한다.

    이게 깨지면 남들이 보고 있는 공개 조합의 가격이 갱신되지 않는다.
    """
    wid = _make_watch_item(mysql_settings, "keepalive")
    watchlist_repo.link_user_watchlist(mysql_settings, author, wid)
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}유지")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    # 작성자가 워치리스트에서 뺌 → user_watchlist 참조는 0이 되지만 조합이 남아 있다
    watchlist_repo.unlink_user_watchlist(mysql_settings, author, wid)
    deactivated = watchlist_repo.deactivate_if_orphaned(mysql_settings, wid)

    assert deactivated is False, "조합에 담겨 있는데 크롤링이 꺼졌다"
    assert _is_active(mysql_settings, wid) == 1


@pytest.mark.integration
def test_item_deactivates_when_no_reference_left(mysql_settings, author):
    """워치리스트에도 조합에도 없으면 그때는 크롤링을 끈다."""
    wid = _make_watch_item(mysql_settings, "orphan")
    watchlist_repo.link_user_watchlist(mysql_settings, author, wid)
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}고아")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    watchlist_repo.unlink_user_watchlist(mysql_settings, author, wid)
    build_repo.remove_build_item(mysql_settings, build_id, wid)
    deactivated = watchlist_repo.deactivate_if_orphaned(mysql_settings, wid)

    assert deactivated is True
    assert _is_active(mysql_settings, wid) == 0


def _is_active(settings, watchlist_id: int) -> int:
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_active FROM stg_watchlist WHERE id = %s", (watchlist_id,))
            return cur.fetchone()[0]


# ── 일별 가격 조회 (SQL) ─────────────────────────────────────────────────────


def _seed_prices(settings, pcode: str, points: list[tuple[datetime, int]]) -> None:
    """이 pcode 에 대응하는 stg_products + stg_price_history 를 심는다.

    stg_watchlist ↔ stg_products 는 FK 가 없고 URL 안의 pcode 문자열로만
    연결되므로(WATCHLIST_PRODUCT_JOIN), url 을 다나와 형식으로 맞춰준다.
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg_products (site, category, product_name, url)
                VALUES ('다나와', 'CPU', %s, %s)
                ON DUPLICATE KEY UPDATE
                    product_id = LAST_INSERT_ID(product_id), url = VALUES(url)
                """,
                (f"{TEST_PREFIX}상품 {pcode}", f"https://prod.danawa.com/info/?pcode={pcode}"),
            )
            product_id = cur.lastrowid
            for crawled_at, price in points:
                cur.execute(
                    "INSERT IGNORE INTO stg_price_history (product_id, price, crawled_at) "
                    "VALUES (%s, %s, %s)",
                    (product_id, price, crawled_at),
                )


@pytest.mark.integration
def test_daily_prices_keep_only_last_crawl_of_each_day(mysql_settings, author):
    """하루 4회 크롤링되므로 그날 마지막 값만 남아야 한다."""
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}일별")
    wid = _make_watch_item(mysql_settings, "daily1")
    build_repo.add_build_item(mysql_settings, build_id, wid)
    _seed_prices(mysql_settings, f"{TEST_PREFIX}daily1", [
        (datetime(2026, 7, 1, 1, 0), 500),
        (datetime(2026, 7, 1, 6, 0), 490),
        (datetime(2026, 7, 1, 20, 0), 470),   # 그날 마지막
        (datetime(2026, 7, 2, 6, 0), 460),
    ])

    prices = build_repo.get_daily_part_prices(mysql_settings, build_id)

    assert prices[wid] == [(date(2026, 7, 1), 470), (date(2026, 7, 2), 460)]


@pytest.mark.integration
def test_daily_prices_grouped_by_part(mysql_settings, author):
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}부품별")
    wid_a = _make_watch_item(mysql_settings, "grpA")
    wid_b = _make_watch_item(mysql_settings, "grpB")
    build_repo.add_build_item(mysql_settings, build_id, wid_a)
    build_repo.add_build_item(mysql_settings, build_id, wid_b)
    _seed_prices(mysql_settings, f"{TEST_PREFIX}grpA", [(datetime(2026, 7, 1, 6, 0), 500)])
    _seed_prices(mysql_settings, f"{TEST_PREFIX}grpB", [(datetime(2026, 7, 1, 6, 0), 300)])

    prices = build_repo.get_daily_part_prices(mysql_settings, build_id)

    assert set(prices) == {wid_a, wid_b}
    assert prices[wid_a] == [(date(2026, 7, 1), 500)]
    assert prices[wid_b] == [(date(2026, 7, 1), 300)]


@pytest.mark.integration
def test_part_without_price_history_returns_empty_list(mysql_settings, author):
    """이력 없는 부품도 키는 남는다 — 집계 쪽이 '총액 불가'를 판정하는 데 쓴다."""
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}이력없음")
    wid = _make_watch_item(mysql_settings, "noprice")
    build_repo.add_build_item(mysql_settings, build_id, wid)

    prices = build_repo.get_daily_part_prices(mysql_settings, build_id)

    assert prices == {wid: []}


@pytest.mark.integration
def test_empty_build_returns_empty_dict(mysql_settings, author):
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}빈조합")

    assert build_repo.get_daily_part_prices(mysql_settings, build_id) == {}


@pytest.mark.integration
def test_repo_output_feeds_trend_aggregation(mysql_settings, author):
    """repo(SQL) → build_trend(순수 함수) 연결이 실제로 맞물리는지 확인.

    부품 두 개의 이력 시작일이 다르고 중간에 공백이 있는, 실제로 자주 나올
    형태로 총액이 나오는지 본다.
    """
    build_id = build_repo.create_build(mysql_settings, author, f"{TEST_PREFIX}연결")
    wid_cpu = _make_watch_item(mysql_settings, "e2eCPU")
    wid_gpu = _make_watch_item(mysql_settings, "e2eGPU")
    build_repo.add_build_item(mysql_settings, build_id, wid_cpu)
    build_repo.add_build_item(mysql_settings, build_id, wid_gpu)

    _seed_prices(mysql_settings, f"{TEST_PREFIX}e2eCPU", [
        (datetime(2026, 7, 1, 6, 0), 300),
        (datetime(2026, 7, 3, 6, 0), 280),   # 7/2 공백 → forward fill 대상
    ])
    _seed_prices(mysql_settings, f"{TEST_PREFIX}e2eGPU", [
        (datetime(2026, 7, 2, 6, 0), 900),   # CPU 보다 늦게 시작
        (datetime(2026, 7, 3, 6, 0), 880),
    ])

    points = build_daily_totals(build_repo.get_daily_part_prices(mysql_settings, build_id))

    # 두 부품이 모두 값을 갖는 7/2부터 시작
    assert [p.date for p in points] == [date(2026, 7, 2), date(2026, 7, 3)]
    assert [p.total for p in points] == [1200, 1160]   # 300+900, 280+880
