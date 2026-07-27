"""Integration test: 부품 조합 API.

권한 경계가 이 기능의 핵심이라 거기에 집중한다.
  - 읽기는 누구나 (조합은 공개 게시물)
  - 쓰기는 작성자만
  - 부품은 내 워치리스트 것만

FastAPI TestClient + 실제 MySQL 로 엔드투엔드 확인한다.
"""

import pytest
from fastapi.testclient import TestClient

from src.api import watchlist_repo
from src.api.main import app
from src.common.mysql_client import get_connection
from tests.integration.conftest import TEST_PREFIX

client = TestClient(app)


# ── 픽스처 ───────────────────────────────────────────────────────────────────


def _make_user(settings, username: str) -> int:
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                (username, "dummy-hash"),
            )
            return cur.lastrowid


@pytest.fixture(scope="session")
def owner(mysql_settings):
    return _make_user(mysql_settings, f"{TEST_PREFIX}rt_owner")


@pytest.fixture(scope="session")
def stranger(mysql_settings):
    return _make_user(mysql_settings, f"{TEST_PREFIX}rt_stranger")


@pytest.fixture
def owned_watch_item(mysql_settings, owner):
    """owner 의 워치리스트에 담긴 상품."""
    wid = watchlist_repo.upsert_watchlist_item(
        mysql_settings, site="다나와", query=f"{TEST_PREFIX}rt",
        pcode=f"{TEST_PREFIX}rt", product_name=f"{TEST_PREFIX}상품",
        category="CPU", brand=None,
    )
    watchlist_repo.link_user_watchlist(mysql_settings, owner, wid)
    return wid


def _create_build(owner_id: int, name: str) -> int:
    res = client.post(f"/users/{owner_id}/builds", json={"name": name})
    assert res.status_code == 201, res.text
    return res.json()["build_id"]


# ── 생성 ─────────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_create_build(owner):
    res = client.post(f"/users/{owner}/builds", json={"name": f"{TEST_PREFIX}생성"})

    assert res.status_code == 201
    body = res.json()
    assert body["name"] == f"{TEST_PREFIX}생성"
    assert body["author_id"] == owner
    assert body["item_count"] == 0


@pytest.mark.integration
def test_duplicate_name_returns_409(owner):
    name = f"{TEST_PREFIX}중복409"
    client.post(f"/users/{owner}/builds", json={"name": name})

    res = client.post(f"/users/{owner}/builds", json={"name": name})

    assert res.status_code == 409


@pytest.mark.integration
def test_create_for_unknown_user_returns_404():
    res = client.post("/users/99999999/builds", json={"name": f"{TEST_PREFIX}없는유저"})

    assert res.status_code == 404


@pytest.mark.integration
def test_blank_name_is_rejected(owner):
    res = client.post(f"/users/{owner}/builds", json={"name": ""})

    assert res.status_code == 422


# ── 공개 읽기 ────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_anyone_can_read_build_detail(owner, owned_watch_item):
    """작성자가 아니어도 조합 상세를 볼 수 있다 (인증 헤더 없이 호출)."""
    build_id = _create_build(owner, f"{TEST_PREFIX}공개상세")
    client.post(
        f"/users/{owner}/builds/{build_id}/items",
        json={"watchlist_id": owned_watch_item},
    )

    res = client.get(f"/api/builds/{build_id}")

    assert res.status_code == 200
    body = res.json()
    assert body["build"]["build_id"] == build_id
    assert len(body["items"]) == 1
    assert body["items"][0]["watchlist_id"] == owned_watch_item


@pytest.mark.integration
def test_public_list_includes_build(owner):
    build_id = _create_build(owner, f"{TEST_PREFIX}목록노출")

    res = client.get("/api/builds", params={"limit": 100})

    assert res.status_code == 200
    assert any(b["build_id"] == build_id for b in res.json())


@pytest.mark.integration
def test_unknown_build_returns_404():
    assert client.get("/api/builds/99999999").status_code == 404


# ── 쓰기 권한 ────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_stranger_cannot_delete_build(owner, stranger):
    """남의 조합은 삭제할 수 없고, 실제로 지워지지도 않아야 한다."""
    build_id = _create_build(owner, f"{TEST_PREFIX}삭제방어")

    res = client.delete(f"/users/{stranger}/builds/{build_id}")

    assert res.status_code == 404
    assert client.get(f"/api/builds/{build_id}").status_code == 200, "남이 지워버렸다"


@pytest.mark.integration
def test_stranger_cannot_rename_build(owner, stranger):
    build_id = _create_build(owner, f"{TEST_PREFIX}이름방어")

    res = client.patch(
        f"/users/{stranger}/builds/{build_id}", json={"name": f"{TEST_PREFIX}탈취"}
    )

    assert res.status_code == 404
    assert client.get(f"/api/builds/{build_id}").json()["build"]["name"] == f"{TEST_PREFIX}이름방어"


@pytest.mark.integration
def test_stranger_cannot_add_item(owner, stranger, owned_watch_item):
    build_id = _create_build(owner, f"{TEST_PREFIX}부품방어")

    res = client.post(
        f"/users/{stranger}/builds/{build_id}/items",
        json={"watchlist_id": owned_watch_item},
    )

    assert res.status_code == 404
    assert client.get(f"/api/builds/{build_id}").json()["items"] == []


@pytest.mark.integration
def test_owner_can_rename_own_build(owner):
    build_id = _create_build(owner, f"{TEST_PREFIX}원래이름")

    res = client.patch(
        f"/users/{owner}/builds/{build_id}", json={"name": f"{TEST_PREFIX}바뀐이름"}
    )

    assert res.status_code == 200
    assert res.json()["name"] == f"{TEST_PREFIX}바뀐이름"


@pytest.mark.integration
def test_owner_can_delete_own_build(owner):
    build_id = _create_build(owner, f"{TEST_PREFIX}내가삭제")

    assert client.delete(f"/users/{owner}/builds/{build_id}").status_code == 204
    assert client.get(f"/api/builds/{build_id}").status_code == 404


# ── 부품 출처 제한 ───────────────────────────────────────────────────────────


@pytest.mark.integration
def test_cannot_add_item_not_in_my_watchlist(owner, stranger, mysql_settings):
    """남의 워치리스트에만 있는 상품은 내 조합에 못 넣는다."""
    others_wid = watchlist_repo.upsert_watchlist_item(
        mysql_settings, site="다나와", query=f"{TEST_PREFIX}others",
        pcode=f"{TEST_PREFIX}others", product_name=f"{TEST_PREFIX}남의상품",
        category="GPU", brand=None,
    )
    watchlist_repo.link_user_watchlist(mysql_settings, stranger, others_wid)
    build_id = _create_build(owner, f"{TEST_PREFIX}출처제한")

    res = client.post(
        f"/users/{owner}/builds/{build_id}/items", json={"watchlist_id": others_wid}
    )

    assert res.status_code == 404
    assert client.get(f"/api/builds/{build_id}").json()["items"] == []


@pytest.mark.integration
def test_add_and_remove_item(owner, owned_watch_item):
    build_id = _create_build(owner, f"{TEST_PREFIX}추가제거")

    add = client.post(
        f"/users/{owner}/builds/{build_id}/items",
        json={"watchlist_id": owned_watch_item},
    )
    assert add.status_code == 201
    assert len(add.json()) == 1

    rm = client.delete(f"/users/{owner}/builds/{build_id}/items/{owned_watch_item}")
    assert rm.status_code == 204
    assert client.get(f"/api/builds/{build_id}").json()["items"] == []


@pytest.mark.integration
def test_remove_item_not_in_build_returns_404(owner, owned_watch_item):
    build_id = _create_build(owner, f"{TEST_PREFIX}없는부품제거")

    res = client.delete(f"/users/{owner}/builds/{build_id}/items/{owned_watch_item}")

    assert res.status_code == 404


# ── 추이 ─────────────────────────────────────────────────────────────────────


@pytest.mark.integration
def test_trend_reports_reason_when_no_items(owner):
    """부품이 없으면 빈 추이 + 이유를 알려준다(500 이 아니라)."""
    build_id = _create_build(owner, f"{TEST_PREFIX}추이없음")

    res = client.get(f"/api/builds/{build_id}/price-trend")

    assert res.status_code == 200
    body = res.json()
    assert body["points"] == []
    assert "부품이 없습니다" in body["unavailable_reason"]


@pytest.mark.integration
def test_trend_reports_reason_when_price_missing(owner, owned_watch_item):
    """가격 이력이 아직 없는 부품이 있으면 그 사실을 알려준다."""
    build_id = _create_build(owner, f"{TEST_PREFIX}가격없음")
    client.post(
        f"/users/{owner}/builds/{build_id}/items",
        json={"watchlist_id": owned_watch_item},
    )

    res = client.get(f"/api/builds/{build_id}/price-trend")

    assert res.status_code == 200
    assert res.json()["points"] == []
    assert "수집되지 않은" in res.json()["unavailable_reason"]


@pytest.mark.integration
def test_trend_is_public(owner):
    """추이도 공개 — 작성자가 아니어도 조회된다."""
    build_id = _create_build(owner, f"{TEST_PREFIX}추이공개")

    assert client.get(f"/api/builds/{build_id}/price-trend").status_code == 200


@pytest.mark.integration
def test_trend_days_param_is_validated(owner):
    build_id = _create_build(owner, f"{TEST_PREFIX}days검증")

    assert client.get(f"/api/builds/{build_id}/price-trend", params={"days": 0}).status_code == 422
