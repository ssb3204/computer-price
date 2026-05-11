"""대시보드 에러 핸들링 유닛 테스트."""

import os
from unittest.mock import patch

import dash
import dash_bootstrap_components as dbc
import pytest

from src.dashboard.helpers import db_error_ui


# ── db_error_ui 헬퍼 테스트 ───────────────────────────────────────────────────


def test_db_error_ui_returns_alert():
    result = db_error_ui()
    assert isinstance(result, dbc.Alert)


def test_db_error_ui_default_message():
    result = db_error_ui()
    assert result.color == "danger"
    assert "데이터베이스 연결 실패" in result.children


def test_db_error_ui_custom_message():
    result = db_error_ui("Snowflake timeout")
    assert result.color == "danger"
    assert "Snowflake timeout" in result.children


def test_db_error_ui_has_danger_color():
    result = db_error_ui("오류 발생")
    assert result.color == "danger"


# ── 콜백 에러 처리 통합 테스트 ────────────────────────────────────────────────

_DUMMY_SF_ENV = {
    "SNOWFLAKE_ACCOUNT": "dummy",
    "SNOWFLAKE_USER": "dummy",
    "SNOWFLAKE_PASSWORD": "dummy",
}


def _get_inner_callback(app, target_output_id: str):
    """callback_map에서 output id를 포함하는 __wrapped__ 함수 반환."""
    for key, cb in app.callback_map.items():
        if target_output_id in key:
            return cb["callback"].__wrapped__
    return None


@pytest.fixture(scope="module")
def dash_app():
    """Snowflake 더미 env로 콜백 등록한 Dash 앱 (모듈 공유)."""
    from flask_caching import Cache

    with patch.dict(os.environ, _DUMMY_SF_ENV):
        from src.dashboard.callbacks import register_callbacks

        app = dash.Dash(__name__, suppress_callback_exceptions=True)
        app.layout = dash.html.Div(id="dummy")
        # NullCache: 테스트에서는 캐시를 비활성화해 매 호출마다 실제 함수 실행
        cache = Cache(app.server, config={"CACHE_TYPE": "NullCache"})
        register_callbacks(app, cache)
        yield app


def test_update_stats_returns_error_ui_on_db_failure(dash_app):
    """update_stats 콜백: DB 실패 시 dbc.Alert 반환."""
    cb = _get_inner_callback(dash_app, "product-stats-table")
    if cb is None:
        pytest.skip("product-stats-table 콜백을 찾지 못함")

    with patch("src.dashboard.callbacks._get_conn", side_effect=Exception("conn refused")):
        result = cb(0)

    assert isinstance(result, dbc.Alert)
    assert result.color == "danger"


def test_update_prices_table_returns_error_ui_on_db_failure(dash_app):
    """update_prices_table 콜백: DB 실패 시 dbc.Alert 반환."""
    cb = _get_inner_callback(dash_app, "full-prices-table")
    if cb is None:
        pytest.skip("full-prices-table 콜백을 찾지 못함")

    with patch("src.dashboard.callbacks._get_conn", side_effect=Exception("timeout")):
        result = cb("ALL", "ALL")

    assert isinstance(result, dbc.Alert)
    assert result.color == "danger"


def test_update_alerts_returns_error_ui_on_db_failure(dash_app):
    """update_alerts_table 콜백: DB 실패 시 dbc.Alert 반환."""
    cb = _get_inner_callback(dash_app, "alerts-table")
    if cb is None:
        pytest.skip("alerts-table 콜백을 찾지 못함")

    with patch("src.dashboard.callbacks._get_conn", side_effect=Exception("auth failed")):
        result = cb("ALL", "ALL", 0)

    assert isinstance(result, dbc.Alert)
    assert result.color == "danger"


def test_update_category_detail_returns_error_ui_on_db_failure(dash_app):
    """update_category_detail 콜백: DB 실패 시 dbc.Alert 반환."""
    cb = _get_inner_callback(dash_app, "category-detail-table")
    if cb is None:
        pytest.skip("category-detail-table 콜백을 찾지 못함")

    with patch("src.dashboard.callbacks._get_conn", side_effect=Exception("network error")):
        result = cb(0)

    assert isinstance(result, dbc.Alert)
    assert result.color == "danger"
