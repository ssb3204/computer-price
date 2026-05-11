"""Dash web application — Snowflake 연동 대시보드."""

import logging
import os
import threading
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from flask_caching import Cache

from src.dashboard.callbacks import register_callbacks

logger = logging.getLogger(__name__)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="컴퓨터 가격 모니터",
)

# SimpleCache: 메모리 기반, 외부 서버 불필요. TTL=1800초(30분)
cache = Cache(app.server, config={
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 1800,
})

# ── Layout ──

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3("컴퓨터 가격 모니터", className="text-light mb-3"),
            dbc.Nav([
                dbc.NavLink("대시보드", href="/", active="exact"),
                dbc.NavLink("가격 정보", href="/prices", active="exact"),
                dbc.NavLink("가격 추이", href="/trends", active="exact"),
                dbc.NavLink("가격 알림", href="/alerts", active="exact"),
                dbc.NavLink("크롤링 대상", href="/watchlist", active="exact"),
                dbc.NavLink("파이프라인 ↗", href="https://github.com/ssb3204/computer-price/actions", target="_blank"),
            ], vertical=True, pills=True),
        ], width=2, className="bg-dark vh-100 pt-3 position-fixed",
           style={"overflowY": "auto"}),

        dbc.Col([
            dcc.Location(id="url", refresh=False),
            html.Div(id="page-content"),
        ], width=10, className="pt-3", style={"marginLeft": "16.67%"}),
    ]),

    dcc.Interval(id="refresh-interval", interval=300_000, n_intervals=0),
], fluid=True)

# ── Callbacks ──

fetchers = register_callbacks(app, cache)


def _warm_cache(fetchers: dict) -> None:
    """앱 시작 시 백그라운드에서 모든 캐시를 미리 채운다."""
    logger.info("[Cache Warming] 시작")
    for name, fn in fetchers.items():
        try:
            fn()
            logger.info("[Cache Warming] %s 완료", name)
        except Exception as exc:
            logger.warning("[Cache Warming] %s 실패: %s", name, exc)
    logger.info("[Cache Warming] 전체 완료")


threading.Thread(
    target=_warm_cache,
    args=(fetchers,),
    daemon=True,
    name="cache-warmer",
).start()

server = app.server

if __name__ == "__main__":
    debug_mode = os.environ.get("DASH_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=8050, debug=debug_mode)
