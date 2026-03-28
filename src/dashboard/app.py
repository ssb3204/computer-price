"""Dash web application — Snowflake 연동 대시보드."""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html

from src.dashboard.callbacks import register_callbacks

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="컴퓨터 가격 모니터",
)

# ── Layout ──

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3("컴퓨터 가격 모니터", className="text-light mb-3"),
            dbc.Nav([
                dbc.NavLink("대시보드", href="/", active="exact"),
                dbc.NavLink("전체 가격표", href="/prices", active="exact"),
                dbc.NavLink("카테고리 요약", href="/categories", active="exact"),
                dbc.NavLink("상품 통계", href="/stats", active="exact"),
                dbc.NavLink("가격 추이", href="/trends", active="exact"),
                dbc.NavLink("가격 알림", href="/alerts", active="exact"),
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

register_callbacks(app)

server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
