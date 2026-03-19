"""Main dashboard overview page."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def overview_layout():
    return dbc.Container([
        html.H2("대시보드 개요", className="mb-4"),

        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H6("추적 제품 수", className="card-subtitle text-muted"),
                    html.H3(id="total-products", children="--"),
                ])
            ]), width=3),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H6("오늘 가격 변동", className="card-subtitle text-muted"),
                    html.H3(id="changes-today", children="--"),
                ])
            ]), width=3),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H6("활성 알림", className="card-subtitle text-muted"),
                    html.H3(id="active-alerts", children="--"),
                ])
            ]), width=3),
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H6("최저가 갱신", className="card-subtitle text-muted"),
                    html.H3(id="new-lows", children="--"),
                ])
            ]), width=3),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                html.H5("주간 최대 하락 TOP 10"),
                dbc.Table(id="top-drops-table", bordered=True, dark=True, hover=True, striped=True),
            ], width=8),
            dbc.Col([
                html.H5("카테고리 분포"),
                dcc.Graph(id="category-pie"),
            ], width=4),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                html.H5("최근 알림"),
                html.Div(id="recent-alerts-list"),
            ]),
        ]),

        dcc.Interval(id="refresh-interval", interval=60_000, n_intervals=0),
    ])
