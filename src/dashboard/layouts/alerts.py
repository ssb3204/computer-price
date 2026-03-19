"""Alert feed page."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def alerts_layout():
    return dbc.Container([
        html.H2("가격 알림", className="mb-4"),

        dbc.Row([
            dbc.Col([
                html.Label("알림 유형"),
                dcc.Dropdown(
                    id="alert-type-filter",
                    options=[
                        {"label": "전체", "value": "ALL"},
                        {"label": "최저가 갱신", "value": "NEW_LOW"},
                        {"label": "최고가 갱신", "value": "NEW_HIGH"},
                        {"label": "가격 하락", "value": "PRICE_DROP"},
                        {"label": "가격 급등", "value": "PRICE_SPIKE"},
                    ],
                    value="ALL",
                ),
            ], width=3),
            dbc.Col([
                html.Label("카테고리"),
                dcc.Dropdown(id="alert-category-filter"),
            ], width=3),
            dbc.Col([
                html.Label("기간"),
                dcc.DatePickerRange(id="alert-date-range"),
            ], width=4),
        ], className="mb-4"),

        dbc.Table(id="alerts-table", bordered=True, dark=True, hover=True, striped=True),

        dcc.Interval(id="alerts-refresh-interval", interval=30_000, n_intervals=0),
    ])
