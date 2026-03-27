"""Alert feed page."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def alerts_layout():
    return dbc.Container([
        html.H2("가격 알림", className="mb-4"),

        dbc.Row([
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="alert-btn-ALL", color="primary", outline=False, size="sm"),
                    dbc.Button("최저가", id="alert-btn-NEW_LOW", color="primary", outline=True, size="sm"),
                    dbc.Button("최고가", id="alert-btn-NEW_HIGH", color="primary", outline=True, size="sm"),
                    dbc.Button("하락", id="alert-btn-PRICE_DROP", color="primary", outline=True, size="sm"),
                    dbc.Button("급등", id="alert-btn-PRICE_SPIKE", color="primary", outline=True, size="sm"),
                ]),
                dcc.Store(id="alert-type-filter", data="ALL"),
            ], width="auto", className="me-3"),
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="alert-cat-btn-ALL", color="info", outline=False, size="sm"),
                    dbc.Button("CPU", id="alert-cat-btn-CPU", color="info", outline=True, size="sm"),
                    dbc.Button("GPU", id="alert-cat-btn-GPU", color="info", outline=True, size="sm"),
                    dbc.Button("RAM", id="alert-cat-btn-RAM", color="info", outline=True, size="sm"),
                    dbc.Button("SSD", id="alert-cat-btn-SSD", color="info", outline=True, size="sm"),
                ]),
                dcc.Store(id="alert-category-filter", data="ALL"),
            ], width="auto"),
        ], className="mb-4 align-items-center"),

        html.Div(id="alerts-table"),

        dcc.Interval(id="alerts-refresh-interval", interval=30_000, n_intervals=0),
    ])
