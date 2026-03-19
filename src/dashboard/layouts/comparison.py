"""Cross-site price comparison page."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def comparison_layout():
    return dbc.Container([
        html.H2("사이트 간 가격 비교", className="mb-4"),

        dbc.Row([
            dbc.Col([
                html.Label("제품 선택"),
                dcc.Dropdown(id="comparison-product-selector", className="mb-2"),
            ], width=8),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                dcc.Graph(id="comparison-bar-chart"),
            ], width=7),
            dbc.Col([
                html.H5("가격 상세"),
                dbc.Table(id="comparison-detail-table", bordered=True, dark=True, hover=True),
                html.Div(id="best-price-badge"),
            ], width=5),
        ]),
    ])
