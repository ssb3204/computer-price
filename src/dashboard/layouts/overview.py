"""대시보드 개요 페이지 레이아웃."""

import dash_bootstrap_components as dbc
from dash import html


def overview_page():
    return html.Div([
        html.H2("대시보드 개요", className="mb-4"),
        dbc.Row(id="summary-cards", className="mb-4"),
        dbc.Row([
            dbc.Col([
                html.H5("카테고리별 가격 분포", className="mb-3"),
                html.Div(id="category-summary-table"),
            ], width=12),
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([
                html.H5("최신 가격 (최저가 순)", className="mb-3"),
                html.Div(id="latest-prices-table"),
            ], width=12),
        ]),
    ])
