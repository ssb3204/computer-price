"""Product detail page with price trend charts."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def product_detail_layout():
    return dbc.Container([
        html.H2("제품 상세", className="mb-4"),

        dbc.Row([
            dbc.Col([
                html.Label("카테고리"),
                dcc.Dropdown(
                    id="category-selector",
                    options=[
                        {"label": "CPU", "value": "CPU"},
                        {"label": "GPU", "value": "GPU"},
                        {"label": "RAM", "value": "RAM"},
                        {"label": "SSD", "value": "SSD"},
                        {"label": "메인보드", "value": "Mainboard"},
                    ],
                    value="CPU",
                    className="mb-2",
                ),
            ], width=3),
            dbc.Col([
                html.Label("제품 선택"),
                dcc.Dropdown(id="product-selector", className="mb-2"),
            ], width=6),
            dbc.Col([
                html.Label("기간"),
                dbc.ButtonGroup([
                    dbc.Button("7일", id="btn-7d", color="primary", outline=True, size="sm"),
                    dbc.Button("30일", id="btn-30d", color="primary", outline=True, size="sm"),
                    dbc.Button("90일", id="btn-90d", color="primary", outline=True, size="sm"),
                ]),
            ], width=3),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                dcc.Graph(id="price-trend-chart"),
            ]),
        ], className="mb-4"),

        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardBody([
                    html.H6("구매 시점 판단"),
                    html.Div(id="purchase-timing-indicator"),
                    html.Small(
                        "90일 최저가 대비 5% 이내 = 구매 적기 (초록), 90일 최고가 대비 5% 이내 = 대기 권장 (빨강)",
                        className="text-muted",
                    ),
                ])
            ]), width=12),
        ]),
    ])
