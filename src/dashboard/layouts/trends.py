"""가격 추이 페이지 레이아웃."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def trends_page():
    return html.Div([
        html.H2("가격 추이", className="mb-4"),

        # ── Filters ──
        dbc.Row([
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="cat-btn-ALL", color="primary", outline=False, size="sm"),
                    dbc.Button("CPU", id="cat-btn-CPU", color="primary", outline=True, size="sm"),
                    dbc.Button("GPU", id="cat-btn-GPU", color="primary", outline=True, size="sm"),
                    dbc.Button("RAM", id="cat-btn-RAM", color="primary", outline=True, size="sm"),
                    dbc.Button("SSD", id="cat-btn-SSD", color="primary", outline=True, size="sm"),
                ]),
                dcc.Store(id="trend-category-filter", data="ALL"),
            ], width="auto", className="me-3"),
            dbc.Col([
                dbc.Input(
                    id="trend-search-input",
                    type="text",
                    placeholder="상품 검색 (예: 9070 XT, 7800X3D)",
                    debounce=True,
                    size="sm",
                ),
            ], width=3, className="me-3"),
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="period-btn-0", color="info", outline=False, size="sm"),
                    dbc.Button("7일", id="period-btn-7", color="info", outline=True, size="sm"),
                    dbc.Button("14일", id="period-btn-14", color="info", outline=True, size="sm"),
                    dbc.Button("30일", id="period-btn-30", color="info", outline=True, size="sm"),
                ]),
                dcc.Store(id="trend-period", data=0),
            ], width="auto"),
        ], className="mb-4 align-items-center"),

        # ── Line Chart ──
        dbc.Row([
            dbc.Col(
                dcc.Loading(type="circle", children=dcc.Graph(id="trend-chart")),
                width=12,
            ),
        ], className="mb-4"),

        # ── Summary Cards ──
        dcc.Loading(type="circle", children=dbc.Row(id="trend-summary", className="mb-4")),

        # ── Today Crawl Comparison ──
        dbc.Row([
            dbc.Col([
                html.H5("오늘 크롤링 비교", className="mb-3"),
                dcc.Loading(type="circle", children=html.Div(id="today-comparison-table")),
            ], width=12),
        ]),
    ])
