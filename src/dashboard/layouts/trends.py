"""가격 추이 페이지 레이아웃."""

import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html


def trends_page():
    return html.Div([
        html.H2("가격 추이", className="mb-4"),

        # ── Filters ──
        dbc.Row([
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="cat-btn-ALL", color="primary", outline=False, size="sm"),
                    dbc.Button("CPU",  id="cat-btn-CPU", color="primary", outline=True,  size="sm"),
                    dbc.Button("GPU",  id="cat-btn-GPU", color="primary", outline=True,  size="sm"),
                    dbc.Button("RAM",  id="cat-btn-RAM", color="primary", outline=True,  size="sm"),
                    dbc.Button("SSD",  id="cat-btn-SSD", color="primary", outline=True,  size="sm"),
                ]),
                dcc.Store(id="trend-category-filter", data="ALL"),
            ], width="auto", className="me-3"),
            dbc.Col([
                dbc.Input(
                    id="trend-search-input",
                    type="text",
                    placeholder="상품 검색 (예: 9070 XT, 7800X3D) — 또는 아래 표에서 상품명 클릭",
                    debounce=True,
                    size="sm",
                ),
            ], width=4, className="me-3"),
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="period-btn-0",  color="info", outline=False, size="sm"),
                    dbc.Button("7일",  id="period-btn-7",  color="info", outline=True,  size="sm"),
                    dbc.Button("14일", id="period-btn-14", color="info", outline=True,  size="sm"),
                    dbc.Button("30일", id="period-btn-30", color="info", outline=True,  size="sm"),
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
                html.H5("오늘 크롤링 비교", className="mb-1"),
                html.Small("상품명 셀 클릭 → 위 차트에 가격 추이 표시",
                           className="text-muted d-block mb-3"),
                dcc.Loading(type="circle", children=dash_table.DataTable(
                    id="today-comparison-datatable",
                    columns=[],
                    data=[],
                    page_size=50,
                    sort_action="native",
                    filter_action="native",
                    style_table={"overflowX": "auto"},
                    style_header={
                        "backgroundColor": "#2c2c2c",
                        "color": "#ffffff",
                        "fontWeight": "bold",
                        "border": "1px solid #444",
                    },
                    style_cell={
                        "backgroundColor": "#1e1e1e",
                        "color": "#cccccc",
                        "border": "1px solid #444",
                        "padding": "6px 10px",
                        "textAlign": "left",
                        "fontSize": "0.88rem",
                        "maxWidth": "320px",
                        "overflow": "hidden",
                        "textOverflow": "ellipsis",
                    },
                    style_data_conditional=[
                        {
                            "if": {"column_id": "product_name"},
                            "color": "#58a6ff",
                            "cursor": "pointer",
                            "textDecoration": "underline",
                        },
                        {
                            "if": {"filter_query": '{price_change_2} = "up"', "column_id": "2차"},
                            "color": "#ff6b6b",
                        },
                        {
                            "if": {"filter_query": '{price_change_2} = "down"', "column_id": "2차"},
                            "color": "#51cf66",
                        },
                        {
                            "if": {"filter_query": '{price_change_3} = "up"', "column_id": "3차"},
                            "color": "#ff6b6b",
                        },
                        {
                            "if": {"filter_query": '{price_change_3} = "down"', "column_id": "3차"},
                            "color": "#51cf66",
                        },
                        {
                            "if": {"filter_query": '{price_change_4} = "up"', "column_id": "4차"},
                            "color": "#ff6b6b",
                        },
                        {
                            "if": {"filter_query": '{price_change_4} = "down"', "column_id": "4차"},
                            "color": "#51cf66",
                        },
                    ],
                    tooltip_delay=0,
                    tooltip_duration=None,
                )),
            ], width=12),
        ]),
    ])
