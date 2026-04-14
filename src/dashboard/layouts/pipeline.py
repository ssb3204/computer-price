"""파이프라인 모니터링 페이지 레이아웃."""

import dash_bootstrap_components as dbc
from dash import dash_table, dcc, html


def pipeline_page():
    return html.Div([
        html.H2("파이프라인 모니터링", className="mb-4"),

        # ── 요약 카드 ──
        dcc.Loading(type="circle", children=dbc.Row(id="pipeline-summary-cards", className="mb-4")),

        # ── 실행시간 추이 + 스텝별 소요시간 ──
        dbc.Row([
            dbc.Col([
                html.H5("실행시간 추이 (최근 20회)", className="mb-2"),
                dcc.Loading(type="circle", children=dcc.Graph(id="pipeline-duration-chart")),
            ], width=6),
            dbc.Col([
                html.H5("스텝별 소요시간 누적 (최근 20회)", className="mb-2"),
                dcc.Loading(type="circle", children=dcc.Graph(id="pipeline-step-bar-chart")),
            ], width=6),
        ], className="mb-4"),

        # ── 실행 이력 테이블 ──
        dbc.Row([
            dbc.Col([
                html.H5("실행 이력", className="mb-1"),
                html.Small("행 클릭 → 아래 스텝 상세 표시", className="text-muted d-block mb-2"),
                dcc.Loading(type="circle", children=dash_table.DataTable(
                    id="pipeline-runs-table",
                    columns=[],
                    data=[],
                    page_size=10,
                    sort_action="native",
                    row_selectable="single",
                    selected_rows=[],
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
                    },
                    style_data_conditional=[
                        {"if": {"filter_query": '{status} = "SUCCESS"', "column_id": "status"},
                         "color": "#51cf66", "fontWeight": "bold"},
                        {"if": {"filter_query": '{status} = "PARTIAL"', "column_id": "status"},
                         "color": "#fcc419", "fontWeight": "bold"},
                        {"if": {"filter_query": '{status} = "FAILED"',  "column_id": "status"},
                         "color": "#ff6b6b", "fontWeight": "bold"},
                        {"if": {"state": "selected"},
                         "backgroundColor": "#2a3a4a", "border": "1px solid #58a6ff"},
                    ],
                )),
            ], width=12),
        ], className="mb-4"),

        # ── 스텝 상세 ──
        dbc.Row([
            dbc.Col([
                html.H5(id="pipeline-step-title", children="스텝 상세 (실행을 선택하세요)", className="mb-2"),
                dbc.Row([
                    dbc.Col(
                        dcc.Loading(type="circle", children=dcc.Graph(id="pipeline-funnel-chart")),
                        width=6,
                    ),
                    dbc.Col(
                        dcc.Loading(type="circle", children=dash_table.DataTable(
                            id="pipeline-step-table",
                            columns=[],
                            data=[],
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
                            },
                            style_data_conditional=[
                                {"if": {"filter_query": '{status} = "SUCCESS"', "column_id": "status"},
                                 "color": "#51cf66", "fontWeight": "bold"},
                                {"if": {"filter_query": '{status} = "PARTIAL"', "column_id": "status"},
                                 "color": "#fcc419", "fontWeight": "bold"},
                                {"if": {"filter_query": '{status} = "FAILED"',  "column_id": "status"},
                                 "color": "#ff6b6b", "fontWeight": "bold"},
                            ],
                        )),
                        width=6,
                    ),
                ]),
            ], width=12),
        ]),

        dcc.Store(id="pipeline-runs-store"),
    ])
