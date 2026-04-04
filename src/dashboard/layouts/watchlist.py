"""Watch list management page."""

import dash_bootstrap_components as dbc
from dash import dcc, html

_CATEGORY_OPTIONS = [
    {"label": "CPU", "value": "CPU"},
    {"label": "GPU", "value": "GPU"},
    {"label": "RAM", "value": "RAM"},
    {"label": "SSD", "value": "SSD"},
]


def watchlist_page():
    return dbc.Container([
        html.H2("크롤링 대상 관리", className="mb-4"),

        # ── 상품 검색 섹션 ──
        dbc.Card(dbc.CardBody([
            html.H5("상품 검색 및 추가", className="card-title mb-3"),
            dbc.Row([
                dbc.Col(
                    dbc.Select(
                        id="watch-category-select",
                        options=_CATEGORY_OPTIONS,
                        value="GPU",
                    ),
                    width=2,
                ),
                dbc.Col(
                    dbc.Input(
                        id="watch-search-input",
                        placeholder="검색어 입력 (예: RTX 5070)",
                        type="text",
                    ),
                    width=8,
                ),
                dbc.Col(
                    dbc.Button("검색", id="watch-search-btn", color="primary", className="w-100"),
                    width=2,
                ),
            ], className="g-2 mb-3"),
            dcc.Loading(type="circle", children=html.Div(id="watch-search-results")),
        ]), color="dark", className="mb-4"),

        # ── 현재 크롤링 대상 ──
        dbc.Card(dbc.CardBody([
            html.H5("현재 크롤링 대상", className="card-title mb-3"),
            dcc.Loading(type="circle", children=html.Div(id="watch-list-table")),
        ]), color="dark"),

        # ── 삭제 확인 모달 ──
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle("삭제 확인")),
            dbc.ModalBody("이 항목을 크롤링 대상에서 삭제하시겠습니까?"),
            dbc.ModalFooter([
                dbc.Button("삭제", id="watch-del-confirm-btn", color="danger", className="me-2"),
                dbc.Button("취소", id="watch-del-cancel-btn", color="secondary"),
            ]),
        ], id="watch-del-confirm-modal", is_open=False),

        # ── Hidden stores ──
        dcc.Store(id="watch-search-store", data=[]),
        dcc.Store(id="watch-refresh-trigger", data=0),
        dcc.Store(id="watch-pending-del-id", data=None),
    ])
