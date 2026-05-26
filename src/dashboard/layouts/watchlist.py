"""Watch list management page."""

import dash_bootstrap_components as dbc
from dash import dcc, html

_CATEGORY_OPTIONS = [
    {"label": "CPU", "value": "CPU"},
    {"label": "GPU", "value": "GPU"},
    {"label": "RAM", "value": "RAM"},
    {"label": "SSD", "value": "SSD"},
]


def _search_card(
    category_id: str,
    input_id: str,
    btn_id: str,
    results_id: str,
    default_category: str = "GPU",
) -> dbc.Card:
    return dbc.Card(dbc.CardBody([
        html.H5("상품 검색 및 추가", className="card-title mb-3"),
        dbc.Row([
            dbc.Col(
                dbc.Select(id=category_id, options=_CATEGORY_OPTIONS, value=default_category),
                width=2,
            ),
            dbc.Col(
                dbc.Input(id=input_id, placeholder="검색어 입력 (예: RTX 5070)", type="text"),
                width=8,
            ),
            dbc.Col(
                dbc.Button("검색", id=btn_id, color="primary", className="w-100"),
                width=2,
            ),
        ], className="g-2 mb-3"),
        dcc.Loading(type="circle", children=html.Div(id=results_id)),
    ]), color="dark", className="mb-4")


def _watch_list_card(list_id: str) -> dbc.Card:
    return dbc.Card(dbc.CardBody([
        html.H5("현재 크롤링 대상", className="card-title mb-3"),
        dcc.Loading(type="circle", children=html.Div(id=list_id)),
    ]), color="dark")


def _danawa_tab():
    return dbc.Tab(label="다나와", tab_id="tab-danawa", children=[
        html.Div(className="mt-4", children=[
            _search_card(
                category_id="watch-category-select",
                input_id="watch-search-input",
                btn_id="watch-search-btn",
                results_id="watch-search-results",
            ),
            _watch_list_card("watch-list-table"),
        ]),
    ])


def _pc_estimate_tab():
    return dbc.Tab(label="견적왕", tab_id="tab-pc-estimate", children=[
        html.Div(className="mt-4", children=[
            _search_card(
                category_id="pcest-category-select",
                input_id="pcest-search-input",
                btn_id="pcest-search-btn",
                results_id="pcest-search-results",
            ),
            _watch_list_card("pcest-list-table"),
        ]),
    ])


def _compuzone_tab():
    return dbc.Tab(label="컴퓨존", tab_id="tab-compuzone", children=[
        html.Div(className="mt-4", children=[
            _search_card(
                category_id="compuzone-category-select",
                input_id="compuzone-search-input",
                btn_id="compuzone-search-btn",
                results_id="compuzone-search-results",
            ),
            _watch_list_card("compuzone-list-table"),
        ]),
    ])


def watchlist_page():
    return dbc.Container([
        html.H2("크롤링 대상 관리", className="mb-4"),

        dbc.Tabs(id="watchlist-site-tabs", active_tab="tab-danawa", children=[
            _danawa_tab(),
            _pc_estimate_tab(),
            _compuzone_tab(),
        ]),

        # ── 삭제 확인 모달 (다나와/견적왕 공유) ──
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
        dcc.Store(id="pcest-search-store", data=[]),
        dcc.Store(id="pcest-refresh-trigger", data=0),
        dcc.Store(id="compuzone-search-store", data=[]),
        dcc.Store(id="compuzone-refresh-trigger", data=0),
    ])
