"""전체 가격표 + 상품 통계 통합 페이지."""

import dash_bootstrap_components as dbc
from dash import dcc, html


def prices_page():
    return html.Div([
        html.H2("가격 정보", className="mb-4"),
        dbc.Tabs([
            dbc.Tab(label="전체 가격표", tab_id="tab-prices", children=[
                html.Div(className="mt-3", children=[
                    dbc.Row([
                        dbc.Col([
                            dbc.ButtonGroup([
                                dbc.Button("전체",  id="price-cat-btn-ALL", color="primary", outline=False, size="sm"),
                                dbc.Button("CPU",   id="price-cat-btn-CPU", color="primary", outline=True,  size="sm"),
                                dbc.Button("GPU",   id="price-cat-btn-GPU", color="primary", outline=True,  size="sm"),
                                dbc.Button("RAM",   id="price-cat-btn-RAM", color="primary", outline=True,  size="sm"),
                                dbc.Button("SSD",   id="price-cat-btn-SSD", color="primary", outline=True,  size="sm"),
                            ]),
                            dcc.Store(id="price-category-filter", data="ALL"),
                        ], width="auto", className="me-3"),
                        dbc.Col([
                            dbc.ButtonGroup([
                                dbc.Button("전체",   id="price-site-btn-ALL",   color="info", outline=False, size="sm"),
                                dbc.Button("다나와", id="price-site-btn-다나와", color="info", outline=True,  size="sm"),
                                dbc.Button("컴퓨존", id="price-site-btn-컴퓨존", color="info", outline=True,  size="sm"),
                                dbc.Button("견적왕", id="price-site-btn-견적왕", color="info", outline=True,  size="sm"),
                            ]),
                            dcc.Store(id="price-site-filter", data="ALL"),
                        ], width="auto"),
                    ], className="mb-3 align-items-center"),
                    dcc.Loading(type="circle", children=html.Div(id="full-prices-table")),
                ]),
            ]),
            dbc.Tab(label="상품 통계", tab_id="tab-stats", children=[
                html.Div(className="mt-3", children=[
                    dcc.Loading(type="circle", children=html.Div(id="product-stats-table")),
                ]),
            ]),
        ], id="prices-tabs", active_tab="tab-prices"),
    ])
