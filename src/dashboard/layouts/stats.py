"""상품 통계 페이지 레이아웃."""

from dash import dcc, html


def stats_page():
    return html.Div([
        html.H2("상품 통계", className="mb-4"),
        dcc.Loading(type="circle", children=html.Div(id="product-stats-table")),
    ])
