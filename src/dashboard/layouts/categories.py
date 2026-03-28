"""카테고리 요약 페이지 레이아웃."""

from dash import html


def categories_page():
    return html.Div([
        html.H2("카테고리 요약", className="mb-4"),
        html.Div(id="category-detail-table"),
    ])
