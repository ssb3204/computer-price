"""Dash web application entrypoint."""

import dash
import dash_bootstrap_components as dbc
from dash import html

from src.dashboard.layouts.alerts import alerts_layout
from src.dashboard.layouts.comparison import comparison_layout
from src.dashboard.layouts.overview import overview_layout
from src.dashboard.layouts.product_detail import product_detail_layout

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="컴퓨터 가격 모니터",
)

sidebar = dbc.Nav(
    [
        dbc.NavLink("대시보드", href="/", active="exact"),
        dbc.NavLink("제품 상세", href="/product", active="exact"),
        dbc.NavLink("사이트 비교", href="/comparison", active="exact"),
        dbc.NavLink("알림", href="/alerts", active="exact"),
    ],
    vertical=True,
    pills=True,
    className="bg-dark p-3",
)

app.layout = dbc.Container(
    [
        dbc.Row([
            dbc.Col(
                [html.H4("컴퓨터 가격 모니터", className="text-light mb-3"), sidebar],
                width=2,
                className="bg-dark vh-100 pt-3",
            ),
            dbc.Col(
                dash.page_container if hasattr(dash, "page_container") else html.Div(id="page-content"),
                width=10,
                className="pt-3",
            ),
        ]),
    ],
    fluid=True,
)


@app.callback(
    dash.Output("page-content", "children"),
    dash.Input("url", "pathname"),
)
def display_page(pathname: str):
    if pathname == "/product":
        return product_detail_layout()
    if pathname == "/comparison":
        return comparison_layout()
    if pathname == "/alerts":
        return alerts_layout()
    return overview_layout()


server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
