"""Dash web application — Snowflake 연동 대시보드."""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.dashboard.data_access.snowflake_queries import (
    get_category_price_summary,
    get_latest_prices_all,
    get_product_stats,
    get_summary_stats,
)

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="컴퓨터 가격 모니터",
)

sf_settings = SnowflakeSettings()

STOCK_DISPLAY = {
    "in_stock": "판매중",
    "out_of_stock": "품절",
    "unknown": "-",
}

SITE_DISPLAY = ["다나와", "컴퓨존", "견적왕"]


def _get_conn():
    return get_connection(sf_settings)


def _make_price_table(df, max_rows=None):
    """DataFrame → dbc.Table with clickable product names."""
    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    rows_to_show = df.head(max_rows) if max_rows else df

    header = html.Thead(html.Tr([
        html.Th("카테고리"), html.Th("사이트"), html.Th("상품명"),
        html.Th("가격"), html.Th("재고"),
    ]))

    body_rows = []
    for _, row in rows_to_show.iterrows():
        name = str(row["product_name"])[:80]
        url = row.get("url", "")
        if url:
            name_cell = html.A(name, href=url, target="_blank", className="text-info")
        else:
            name_cell = name

        price = f"{int(row['price']):,}원"
        stock = STOCK_DISPLAY.get(row.get("stock_status", "unknown"), "-")

        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(name_cell),
            html.Td(price),
            html.Td(stock),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")


def _make_stats_table(df):
    """상품 통계 DataFrame → dbc.Table with clickable names."""
    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    header = html.Thead(html.Tr([
        html.Th("카테고리"), html.Th("사이트"), html.Th("상품명"),
        html.Th("평균가"), html.Th("최저가"), html.Th("최고가"), html.Th("수집횟수"),
    ]))

    body_rows = []
    for _, row in df.iterrows():
        name = str(row["product_name"])[:60]
        url = row.get("url", "")
        if url:
            name_cell = html.A(name, href=url, target="_blank", className="text-info")
        else:
            name_cell = name

        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(name_cell),
            html.Td(f"{int(float(row['overall_avg'])):,}원"),
            html.Td(f"{int(row['all_time_low']):,}원"),
            html.Td(f"{int(row['all_time_high']):,}원"),
            html.Td(str(row["total_records"])),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")


# ── Layout ──

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H3("컴퓨터 가격 모니터", className="text-light mb-3"),
            dbc.Nav([
                dbc.NavLink("대시보드", href="/", active="exact"),
                dbc.NavLink("전체 가격표", href="/prices", active="exact"),
                dbc.NavLink("카테고리 요약", href="/categories", active="exact"),
                dbc.NavLink("상품 통계", href="/stats", active="exact"),
            ], vertical=True, pills=True),
        ], width=2, className="bg-dark vh-100 pt-3 position-fixed",
           style={"overflowY": "auto"}),

        dbc.Col([
            dcc.Location(id="url", refresh=False),
            html.Div(id="page-content"),
        ], width=10, className="pt-3", style={"marginLeft": "16.67%"}),
    ]),

    dcc.Interval(id="refresh-interval", interval=300_000, n_intervals=0),
], fluid=True)


# ── Page routing ──

def overview_page():
    return html.Div([
        html.H2("대시보드 개요", className="mb-4"),
        dbc.Row(id="summary-cards", className="mb-4"),
        dbc.Row([
            dbc.Col([
                html.H5("카테고리별 가격 분포", className="mb-3"),
                html.Div(id="category-summary-table"),
            ], width=12),
        ], className="mb-4"),
        dbc.Row([
            dbc.Col([
                html.H5("최신 가격 (최저가 순)", className="mb-3"),
                html.Div(id="latest-prices-table"),
            ], width=12),
        ]),
    ])


def prices_page():
    return html.Div([
        html.H2("전체 가격표", className="mb-4"),
        dbc.Row([
            dbc.Col([
                html.Label("카테고리 필터"),
                dcc.Dropdown(
                    id="price-category-filter",
                    options=[{"label": "전체", "value": "ALL"}] + [
                        {"label": c, "value": c} for c in ["CPU", "GPU", "RAM", "SSD"]
                    ],
                    value="ALL",
                ),
            ], width=3),
            dbc.Col([
                html.Label("사이트 필터"),
                dcc.Dropdown(
                    id="price-site-filter",
                    options=[{"label": "전체", "value": "ALL"}] + [
                        {"label": s, "value": s} for s in SITE_DISPLAY
                    ],
                    value="ALL",
                ),
            ], width=3),
        ], className="mb-4"),
        html.Div(id="full-prices-table"),
    ])


def categories_page():
    return html.Div([
        html.H2("카테고리 요약", className="mb-4"),
        html.Div(id="category-detail-table"),
    ])


def stats_page():
    return html.Div([
        html.H2("상품 통계", className="mb-4"),
        html.Div(id="product-stats-table"),
    ])


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/prices":
        return prices_page()
    if pathname == "/categories":
        return categories_page()
    if pathname == "/stats":
        return stats_page()
    return overview_page()


# ── Callbacks: Overview ──

@app.callback(
    [Output("summary-cards", "children"),
     Output("category-summary-table", "children"),
     Output("latest-prices-table", "children")],
    Input("refresh-interval", "n_intervals"),
)
def update_overview(_):
    with _get_conn() as conn:
        stats = get_summary_stats(conn)
        cat_df = get_category_price_summary(conn)
        prices_df = get_latest_prices_all(conn)

    cards = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("추적 상품", className="card-subtitle text-muted"),
            html.H3(f"{stats['total_products']}개"),
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("카테고리", className="card-subtitle text-muted"),
            html.H3(f"{stats['total_categories']}개"),
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("사이트", className="card-subtitle text-muted"),
            html.H3(f"{stats['total_sites']}개"),
        ])), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("오늘 수집", className="card-subtitle text-muted"),
            html.H3(f"{stats['today_records']}건"),
        ])), width=3),
    ])

    # Category summary
    if cat_df.empty:
        cat_table = html.P("데이터 없음", className="text-muted")
    else:
        cat_df.columns = ["카테고리", "상품수", "최저가", "최고가", "평균가"]
        for col in ["최저가", "최고가", "평균가"]:
            cat_df[col] = cat_df[col].apply(lambda x: f"{int(x):,}원" if x else "-")
        cat_table = dbc.Table.from_dataframe(
            cat_df, bordered=True, hover=True, striped=True, color="dark"
        )

    price_table = _make_price_table(prices_df, max_rows=20)

    return cards, cat_table, price_table


# ── Callbacks: Full prices ──

@app.callback(
    Output("full-prices-table", "children"),
    [Input("price-category-filter", "value"),
     Input("price-site-filter", "value")],
)
def update_prices_table(category, site):
    with _get_conn() as conn:
        df = get_latest_prices_all(conn)

    if category and category != "ALL":
        df = df[df["category"] == category]
    if site and site != "ALL":
        df = df[df["site"] == site]

    return _make_price_table(df)


# ── Callbacks: Categories ──

@app.callback(
    Output("category-detail-table", "children"),
    Input("refresh-interval", "n_intervals"),
)
def update_category_detail(_):
    with _get_conn() as conn:
        df = get_category_price_summary(conn)

    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    df.columns = ["카테고리", "상품수", "최저가", "최고가", "평균가"]
    for col in ["최저가", "최고가", "평균가"]:
        df[col] = df[col].apply(lambda x: f"{int(x):,}원" if x else "-")

    return dbc.Table.from_dataframe(
        df, bordered=True, hover=True, striped=True, color="dark"
    )


# ── Callbacks: Stats ──

@app.callback(
    Output("product-stats-table", "children"),
    Input("refresh-interval", "n_intervals"),
)
def update_stats(_):
    with _get_conn() as conn:
        df = get_product_stats(conn)

    return _make_stats_table(df)


server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
