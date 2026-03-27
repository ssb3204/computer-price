"""Dash web application — Snowflake 연동 대시보드."""

from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html
from dash.dependencies import Input, Output

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.dashboard.data_access.snowflake_queries import (
    get_alerts,
    get_category_price_summary,
    get_latest_prices_all,
    get_price_trend,
    get_product_stats,
    get_summary_stats,
    get_today_crawl_comparison,
)
from src.dashboard.layouts.alerts import alerts_layout

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
    title="컴퓨터 가격 모니터",
)

sf_settings = SnowflakeSettings()

SITE_DISPLAY = ["다나와", "컴퓨존", "견적왕"]


def _get_conn():
    return get_connection(sf_settings)


def _make_price_table(df, max_rows=None):
    """DataFrame → dbc.Table with clickable product names."""
    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    rows_to_show = df.head(max_rows) if max_rows else df

    header = html.Thead(html.Tr([
        html.Th("카테고리"), html.Th("사이트"), html.Th("상품명"), html.Th("가격"),
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

        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(name_cell),
            html.Td(price),
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
                dbc.NavLink("가격 추이", href="/trends", active="exact"),
                dbc.NavLink("가격 알림", href="/alerts", active="exact"),
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
                dbc.ButtonGroup([
                    dbc.Button("전체", id="price-cat-btn-ALL", color="primary", outline=False, size="sm"),
                    dbc.Button("CPU", id="price-cat-btn-CPU", color="primary", outline=True, size="sm"),
                    dbc.Button("GPU", id="price-cat-btn-GPU", color="primary", outline=True, size="sm"),
                    dbc.Button("RAM", id="price-cat-btn-RAM", color="primary", outline=True, size="sm"),
                    dbc.Button("SSD", id="price-cat-btn-SSD", color="primary", outline=True, size="sm"),
                ]),
                dcc.Store(id="price-category-filter", data="ALL"),
            ], width="auto", className="me-3"),
            dbc.Col([
                dbc.ButtonGroup([
                    dbc.Button("전체", id="price-site-btn-ALL", color="info", outline=False, size="sm"),
                    dbc.Button("다나와", id="price-site-btn-다나와", color="info", outline=True, size="sm"),
                    dbc.Button("컴퓨존", id="price-site-btn-컴퓨존", color="info", outline=True, size="sm"),
                    dbc.Button("견적왕", id="price-site-btn-견적왕", color="info", outline=True, size="sm"),
                ]),
                dcc.Store(id="price-site-filter", data="ALL"),
            ], width="auto"),
        ], className="mb-4 align-items-center"),
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
            dbc.Col(dcc.Graph(id="trend-chart"), width=12),
        ], className="mb-4"),

        # ── Weekly Summary Cards ──
        dbc.Row(id="trend-summary", className="mb-4"),

        # ── Today Crawl Comparison ──
        dbc.Row([
            dbc.Col([
                html.H5("오늘 크롤링 비교 (1차 vs 2차)", className="mb-3"),
                html.Div(id="today-comparison-table"),
            ], width=12),
        ]),
    ])


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/prices":
        return prices_page()
    if pathname == "/categories":
        return categories_page()
    if pathname == "/stats":
        return stats_page()
    if pathname == "/trends":
        return trends_page()
    if pathname == "/alerts":
        return alerts_layout()
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
    [Input("price-category-filter", "data"),
     Input("price-site-filter", "data")],
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


# ── Callbacks: Trends ──

SITE_COLORS = {"다나와": "#3498db", "컴퓨존": "#e67e22", "견적왕": "#2ecc71"}


def _empty_chart(message: str) -> go.Figure:
    """빈 차트에 안내 메시지 표시."""
    fig = go.Figure()
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[{
            "text": message,
            "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5,
            "showarrow": False,
            "font": {"size": 16, "color": "#aaa"},
        }],
    )
    return fig


@app.callback(
    [Output("trend-chart", "figure"),
     Output("trend-summary", "children")],
    [Input("trend-category-filter", "data"),
     Input("trend-search-input", "value"),
     Input("trend-period", "data")],
)
def update_trend_chart(category, search, days):
    if not search:
        return _empty_chart("검색어를 입력하면 사이트별 가격 추이를 볼 수 있습니다"), []

    with _get_conn() as conn:
        df = get_price_trend(conn, category=category, search=search, days=days if days else None)

    if df.empty:
        return _empty_chart(f'"{search}" 검색 결과 없음'), []

    # ── Line Chart ──
    fig = px.line(
        df, x="crawled_at", y="price", color="site",
        color_discrete_map=SITE_COLORS,
        markers=True,
        labels={"crawled_at": "시간", "price": "가격 (원)", "site": "사이트"},
        title=f'"{search}" 사이트별 최저가 추이',
    )
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend={"orientation": "h", "y": -0.15},
    )
    fig.update_yaxes(tickformat=",", title="가격 (원)")
    fig.update_xaxes(title="")

    # ── Summary Cards ──
    week_low = int(df["price"].min())
    week_high = int(df["price"].max())
    low_site = df.loc[df["price"].idxmin(), "site"]
    high_site = df.loc[df["price"].idxmax(), "site"]
    num_sites = df["site"].nunique()

    cards = [
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("최저가", className="card-subtitle text-muted"),
            html.H3(f"{week_low:,}원", className="text-success"),
            html.Small(low_site, className="text-muted"),
        ]), color="dark"), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("최고가", className="card-subtitle text-muted"),
            html.H3(f"{week_high:,}원", className="text-danger"),
            html.Small(high_site, className="text-muted"),
        ]), color="dark"), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("가격차", className="card-subtitle text-muted"),
            html.H3(f"{week_high - week_low:,}원"),
            html.Small(f"{num_sites}개 사이트 비교", className="text-muted"),
        ]), color="dark"), width=3),
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H6("데이터 포인트", className="card-subtitle text-muted"),
            html.H3(f"{len(df)}건"),
            html.Small("전체 기간" if not days else f"최근 {days}일", className="text-muted"),
        ]), color="dark"), width=3),
    ]

    return fig, cards


@app.callback(
    Output("today-comparison-table", "children"),
    [Input("trend-category-filter", "data"),
     Input("trend-search-input", "value")],
)
def update_today_comparison(category, search):
    with _get_conn() as conn:
        df = get_today_crawl_comparison(conn, category=category, search=search)

    if df.empty:
        return html.P("오늘 크롤링 데이터 없음", className="text-muted")

    header = html.Thead(html.Tr([
        html.Th("사이트"), html.Th("카테고리"), html.Th("상품명"),
        html.Th("1차 가격"), html.Th("2차 가격"),
        html.Th("변동"), html.Th("상태"),
    ]))

    body_rows = []
    for _, row in df.iterrows():
        price_1st = f"{int(row['price_1st']):,}원"
        price_2nd = f"{int(row['price_2nd']):,}원" if row["price_2nd"] else "-"
        diff = int(row["price_diff"])
        diff_text = f"{diff:+,}원" if diff != 0 else "0원"
        status = str(row["change_status"])

        # 상태별 색상
        if "상승" in status:
            status_class = "text-danger"
        elif "하락" in status:
            status_class = "text-success"
        else:
            status_class = "text-muted"

        body_rows.append(html.Tr([
            html.Td(row["site"]),
            html.Td(row["category"]),
            html.Td(str(row["product_name"])[:60]),
            html.Td(price_1st),
            html.Td(price_2nd),
            html.Td(diff_text, className=status_class),
            html.Td(status, className=status_class),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")


# ── Callbacks: Alerts ──

ALERT_TYPE_DISPLAY = {
    "NEW_LOW": "🔵 최저가 갱신",
    "NEW_HIGH": "🔴 최고가 갱신",
    "PRICE_DROP": "🟢 가격 하락",
    "PRICE_SPIKE": "🔴 가격 급등",
}

ALERT_TYPE_CLASS = {
    "NEW_LOW": "text-info",
    "NEW_HIGH": "text-danger",
    "PRICE_DROP": "text-success",
    "PRICE_SPIKE": "text-danger",
}


@app.callback(
    Output("alerts-table", "children"),
    [Input("alert-type-filter", "data"),
     Input("alert-category-filter", "data"),
     Input("alerts-refresh-interval", "n_intervals")],
)
def update_alerts_table(alert_type, category, _):
    with _get_conn() as conn:
        df = get_alerts(conn, alert_type=alert_type, category=category)

    if df.empty:
        return html.P("알림 없음", className="text-muted")

    cards = []
    for _, row in df.iterrows():
        alert_type_raw = str(row["alert_type"])
        type_display = ALERT_TYPE_DISPLAY.get(alert_type_raw, alert_type_raw)
        type_class = ALERT_TYPE_CLASS.get(alert_type_raw, "")

        name = str(row["product_name"])[:60]
        url = row.get("url", "")
        name_cell = html.A(name, href=url, target="_blank", className="text-info") if url else name

        old_price = f"{int(row['old_price']):,}원" if row["old_price"] else "-"
        new_price = f"{int(row['new_price']):,}원"
        change_pct = f"{float(row['change_pct']):+.1f}%" if row["change_pct"] is not None else "-"

        created = str(row["created_at"])[:16]

        cards.append(dbc.Card(dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Span(type_display, className=f"fw-bold {type_class}"),
                    html.Span(f"  {row['category']} · {row['site']}", className="text-muted ms-2"),
                ], width=8),
                dbc.Col(
                    html.Small(created, className="text-muted"),
                    width=4, className="text-end",
                ),
            ], className="mb-2"),
            html.Div(name_cell, className="mb-2"),
            dbc.Row([
                dbc.Col(html.Span(f"{old_price} → {new_price}", className="text-light"), width="auto"),
                dbc.Col(html.Span(change_pct, className=f"fw-bold {type_class}"), width="auto"),
            ]),
        ]), color="dark", className="mb-2"))

    return cards



# ── Button Toggle Helper ──

def _register_button_toggle(store_id, btn_prefix, items, default=None):
    """버튼 그룹 토글 콜백을 등록하는 헬퍼."""
    if default is None:
        default = items[0]

    @app.callback(
        [Output(store_id, "data")] +
        [Output(f"{btn_prefix}{v}", "outline") for v in items],
        [Input(f"{btn_prefix}{v}", "n_clicks") for v in items],
        prevent_initial_call=True,
    )
    def _toggle(*n_clicks):
        ctx = dash.callback_context
        if not ctx.triggered:
            return [default] + [v != default for v in items]
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]
        selected_str = button_id.replace(btn_prefix, "")
        selected = type(default)(selected_str) if not isinstance(default, str) else selected_str
        return [selected] + [v != selected for v in items]

    return _toggle


CATEGORIES = ["ALL", "CPU", "GPU", "RAM", "SSD"]

_register_button_toggle("trend-category-filter", "cat-btn-", CATEGORIES, "ALL")
_register_button_toggle("trend-period", "period-btn-", [0, 7, 14, 30], 0)
_register_button_toggle("price-category-filter", "price-cat-btn-", CATEGORIES, "ALL")
_register_button_toggle("price-site-filter", "price-site-btn-", ["ALL", "다나와", "컴퓨존", "견적왕"], "ALL")
_register_button_toggle("alert-type-filter", "alert-btn-", ["ALL", "NEW_LOW", "NEW_HIGH", "PRICE_DROP", "PRICE_SPIKE"], "ALL")
_register_button_toggle("alert-category-filter", "alert-cat-btn-", CATEGORIES, "ALL")


server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=True)
