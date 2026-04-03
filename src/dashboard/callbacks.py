"""대시보드 콜백 등록."""

import logging

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html
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
from src.dashboard.helpers import (
    ALERT_TYPE_CLASS,
    ALERT_TYPE_DISPLAY,
    CATEGORIES,
    SITE_COLORS,
    db_error_ui,
    empty_chart,
    make_price_table,
    make_stats_table,
)
from src.dashboard.layouts.alerts import alerts_layout
from src.dashboard.layouts.categories import categories_page
from src.dashboard.layouts.overview import overview_page
from src.dashboard.layouts.prices import prices_page
from src.dashboard.layouts.stats import stats_page
from src.dashboard.layouts.trends import trends_page

logger = logging.getLogger(__name__)

_sf_settings = None


def _get_conn():
    global _sf_settings
    if _sf_settings is None:
        _sf_settings = SnowflakeSettings()
    return get_connection(_sf_settings)


# ── Button Toggle Helper ──

def _register_button_toggle(app, store_id, btn_prefix, items, default=None):
    """버튼 그룹 토글 콜백을 등록하는 헬퍼."""
    if default is None:
        default = items[0]

    @app.callback(
        [Output(store_id, "data")]
        + [Output(f"{btn_prefix}{v}", "outline") for v in items],
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


def register_callbacks(app):
    """app 에 모든 콜백을 등록한다."""

    # ── Page routing ──

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

    # ── Overview ──

    @app.callback(
        [Output("summary-cards", "children"),
         Output("category-summary-table", "children"),
         Output("latest-prices-table", "children")],
        Input("refresh-interval", "n_intervals"),
    )
    def update_overview(_):
        try:
            with _get_conn() as conn:
                stats = get_summary_stats(conn)
                cat_df = get_category_price_summary(conn)
                prices_df = get_latest_prices_all(conn)
        except Exception as e:
            logger.exception("Overview 데이터 로드 실패")
            err = db_error_ui()
            return err, err, err

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

        if cat_df.empty:
            cat_table = html.P("데이터 없음", className="text-muted")
        else:
            cat_df.columns = ["카테고리", "상품수", "최저가", "최고가", "평균가"]
            for col in ["최저가", "최고가", "평균가"]:
                cat_df[col] = cat_df[col].apply(lambda x: f"{int(x):,}원" if x else "-")
            cat_table = dbc.Table.from_dataframe(
                cat_df, bordered=True, hover=True, striped=True, color="dark"
            )

        price_table = make_price_table(prices_df, max_rows=20)
        return cards, cat_table, price_table

    # ── Full prices ──

    @app.callback(
        Output("full-prices-table", "children"),
        [Input("price-category-filter", "data"),
         Input("price-site-filter", "data")],
    )
    def update_prices_table(category, site):
        try:
            with _get_conn() as conn:
                df = get_latest_prices_all(conn)
        except Exception as e:
            logger.exception("가격표 데이터 로드 실패")
            return db_error_ui()

        if category and category != "ALL":
            df = df[df["category"] == category]
        if site and site != "ALL":
            df = df[df["site"] == site]

        return make_price_table(df)

    # ── Categories ──

    @app.callback(
        Output("category-detail-table", "children"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_category_detail(_):
        try:
            with _get_conn() as conn:
                df = get_category_price_summary(conn)
        except Exception as e:
            logger.exception("카테고리 데이터 로드 실패")
            return db_error_ui()

        if df.empty:
            return html.P("데이터 없음", className="text-muted")

        df.columns = ["카테고리", "상품수", "최저가", "최고가", "평균가"]
        for col in ["최저가", "최고가", "평균가"]:
            df[col] = df[col].apply(lambda x: f"{int(x):,}원" if x else "-")

        return dbc.Table.from_dataframe(
            df, bordered=True, hover=True, striped=True, color="dark"
        )

    # ── Stats ──

    @app.callback(
        Output("product-stats-table", "children"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_stats(_):
        try:
            with _get_conn() as conn:
                df = get_product_stats(conn)
        except Exception as e:
            logger.exception("상품 통계 데이터 로드 실패")
            return db_error_ui()
        return make_stats_table(df)

    # ── Trends ──

    @app.callback(
        [Output("trend-chart", "figure"),
         Output("trend-summary", "children")],
        [Input("trend-category-filter", "data"),
         Input("trend-search-input", "value"),
         Input("trend-period", "data")],
    )
    def update_trend_chart(category, search, days):
        if not search:
            return empty_chart("검색어를 입력하면 사이트별 가격 추이를 볼 수 있습니다"), []

        try:
            with _get_conn() as conn:
                df = get_price_trend(conn, category=category, search=search, days=days if days else None)
        except Exception as e:
            logger.exception("가격 추이 데이터 로드 실패")
            return empty_chart("데이터베이스 연결 실패"), []

        if df.empty:
            return empty_chart(f'"{search}" 검색 결과 없음'), []

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
            hoverlabel={
                "bgcolor": "rgba(30,30,30,0.95)",
                "font_color": "#ffffff",
                "bordercolor": "#555555",
            },
        )
        fig.update_traces(hovertemplate="%{y:,.0f}원<extra>%{fullData.name}</extra>")
        fig.update_yaxes(tickformat=",", title="가격 (원)")
        fig.update_xaxes(title="")

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
        try:
            with _get_conn() as conn:
                df = get_today_crawl_comparison(conn, category=category, search=search)
        except Exception as e:
            logger.exception("오늘 크롤링 비교 데이터 로드 실패")
            return db_error_ui()

        if df.empty:
            return html.P("오늘 크롤링 데이터 없음", className="text-muted")

        header = html.Thead(html.Tr([
            html.Th("사이트"), html.Th("카테고리"), html.Th("상품명"),
            html.Th("1차"), html.Th("2차"), html.Th("3차"), html.Th("4차"),
        ]))

        def _fmt(val):
            return f"{int(val):,}원" if val is not None and str(val) not in ("", "None", "nan") else "-"

        def _cell(val, prev_val):
            text = _fmt(val)
            if val is None or str(val) in ("", "None", "nan"):
                return html.Td(text, className="text-muted")
            if prev_val is not None and str(prev_val) not in ("", "None", "nan"):
                if float(val) > float(prev_val):
                    return html.Td(text, className="text-danger")
                if float(val) < float(prev_val):
                    return html.Td(text, className="text-success")
            return html.Td(text)

        body_rows = []
        for _, row in df.iterrows():
            p1 = row["price_1st"]
            p2 = row.get("price_2nd")
            p3 = row.get("price_3rd")
            p4 = row.get("price_4th")
            body_rows.append(html.Tr([
                html.Td(row["site"]),
                html.Td(row["category"]),
                html.Td(str(row["product_name"])[:60]),
                html.Td(_fmt(p1)),
                _cell(p2, p1),
                _cell(p3, p2),
                _cell(p4, p3),
            ]))

        body = html.Tbody(body_rows)
        return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")

    # ── Alerts ──

    @app.callback(
        Output("alerts-table", "children"),
        [Input("alert-type-filter", "data"),
         Input("alert-category-filter", "data"),
         Input("alerts-refresh-interval", "n_intervals")],
    )
    def update_alerts_table(alert_type, category, _):
        try:
            with _get_conn() as conn:
                df = get_alerts(conn, alert_type=alert_type, category=category)
        except Exception as e:
            logger.exception("알림 데이터 로드 실패")
            return db_error_ui()

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

    # ── Button Toggles ──

    _register_button_toggle(app, "trend-category-filter", "cat-btn-", CATEGORIES, "ALL")
    _register_button_toggle(app, "trend-period", "period-btn-", [0, 7, 14, 30], 0)
    _register_button_toggle(app, "price-category-filter", "price-cat-btn-", CATEGORIES, "ALL")
    _register_button_toggle(app, "price-site-filter", "price-site-btn-", ["ALL", "다나와", "컴퓨존", "견적왕"], "ALL")
    _register_button_toggle(app, "alert-type-filter", "alert-btn-", ["ALL", "NEW_LOW", "NEW_HIGH", "PRICE_DROP", "PRICE_SPIKE"], "ALL")
    _register_button_toggle(app, "alert-category-filter", "alert-cat-btn-", CATEGORIES, "ALL")
