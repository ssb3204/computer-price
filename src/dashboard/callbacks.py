"""대시보드 콜백 등록."""

import json
import logging
import threading

import dash
import pandas as pd
import dash_bootstrap_components as dbc
import plotly.express as px
from dash import html
from dash.dependencies import ALL, Input, Output, State

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection
from src.crawlers.danawa import search_products
from src.dashboard.data_access.snowflake_queries import (
    add_watch_product,
    get_alerts,
    get_category_price_summary,
    get_latest_prices_all,
    get_price_trend,
    get_product_stats,
    get_summary_stats,
    get_today_crawl_comparison,
    get_watch_products,
    remove_watch_product,
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
    send_slack_watch_change,
)
from src.dashboard.layouts.alerts import alerts_layout
from src.dashboard.layouts.overview import overview_page
from src.dashboard.layouts.prices import prices_page
from src.dashboard.layouts.trends import trends_page
from src.dashboard.layouts.watchlist import watchlist_page

logger = logging.getLogger(__name__)

_sf_settings = None
_sf_lock = threading.Lock()


def _get_conn():
    global _sf_settings
    if _sf_settings is None:
        with _sf_lock:
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


def register_callbacks(app, cache):
    """app 에 모든 콜백을 등록한다."""

    # ── 캐시 래퍼 (TTL 1800초=30분, watchlist CRUD 제외) ──
    @cache.memoize(timeout=1800)
    def _fetch_summary_stats():
        with _get_conn() as conn:
            return get_summary_stats(conn)

    @cache.memoize(timeout=1800)
    def _fetch_category_price_summary():
        with _get_conn() as conn:
            return get_category_price_summary(conn)

    @cache.memoize(timeout=1800)
    def _fetch_latest_prices_all():
        with _get_conn() as conn:
            return get_latest_prices_all(conn)

    @cache.memoize(timeout=1800)
    def _fetch_product_stats():
        with _get_conn() as conn:
            return get_product_stats(conn)

    @cache.memoize(timeout=1800)
    def _fetch_alerts(alert_type=None, category=None):
        with _get_conn() as conn:
            return get_alerts(conn, alert_type=alert_type, category=category)

    @cache.memoize(timeout=1800)
    def _fetch_today_crawl_comparison(category=None):
        with _get_conn() as conn:
            return get_today_crawl_comparison(conn, category=category)

    @cache.memoize(timeout=1800)
    def _fetch_price_trend(category=None, search=None, days=None):
        with _get_conn() as conn:
            return get_price_trend(conn, category=category, search=search, days=days)

    # ── Page routing ──

    @app.callback(Output("page-content", "children"), Input("url", "pathname"))
    def display_page(pathname):
        if pathname == "/prices":
            return prices_page()
        if pathname == "/trends":
            return trends_page()
        if pathname == "/alerts":
            return alerts_layout()
        if pathname == "/watchlist":
            return watchlist_page()
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
            stats = _fetch_summary_stats()
            cat_df = _fetch_category_price_summary()
            prices_df = _fetch_latest_prices_all()
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
            df = _fetch_latest_prices_all()
        except Exception as e:
            logger.exception("가격표 데이터 로드 실패")
            return db_error_ui()

        if category and category != "ALL":
            df = df[df["category"] == category]
        if site and site != "ALL":
            df = df[df["site"] == site]

        return make_price_table(df)

    # ── Stats ──

    @app.callback(
        Output("product-stats-table", "children"),
        Input("refresh-interval", "n_intervals"),
    )
    def update_stats(_):
        try:
            df = _fetch_product_stats()
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
            df = _fetch_price_trend(category=category, search=search, days=days if days else None)
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
        fig.update_traces(hovertemplate=f"<b>{search}</b><br>%{{y:,.0f}}원<extra>%{{fullData.name}}</extra>")
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
        [Output("today-comparison-datatable", "data"),
         Output("today-comparison-datatable", "columns")],
        [Input("trend-category-filter", "data")],
    )
    def update_today_comparison(category):
        def _fmt(val):
            return f"{int(val):,}원" if val is not None and str(val) not in ("", "None", "nan") else "-"

        def _change(val, prev_val):
            if val is None or str(val) in ("", "None", "nan"):
                return ""
            if prev_val is None or str(prev_val) in ("", "None", "nan"):
                return ""
            return "up" if float(val) > float(prev_val) else ("down" if float(val) < float(prev_val) else "")

        try:
            df = _fetch_today_crawl_comparison(category=category)
        except Exception as e:
            logger.exception("오늘 크롤링 비교 데이터 로드 실패")
            return [], []

        if df.empty:
            return [], []

        rows = []
        for _, row in df.iterrows():
            p1 = row["price_1st"]
            p2 = row.get("price_2nd")
            p3 = row.get("price_3rd")
            p4 = row.get("price_4th")
            rows.append({
                "site": row["site"],
                "category": row["category"],
                "product_name": str(row["product_name"]),
                "1차": _fmt(p1),
                "2차": _fmt(p2),
                "3차": _fmt(p3),
                "4차": _fmt(p4),
                "price_change_2": _change(p2, p1),
                "price_change_3": _change(p3, p2),
                "price_change_4": _change(p4, p3),
            })

        columns = [
            {"name": "사이트",   "id": "site"},
            {"name": "카테고리", "id": "category"},
            {"name": "상품명",   "id": "product_name"},
            {"name": "1차",      "id": "1차"},
            {"name": "2차",      "id": "2차"},
            {"name": "3차",      "id": "3차"},
            {"name": "4차",      "id": "4차"},
        ]
        return rows, columns

    @app.callback(
        Output("trend-search-input", "value"),
        Input("today-comparison-datatable", "active_cell"),
        State("today-comparison-datatable", "data"),
        prevent_initial_call=True,
    )
    def click_today_table_cell(active_cell, data):
        if not active_cell or not data:
            raise dash.exceptions.PreventUpdate
        if active_cell.get("column_id") != "product_name":
            raise dash.exceptions.PreventUpdate
        product_name = data[active_cell["row"]]["product_name"]
        logger.info(f"[table-click] product_name={product_name!r}")
        return product_name

    # ── Alerts ──

    @app.callback(
        Output("alerts-table", "children"),
        [Input("alert-type-filter", "data"),
         Input("alert-category-filter", "data"),
         Input("alerts-refresh-interval", "n_intervals")],
    )
    def update_alerts_table(alert_type, category, _):
        try:
            df = _fetch_alerts(alert_type=alert_type, category=category)
        except Exception as e:
            logger.exception("알림 데이터 로드 실패")
            return db_error_ui()

        if df.empty:
            return html.P("알림 없음", className="text-muted")

        from datetime import date as date_type
        import pandas as pd

        today = date_type.today()
        yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)

        def _date_label(dt_str: str) -> str:
            d = pd.Timestamp(dt_str).date()
            if d == today:
                return "오늘"
            if d == yesterday.date():
                return "어제"
            return str(d)

        df["_date_key"] = df["created_at"].apply(lambda x: pd.Timestamp(str(x)).date())
        output = []
        current_date = None

        for _, row in df.iterrows():
            row_date = row["_date_key"]
            if row_date != current_date:
                current_date = row_date
                output.append(
                    html.H6(_date_label(str(row["created_at"])),
                            className="text-secondary mt-3 mb-2 border-bottom pb-1")
                )

            alert_type_raw = str(row["alert_type"])
            type_display = ALERT_TYPE_DISPLAY.get(alert_type_raw, alert_type_raw)
            type_class = ALERT_TYPE_CLASS.get(alert_type_raw, "")

            name = str(row["product_name"])[:60]
            url = row.get("url", "")
            name_cell = html.A(name, href=url, target="_blank", className="text-info") if url else name

            old_price = f"{int(row['old_price']):,}원" if row["old_price"] else "-"
            new_price = f"{int(row['new_price']):,}원"
            change_pct = f"{float(row['change_pct']):+.1f}%" if row["change_pct"] is not None else "-"

            created = str(row["created_at"])[11:16]

            output.append(dbc.Card(dbc.CardBody([
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

        return output


    # ── Button Toggles ──

    _register_button_toggle(app, "trend-category-filter", "cat-btn-", CATEGORIES, "ALL")
    _register_button_toggle(app, "trend-period", "period-btn-", [0, 7, 14, 30], 0)
    _register_button_toggle(app, "price-category-filter", "price-cat-btn-", CATEGORIES, "ALL")
    _register_button_toggle(app, "price-site-filter", "price-site-btn-", ["ALL", "다나와", "컴퓨존", "견적왕"], "ALL")
    _register_button_toggle(app, "alert-type-filter", "alert-btn-", ["ALL", "NEW_LOW", "NEW_HIGH", "PRICE_DROP", "PRICE_SPIKE"], "ALL")
    _register_button_toggle(app, "alert-category-filter", "alert-cat-btn-", CATEGORIES, "ALL")

    # ── Watch list ──

    @app.callback(
        [Output("watch-search-store", "data"),
         Output("watch-search-results", "children")],
        Input("watch-search-btn", "n_clicks"),
        [State("watch-category-select", "value"),
         State("watch-search-input", "value")],
        prevent_initial_call=True,
    )
    def do_watch_search(_, category, query):
        if not query:
            return [], dbc.Alert("검색어를 입력하세요.", color="warning")
        results = search_products(query, max_results=10)
        if not results:
            return [], html.P("검색 결과 없음", className="text-muted")

        stored = [
            {"pcode": r.pcode, "product_name": r.product_name, "url": r.url}
            for r in results
        ]
        cards = [
            dbc.Card(dbc.CardBody(
                dbc.Row([
                    dbc.Col([
                        html.Div(r["product_name"][:80], className="text-light fw-bold"),
                        html.Small(f"pcode: {r['pcode']}", className="text-muted"),
                    ], width=10),
                    dbc.Col(
                        dbc.Button(
                            "추가",
                            id={"type": "watch-add-btn", "index": i},
                            color="success", size="sm",
                        ),
                        width=2, className="text-end d-flex align-items-center justify-content-end",
                    ),
                ])
            ), color="dark", className="mb-2")
            for i, r in enumerate(stored)
        ]
        return stored, cards

    @app.callback(
        Output("watch-list-table", "children"),
        Input("watch-refresh-trigger", "data"),
    )
    def load_watch_list(_):
        try:
            with _get_conn() as conn:
                df = get_watch_products(conn)
        except Exception as e:
            return db_error_ui(str(e))

        if df.empty:
            return html.P("크롤링 대상이 없습니다.", className="text-muted")

        rows = []
        for _, row in df.iterrows():
            watch_id = str(int(row["id"]))
            _pname = row.get("product_name")
            display_name = str(_pname if (_pname and not pd.isna(_pname)) else row["query"])[:80]
            rows.append(html.Tr([
                html.Td(row["category"]),
                html.Td(row.get("brand") or "-"),
                html.Td(display_name),
                html.Td(str(row.get("added_at", ""))[:10]),
                html.Td(
                    dbc.Button(
                        "삭제",
                        id={"type": "watch-del-btn", "index": watch_id},
                        color="danger", size="sm",
                    )
                ),
            ]))

        header = html.Thead(html.Tr([
            html.Th("카테고리"), html.Th("브랜드"), html.Th("상품명"),
            html.Th("추가일"), html.Th(""),
        ]))
        return dbc.Table([header, html.Tbody(rows)],
                         bordered=True, hover=True, striped=True, color="dark")

    # ── 추가 버튼 처리 ──
    @app.callback(
        Output("watch-refresh-trigger", "data", allow_duplicate=True),
        Input({"type": "watch-add-btn", "index": ALL}, "n_clicks"),
        [
            State("watch-search-store", "data"),
            State("watch-category-select", "value"),
            State("watch-refresh-trigger", "data"),
        ],
        prevent_initial_call=True,
    )
    def handle_watch_add(add_clicks, search_results, category, current_trigger):
        ctx = dash.callback_context
        if not ctx.triggered or not ctx.triggered[0]["value"]:
            raise dash.exceptions.PreventUpdate

        triggered_prop = ctx.triggered[0]["prop_id"]
        btn_idx = json.loads(triggered_prop.split(".")[0])["index"]

        try:
            with _get_conn() as conn:
                if search_results:
                    product = search_results[int(btn_idx)]
                    add_watch_product(
                        conn,
                        query=product["product_name"],
                        pcode=product["pcode"],
                        product_name=product["product_name"],
                        category=category or "기타",
                        brand=None,
                    )
                    df_after = get_watch_products(conn)
                    send_slack_watch_change("추가", product, df_after)
        except Exception:
            logger.exception("watchlist 추가 실패")

        return (current_trigger or 0) + 1

    # ── 삭제 버튼 → 모달 열기 ──
    @app.callback(
        [Output("watch-del-confirm-modal", "is_open"),
         Output("watch-pending-del-id", "data")],
        Input({"type": "watch-del-btn", "index": ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def open_del_modal(del_clicks):
        ctx = dash.callback_context
        if not ctx.triggered or not ctx.triggered[0]["value"]:
            raise dash.exceptions.PreventUpdate

        triggered_prop = ctx.triggered[0]["prop_id"]
        watch_id = json.loads(triggered_prop.split(".")[0])["index"]
        return True, watch_id

    # ── 모달 확인/취소 ──
    @app.callback(
        [Output("watch-del-confirm-modal", "is_open", allow_duplicate=True),
         Output("watch-refresh-trigger", "data", allow_duplicate=True)],
        [Input("watch-del-confirm-btn", "n_clicks"),
         Input("watch-del-cancel-btn", "n_clicks")],
        [State("watch-pending-del-id", "data"),
         State("watch-refresh-trigger", "data")],
        prevent_initial_call=True,
    )
    def handle_del_confirm(confirm_clicks, cancel_clicks, watch_id, current_trigger):
        ctx = dash.callback_context
        if not ctx.triggered or not ctx.triggered[0]["value"]:
            raise dash.exceptions.PreventUpdate

        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
        if triggered_id == "watch-del-cancel-btn":
            return False, dash.no_update

        if watch_id is None:
            return False, dash.no_update

        try:
            with _get_conn() as conn:
                df_before = get_watch_products(conn)
                deleted_rows = df_before[df_before["id"] == int(watch_id)]
                remove_watch_product(conn, int(watch_id))
                df_after = get_watch_products(conn)
                if not deleted_rows.empty:
                    r = deleted_rows.iloc[0]
                    send_slack_watch_change("삭제", {
                        "product_name": r.get("product_name") or r["query"],
                        "pcode": r["pcode"],
                        "category": r["category"],
                    }, df_after)
        except Exception:
            logger.exception("watchlist 삭제 실패")

        return False, (current_trigger or 0) + 1

    # 캐시 워밍에서 호출할 수 있도록 fetcher 함수들을 반환.
    # 워밍은 각 함수를 인자 없이(기본값으로) 호출하므로, 파라미터 없는 전체 데이터 쿼리만 포함한다.
    # _fetch_alerts / _fetch_today_crawl_comparison 은 category=None 기본값으로 전체 데이터를 워밍.
    # _fetch_price_trend 는 search 키워드 없이는 의미 있는 결과가 없으므로 워밍 대상에서 제외.
    return {
        "summary_stats":          _fetch_summary_stats,
        "category_price_summary": _fetch_category_price_summary,
        "latest_prices_all":      _fetch_latest_prices_all,
        "product_stats":          _fetch_product_stats,
        "alerts":                 _fetch_alerts,
        "today_crawl_comparison": _fetch_today_crawl_comparison,
    }
