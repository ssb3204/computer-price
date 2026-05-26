"""대시보드 공유 헬퍼 함수 및 상수."""

import logging
import os

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import requests as _requests
from dash import html

from src.crawlers.parser_utils import CATEGORIES as _CATEGORIES_BASE

logger = logging.getLogger(__name__)

# ── 상수 ──

SITE_COLORS = {"다나와": "#3498db", "컴퓨존": "#e67e22", "견적왕": "#2ecc71"}

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

CATEGORIES = ["ALL", *_CATEGORIES_BASE]


# ── UI 헬퍼 ──

def db_error_ui(message: str = "데이터베이스 연결 실패") -> dbc.Alert:
    """DB 연결/쿼리 오류 시 표시할 에러 배너."""
    return dbc.Alert(
        [
            html.Strong("연결 오류: "),
            message,
        ],
        color="danger",
        className="mt-2",
    )


def empty_chart(message: str) -> go.Figure:
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


# ── 테이블 빌더 ──

_GREEN_DOT = html.Span("● ", style={"color": "#2ecc71", "fontSize": "0.75em", "verticalAlign": "middle"})


def _name_cell(row) -> object:
    """is_watchlist 여부에 따라 초록 점 + 이름 셀을 반환한다."""
    name = str(row["product_name"])[:80]
    url = row.get("url", "")
    is_watch = bool(row.get("is_watchlist", False))

    link = html.A(name, href=url, target="_blank", className="text-info") if url else name
    if is_watch:
        return html.Span([_GREEN_DOT, link])
    return link


def make_price_table(df, max_rows=None):
    """DataFrame → dbc.Table with clickable product names."""
    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    rows_to_show = df.head(max_rows) if max_rows else df

    header = html.Thead(html.Tr([
        html.Th("카테고리"), html.Th("사이트"), html.Th("상품명"), html.Th("가격"),
    ]))

    body_rows = []
    for _, row in rows_to_show.iterrows():
        price = f"{int(row['price']):,}원"
        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(_name_cell(row)),
            html.Td(price),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")


_SITE_LABEL = {"danawa": "다나와", "kjwwang": "견적왕", "compuzone": "컴퓨존"}


def send_slack_watch_change(action: str, product_info: dict, watch_list_df, site: str = "danawa") -> None:
    """Watch list 추가/삭제 시 Slack Incoming Webhook으로 알림 전송.

    Args:
        action: "추가" 또는 "삭제"
        product_info: {"product_name", "pcode"/"product_no"/"pd_no", "category"} 키를 가진 dict
        watch_list_df: 변경 후 전체 사이트 통합 watch list DataFrame
        site: 변경이 발생한 사이트 ("danawa" | "kjwwang" | "compuzone")
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    name = product_info.get("product_name") or product_info.get("query", "")
    pcode = product_info.get("pcode") or product_info.get("product_no") or product_info.get("pd_no", "")
    category = product_info.get("category", "")
    action_icon = "➕" if action == "추가" else "➖"
    site_label = _SITE_LABEL.get(site, site)

    if watch_list_df.empty:
        list_text = "  (없음)"
    else:
        lines = []
        for site_key, site_name in _SITE_LABEL.items():
            site_rows = watch_list_df[watch_list_df["site"] == site_key]
            if site_rows.empty:
                continue
            lines.append(f"  *{site_name}*")
            for _, row in site_rows.iterrows():
                item_name = row.get("product_name") or row["query"]
                lines.append(f"    • [{row['category']}] {item_name}")
        list_text = "\n".join(lines)

    total = len(watch_list_df)
    text = (
        f"{action_icon} *[{site_label}] 크롤링 대상 {action}*\n"
        f"상품: {name}\n"
        f"카테고리: {category}  |  코드: {pcode}\n\n"
        f"*전체 크롤링 대상 ({total}개):*\n{list_text}"
    )

    try:
        _requests.post(webhook_url, json={"text": text}, timeout=10)
    except Exception as exc:
        logger.warning("Slack 알림 전송 실패: %s", exc)


def make_stats_table(df):
    """상품 통계 DataFrame → dbc.Table with clickable names."""
    if df.empty:
        return html.P("데이터 없음", className="text-muted")

    header = html.Thead(html.Tr([
        html.Th("카테고리"), html.Th("사이트"), html.Th("상품명"),
        html.Th("평균가"), html.Th("최저가"), html.Th("최고가"), html.Th("수집횟수"),
    ]))

    body_rows = []
    for _, row in df.iterrows():
        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(_name_cell(row)),
            html.Td(f"{int(float(row['avg_price'])):,}원"),
            html.Td(f"{int(row['min_price_ever']):,}원"),
            html.Td(f"{int(row['max_price_ever']):,}원"),
            html.Td(str(row["total_records"])),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")
