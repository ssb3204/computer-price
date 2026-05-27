"""대시보드 공유 헬퍼 함수 및 상수."""

import logging
import os

import dash_bootstrap_components as dbc
import pandas as pd
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


def _price_str(val) -> str:
    """가격 값 → '1,234,000원' 또는 '-'. None/NaN/변환불가 모두 방어."""
    try:
        return f"{int(float(val)):,}원" if val is not None and not pd.isna(val) else "-"
    except (TypeError, ValueError):
        return "-"


def send_slack_watch_change(action: str, product_info: dict, watch_list_df: pd.DataFrame, site: str = "다나와") -> None:
    """Watch list 추가/삭제 시 Slack Incoming Webhook으로 알림 전송.

    Args:
        action: "추가" 또는 "삭제"
        product_info: {"product_name", "pcode"/"product_no"/"pd_no", "category"} 키를 가진 dict
        watch_list_df: 변경 후 전체 사이트 통합 watch list DataFrame
        site: 변경이 발생한 사이트 ("다나와" | "견적왕" | "컴퓨존")
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    name = product_info.get("product_name") or product_info.get("query", "")
    pcode = product_info.get("pcode") or product_info.get("product_no") or product_info.get("pd_no", "")
    category = product_info.get("category", "")
    action_icon = "➕" if action == "추가" else "➖"
    site_label = site

    # Slack 알림은 활성 크롤링 대상만 표시
    if "is_active" in watch_list_df.columns:
        watch_list_df = watch_list_df[watch_list_df["is_active"] == True]  # noqa: E712

    if watch_list_df.empty:
        list_text = "  (없음)"
    else:
        lines = []
        for site_name in ("다나와", "컴퓨존", "견적왕"):
            site_rows = watch_list_df[watch_list_df["site"] == site_name]
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
        total = row["total_records"]
        total_str = str(int(total)) if (total is not None and not pd.isna(total)) else "-"
        body_rows.append(html.Tr([
            html.Td(row["category"]),
            html.Td(row["site"]),
            html.Td(_name_cell(row)),
            html.Td(_price_str(row["avg_price"])),
            html.Td(_price_str(row["min_price_ever"])),
            html.Td(_price_str(row["max_price_ever"])),
            html.Td(total_str),
        ]))

    body = html.Tbody(body_rows)
    return dbc.Table([header, body], bordered=True, hover=True, striped=True, color="dark")


def make_watchlist_table(df: pd.DataFrame, del_btn_type: str):
    """Watchlist DataFrame → dbc.Table (활성/비활성 모두 표시).

    활성(IS_ACTIVE=True): 초록 점 + 현재 가격 + 마지막 크롤링 시간
    비활성(IS_ACTIVE=False): 회색 점 + 마지막 가격 + 마지막 크롤링 시간 (삭제 버튼 없음)
    """
    if df.empty:
        return html.P("크롤링 대상이 없습니다.", className="text-muted")

    rows = []
    for _, row in df.iterrows():
        watch_id = str(int(row["id"]))
        is_active = bool(row.get("is_active", True))

        _pname = row.get("product_name")
        display_name = str(_pname if (_pname and not pd.isna(_pname)) else row["query"])[:80]

        status = html.Span(
            "●",
            style={"color": "#2ecc71" if is_active else "#6c757d", "fontSize": "10px"},
            title="크롤링 중" if is_active else "크롤링 중지",
        )

        price_str = _price_str(row.get("price"))

        last_crawled = row.get("last_crawled_at")
        lc_str = str(last_crawled)[:16] if (last_crawled is not None and not pd.isna(last_crawled)) else "-"

        muted = {"color": "#888"} if not is_active else {}

        action_cell = (
            dbc.Button("삭제", id={"type": del_btn_type, "index": watch_id}, color="danger", size="sm")
            if is_active
            else html.Span("-", style={"color": "#555"})
        )

        rows.append(html.Tr([
            html.Td(status),
            html.Td(row["category"], style=muted),
            html.Td(row.get("brand") or "-", style=muted),
            html.Td(display_name, style=muted),
            html.Td(price_str, style=muted),
            html.Td(lc_str, style={"fontSize": "11px", "color": "#aaa"}),
            html.Td(action_cell),
        ]))

    header = html.Thead(html.Tr([
        html.Th(""), html.Th("카테고리"), html.Th("브랜드"), html.Th("상품명"),
        html.Th("최근 가격"), html.Th("마지막 크롤링"), html.Th(""),
    ]))
    return dbc.Table([header, html.Tbody(rows)], bordered=True, hover=True, striped=True, color="dark")
