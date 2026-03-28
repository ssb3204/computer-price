"""대시보드 공유 헬퍼 함수 및 상수."""

import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html

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

CATEGORIES = ["ALL", "CPU", "GPU", "RAM", "SSD"]


# ── 테이블 빌더 ──

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
