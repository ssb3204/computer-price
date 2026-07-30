"""Unit tests for DanawaCrawler — inline HTML fixtures.

tmp/ 에 저장해둔 HTML 파일에 의존하면 tmp/ 가 .gitignore 대상이라
CI 러너에서 전 테스트가 skip 된다. 그래서 fixture 를 전부 inline 으로 둔다.
"""

from unittest.mock import patch

from bs4 import BeautifulSoup

from src.crawlers.danawa import (
    PRODUCT_BASE,
    DanawaCrawler,
    _extract_name,
    _extract_pcode,
    _extract_price_text,
    _extract_url,
    _is_real_product,
)
from tests.unit.conftest import FakeClock

# ── HTML fixture 빌더 ────────────────────────────────────────────────────────

_DEFAULT_HREF = "https://prod.danawa.com/info/?pcode={pcode}&cate=112753"


def _product_li(
    pcode: str,
    name: str = "테스트 상품",
    price: str = "450,000원",
    *,
    href: str | None = None,
    ad_prefix: str | None = None,
    with_price: bool = True,
    with_name: bool = True,
) -> str:
    """li.prod_item 한 개짜리 HTML 조각.

    ad_prefix 를 주면 광고 아이템(adReaderProductItem*/adPointProductItem*)이 된다.
    """
    item_id = f"{ad_prefix}{pcode}" if ad_prefix else f"productItem{pcode}"
    link = _DEFAULT_HREF.format(pcode=pcode) if href is None else href

    name_block = f'<div class="prod_name"><a href="{link}">{name}</a></div>' if with_name else ""
    price_block = f'<div class="price_sect"><strong>{price}</strong></div>' if with_price else ""

    return f'<li class="prod_item" id="{item_id}">{name_block}{price_block}</li>'


def _search_page(*items: str) -> str:
    return f"<html><body><ul>{''.join(items)}</ul></body></html>"


def _first_item(html: str):
    return BeautifulSoup(html, "html.parser").select_one("li.prod_item")


# ── 파서 헬퍼 (순수 함수) ────────────────────────────────────────────────────


class TestHelperExtraction:
    def test_pcode_from_item_id(self):
        item = _first_item(_product_li("19627934"))
        assert _extract_pcode(item) == "19627934"

    def test_pcode_from_href_when_id_not_product_item(self):
        """id 가 productItem* 형식이 아니면 링크의 pcode= 로 폴백한다."""
        html = (
            '<li class="prod_item" id="somethingElse">'
            '  <div class="prod_name"><a href="/info/?pcode=12345678">이름</a></div>'
            "</li>"
        )
        assert _extract_pcode(_first_item(html)) == "12345678"

    def test_name_and_price_text(self):
        item = _first_item(_product_li("111", name="AMD 라이젠 7800X3D", price="450,000원"))
        assert _extract_name(item) == "AMD 라이젠 7800X3D"
        assert _extract_price_text(item) == "450,000원"

    def test_missing_price_returns_none(self):
        item = _first_item(_product_li("111", with_price=False))
        assert _extract_price_text(item) is None

    def test_is_real_product_accepts_product_item(self):
        assert _is_real_product(_first_item(_product_li("111"))) is True

    def test_is_real_product_rejects_ads(self):
        """CLAUDE.md 규칙: adReaderProductItem*/adPointProductItem* 은 광고다."""
        for prefix in ("adReaderProductItem", "adPointProductItem"):
            item = _first_item(_product_li("111", ad_prefix=prefix))
            assert _is_real_product(item) is False, f"{prefix} 가 실제 상품으로 통과했다"

    def test_extract_url_returns_absolute_link(self):
        item = _first_item(_product_li("111"))
        assert _extract_url(item) == _DEFAULT_HREF.format(pcode="111")

    def test_extract_url_ignores_relative_link(self):
        """상대경로는 빈 문자열 — 호출부가 PRODUCT_BASE + pcode 로 대체한다."""
        item = _first_item(_product_li("111", href="/info/?pcode=111"))
        assert _extract_url(item) == ""


# ── crawl_raw ────────────────────────────────────────────────────────────────


class TestCrawlRaw:
    def test_matched_target_produces_raw_price(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(_product_li("19627934", name="AMD 라이젠 7800X3D", price="450,000원"))

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert len(results) == 1
        raw = results[0]
        assert raw.site == "danawa"
        assert raw.category == "CPU"
        assert raw.brand == "AMD"
        assert raw.product_name == "AMD 라이젠 7800X3D"
        assert raw.price_text == "450,000원"
        assert "19627934" in raw.url
        assert raw.crawled_at is not None

    def test_non_matching_pcode_is_ignored(self, make_watch_conn):
        """워치리스트 pcode 와 다른 상품은 수집하지 않는다.

        (카테고리 상위 N개를 담던 옛 방식과 달라진 핵심 지점)
        """
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(
            _product_li("99999999", name="전혀 다른 상품"),
            _product_li("88888888", name="이것도 다른 상품"),
        )

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_all_targets_share_one_crawled_at(self, make_watch_conn):
        """한 크롤링 회차의 모든 상품은 같은 crawled_at 을 가져야 한다.

        대상마다 now() 를 부르면 같은 회차인데 시각이 갈린다.
        stg_price_history 자연키가 (product_id, crawled_at) 이라 하위 계층의
        시계열 정렬과 일별 집계가 이 값에 직접 의존한다.
        """
        conn = make_watch_conn([
            ("라이젠 7800X3D", "19627934", "CPU", "AMD"),
            ("RTX 5070", "77379452", "GPU", "NVIDIA"),
        ])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(_product_li("19627934"), _product_li("77379452"))

        with (
            patch.object(crawler, "_fetch_with_retry", return_value=page),
            patch("src.crawlers.danawa.datetime", FakeClock()),
        ):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert results[0].crawled_at == results[1].crawled_at

    def test_ad_item_with_same_pcode_is_skipped(self, make_watch_conn):
        """pcode 가 같아도 광고 아이템이면 건너뛰고 실제 상품을 집는다."""
        conn = make_watch_conn([("RTX 5070", "77379452", "GPU", "NVIDIA")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(
            _product_li("77379452", name="광고 카드", price="1원", ad_prefix="adReaderProductItem"),
            _product_li("77379452", name="진짜 RTX 5070", price="900,000원"),
        )

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].product_name == "진짜 RTX 5070"
        assert results[0].price_text == "900,000원"

    def test_empty_watchlist_does_not_fetch(self, make_watch_conn):
        crawler = DanawaCrawler(conn=make_watch_conn([]))

        with patch.object(crawler, "_fetch_with_retry") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_fetch_failure_skips_only_that_target(self, make_watch_conn):
        """한 대상의 fetch 가 실패해도 나머지 대상은 계속 수집한다."""
        conn = make_watch_conn([
            ("라이젠 7800X3D", "19627934", "CPU", "AMD"),
            ("RTX 5070", "77379452", "GPU", "NVIDIA"),
        ])
        crawler = DanawaCrawler(conn=conn)
        ok_page = _search_page(_product_li("77379452", name="RTX 5070", price="900,000원"))

        def fake_fetch(url: str) -> str | None:
            return None if "7800X3D" in url else ok_page

        with patch.object(crawler, "_fetch_with_retry", side_effect=fake_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].category == "GPU"

    def test_missing_price_skips_item(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(_product_li("19627934", with_price=False))

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_missing_name_skips_item(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(_product_li("19627934", with_name=False))

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_multiple_targets_each_collected(self, make_watch_conn):
        conn = make_watch_conn([
            ("라이젠 7800X3D", "19627934", "CPU", "AMD"),
            ("RTX 5070", "77379452", "GPU", "NVIDIA"),
        ])
        crawler = DanawaCrawler(conn=conn)
        pages = {
            "19627934": _search_page(_product_li("19627934", name="AMD 라이젠 7800X3D")),
            "77379452": _search_page(_product_li("77379452", name="RTX 5070")),
        }

        def fake_fetch(url: str) -> str:
            return pages["19627934"] if "7800X3D" in url else pages["77379452"]

        with patch.object(crawler, "_fetch_with_retry", side_effect=fake_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert {r.category for r in results} == {"CPU", "GPU"}

    def test_relative_url_falls_back_to_product_base(self, make_watch_conn):
        """링크가 절대 URL이 아니면 pcode 기반 정규 URL로 대체한다."""
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)
        page = _search_page(_product_li("19627934", href="/info/?pcode=19627934"))

        with patch.object(crawler, "_fetch_with_retry", return_value=page):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].url == f"{PRODUCT_BASE}19627934"

    def test_search_url_contains_query(self, make_watch_conn):
        """검색 URL에 워치리스트의 query 가 실려 나가는지 확인."""
        conn = make_watch_conn([("라이젠 7800X3D", "19627934", "CPU", "AMD")])
        crawler = DanawaCrawler(conn=conn)

        with patch.object(crawler, "_fetch_with_retry", return_value=None) as mock_fetch:
            crawler.crawl_raw()

        called_url = mock_fetch.call_args[0][0]
        assert "라이젠 7800X3D" in called_url
        assert "tab=goods" in called_url
