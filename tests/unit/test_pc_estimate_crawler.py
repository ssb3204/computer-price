"""Unit tests for PCEstimateCrawler using inline minimal HTML fixtures."""

from unittest.mock import patch

import pytest

from src.crawlers.pc_estimate import CATEGORY_TARGETS, PCEstimateCrawler


def _make_item(href: str, name: str, price: str) -> str:
    """li.list 한 개짜리 HTML 조각 생성."""
    return (
        f'<li class="list">'
        f'  <a class="name" href="{href}">{name}</a>'
        f'  <span class="card">{price}</span>'
        f"</li>"
    )


def _make_page(items_html: str) -> str:
    return f"<html><body><ul>{items_html}</ul></body></html>"


SAMPLE_CPU_HTML = _make_page(
    _make_item("/shop/view.html?id=1001", "AMD 라이젠 7 7800X3D", "450,000원")
    + _make_item("/shop/view.html?id=1002", "Intel i7-14700K", "480,000원")
    + _make_item("/shop/view.html?id=1003", "AMD 라이젠 5 7600", "200,000원")
)

SAMPLE_GPU_HTML = _make_page(
    _make_item("/shop/view.html?id=2001", "RTX 5070", "900,000원")
    + _make_item("/shop/view.html?id=2002", "RTX 5070 Ti", "1,100,000원")
    + _make_item("/shop/view.html?id=2003", "RX 9070 XT", "850,000원")
)

SAMPLE_RAM_HTML = _make_page(
    _make_item("/shop/view.html?id=3001", "삼성 DDR5-6000 16GB", "85,000원")
    + _make_item("/shop/view.html?id=3002", "SK하이닉스 DDR5-5600 32GB", "120,000원")
    + _make_item("/shop/view.html?id=3003", "G.SKILL Trident Z5 32GB", "150,000원")
)

SAMPLE_SSD_HTML = _make_page(
    _make_item("/shop/view.html?id=4001", "삼성 990 PRO 2TB", "220,000원")
    + _make_item("/shop/view.html?id=4002", "WD Black SN850X 1TB", "130,000원")
    + _make_item("/shop/view.html?id=4003", "SK하이닉스 P41 1TB", "110,000원")
)


CATEGORY_HTML_MAP = {
    "9": SAMPLE_CPU_HTML,
    "12": SAMPLE_GPU_HTML,
    "10": SAMPLE_RAM_HTML,
    "243": SAMPLE_SSD_HTML,
}


class TestCrawlRaw:
    """crawl_raw()가 HTML을 올바르게 파싱하는지 검증."""

    def _mock_fetch(self, target):
        return CATEGORY_HTML_MAP.get(target.cate2)

    def test_returns_correct_count(self):
        crawler = PCEstimateCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        # 4 categories × 3 top_n = 12
        assert len(results) == 12

    def test_all_categories_present(self):
        crawler = PCEstimateCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        categories = {r.category for r in results}
        assert categories == {"CPU", "GPU", "RAM", "SSD"}

    def test_fields_populated(self):
        crawler = PCEstimateCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        for raw in results:
            assert raw.site == "pc_estimate"
            assert raw.product_name != ""
            assert raw.price_text != ""
            assert raw.crawled_at is not None

    def test_product_url_format(self):
        crawler = PCEstimateCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        for raw in results:
            assert raw.url.startswith("https://kjwwang.com/")
            assert "/shop/view.html?id=" in raw.url

    def test_specific_values(self):
        crawler = PCEstimateCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert cpu_items[0].product_name == "AMD 라이젠 7 7800X3D"
        assert cpu_items[0].price_text == "450,000원"

    def test_top_n_limits(self):
        """top_n=3인데 항목이 5개면 3개만 가져오는지 확인."""
        extra_html = _make_page(
            _make_item("/shop/view.html?id=1", "Product 1", "100원")
            + _make_item("/shop/view.html?id=2", "Product 2", "200원")
            + _make_item("/shop/view.html?id=3", "Product 3", "300원")
            + _make_item("/shop/view.html?id=4", "Product 4", "400원")
            + _make_item("/shop/view.html?id=5", "Product 5", "500원")
        )
        crawler = PCEstimateCrawler()

        def mock_fetch(target):
            if target.cate2 == "9":
                return extra_html
            return CATEGORY_HTML_MAP.get(target.cate2)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 3

    def test_fetch_failure_skips_category(self):
        """한 카테고리 fetch 실패 시 나머지는 정상 수집."""
        crawler = PCEstimateCrawler()

        def mock_fetch(target):
            if target.cate2 == "9":
                return None  # CPU 실패
            return CATEGORY_HTML_MAP.get(target.cate2)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 9  # 3 categories × 3
        categories = {r.category for r in results}
        assert "CPU" not in categories

    def test_missing_price_skips_item(self):
        """span.card 없는 항목은 건너뛰기."""
        html = _make_page(
            '<li class="list">'
            '  <a class="name" href="/shop/view.html?id=999">No Price</a>'
            "</li>"
            + _make_item("/shop/view.html?id=1001", "Valid Item", "100,000원")
        )
        crawler = PCEstimateCrawler()

        def mock_fetch(target):
            if target.cate2 == "9":
                return html
            return CATEGORY_HTML_MAP.get(target.cate2)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 1
        assert cpu_items[0].product_name == "Valid Item"

    def test_missing_name_skips_item(self):
        """a.name 없는 항목은 건너뛰기."""
        html = _make_page(
            '<li class="list">'
            '  <span class="card">100,000원</span>'
            "</li>"
            + _make_item("/shop/view.html?id=1001", "Valid Item", "200,000원")
        )
        crawler = PCEstimateCrawler()

        def mock_fetch(target):
            if target.cate2 == "9":
                return html
            return CATEGORY_HTML_MAP.get(target.cate2)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 1
        assert cpu_items[0].product_name == "Valid Item"

    def test_empty_href_produces_empty_url(self):
        """href가 빈 문자열이면 url도 빈 문자열."""
        html = _make_page(
            '<li class="list">'
            '  <a class="name" href="">Some Product</a>'
            '  <span class="card">50,000원</span>'
            "</li>"
        )
        crawler = PCEstimateCrawler()

        def mock_fetch(target):
            if target.cate2 == "9":
                return html
            return CATEGORY_HTML_MAP.get(target.cate2)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 1
        assert cpu_items[0].url == ""
