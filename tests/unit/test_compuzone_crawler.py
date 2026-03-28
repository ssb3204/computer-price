"""Unit tests for CompuzoneCrawler using inline minimal HTML fixtures."""

from unittest.mock import patch

import pytest

from src.crawlers.compuzone import CATEGORY_TARGETS, CompuzoneCrawler


def _make_item(pno: str, name: str, price: str) -> str:
    """li.li-obj 한 개짜리 HTML 조각 생성."""
    return (
        f'<li class="li-obj" id="li-pno-{pno}">'
        f'  <a class="prd_info_name">{name}</a>'
        f'  <div class="prd_price" data-price="{price}"></div>'
        f"</li>"
    )


def _make_page(items_html: str) -> str:
    return f"<html><body><ul>{items_html}</ul></body></html>"


SAMPLE_CPU_HTML = _make_page(
    _make_item("100001", "AMD 라이젠 7 7800X3D", "450000")
    + _make_item("100002", "Intel i7-14700K", "480000")
    + _make_item("100003", "AMD 라이젠 5 7600", "200000")
)

SAMPLE_GPU_HTML = _make_page(
    _make_item("200001", "RTX 5070", "900000")
    + _make_item("200002", "RTX 5070 Ti", "1100000")
    + _make_item("200003", "RX 9070 XT", "850000")
)

SAMPLE_RAM_HTML = _make_page(
    _make_item("300001", "삼성 DDR5-6000 16GB", "85000")
    + _make_item("300002", "SK하이닉스 DDR5-5600 32GB", "120000")
    + _make_item("300003", "G.SKILL Trident Z5 32GB", "150000")
)

SAMPLE_SSD_HTML = _make_page(
    _make_item("400001", "삼성 990 PRO 2TB", "220000")
    + _make_item("400002", "WD Black SN850X 1TB", "130000")
    + _make_item("400003", "SK하이닉스 P41 1TB", "110000")
)


CATEGORY_HTML_MAP = {
    "1012": SAMPLE_CPU_HTML,
    "1016": SAMPLE_GPU_HTML,
    "1014": SAMPLE_RAM_HTML,
    "1276": SAMPLE_SSD_HTML,
}


class TestCrawlRaw:
    """crawl_raw()가 HTML을 올바르게 파싱하는지 검증."""

    def _mock_fetch(self, target):
        return CATEGORY_HTML_MAP.get(target.medium_div_no)

    def test_returns_correct_count(self):
        crawler = CompuzoneCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        # 4 categories × 3 top_n = 12
        assert len(results) == 12

    def test_all_categories_present(self):
        crawler = CompuzoneCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        categories = {r.category for r in results}
        assert categories == {"CPU", "GPU", "RAM", "SSD"}

    def test_fields_populated(self):
        crawler = CompuzoneCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        for raw in results:
            assert raw.site == "compuzone"
            assert raw.product_name != ""
            assert raw.price_text != ""
            assert raw.crawled_at is not None

    def test_product_url_format(self):
        crawler = CompuzoneCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        for raw in results:
            assert "ProductNo=" in raw.url
            assert raw.url.startswith("https://www.compuzone.co.kr/")

    def test_specific_values(self):
        crawler = CompuzoneCrawler()
        with patch.object(crawler, "_fetch_category_html", side_effect=self._mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert cpu_items[0].product_name == "AMD 라이젠 7 7800X3D"
        assert cpu_items[0].price_text == "450000"

    def test_top_n_limits(self):
        """top_n=3인데 항목이 5개면 3개만 가져오는지 확인."""
        extra_html = _make_page(
            _make_item("100001", "Product 1", "100")
            + _make_item("100002", "Product 2", "200")
            + _make_item("100003", "Product 3", "300")
            + _make_item("100004", "Product 4", "400")
            + _make_item("100005", "Product 5", "500")
        )
        crawler = CompuzoneCrawler()

        def mock_fetch(target):
            if target.medium_div_no == "1012":
                return extra_html
            return CATEGORY_HTML_MAP.get(target.medium_div_no)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 3

    def test_fetch_failure_skips_category(self):
        """한 카테고리 fetch 실패 시 나머지는 정상 수집."""
        crawler = CompuzoneCrawler()

        def mock_fetch(target):
            if target.medium_div_no == "1012":
                return None  # CPU 실패
            return CATEGORY_HTML_MAP.get(target.medium_div_no)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 9  # 3 categories × 3
        categories = {r.category for r in results}
        assert "CPU" not in categories

    def test_missing_price_skips_item(self):
        """data-price 없는 항목은 건너뛰기."""
        html = _make_page(
            '<li class="li-obj" id="li-pno-999">'
            '  <a class="prd_info_name">No Price Item</a>'
            '  <div class="prd_price"></div>'
            "</li>"
            + _make_item("100001", "Valid Item", "100000")
        )
        crawler = CompuzoneCrawler()

        def mock_fetch(target):
            if target.medium_div_no == "1012":
                return html
            return CATEGORY_HTML_MAP.get(target.medium_div_no)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 1
        assert cpu_items[0].product_name == "Valid Item"

    def test_missing_name_skips_item(self):
        """a.prd_info_name 없는 항목은 건너뛰기."""
        html = _make_page(
            '<li class="li-obj" id="li-pno-999">'
            '  <div class="prd_price" data-price="100000"></div>'
            "</li>"
            + _make_item("100001", "Valid Item", "200000")
        )
        crawler = CompuzoneCrawler()

        def mock_fetch(target):
            if target.medium_div_no == "1012":
                return html
            return CATEGORY_HTML_MAP.get(target.medium_div_no)

        with patch.object(crawler, "_fetch_category_html", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        cpu_items = [r for r in results if r.category == "CPU"]
        assert len(cpu_items) == 1
        assert cpu_items[0].product_name == "Valid Item"
