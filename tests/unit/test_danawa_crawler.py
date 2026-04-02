"""Unit tests for DanawaCrawler using saved HTML fixtures."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.crawlers.danawa import (
    DanawaCrawler,
    _extract_name,
    _extract_pcode,
    _extract_price_text,
    _is_real_product,
)

FIXTURE_DIR = Path(__file__).resolve().parent.parent.parent / "tmp"


def _has_fixtures() -> bool:
    return (FIXTURE_DIR / "search_7800x3d.html").exists()


skip_no_fixtures = pytest.mark.skipif(
    not _has_fixtures(), reason="HTML fixtures not found in tmp/"
)


def _make_crawler() -> DanawaCrawler:
    """Mock conn을 주입한 DanawaCrawler 인스턴스 생성."""
    mock_conn = MagicMock()
    return DanawaCrawler(conn=mock_conn)


# ── Helper extraction tests ──


class TestHelperExtraction:
    """Test the helper functions with real HTML fragments."""

    @skip_no_fixtures
    def test_extract_from_search_page(self):
        from bs4 import BeautifulSoup

        html = (FIXTURE_DIR / "search_7800x3d.html").read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.prod_item")

        assert len(items) > 0

        # First real product item
        real_items = [i for i in items if _is_real_product(i)]
        assert len(real_items) > 0

        first = real_items[0]
        pcode = _extract_pcode(first)
        name = _extract_name(first)
        price_text = _extract_price_text(first)

        assert pcode is not None
        assert pcode.isdigit()
        assert name is not None
        assert len(name) > 0
        assert price_text is not None
        assert len(price_text) > 0

    @skip_no_fixtures
    def test_ad_items_filtered_out(self):
        from bs4 import BeautifulSoup

        html = (FIXTURE_DIR / "search_7800x3d.html").read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.prod_item")

        real_items = [i for i in items if _is_real_product(i)]
        assert len(real_items) > 0

        for item in real_items:
            item_id = item.get("id", "")
            assert item_id.startswith("productItem"), f"Non-real item slipped through: {item_id}"


# ── crawl_raw tests ──


class TestCrawlRaw:
    """Test the crawl_raw() method with mocked HTTP and DB."""

    @skip_no_fixtures
    def test_crawl_raw_returns_raw_prices(self):
        crawler = _make_crawler()

        watch_products = [
            {"query": "라이젠 7800X3D", "pcode": "19627934", "category": "CPU", "brand": "AMD"},
            {"query": "RTX 5070",      "pcode": "77379452", "category": "GPU", "brand": "NVIDIA"},
            {"query": "RTX 5070 Ti",   "pcode": "76464143", "category": "GPU", "brand": "NVIDIA"},
            {"query": "RX 9070 XT",    "pcode": "77381483", "category": "GPU", "brand": "AMD"},
        ]

        fixtures = {
            "라이젠 7800X3D": "search_7800x3d.html",
            "RTX 5070&tab":  "search_rtx5070.html",
            "RTX 5070 Ti":   "search_rtx5070ti.html",
            "RX 9070 XT":    "search_rx9070xt.html",
        }

        def mock_fetch(url: str) -> str | None:
            for key, fname in fixtures.items():
                if key in url:
                    return (FIXTURE_DIR / fname).read_text(encoding="utf-8")
            return None

        with (
            patch.object(crawler, "_load_watch_products", return_value=watch_products),
            patch.object(crawler, "_fetch_with_retry", side_effect=mock_fetch),
        ):
            results = crawler.crawl_raw()

        assert len(results) == 4

        categories = {r.category for r in results}
        assert "CPU" in categories
        assert "GPU" in categories

        for raw in results:
            assert raw.site == "danawa"
            assert raw.product_name != ""
            assert raw.price_text != ""
