"""Unit tests for DanawaCrawler using saved HTML fixtures."""

from pathlib import Path

import pytest

from src.crawlers.danawa import (
    CATEGORY_TARGETS,
    PCODE_TARGETS,
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

        html = (FIXTURE_DIR / "category_ram.html").read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("li.prod_item")

        ad_items = [i for i in items if not _is_real_product(i)]
        real_items = [i for i in items if _is_real_product(i)]

        # There should be some ads and some real items
        assert len(ad_items) > 0, "Expected some ad items"
        assert len(real_items) > 0, "Expected some real items"

        # Ad items should NOT start with "productItem" (they use adReader* or adPoint*)
        for item in ad_items:
            item_id = item.get("id", "")
            assert not item_id.startswith("productItem"), f"Unexpected real-looking ad: {item_id}"


# ── crawl_raw tests ──


class TestCrawlRaw:
    """Test the crawl_raw() method with mocked HTTP."""

    @skip_no_fixtures
    def test_crawl_raw_returns_raw_prices(self):
        from unittest.mock import patch

        crawler = DanawaCrawler()
        fixtures = {
            "라이젠 7800X3D": "search_7800x3d.html",
            "RTX 5070&tab": "search_rtx5070.html",
            "RTX 5070 Ti": "search_rtx5070ti.html",
            "RX 9070 XT": "search_rx9070xt.html",
        }

        def mock_fetch(url: str) -> str | None:
            for key, fname in fixtures.items():
                if key in url:
                    return (FIXTURE_DIR / fname).read_text(encoding="utf-8")
            if "cate=112752" in url:
                return (FIXTURE_DIR / "category_ram.html").read_text(encoding="utf-8")
            if "cate=112760" in url:
                return (FIXTURE_DIR / "category_ssd.html").read_text(encoding="utf-8")
            return None

        with patch.object(crawler, "_fetch_with_retry", side_effect=mock_fetch):
            results = crawler.crawl_raw()

        # 4 pcode targets + 3 RAM + 3 SSD = 10
        assert len(results) == 10

        categories = {r.category for r in results}
        assert "CPU" in categories
        assert "GPU" in categories
        assert "RAM" in categories
        assert "SSD" in categories

        # All should have valid data
        for raw in results:
            assert raw.site == "danawa"
            assert raw.product_name != ""
            assert raw.price_text != ""
