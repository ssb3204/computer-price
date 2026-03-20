"""Unit tests for DanawaCrawler using saved HTML fixtures."""

import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from src.crawlers.danawa import (
    CATEGORY_TARGETS,
    PCODE_TARGETS,
    DanawaCrawler,
    _extract_name,
    _extract_pcode,
    _extract_price,
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
        price = _extract_price(first)

        assert pcode is not None
        assert pcode.isdigit()
        assert name is not None
        assert len(name) > 0
        assert price is not None
        assert price > 0

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


# ── Crawler strategy tests ──


class TestPcodeStrategy:
    """Test pcode-based search strategy."""

    @skip_no_fixtures
    def test_find_7800x3d_by_pcode(self):
        crawler = DanawaCrawler()
        html = (FIXTURE_DIR / "search_7800x3d.html").read_text(encoding="utf-8")
        target = PCODE_TARGETS[0]  # 7800X3D

        # Mock _fetch_with_retry to return saved HTML
        with patch.object(crawler, "_fetch_with_retry", return_value=html):
            results = crawler._crawl_by_pcode(target)

        assert len(results) == 1
        raw = results[0]
        assert raw.site == "danawa"
        assert raw.category == "CPU"
        assert raw.brand == "AMD"
        assert raw.price > 400_000  # 7800X3D should be > 400k KRW
        assert "7800X3D" in raw.product_name or "7800x3d" in raw.product_name.lower()
        assert raw.url != ""
        assert isinstance(raw.crawled_at, datetime)

    @skip_no_fixtures
    def test_pcode_not_found_returns_empty(self):
        crawler = DanawaCrawler()
        html = (FIXTURE_DIR / "search_7800x3d.html").read_text(encoding="utf-8")

        # Use a non-existent pcode
        from src.crawlers.danawa import TargetProduct

        fake_target = TargetProduct(
            pcode="99999999", query="라이젠 7800X3D", category="CPU", brand="AMD"
        )
        with patch.object(crawler, "_fetch_with_retry", return_value=html):
            results = crawler._crawl_by_pcode(fake_target)

        assert results == []


class TestCategoryStrategy:
    """Test category ranking strategy."""

    @skip_no_fixtures
    def test_ram_top_3(self):
        crawler = DanawaCrawler()
        html = (FIXTURE_DIR / "category_ram.html").read_text(encoding="utf-8")
        target = CATEGORY_TARGETS[0]  # RAM

        with patch.object(crawler, "_fetch_with_retry", return_value=html):
            results = crawler._crawl_category(target)

        assert len(results) == target.top_n
        for raw in results:
            assert raw.site == "danawa"
            assert raw.category == "RAM"
            assert raw.price > 0
            assert raw.product_name != ""

    @skip_no_fixtures
    def test_ssd_top_3(self):
        crawler = DanawaCrawler()
        html = (FIXTURE_DIR / "category_ssd.html").read_text(encoding="utf-8")
        target = CATEGORY_TARGETS[1]  # SSD

        with patch.object(crawler, "_fetch_with_retry", return_value=html):
            results = crawler._crawl_category(target)

        assert len(results) == target.top_n
        for raw in results:
            assert raw.site == "danawa"
            assert raw.category == "SSD"
            assert raw.price > 0


class TestFullCrawl:
    """Test the full crawl() method with mocked HTTP."""

    @skip_no_fixtures
    def test_full_crawl_returns_10_products(self):
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
            results = crawler.crawl()

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
            assert raw.price > 0
            assert raw.product_name != ""
            assert raw.url != ""
