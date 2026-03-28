"""Crawler for shop.danawa.com — search & category pages.

Two strategies:
  1. Search by pcode (CPU, GPU) — exact product lookup via search page
  2. Category ranking (RAM, SSD) — top-N from category listing page
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from bs4 import BeautifulSoup, Tag

from src.common.models import RawCrawledPrice
from src.crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)

SEARCH_URL = "https://search.danawa.com/dsearch.php"
CATEGORY_URL = "https://prod.danawa.com/list/"
PRODUCT_BASE = "https://prod.danawa.com/info/?pcode="


@dataclass(frozen=True)
class TargetProduct:
    """A product to track by exact pcode from search results."""
    pcode: str
    query: str
    category: str
    brand: str


@dataclass(frozen=True)
class TargetCategory:
    """A category page from which to take top-N products by ranking."""
    cate_id: str
    category: str
    top_n: int


# ── Target configuration ──

PCODE_TARGETS: tuple[TargetProduct, ...] = (
    TargetProduct(pcode="19627934", query="라이젠 7800X3D", category="CPU", brand="AMD"),
    TargetProduct(pcode="77379452", query="RTX 5070", category="GPU", brand="NVIDIA"),
    TargetProduct(pcode="76464143", query="RTX 5070 Ti", category="GPU", brand="NVIDIA"),
    TargetProduct(pcode="77381483", query="RX 9070 XT", category="GPU", brand="AMD"),
)

CATEGORY_TARGETS: tuple[TargetCategory, ...] = (
    TargetCategory(cate_id="112752", category="RAM", top_n=3),
    TargetCategory(cate_id="112760", category="SSD", top_n=3),
)


def _is_real_product(item: Tag) -> bool:
    """Return True if the list item is a real product (not an ad)."""
    item_id = item.get("id", "")
    return isinstance(item_id, str) and item_id.startswith("productItem")


def _extract_pcode(item: Tag) -> str | None:
    """Extract numeric pcode from a li.prod_item element."""
    item_id = item.get("id", "")
    if isinstance(item_id, str) and item_id.startswith("productItem"):
        code = item_id.removeprefix("productItem")
        return code if code.isdigit() else None

    link = item.select_one(".prod_name a[href]")
    if link:
        match = re.search(r"pcode=(\d+)", link.get("href", ""))
        if match:
            return match.group(1)
    return None


def _extract_name(item: Tag) -> str | None:
    el = item.select_one(".prod_name a")
    return el.get_text(strip=True) if el else None


def _extract_price_text(item: Tag) -> str | None:
    """가격 원본 텍스트를 그대로 반환."""
    el = item.select_one(".price_sect strong")
    return el.get_text(strip=True) if el else None


def _extract_url(item: Tag) -> str:
    link = item.select_one(".prod_name a[href]")
    if link:
        href = link.get("href", "")
        if href.startswith("http"):
            return href
    return ""


class DanawaCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "danawa"

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — 가격을 원본 텍스트로 보존."""
        all_raw: list[RawCrawledPrice] = []

        for target in PCODE_TARGETS:
            url = f"{SEARCH_URL}?query={target.query}&tab=goods"
            html = self._fetch_with_retry(url)
            if html is None:
                continue
            soup = BeautifulSoup(html, "html.parser")
            now = datetime.now(timezone.utc)

            for item in soup.select("li.prod_item"):
                if not _is_real_product(item):
                    continue
                pcode = _extract_pcode(item)
                if pcode != target.pcode:
                    continue
                name = _extract_name(item)
                price_text = _extract_price_text(item)
                if name is None or price_text is None:
                    break
                product_url = _extract_url(item) or f"{PRODUCT_BASE}{target.pcode}"
                all_raw.append(RawCrawledPrice(
                    site="danawa", category=target.category,
                    product_name=name, price_text=price_text,
                    brand=target.brand, url=product_url,
                    crawled_at=now,
                ))
                break

        for target in CATEGORY_TARGETS:
            url = f"{CATEGORY_URL}?cate={target.cate_id}"
            html = self._fetch_with_retry(url)
            if html is None:
                continue
            soup = BeautifulSoup(html, "html.parser")
            now = datetime.now(timezone.utc)
            count = 0

            for item in soup.select("li.prod_item"):
                if not _is_real_product(item):
                    continue
                if count >= target.top_n:
                    break
                name = _extract_name(item)
                price_text = _extract_price_text(item)
                if name is None or price_text is None:
                    continue
                product_url = _extract_url(item) or (
                    f"{PRODUCT_BASE}{_extract_pcode(item)}" if _extract_pcode(item) else ""
                )
                all_raw.append(RawCrawledPrice(
                    site="danawa", category=target.category,
                    product_name=name, price_text=price_text,
                    brand=None, url=product_url,
                    crawled_at=now,
                ))
                count += 1

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
