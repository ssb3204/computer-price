"""Crawler for kjwwang.com (견적왕) — AJAX POST-based product list.

견적왕 loads product data via POST to product_list_include_plist.php.
Each category returns 10 items per page.
We take top-N from each category sorted by popularity.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from src.common.models import RawPrice
from src.crawlers.base import BaseCrawler
from src.crawlers.parser_utils import parse_korean_price

logger = logging.getLogger(__name__)

LIST_URL = "https://kjwwang.com/skin/shop/basic/product_list_include_plist.php"
DETAIL_BASE = "https://kjwwang.com"


@dataclass(frozen=True)
class TargetCategory:
    """A 견적왕 category to crawl top-N products from."""
    cate2: str
    category: str
    top_n: int


CATEGORY_TARGETS: tuple[TargetCategory, ...] = (
    TargetCategory(cate2="9", category="CPU", top_n=3),
    TargetCategory(cate2="12", category="GPU", top_n=3),
    TargetCategory(cate2="10", category="RAM", top_n=3),
    TargetCategory(cate2="243", category="SSD", top_n=3),
)


class PCEstimateCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "pc_estimate"

    def get_target_urls(self) -> list[str]:
        """Not used directly — crawl() is overridden."""
        return [LIST_URL]

    def parse_page(self, html: str, url: str) -> list[RawPrice]:
        """Fallback: not used directly since crawl() is overridden."""
        return self._parse_list_html(html, category="Unknown", top_n=10)

    def crawl(self) -> list[RawPrice]:
        """Override base crawl to use POST requests per category."""
        all_prices: list[RawPrice] = []

        for target in CATEGORY_TARGETS:
            prices = self._crawl_category(target)
            all_prices.extend(prices)

        logger.info("Crawled %d total prices from %s", len(all_prices), self.site_name)
        return all_prices

    def _crawl_category(self, target: TargetCategory) -> list[RawPrice]:
        """Fetch one category page via POST and extract top-N products."""
        self._rate_limit()

        form_data = {
            "depth": "2",
            "cate1": "2",
            "cate2": target.cate2,
            "page": "1",
            "list_sort_type": "popularProduct",
            "view_type": "list",
        }
        headers = {
            "Referer": f"https://kjwwang.com/shop/product_list.html?cate1=2&cate2={target.cate2}",
        }

        try:
            resp = self._session.post(LIST_URL, data=form_data, headers=headers, timeout=30)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except Exception:
            logger.exception("Failed to fetch %s category %s", self.site_name, target.category)
            return []

        return self._parse_list_html(resp.text, target.category, target.top_n)

    def _parse_list_html(self, html: str, category: str, top_n: int) -> list[RawPrice]:
        """Parse product list HTML and return top-N RawPrice items."""
        soup = BeautifulSoup(html, "html.parser")
        now = datetime.now(timezone.utc)
        prices: list[RawPrice] = []

        for item in soup.select("li.list"):
            if len(prices) >= top_n:
                break

            name_tag = item.select_one("a.name")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            price_tag = item.select_one("span.card")
            if not price_tag:
                continue
            price = parse_korean_price(price_tag.get_text(strip=True))
            if price is None:
                continue

            href = name_tag.get("href", "")
            product_url = f"{DETAIL_BASE}{href}" if href else ""

            prices.append(RawPrice(
                product_name=name,
                category=category,
                brand=None,
                site="pc_estimate",
                price=price,
                url=product_url,
                crawled_at=now,
            ))
            logger.info("Found %s #%d: %s = %d원", category, len(prices), name[:40], price)

        if len(prices) < top_n:
            logger.warning(
                "Only found %d/%d products for %s",
                len(prices), top_n, category,
            )
        return prices
