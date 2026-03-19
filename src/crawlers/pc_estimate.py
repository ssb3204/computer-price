"""Crawler for pc-estimate.com (견적왕)."""

from datetime import datetime, timezone

from bs4 import BeautifulSoup

from src.common.models import RawPrice
from src.crawlers.base import BaseCrawler
from src.crawlers.parser_utils import classify_category, normalize_product_name, parse_korean_price

PC_ESTIMATE_CATEGORIES: dict[str, str] = {
    "CPU": "cpu",
    "GPU": "vga",
    "RAM": "memory",
    "SSD": "ssd",
    "Mainboard": "mainboard",
}


class PCEstimateCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "pc_estimate"

    def get_target_urls(self) -> list[str]:
        urls = []
        for cat_slug in PC_ESTIMATE_CATEGORIES.values():
            for page in range(1, 4):
                urls.append(
                    f"https://pc-estimate.com/category/{cat_slug}?page={page}"
                )
        return urls

    def parse_page(self, html: str, url: str) -> list[RawPrice]:
        soup = BeautifulSoup(html, "html.parser")
        prices: list[RawPrice] = []
        now = datetime.now(timezone.utc)

        product_items = soup.select(".product-item, .estimate-item, .parts-list tr")

        for item in product_items:
            name_el = item.select_one(".product-name a, .item-title a, td.name a")
            price_el = item.select_one(".product-price, .item-price, td.price")

            if not name_el or not price_el:
                continue

            name = name_el.get_text(strip=True)
            price_val = parse_korean_price(price_el.get_text(strip=True))

            if not name or price_val is None:
                continue

            link = name_el.get("href", "")
            product_url = (
                f"https://pc-estimate.com{link}" if link.startswith("/") else link
            )

            prices.append(RawPrice(
                product_name=name,
                category=classify_category(name),
                brand=None,
                site="pc_estimate",
                price=price_val,
                url=product_url,
                crawled_at=now,
            ))

        return prices
