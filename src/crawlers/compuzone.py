"""Crawler for compuzone.co.kr."""

from datetime import datetime, timezone

from bs4 import BeautifulSoup

from src.common.models import RawPrice
from src.crawlers.base import BaseCrawler
from src.crawlers.parser_utils import classify_category, normalize_product_name, parse_korean_price

COMPUZONE_CATEGORIES: dict[str, str] = {
    "CPU": "CPU",
    "GPU": "VGA",
    "RAM": "RAM",
    "SSD": "SSD",
    "Mainboard": "M/B",
}


class CompuzoneCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "compuzone"

    def get_target_urls(self) -> list[str]:
        urls = []
        for cat_code in COMPUZONE_CATEGORIES.values():
            for page in range(1, 4):
                urls.append(
                    f"https://www.compuzone.co.kr/product/product_list.htm?BigDivCode=PMB&MedijDivCode={cat_code}&page={page}"
                )
        return urls

    def parse_page(self, html: str, url: str) -> list[RawPrice]:
        soup = BeautifulSoup(html, "html.parser")
        prices: list[RawPrice] = []
        now = datetime.now(timezone.utc)

        product_items = soup.select(".product-list .item, .prod_list li, .product_wrap .product_item")

        for item in product_items:
            name_el = item.select_one(".prd_name a, .prod_name a, .item_name a")
            price_el = item.select_one(".prd_price strong, .price em, .item_price strong")

            if not name_el or not price_el:
                continue

            name = name_el.get_text(strip=True)
            price_val = parse_korean_price(price_el.get_text(strip=True))

            if not name or price_val is None:
                continue

            link = name_el.get("href", "")
            product_url = (
                f"https://www.compuzone.co.kr{link}" if link.startswith("/") else link
            )

            prices.append(RawPrice(
                product_name=name,
                category=classify_category(name),
                brand=None,
                site="compuzone",
                price=price_val,
                url=product_url,
                crawled_at=now,
            ))

        return prices
