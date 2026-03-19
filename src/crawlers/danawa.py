"""Crawler for shop.danawa.com."""

from datetime import datetime, timezone

from bs4 import BeautifulSoup

from src.common.models import RawPrice
from src.crawlers.base import BaseCrawler
from src.crawlers.parser_utils import classify_category, normalize_product_name, parse_korean_price

# Danawa category IDs for computer parts
DANAWA_CATEGORIES: dict[str, str] = {
    "CPU": "112747",
    "GPU": "112753",
    "RAM": "112752",
    "SSD": "112760",
    "Mainboard": "112751",
}


class DanawaCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "danawa"

    def get_target_urls(self) -> list[str]:
        urls = []
        for cat_id in DANAWA_CATEGORIES.values():
            for page in range(1, 4):  # First 3 pages per category
                urls.append(
                    f"https://shop.danawa.com/virtualestimate/?controller=estimateMain&methods=lists&categoryCode={cat_id}&page={page}"
                )
        return urls

    def parse_page(self, html: str, url: str) -> list[RawPrice]:
        soup = BeautifulSoup(html, "html.parser")
        prices: list[RawPrice] = []
        now = datetime.now(timezone.utc)

        product_items = soup.select(".prod_item, .product_list li, .main_prodlist .prod_info")

        for item in product_items:
            name_el = item.select_one(".prod_name a, .prod_tit a, .info_tit a")
            price_el = item.select_one(".price_sect strong, .prod_pricelist .price_sect em")

            if not name_el or not price_el:
                continue

            name = name_el.get_text(strip=True)
            price_val = parse_korean_price(price_el.get_text(strip=True))

            if not name or price_val is None:
                continue

            link = name_el.get("href", "")
            product_url = f"https://shop.danawa.com{link}" if link.startswith("/") else link

            prices.append(RawPrice(
                product_name=name,
                category=classify_category(name),
                brand=None,
                site="danawa",
                price=price_val,
                url=product_url,
                crawled_at=now,
            ))

        return prices
