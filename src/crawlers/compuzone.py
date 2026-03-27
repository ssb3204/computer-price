"""Crawler for compuzone.co.kr — AJAX POST-based product list.

Compuzone loads product data via POST to product_list.php.
Each category returns up to 20 items per page (ScrollPage).
We take top-N from each category sorted by recommendation.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from src.common.models import RawCrawledPrice, RawPrice
from src.crawlers.base import BaseCrawler
from src.crawlers.parser_utils import parse_korean_price

logger = logging.getLogger(__name__)

LIST_URL = "https://www.compuzone.co.kr/product/product_list.php"
DETAIL_BASE = "https://www.compuzone.co.kr/product/product_detail.htm"


@dataclass(frozen=True)
class TargetCategory:
    """A compuzone category to crawl top-N products from."""
    medium_div_no: str
    category: str
    top_n: int


CATEGORY_TARGETS: tuple[TargetCategory, ...] = (
    TargetCategory(medium_div_no="1012", category="CPU", top_n=3),
    TargetCategory(medium_div_no="1016", category="GPU", top_n=3),
    TargetCategory(medium_div_no="1014", category="RAM", top_n=3),
    TargetCategory(medium_div_no="1276", category="SSD", top_n=3),
)


class CompuzoneCrawler(BaseCrawler):
    @property
    def site_name(self) -> str:
        return "compuzone"

    def get_target_urls(self) -> list[str]:
        """Not used directly — crawl() is overridden."""
        return [LIST_URL]

    def parse_page(self, html: str, url: str) -> list[RawPrice]:
        """Fallback: not used directly since crawl() is overridden."""
        return self._parse_list_html(html, category="Unknown", top_n=20)

    def _fetch_category_html(self, target: TargetCategory) -> str | None:
        """카테고리 페이지 POST 요청 → HTML 반환. 실패 시 None."""
        self._rate_limit()
        form_data = {
            "actype": "getList", "BigDivNo": "4",
            "MediumDivNo": target.medium_div_no, "DivNo": "0",
            "PageCount": "20", "StartNum": "0", "PageNum": "1",
            "PreOrder": "recommand", "lvm": "L", "ps_po": "P",
            "ScrollPage": "1", "ProductType": "list",
            "PageType": "ProductList", "setPricechk": "N",
        }
        try:
            resp = self._session.post(LIST_URL, data=form_data, timeout=30)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            return resp.text
        except requests.RequestException:
            logger.exception("Failed to fetch %s category %s", self.site_name, target.category)
            return None

    def crawl(self) -> list[RawPrice]:
        """Override base crawl to use POST requests per category."""
        all_prices: list[RawPrice] = []

        for target in CATEGORY_TARGETS:
            html = self._fetch_category_html(target)
            if html is None:
                continue
            all_prices.extend(self._parse_list_html(html, target.category, target.top_n))

        logger.info("Crawled %d total prices from %s", len(all_prices), self.site_name)
        return all_prices

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — 가격을 원본 텍스트로 보존."""
        all_raw: list[RawCrawledPrice] = []

        for target in CATEGORY_TARGETS:
            html = self._fetch_category_html(target)
            if html is None:
                continue

            soup = BeautifulSoup(html, "html.parser")
            now = datetime.now(timezone.utc)
            count = 0

            for item in soup.select("li.li-obj"):
                if count >= target.top_n:
                    break
                name_tag = item.select_one("a.prd_info_name")
                if not name_tag:
                    continue
                price_div = item.select_one("div.prd_price")
                if not price_div:
                    continue
                raw_price = price_div.get("data-price")
                if not raw_price:
                    continue

                pno = item.get("id", "").replace("li-pno-", "")
                product_url = (
                    f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo="
                    if pno else ""
                )
                all_raw.append(RawCrawledPrice(
                    site="compuzone", category=target.category,
                    product_name=name_tag.get_text(strip=True),
                    price_text=raw_price,
                    brand=None, url=product_url,
                    crawled_at=now,
                ))
                count += 1

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw

    def _parse_list_html(self, html: str, category: str, top_n: int) -> list[RawPrice]:
        """Parse product list HTML and return top-N RawPrice items."""
        soup = BeautifulSoup(html, "html.parser")
        now = datetime.now(timezone.utc)
        prices: list[RawPrice] = []

        for item in soup.select("li.li-obj"):
            if len(prices) >= top_n:
                break

            name_tag = item.select_one("a.prd_info_name")
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)

            price_div = item.select_one("div.prd_price")
            if not price_div:
                continue
            raw_price = price_div.get("data-price")
            if not raw_price:
                continue
            price = parse_korean_price(raw_price)
            if price is None:
                continue

            # Product number from li id (e.g. "li-pno-1187400")
            pno = item.get("id", "").replace("li-pno-", "")
            product_url = (
                f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo="
                if pno else ""
            )

            prices.append(RawPrice(
                product_name=name,
                category=category,
                brand=None,
                site="compuzone",
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
