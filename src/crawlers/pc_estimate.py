"""Crawler for kjwwang.com (견적왕) — AJAX POST-based product list.

견적왕 loads product data via POST to product_list_include_plist.php.
Each category returns 10 items per page.
We take top-N from each category sorted by popularity.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from src.common.models import RawCrawledPrice
from src.crawlers.base import BaseCrawler

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

    def _fetch_category_html(self, target: TargetCategory) -> str | None:
        """카테고리 페이지 POST 요청 → HTML 반환. 실패 시 None."""
        self._rate_limit()
        form_data = {
            "depth": "2", "cate1": "2", "cate2": target.cate2,
            "page": "1", "list_sort_type": "popularProduct",
            "view_type": "list",
        }
        headers = {
            "Referer": f"https://kjwwang.com/shop/product_list.html?cate1=2&cate2={target.cate2}",
        }
        try:
            resp = self._session.post(LIST_URL, data=form_data, headers=headers, timeout=30)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            return resp.text
        except requests.RequestException:
            logger.exception("Failed to fetch %s category %s", self.site_name, target.category)
            return None

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

            for item in soup.select("li.list"):
                if count >= target.top_n:
                    break
                name_tag = item.select_one("a.name")
                if not name_tag:
                    continue
                price_tag = item.select_one("span.card")
                if not price_tag:
                    continue

                href = name_tag.get("href", "")
                product_url = f"{DETAIL_BASE}{href}" if href.startswith("/") else ""

                all_raw.append(RawCrawledPrice(
                    site="pc_estimate", category=target.category,
                    product_name=name_tag.get_text(strip=True),
                    price_text=price_tag.get_text(strip=True),
                    brand=None, url=product_url,
                    crawled_at=now,
                ))
                count += 1

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
