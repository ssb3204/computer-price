"""Crawler for shop.danawa.com — pcode 기반 검색.

크롤링 대상은 Snowflake WATCHLIST 테이블에서 동적으로 로드.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

from bs4 import BeautifulSoup, Tag
from snowflake.connector import SnowflakeConnection

from src.common.models import RawCrawledPrice
from src.crawlers.base import BaseCrawler, DEFAULT_HEADERS

logger = logging.getLogger(__name__)

SEARCH_URL = "https://search.danawa.com/dsearch.php"
PRODUCT_BASE = "https://prod.danawa.com/info/?pcode="
ALLOWED_DOMAINS = {"danawa.com", "prod.danawa.com", "search.danawa.com", "shop.danawa.com"}


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
            netloc = urlparse(href).netloc
            if netloc in ALLOWED_DOMAINS or any(netloc.endswith("." + d) for d in ALLOWED_DOMAINS):
                return href
    return ""


@dataclass(frozen=True)
class SearchResult:
    """다나와 검색 결과 단일 항목."""
    pcode: str
    product_name: str
    url: str


def search_products(query: str, max_results: int = 10) -> list[SearchResult]:
    """제품명으로 다나와를 검색해 매칭되는 상품 목록을 반환한다.

    Args:
        query: 검색어 (예: "RTX 5080", "라이젠 7800X3D")
        max_results: 최대 반환 개수

    Returns:
        SearchResult 리스트 (최대 max_results개)
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    url = f"{SEARCH_URL}?query={query}&tab=goods"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except requests.RequestException as e:
        logger.error("search_products 요청 실패: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results: list[SearchResult] = []

    for item in soup.select("li.prod_item"):
        if not _is_real_product(item):
            continue
        pcode = _extract_pcode(item)
        name = _extract_name(item)
        if pcode is None or name is None:
            continue
        product_url = _extract_url(item) or f"{PRODUCT_BASE}{pcode}"
        results.append(SearchResult(pcode=pcode, product_name=name, url=product_url))
        if len(results) >= max_results:
            break

    return results


class DanawaCrawler(BaseCrawler):
    def __init__(self, conn: SnowflakeConnection) -> None:
        super().__init__()
        self._conn = conn

    @property
    def site_name(self) -> str:
        return "danawa"

    def _load_watch_products(self) -> list[dict]:
        """WATCHLIST에서 활성 크롤링 대상 로드."""
        cur = self._conn.cursor()
        try:
            cur.execute("USE DATABASE COMPUTER_PRICE")
            cur.execute(
                "SELECT QUERY, PCODE, CATEGORY, BRAND "
                "FROM STAGING.WATCHLIST WHERE IS_ACTIVE = TRUE"
            )
            return [
                {"query": row[0], "pcode": row[1], "category": row[2], "brand": row[3]}
                for row in cur.fetchall()
            ]
        finally:
            cur.close()

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — WATCHLIST 기반."""
        targets = self._load_watch_products()
        all_raw: list[RawCrawledPrice] = []

        for target in targets:
            url = f"{SEARCH_URL}?query={target['query']}&tab=goods"
            html = self._fetch_with_retry(url)
            if html is None:
                continue
            soup = BeautifulSoup(html, "html.parser")
            now = datetime.now(timezone.utc)

            for item in soup.select("li.prod_item"):
                if not _is_real_product(item):
                    continue
                pcode = _extract_pcode(item)
                if pcode != target["pcode"]:
                    continue
                name = _extract_name(item)
                price_text = _extract_price_text(item)
                if name is None or price_text is None:
                    break
                product_url = _extract_url(item) or f"{PRODUCT_BASE}{target['pcode']}"
                all_raw.append(RawCrawledPrice(
                    site="danawa", category=target["category"],
                    product_name=name, price_text=price_text,
                    brand=target["brand"], url=product_url,
                    crawled_at=now,
                ))
                break

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
