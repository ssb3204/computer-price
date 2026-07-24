"""Crawler for shop.danawa.com — pcode 기반 검색.

크롤링 대상은 stg_watchlist 테이블에서 동적으로 로드.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
from pymysql.connections import Connection

from src.common.models import RawCrawledPrice
from src.crawlers.base import DEFAULT_HEADERS, BaseCrawler

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
    return el.get_text(separator=" ", strip=True) if el else None


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


# 검색 URL에 &cate= 를 붙이는 건 실제로 필터링 효과가 없다(실측 확인됨).
# 대신 각 상품 링크 자체에 붙은 cate= 코드로 사후 필터링한다.
# 완제PC/베어본 등은 다른 카테고리 코드(예: 11316681, 112756)를 가지므로 자연히 제외된다.
_CATEGORY_TO_CATE: dict[str, set[str]] = {
    "CPU": {"113990", "113973"},  # AMD, Intel
    "GPU": {"112753"},
    "RAM": {"112752"},
    "SSD": {"112760"},
}


def _extract_cate(item: Tag) -> str | None:
    link = item.select_one(".prod_name a[href]")
    if link:
        match = re.search(r"cate=(\d+)", link.get("href", ""))
        if match:
            return match.group(1)
    return None


@dataclass(frozen=True)
class SearchResult:
    """다나와 검색 결과 단일 항목."""
    pcode: str
    product_name: str
    url: str


def _fetch_product_title(pcode: str) -> str | None:
    """상세 페이지 <title>에서 용량 포함 전체 상품명을 추출한다.

    <title> 태그가 나올 때까지만 읽어 네트워크 비용을 줄인다.
    """
    try:
        resp = requests.get(
            f"{PRODUCT_BASE}{pcode}",
            headers=DEFAULT_HEADERS,
            timeout=10,
            stream=True,
        )
        resp.raise_for_status()
        buf = b""
        for chunk in resp.iter_content(chunk_size=4096):
            buf += chunk
            if b"</title>" in buf:
                resp.close()
                break
        text = buf.decode("utf-8", errors="ignore")
    except requests.RequestException:
        return None

    m = re.search(r"<title>(.+?)</title>", text, re.DOTALL)
    if not m:
        return None
    name = m.group(1).strip()
    return re.sub(r"\s*:\s*다나와.*$", "", name).strip()


def enrich_names_from_detail(results: list[SearchResult]) -> list[SearchResult]:
    """검색 결과 상품명을 상세 페이지 title로 교체해 용량 정보를 포함시킨다.

    병렬 fetch로 지연을 최소화한다.
    """
    with ThreadPoolExecutor(max_workers=5) as executor:
        pcode_to_future = {r.pcode: executor.submit(_fetch_product_title, r.pcode) for r in results}
        enriched: dict[str, str] = {}
        for pcode, future in pcode_to_future.items():
            title = future.result()
            if title:
                enriched[pcode] = title

    return [
        SearchResult(pcode=r.pcode, product_name=enriched.get(r.pcode, r.product_name), url=r.url)
        for r in results
    ]


MAX_SEARCH_PAGES = 5


def search_products(query: str, max_results: int = 10, category: str | None = None) -> list[SearchResult]:
    """제품명으로 다나와를 검색해 매칭되는 상품 목록을 반환한다.

    다나와 검색 결과는 여러 페이지(&page=N)로 나뉘어 있어, 1페이지에서
    max_results를 못 채우면 다음 페이지를 이어서 조회한다(최대 MAX_SEARCH_PAGES).
    페이지에 li.prod_item 자체가 없으면(마지막 페이지 도달) 순회를 멈춘다.

    Args:
        query: 검색어 (예: "RTX 5080", "라이젠 7800X3D")
        max_results: 최대 반환 개수
        category: 카테고리 ("CPU" | "GPU" | "RAM" | "SSD") — 지정 시 해당 PC 부품 카테고리로 제한

    Returns:
        SearchResult 리스트 (최대 max_results개)
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    allowed_cates = _CATEGORY_TO_CATE.get(category.upper()) if category else None

    results: list[SearchResult] = []

    for page in range(1, MAX_SEARCH_PAGES + 1):
        url = f"{SEARCH_URL}?query={query}&tab=goods&page={page}"
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            resp.encoding = "utf-8"
        except requests.RequestException as e:
            logger.error("search_products 요청 실패(page=%d): %s", page, e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.prod_item")
        if not items:
            break

        for item in items:
            if not _is_real_product(item):
                continue
            if allowed_cates is not None and _extract_cate(item) not in allowed_cates:
                continue
            pcode = _extract_pcode(item)
            name = _extract_name(item)
            if pcode is None or name is None:
                continue
            product_url = _extract_url(item) or f"{PRODUCT_BASE}{pcode}"
            results.append(SearchResult(pcode=pcode, product_name=name, url=product_url))
            if len(results) >= max_results:
                return results

    return results


def crawl_single(
    query: str, pcode: str, category: str, brand: str | None = None
) -> RawCrawledPrice | None:
    """단일 상품 즉시 크롤링 — WATCHLIST 추가 직후 호출용."""
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    url = f"{SEARCH_URL}?query={query}&tab=goods"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        resp.encoding = "utf-8"
    except requests.RequestException as e:
        logger.error("danawa crawl_single 실패: %s", e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    now = datetime.now(UTC)
    for item in soup.select("li.prod_item"):
        if not _is_real_product(item):
            continue
        if _extract_pcode(item) != pcode:
            continue
        name = _extract_name(item)
        price_text = _extract_price_text(item)
        if name is None or price_text is None:
            break
        product_url = _extract_url(item) or f"{PRODUCT_BASE}{pcode}"
        return RawCrawledPrice(
            site="danawa", category=category,
            product_name=name, price_text=price_text,
            brand=brand, url=product_url, crawled_at=now,
        )
    logger.warning("danawa crawl_single: pcode=%s 미발견", pcode)
    return None


class DanawaCrawler(BaseCrawler):
    def __init__(self, conn: Connection) -> None:
        super().__init__()
        self._conn = conn

    @property
    def site_name(self) -> str:
        return "danawa"

    def _load_watch_products(self) -> list[dict]:
        """WATCHLIST에서 활성 크롤링 대상 로드."""
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT `query`, `pcode`, `category`, `brand` "
                "FROM `stg_watchlist` WHERE `is_active` = 1 AND `site` = '다나와'"
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
            now = datetime.now(UTC)

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
