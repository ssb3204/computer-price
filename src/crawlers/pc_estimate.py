"""Crawler for kjwwang.com (견적왕) — WATCHLIST 기반 크롤링.

크롤링 대상은 stg_watchlist 테이블에서 동적으로 로드.
카테고리(cate2) + 검색어로 POST 검색 후 pd_no로 정확한 상품을 찾아 가격 수집.
"""

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from bs4 import BeautifulSoup
from pymysql.connections import Connection

from src.common.models import RawCrawledPrice
from src.crawlers.base import REQUEST_TIMEOUT, BaseCrawler

logger = logging.getLogger(__name__)

LIST_URL = "https://kjwwang.com/skin/shop/basic/product_list_include_plist.php"
SEARCH_TOKEN_URL = "https://kjwwang.com/shop/product_search.html"
DETAIL_BASE = "https://kjwwang.com"
MAX_SEARCH_PAGES = 5

CATEGORY_TO_CATE2: dict[str, str] = {
    "CPU": "9",
    "GPU": "12",
    "RAM": "10",
    "SSD": "243",
}


@dataclass(frozen=True)
class SearchResult:
    """견적왕 검색 결과 단일 항목."""
    pd_no: str
    product_name: str
    url: str


def _extract_pd_no(href: str) -> str | None:
    match = re.search(r"pd_no=(\d+)", href)
    return match.group(1) if match else None


def _get_search_token(session: requests.Session, query: str) -> str | None:
    """product_search.html 최초 요청에서 세션 종속 검색 토큰(search_query)을 얻는다.

    실측 확인: 이 토큰 없이 action=pc_estimate_keyword 를 호출하면 검색어/카테고리가
    맞아도 결과가 항상 0건이다. 사이트 검색창(main_search)이 실제로 쓰는 흐름을
    그대로 재현한다 — 이 페이지는 카테고리별 매칭 개수를 보여주는 탭도 함께 내려주는데,
    그 탭에 적힌 개수(예: "그래픽카드 (28)")가 실제 사이트 검색 결과와 정확히 일치함을
    확인했다(요청받은 버그 리포트의 "9070 검색 시 29개" — 전체탭 기준 개수).
    """
    try:
        resp = session.post(SEARCH_TOKEN_URL, data={"main_search": query}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        resp.encoding = "euc-kr"
    except requests.RequestException as e:
        logger.error("검색 토큰 요청 실패: %s", e)
        return None
    match = re.search(r'id="search_query"[^>]*value="([^"]*)"', resp.text)
    return match.group(1) if match else None


def search_products(query: str, category: str, max_results: int = 10) -> list[SearchResult]:
    """카테고리 + 검색어로 견적왕을 검색해 매칭되는 상품 목록을 반환한다.

    kjwwang.com의 진짜 키워드 검색(action=pc_estimate_keyword + search_cate)을 쓴다.
    카테고리(cate1/cate2)만으로 목록을 가져와 이름을 클라이언트에서 문자열로 걸러내던
    이전 방식은, 카테고리 전체가 40페이지 넘게 있고 매칭 상품은 페이지 곳곳에 드문드문
    있어 앞쪽 몇 페이지만 봐서는 실제 검색 결과 대비 턱없이 적게 나오는 문제가 있었다.
    search_cate 코드는 CATEGORY_TO_CATE2 와 동일하다(실측 확인).

    Args:
        query: 검색어 (예: "RTX 5080", "라이젠 7800X3D")
        category: 카테고리 ("CPU" | "GPU" | "RAM" | "SSD")
        max_results: 최대 반환 개수

    Returns:
        SearchResult 리스트 (최대 max_results개)
    """
    cate = CATEGORY_TO_CATE2.get(category.upper())
    if cate is None:
        logger.warning("지원하지 않는 카테고리: %s", category)
        return []

    session = requests.Session()
    token = _get_search_token(session, query)
    if token is None:
        return []

    results: list[SearchResult] = []

    for page in range(1, MAX_SEARCH_PAGES + 1):
        form_data = {
            "sort": "",
            "action": "pc_estimate_keyword",
            "search_word": query,
            "search1": "",
            "search_query": token,
            "search_cate": cate,
            "sprice": "",
            "eprice": "",
            "page": str(page),
            "list_sort_type": "",
            "view_type": "list",
            "timeid": "0",
        }
        try:
            resp = session.post(
                LIST_URL, data=form_data, timeout=REQUEST_TIMEOUT, headers={"Referer": SEARCH_TOKEN_URL}
            )
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("search_products 요청 실패(page=%d): %s", page, e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.list")
        if not items:
            break

        for item in items:
            name_tag = item.select_one("a.name")
            if not name_tag:
                continue
            product_name = name_tag.get_text(separator=" ", strip=True)
            href = name_tag.get("href", "")
            pd_no = _extract_pd_no(href)
            if not pd_no:
                continue
            price_tag = item.select_one("span.card")
            price_text = price_tag.get_text(strip=True) if price_tag else ""
            if not price_text or price_text == "0":
                continue
            product_url = f"{DETAIL_BASE}{href}" if href.startswith("/") else ""
            results.append(SearchResult(
                pd_no=pd_no,
                product_name=product_name,
                url=product_url,
            ))
            if len(results) >= max_results:
                return results

    return results


def crawl_single(
    query: str, pd_no: str, category: str, brand: str | None = None
) -> RawCrawledPrice | None:
    """단일 상품 즉시 크롤링 — WATCHLIST 추가 직후 호출용."""
    cate2 = CATEGORY_TO_CATE2.get(category.upper())
    if cate2 is None:
        logger.warning("지원하지 않는 카테고리: %s", category)
        return None

    session = requests.Session()
    for page in range(1, MAX_SEARCH_PAGES + 1):
        form_data = {
            "depth": "2", "cate1": "2", "cate2": cate2,
            "search_word": query, "page": str(page), "view_type": "list",
        }
        try:
            resp = session.post(LIST_URL, data=form_data, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("kjwwang crawl_single 실패: %s", e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.list")
        if not items:
            break

        now = datetime.now(UTC)
        for item in items:
            name_tag = item.select_one("a.name")
            if not name_tag:
                continue
            href = name_tag.get("href", "")
            if _extract_pd_no(href) != pd_no:
                continue
            price_tag = item.select_one("span.card")
            if not price_tag:
                break
            product_url = f"{DETAIL_BASE}{href}" if href.startswith("/") else ""
            return RawCrawledPrice(
                site="kjwwang", category=category,
                product_name=name_tag.get_text(separator=" ", strip=True),
                price_text=price_tag.get_text(strip=True),
                brand=brand, url=product_url, crawled_at=now,
            )
    logger.warning("kjwwang crawl_single: pd_no=%s 미발견", pd_no)
    return None


class PCEstimateCrawler(BaseCrawler):
    def __init__(self, conn: Connection) -> None:
        super().__init__()
        self._conn = conn

    @property
    def site_name(self) -> str:
        return "kjwwang"

    def _load_watch_products(self) -> list[dict]:
        """WATCHLIST에서 kjwwang 활성 크롤링 대상 로드."""
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT `query`, `pcode`, `category`, `brand` "
                "FROM `stg_watchlist` WHERE `is_active` = 1 AND `site` = '견적왕'"
            )
            return [
                {"query": row[0], "pd_no": row[1], "category": row[2], "brand": row[3]}
                for row in cur.fetchall()
            ]
        finally:
            cur.close()

    def _fetch_search_html(self, query: str, cate2: str, page: int = 1) -> str | None:
        self._rate_limit()
        form_data = {
            "depth": "2", "cate1": "2", "cate2": cate2,
            "search_word": query,
            "page": str(page),
            "view_type": "list",
        }
        try:
            resp = self._session.post(LIST_URL, data=form_data, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            return resp.text
        except requests.RequestException:
            logger.exception("Failed to search %s for query=%s", self.site_name, query)
            return None

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — WATCHLIST 기반."""
        targets = self._load_watch_products()
        all_raw: list[RawCrawledPrice] = []

        for target in targets:
            cate2 = CATEGORY_TO_CATE2.get(target["category"])
            if cate2 is None:
                logger.warning("지원하지 않는 카테고리: %s", target["category"])
                continue

            found = False
            for page in range(1, MAX_SEARCH_PAGES + 1):
                html = self._fetch_search_html(target["query"], cate2, page)
                if html is None:
                    break

                soup = BeautifulSoup(html, "html.parser")
                items = soup.select("li.list")
                if not items:
                    break

                now = datetime.now(UTC)
                for item in items:
                    name_tag = item.select_one("a.name")
                    if not name_tag:
                        continue
                    href = name_tag.get("href", "")
                    pd_no = _extract_pd_no(href)
                    if pd_no != target["pd_no"]:
                        continue

                    price_tag = item.select_one("span.card")
                    if not price_tag:
                        break
                    product_url = f"{DETAIL_BASE}{href}" if href.startswith("/") else ""
                    all_raw.append(RawCrawledPrice(
                        site="kjwwang",
                        category=target["category"],
                        product_name=name_tag.get_text(separator=" ", strip=True),
                        price_text=price_tag.get_text(strip=True),
                        brand=target["brand"],
                        url=product_url,
                        crawled_at=now,
                    ))
                    found = True
                    break

                if found:
                    break

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
