"""Crawler for kjwwang.com (견적왕) — WATCHLIST 기반 크롤링.

크롤링 대상은 stg_watchlist 테이블에서 동적으로 로드.
검색어로 토큰을 받아 검색한 뒤 pd_no로 정확한 상품을 찾아 가격 수집한다
(다나와·컴퓨존과 동일한 단일 경로 구조).

요청 계약은 실측으로 확정했다 — 12개 필드를 하나씩 빼보고 확인:
  search_query : 필수. 없으면 0건.
  search_cate  : 필수. 없으면 다른 카테고리 상품에 밀려 대상이 페이지 밖으로 나간다
                 (삼성 RAM 40개 중 38개가 5페이지 안에서 사라졌다).
  page         : 필수. 없으면 1페이지 고정.
나머지(sort/action/search_word/search1/sprice/eprice/list_sort_type/
view_type/timeid)는 서버가 읽지 않는다.
"""

import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlencode

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

# 폼을 미리 인코딩한 문자열로 넘기면 requests 가 Content-Type 을 붙여주지 않는다.
_FORM_HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}


def _euc_kr_body(fields: dict[str, str]) -> str:
    """폼 데이터를 EUC-KR로 인코딩한다.

    견적왕은 charset=euc-kr 사이트다. requests 에 dict 를 넘기면 UTF-8 로 나가는데,
    서버는 그 바이트를 EUC-KR로 해석하므로 한글 검색어가 깨져 조용히 0건이 된다
    (영문·숫자는 두 인코딩이 동일해서 우연히 통과한다). 브라우저는 문서 charset 을
    보고 알아서 EUC-KR로 보내기 때문에 사람이 손으로 확인하면 늘 정상으로 보인다.
    """
    return urlencode(fields, encoding="euc-kr")


def _search_form(token: str, cate: str, page: int) -> dict[str, str]:
    """검색 요청 폼. 모듈 docstring 의 실측 결과대로 3개 필드만 보낸다."""
    return {"search_query": token, "search_cate": cate, "page": str(page)}


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
    """product_search.html 요청에서 검색어 토큰(search_query)을 얻는다.

    이 토큰이 검색어를 실어 나른다. 평문 search_word 는 서버가 읽지 않는다 —
    토큰(RTX 5080) + search_word=7800X3D 로 보내면 0건이 나온다.

    토큰은 세션이 아니라 검색어에 묶여 있다(실측): 쿠키 없는 새 세션에서도 같은
    토큰이 그대로 동작하고, 검색어가 다르면 토큰도 다르다. 따라서 세션을 유지할
    필요는 없지만 검색어마다 새로 받아야 한다.
    """
    try:
        resp = session.post(
            SEARCH_TOKEN_URL,
            data=_euc_kr_body({"main_search": query}),
            headers=_FORM_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        resp.encoding = "euc-kr"
    except requests.RequestException as e:
        logger.error("검색 토큰 요청 실패: %s", e)
        return None
    match = re.search(r'id="search_query"[^>]*value="([^"]*)"', resp.text)
    return match.group(1) if match else None


def search_products(query: str, category: str, max_results: int = 10) -> list[SearchResult]:
    """카테고리 + 검색어로 견적왕을 검색해 매칭되는 상품 목록을 반환한다.

    정기 크롤링(crawl_raw)과 같은 검색 경로를 쓴다 — 모듈 docstring 참고.

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
        try:
            resp = session.post(
                LIST_URL, data=_search_form(token, cate, page), timeout=REQUEST_TIMEOUT
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
    """단일 상품 즉시 크롤링 — WATCHLIST 추가 직후 호출용.

    정기 크롤링(crawl_raw)과 같은 검색 경로를 쓴다. 둘이 다르면 "등록할 땐
    찾았는데 스케줄링에선 못 찾는" 불일치가 생긴다.
    """
    cate2 = CATEGORY_TO_CATE2.get(category.upper())
    if cate2 is None:
        logger.warning("지원하지 않는 카테고리: %s", category)
        return None

    session = requests.Session()
    token = _get_search_token(session, query)
    if token is None:
        logger.warning("kjwwang crawl_single: 검색 토큰 획득 실패 (query=%s)", query)
        return None

    for page in range(1, MAX_SEARCH_PAGES + 1):
        try:
            resp = session.post(
                LIST_URL, data=_search_form(token, cate2, page), timeout=REQUEST_TIMEOUT
            )
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
        # 토큰은 검색어에 묶여 있으므로 페이지마다 다시 받을 필요가 없다.
        # 같은 검색어를 쓰는 대상끼리도 재사용한다.
        self._token_cache: dict[str, str] = {}

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

    def _token_for(self, query: str) -> str | None:
        """검색어별 토큰. 회차 안에서 한 번만 받는다."""
        if query not in self._token_cache:
            token = _get_search_token(self._session, query)
            if token is None:
                return None
            self._token_cache[query] = token
        return self._token_cache[query]

    def _fetch_search_html(self, query: str, cate2: str, page: int = 1) -> str | None:
        token = self._token_for(query)
        if token is None:
            logger.warning("[%s] 검색 토큰 획득 실패: %s", self.site_name, query)
            return None

        self._rate_limit()
        try:
            resp = self._session.post(
                LIST_URL, data=_search_form(token, cate2, page), timeout=REQUEST_TIMEOUT
            )
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
        # 회차 시각은 한 번만 정한다. 대상·페이지마다 now() 를 부르면 같은 회차인데
        # 상품별로 crawled_at 이 갈리고, stg_price_history 자연키가
        # (product_id, crawled_at) 이라 하위 계층의 시계열이 그만큼 어긋난다.
        now = datetime.now(UTC)

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

            if not found:
                # fallback 이 없으므로 검색 실패가 곧 그 상품의 수집 실패다.
                # 조용히 넘기면 워치리스트가 커졌을 때 부분 실패를 알 수 없다.
                logger.warning(
                    "[%s] 대상 미발견: %s (pd_no=%s)",
                    self.site_name, target["query"], target["pd_no"],
                )

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
