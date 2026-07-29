"""Crawler for compuzone.co.kr — WATCHLIST 기반 크롤링.

크롤링 대상은 stg_watchlist 테이블에서 동적으로 로드.
검색어 + 카테고리로 search_list.php 를 조회해 ProductNo 로 상품을 특정한다
(다나와·견적왕과 동일한 단일 경로 구조).

브라우저에 보이는 div.prdbx 는 AJAX 렌더링 후의 DOM 이다. 크롤러가 받는 응답에는
li.li-obj 가 들어 있으므로 개발자도구 Elements 탭을 보고 셀렉터를 바꾸면 0건이 된다
— 확인은 반드시 Network 탭의 Response 로 한다.
"""

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from bs4 import BeautifulSoup, Tag
from pymysql.connections import Connection

from src.common.models import RawCrawledPrice
from src.crawlers.base import DEFAULT_HEADERS, REQUEST_TIMEOUT, BaseCrawler

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.compuzone.co.kr/search/search_list.php"
DETAIL_BASE = "https://www.compuzone.co.kr/product/product_detail.htm"
MAX_SEARCH_PAGES = 3  # 페이지당 20개 → 대상당 최대 60개 후보 스캔

CATEGORY_MEDIUM_DIV_NO: dict[str, str] = {
    "CPU": "1012",
    "GPU": "1016",
    "RAM": "1014",
    "SSD": "1276",
}

_PNO_PREFIX = "li-pno-"


@dataclass(frozen=True)
class SearchResult:
    """컴퓨존 검색 결과 단일 항목."""
    product_no: str
    product_name: str
    url: str


def _detail_url(product_no: str, medium_div_no: str) -> str:
    return f"{DETAIL_BASE}?ProductNo={product_no}&BigDivNo=4&MediumDivNo={medium_div_no}"


def _extract_product_no(el: Tag) -> str | None:
    """li 요소의 id(li-pno-<숫자>)에서 ProductNo 를 뽑는다."""
    raw_id = el.get("id", "")
    if not isinstance(raw_id, str) or not raw_id.startswith(_PNO_PREFIX):
        return None
    return raw_id.removeprefix(_PNO_PREFIX) or None


def _extract_name_price(el: Tag) -> tuple[str, str] | None:
    """상품명과 가격 원본 문자열. 가격이 없는 항목(품절 등)은 None."""
    name_tag = el.select_one("a.prd_info_name")
    price_div = el.select_one("div.prd_price")
    if name_tag is None or price_div is None:
        return None
    price_text = price_div.get("data-price")
    if not price_text:
        return None
    return name_tag.get_text(strip=True), price_text


def _build_search_params(query: str, page: int = 1) -> dict:
    return {
        "actype": "list",
        "SearchType": "small",
        "SearchText": query,
        "PreOrder": "recommand",
        "PageCount": "20",
        "StartNum": str((page - 1) * 20),
        "PageNum": str(page),
        "ListType": "0",
        "BigDivNo": "",
        "MediumDivNo": "",
        "DivNo": "",
    }


def search_products(query: str, category: str, max_results: int = 10) -> list[SearchResult]:
    """키워드로 컴퓨존 상품 목록을 반환한다 (워치리스트 등록 화면용).

    Args:
        query: 검색어 (예: "RTX 5080", "라이젠 9800X3D")
        category: 카테고리 ("CPU" | "GPU" | "RAM" | "SSD") — MediumDivNo 결정에 사용
        max_results: 최대 반환 개수

    Returns:
        SearchResult 리스트 (최대 max_results개)
    """
    medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(category.upper(), "")

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    results: list[SearchResult] = []

    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(query, page)
        if medium_div_no:
            params["MediumDivNo"] = medium_div_no
        try:
            resp = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("search_products 요청 실패 (page=%d): %s", page, e)
            break

        items = BeautifulSoup(resp.text, "html.parser").select("li.li-obj")
        if not items:
            break

        for el in items:
            product_no = _extract_product_no(el)
            parsed = _extract_name_price(el)
            if product_no is None or parsed is None:
                continue
            results.append(SearchResult(
                product_no=product_no,
                product_name=parsed[0],
                url=_detail_url(product_no, medium_div_no),
            ))
            if len(results) >= max_results:
                return results

    return results


def crawl_single(
    query: str, product_no: str, category: str, brand: str | None = None
) -> RawCrawledPrice | None:
    """단일 상품 즉시 크롤링 — WATCHLIST 추가 직후 호출용.

    정기 크롤링(crawl_raw)과 같은 검색 경로를 쓴다. 둘이 다르면 "등록할 땐
    찾았는데 스케줄링에선 못 찾는" 불일치가 생긴다.
    """
    medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(category.upper())
    if medium_div_no is None:
        logger.warning("compuzone crawl_single: 지원하지 않는 카테고리 %s", category)
        return None

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    now = datetime.now(UTC)

    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(query, page) | {"MediumDivNo": medium_div_no}
        try:
            resp = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("compuzone crawl_single 실패: %s", e)
            break

        items = BeautifulSoup(resp.text, "html.parser").select("li.li-obj")
        if not items:
            break

        for el in items:
            if _extract_product_no(el) != product_no:
                continue
            parsed = _extract_name_price(el)
            if parsed is None:
                break
            return RawCrawledPrice(
                site="compuzone", category=category,
                product_name=parsed[0], price_text=parsed[1],
                brand=brand, url=_detail_url(product_no, medium_div_no),
                crawled_at=now,
            )

    logger.warning("compuzone crawl_single: ProductNo=%s 미발견", product_no)
    return None


class CompuzoneCrawler(BaseCrawler):
    def __init__(self, conn: Connection) -> None:
        super().__init__()
        self._conn = conn

    @property
    def site_name(self) -> str:
        return "compuzone"

    def _load_watch_products(self) -> list[dict]:
        """WATCHLIST에서 compuzone 활성 크롤링 대상 로드."""
        cur = self._conn.cursor()
        try:
            cur.execute(
                "SELECT `query`, `pcode`, `category`, `brand` "
                "FROM `stg_watchlist` WHERE `is_active` = 1 AND `site` = '컴퓨존'"
            )
            return [
                {"query": row[0], "product_no": row[1], "category": row[2], "brand": row[3]}
                for row in cur.fetchall()
            ]
        finally:
            cur.close()

    def _fetch_search_html(self, query: str, medium_div_no: str, page: int = 1) -> str | None:
        """검색 결과 페이지 HTML. self._session 을 써서 공통 헤더·rate limit 을 태운다."""
        self._rate_limit()
        params = _build_search_params(query, page) | {"MediumDivNo": medium_div_no}
        try:
            resp = self._session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            return resp.text
        except requests.RequestException:
            logger.exception("Failed to search %s for query=%s", self.site_name, query)
            return None

    def _search_product_price(
        self, target: dict, crawled_at: datetime, medium_div_no: str
    ) -> RawCrawledPrice | None:
        """검색 결과를 페이지네이션하며 ProductNo 가 일치하는 상품을 찾는다.

        crawled_at 은 회차 시각을 그대로 받는다 — 여기서 now() 를 다시 부르면
        같은 회차인데 상품마다 수집 시각이 달라진다.
        """
        for page in range(1, MAX_SEARCH_PAGES + 1):
            html = self._fetch_search_html(target["query"], medium_div_no, page)
            if html is None:
                return None

            items = BeautifulSoup(html, "html.parser").select("li.li-obj")
            if not items:
                return None

            for el in items:
                if _extract_product_no(el) != target["product_no"]:
                    continue
                parsed = _extract_name_price(el)
                if parsed is None:
                    return None
                return RawCrawledPrice(
                    site=self.site_name, category=target["category"],
                    product_name=parsed[0], price_text=parsed[1],
                    brand=target["brand"],
                    url=_detail_url(target["product_no"], medium_div_no),
                    crawled_at=crawled_at,
                )
        return None

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — WATCHLIST 기반."""
        targets = self._load_watch_products()
        all_raw: list[RawCrawledPrice] = []
        # 회차 시각은 한 번만 정한다. 대상마다 now() 를 부르면 같은 회차인데
        # 상품별로 crawled_at 이 갈리고, stg_price_history 자연키가
        # (product_id, crawled_at) 이라 하위 계층의 시계열이 그만큼 어긋난다.
        now = datetime.now(UTC)

        for target in targets:
            medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(target["category"])
            if medium_div_no is None:
                logger.warning("지원하지 않는 카테고리: %s", target["category"])
                continue

            raw = self._search_product_price(target, now, medium_div_no)
            if raw is None:
                # fallback 이 없으므로 검색 실패가 곧 그 상품의 수집 실패다.
                # 조용히 넘기면 워치리스트가 커졌을 때 부분 실패를 알 수 없다.
                logger.warning(
                    "[%s] 대상 미발견: %s (ProductNo=%s)",
                    self.site_name, target["query"], target["product_no"],
                )
                continue
            all_raw.append(raw)

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
