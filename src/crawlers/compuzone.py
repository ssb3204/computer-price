"""Crawler for compuzone.co.kr — WATCHLIST 기반 크롤링.

크롤링 대상은 Snowflake WATCHLIST 테이블에서 동적으로 로드.
카테고리 AJAX 페이지를 페이지네이션하여 ProductNo로 특정 상품을 찾아 가격 수집.
"""

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from snowflake.connector import SnowflakeConnection

from src.common.models import RawCrawledPrice
from src.crawlers.base import BaseCrawler

logger = logging.getLogger(__name__)

LIST_URL = "https://www.compuzone.co.kr/product/product_list.php"
SEARCH_URL = "https://www.compuzone.co.kr/search/search_list.php"
DETAIL_BASE = "https://www.compuzone.co.kr/product/product_detail.htm"
MAX_CRAWL_PAGES = 5  # 상품당 최대 100개(20×5) 스캔

CATEGORY_MEDIUM_DIV_NO: dict[str, str] = {
    "CPU": "1012",
    "GPU": "1016",
    "RAM": "1014",
    "SSD": "1276",
}


@dataclass(frozen=True)
class SearchResult:
    """컴퓨존 검색 결과 단일 항목."""
    product_no: str
    product_name: str
    url: str


def _fetch_product_title(url: str) -> str | None:
    """상세 페이지 <title>에서 용량 포함 전체 상품명을 추출한다."""
    try:
        resp = requests.get(url, timeout=10, stream=True)
        resp.raise_for_status()
        buf = b""
        for chunk in resp.iter_content(chunk_size=4096):
            buf += chunk
            if b"</title>" in buf:
                resp.close()
                break
        text = buf.decode("euc-kr", errors="ignore")
    except requests.RequestException:
        return None

    m = re.search(r"<title>(.+?)</title>", text, re.DOTALL)
    if not m:
        return None
    name = m.group(1).strip()
    return re.sub(r"\s*:\s*컴퓨존.*$", "", name).strip()


def _fetch_price_from_detail(product_no: str) -> tuple[str, str] | None:
    """상세 페이지에서 직접 상품명·가격을 추출한다 (목록/검색 미노출 상품 대응).

    컴퓨존은 일부 상품(벌크·단종 임박 등)이 카테고리 목록·검색에 노출되지 않으나
    상세 페이지는 유효하게 존재한다. todayCookie JS 호출에서 가격을 추출한다.

    Returns: (product_name, price_str) — 실패 시 None
    """
    url = f"{DETAIL_BASE}?ProductNo={product_no}"
    try:
        resp = requests.get(url, timeout=15, stream=True)
        resp.raise_for_status()
        buf = b""
        for chunk in resp.iter_content(chunk_size=8192):
            buf += chunk
            if b"todayCookie" in buf:
                resp.close()
                break
        text = buf.decode("euc-kr", errors="ignore")
    except requests.RequestException:
        return None

    m_price = re.search(r'todayCookie\([^,]+,\s*"[^"]+",\s*"(\d+)"', text)
    if not m_price:
        return None

    m_title = re.search(r"<title>(.+?)</title>", text, re.DOTALL)
    if not m_title:
        return None
    name = re.sub(r"\s*:\s*컴퓨존.*$", "", m_title.group(1)).strip()
    return name, m_price.group(1)


def enrich_names_from_detail(results: list[SearchResult]) -> list[SearchResult]:
    """검색 결과 상품명을 상세 페이지 title로 교체해 용량 정보를 포함시킨다."""
    with ThreadPoolExecutor(max_workers=5) as executor:
        url_to_future = {r.url: executor.submit(_fetch_product_title, r.url) for r in results}
        enriched: dict[str, str] = {}
        for url, future in url_to_future.items():
            title = future.result()
            if title:
                enriched[url] = title

    return [
        SearchResult(product_no=r.product_no, product_name=enriched.get(r.url, r.product_name), url=r.url)
        for r in results
    ]


def _build_list_form(medium_div_no: str, page: int = 1) -> dict:
    return {
        "actype": "getList", "BigDivNo": "4",
        "MediumDivNo": medium_div_no, "DivNo": "0",
        "PageCount": "20", "StartNum": str((page - 1) * 20), "PageNum": str(page),
        "PreOrder": "recommand", "lvm": "L", "ps_po": "P",
        "ScrollPage": "1", "ProductType": "list",
        "PageType": "ProductList", "setPricechk": "N",
    }


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


MAX_SEARCH_PAGES = 3  # 검색 시 최대 60개 후보 스캔


def search_products(query: str, category: str, max_results: int = 10) -> list[SearchResult]:
    """키워드로 컴퓨존 상품 목록을 반환한다.

    search_list.php?actype=list 엔드포인트를 사용해 실제 키워드 검색 결과를 가져온다.

    Args:
        query: 검색어 (예: "RTX 5080", "라이젠 9800X3D")
        category: 카테고리 ("CPU" | "GPU" | "RAM" | "SSD") — MediumDivNo 결정에 사용
        max_results: 최대 반환 개수

    Returns:
        SearchResult 리스트 (최대 max_results개)
    """
    medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(category.upper(), "")

    session = requests.Session()
    results: list[SearchResult] = []

    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(query, page)
        if medium_div_no:
            params["MediumDivNo"] = medium_div_no
        try:
            resp = session.get(
                SEARCH_URL,
                params=params,
                timeout=30,
            )
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("search_products 요청 실패 (page=%d): %s", page, e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.li-obj")
        if not items:
            break

        for item in items:
            name_tag = item.select_one("a.prd_info_name")
            price_div = item.select_one("div.prd_price")
            if not name_tag or not price_div or not price_div.get("data-price"):
                continue
            pno = item.get("id", "").replace("li-pno-", "")
            if not pno:
                continue
            product_name = name_tag.get_text(strip=True)
            product_url = f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo={medium_div_no}"
            results.append(SearchResult(
                product_no=pno,
                product_name=product_name,
                url=product_url,
            ))
            if len(results) >= max_results:
                return results

    return results


def crawl_single(
    query: str, product_no: str, category: str, brand: str | None = None
) -> RawCrawledPrice | None:
    """단일 상품 즉시 크롤링 — WATCHLIST 추가 직후 호출용."""
    medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(category.upper(), "")
    session = requests.Session()
    now = datetime.now(timezone.utc)

    # 1차: 카테고리 목록에서 product_no 매칭 (crawl_raw와 동일한 주 경로)
    if medium_div_no:
        for page in range(1, MAX_CRAWL_PAGES + 1):
            try:
                resp = session.post(
                    LIST_URL,
                    data=_build_list_form(medium_div_no, page=page),
                    timeout=30,
                )
                resp.raise_for_status()
                resp.encoding = "euc-kr"
            except requests.RequestException as e:
                logger.error("compuzone crawl_single(list) 실패: %s", e)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.select("li.li-obj")
            if not items:
                break

            for item in items:
                pno = item.get("id", "").replace("li-pno-", "")
                if pno != product_no:
                    continue
                name_tag = item.select_one("a.prd_info_name")
                price_div = item.select_one("div.prd_price")
                if not name_tag or not price_div:
                    break
                raw_price = price_div.get("data-price")
                if not raw_price:
                    break
                product_url = f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo={medium_div_no}"
                return RawCrawledPrice(
                    site="compuzone", category=category,
                    product_name=name_tag.get_text(strip=True),
                    price_text=raw_price,
                    brand=brand, url=product_url, crawled_at=now,
                )

    # 2차: 키워드 검색 fallback
    for page in range(1, MAX_SEARCH_PAGES + 1):
        params = _build_search_params(query, page)
        if medium_div_no:
            params["MediumDivNo"] = medium_div_no
        try:
            resp = session.get(SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
            resp.encoding = "euc-kr"
        except requests.RequestException as e:
            logger.error("compuzone crawl_single(search) 실패: %s", e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.li-obj")
        if not items:
            break

        for item in items:
            pno = item.get("id", "").replace("li-pno-", "")
            if pno != product_no:
                continue
            name_tag = item.select_one("a.prd_info_name")
            price_div = item.select_one("div.prd_price")
            if not name_tag or not price_div:
                continue
            raw_price = price_div.get("data-price")
            if not raw_price:
                continue
            product_url = f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo={medium_div_no}"
            return RawCrawledPrice(
                site="compuzone", category=category,
                product_name=name_tag.get_text(strip=True),
                price_text=raw_price,
                brand=brand, url=product_url, crawled_at=now,
            )

    # 3차: 상세 페이지 직접 스크래핑 (목록·검색 미노출 상품 대응)
    detail = _fetch_price_from_detail(product_no)
    if detail:
        name, price_str = detail
        product_url = f"{DETAIL_BASE}?ProductNo={product_no}&BigDivNo=4&MediumDivNo={medium_div_no}"
        logger.info("compuzone crawl_single(detail): product_no=%s 발견", product_no)
        return RawCrawledPrice(
            site="compuzone", category=category,
            product_name=name, price_text=price_str,
            brand=brand, url=product_url, crawled_at=now,
        )

    logger.warning("compuzone crawl_single: product_no=%s 미발견", product_no)
    return None


class CompuzoneCrawler(BaseCrawler):
    def __init__(self, conn: SnowflakeConnection) -> None:
        super().__init__()
        self._conn = conn

    @property
    def site_name(self) -> str:
        return "compuzone"

    def _load_watch_products(self) -> list[dict]:
        """WATCHLIST에서 compuzone 활성 크롤링 대상 로드."""
        cur = self._conn.cursor()
        try:
            cur.execute("USE DATABASE COMPUTER_PRICE")
            cur.execute(
                "SELECT QUERY, PCODE, CATEGORY, BRAND "
                "FROM STAGING.WATCHLIST WHERE IS_ACTIVE = TRUE AND SITE = 'compuzone'"
            )
            return [
                {"query": row[0], "product_no": row[1], "category": row[2], "brand": row[3]}
                for row in cur.fetchall()
            ]
        finally:
            cur.close()

    def _fetch_category_html(self, medium_div_no: str, page: int = 1) -> str | None:
        self._rate_limit()
        try:
            resp = self._session.post(
                LIST_URL,
                data=_build_list_form(medium_div_no, page=page),
                timeout=30,
            )
            resp.raise_for_status()
            resp.encoding = "euc-kr"
            return resp.text
        except requests.RequestException:
            logger.exception("Failed to fetch %s category %s", self.site_name, medium_div_no)
            return None

    def _search_product_price(self, target: dict) -> "RawCrawledPrice | None":
        """검색 API로 특정 ProductNo 상품의 가격을 찾는다 (카테고리 리스트 fallback)."""
        medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(target["category"], "")
        session = requests.Session()

        for page in range(1, MAX_SEARCH_PAGES + 1):
            params = _build_search_params(target["query"], page)
            if medium_div_no:
                params["MediumDivNo"] = medium_div_no
            try:
                self._rate_limit()
                resp = session.get(SEARCH_URL, params=params, timeout=30)
                resp.raise_for_status()
                resp.encoding = "euc-kr"
            except requests.RequestException as e:
                logger.error("검색 fallback 요청 실패 (page=%d): %s", page, e)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.select("li.li-obj")
            if not items:
                break

            now = datetime.now(timezone.utc)
            for item in items:
                pno = item.get("id", "").replace("li-pno-", "")
                if pno != target["product_no"]:
                    continue
                name_tag = item.select_one("a.prd_info_name")
                price_div = item.select_one("div.prd_price")
                if not name_tag or not price_div:
                    continue
                raw_price = price_div.get("data-price")
                if not raw_price:
                    continue
                product_url = (
                    f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo={medium_div_no}"
                )
                return RawCrawledPrice(
                    site="compuzone", category=target["category"],
                    product_name=name_tag.get_text(strip=True),
                    price_text=raw_price,
                    brand=target["brand"], url=product_url,
                    crawled_at=now,
                )
        return None

    def crawl_raw(self) -> list[RawCrawledPrice]:
        """Raw 데이터 수집 — WATCHLIST 기반."""
        targets = self._load_watch_products()
        all_raw: list[RawCrawledPrice] = []

        for target in targets:
            medium_div_no = CATEGORY_MEDIUM_DIV_NO.get(target["category"])
            if medium_div_no is None:
                logger.warning("지원하지 않는 카테고리: %s", target["category"])
                continue

            found = False
            for page in range(1, MAX_CRAWL_PAGES + 1):
                html = self._fetch_category_html(medium_div_no, page=page)
                if html is None:
                    break

                soup = BeautifulSoup(html, "html.parser")
                items = soup.select("li.li-obj")
                if not items:
                    break

                now = datetime.now(timezone.utc)
                for item in items:
                    pno = item.get("id", "").replace("li-pno-", "")
                    if pno != target["product_no"]:
                        continue

                    name_tag = item.select_one("a.prd_info_name")
                    price_div = item.select_one("div.prd_price")
                    if not name_tag or not price_div:
                        break
                    raw_price = price_div.get("data-price")
                    if not raw_price:
                        break

                    product_url = (
                        f"{DETAIL_BASE}?ProductNo={pno}&BigDivNo=4&MediumDivNo={medium_div_no}"
                    )
                    all_raw.append(RawCrawledPrice(
                        site="compuzone", category=target["category"],
                        product_name=name_tag.get_text(strip=True),
                        price_text=raw_price,
                        brand=target["brand"], url=product_url,
                        crawled_at=now,
                    ))
                    found = True
                    break

                if found:
                    break

            if not found:
                logger.info(
                    "카테고리 리스트 미발견, 검색 fallback: %s (ProductNo=%s)",
                    target["query"], target["product_no"],
                )
                result = self._search_product_price(target)
                if result:
                    all_raw.append(result)
                else:
                    # 최종 fallback: 상세 페이지 직접 스크래핑
                    detail = _fetch_price_from_detail(target["product_no"])
                    if detail:
                        name, price_str = detail
                        mdno = CATEGORY_MEDIUM_DIV_NO.get(target["category"], "")
                        product_url = (
                            f"{DETAIL_BASE}?ProductNo={target['product_no']}"
                            f"&BigDivNo=4&MediumDivNo={mdno}"
                        )
                        all_raw.append(RawCrawledPrice(
                            site="compuzone", category=target["category"],
                            product_name=name, price_text=price_str,
                            brand=target["brand"], url=product_url,
                            crawled_at=datetime.now(timezone.utc),
                        ))
                        logger.info(
                            "상세 페이지 fallback 성공: %s (ProductNo=%s)",
                            target["query"], target["product_no"],
                        )
                    else:
                        logger.warning(
                            "모든 경로 실패: %s (ProductNo=%s)",
                            target["query"], target["product_no"],
                        )

        logger.info("Crawled %d raw prices from %s", len(all_raw), self.site_name)
        return all_raw
