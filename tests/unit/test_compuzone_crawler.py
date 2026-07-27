"""Unit tests for CompuzoneCrawler — inline HTML fixtures, WATCHLIST 기반.

컴퓨존은 3단계 fallback 구조라 각 단계의 전환 지점을 따로 검증한다:
  ① product_list.php POST (카테고리 목록, 최대 5페이지 페이지네이션)
  ② search_list.php GET  (_search_product_price)
  ③ 상세페이지 정규식     (_fetch_price_from_detail)
"""

from unittest.mock import patch

import pytest

from src.common.models import RawCrawledPrice
from src.crawlers.compuzone import CATEGORY_MEDIUM_DIV_NO, MAX_CRAWL_PAGES, CompuzoneCrawler


@pytest.fixture(autouse=True)
def block_network_fallbacks():
    """②③ fallback 을 기본 차단한다.

    ① 경로만 검증하는 테스트에서 코드가 회귀하면 fallback 으로 넘어가 실제
    HTTP 요청이 나가버린다(_search_product_price 는 자체 requests.Session 을,
    _fetch_price_from_detail 은 모듈 레벨 requests 를 쓴다). 그러면 테스트가
    느려지고 네트워크 상태에 따라 결과가 흔들린다.

    fallback 자체를 검증하는 테스트는 안쪽에서 다시 patch 해 덮어쓴다.
    """
    with (
        patch.object(CompuzoneCrawler, "_search_product_price", return_value=None),
        patch("src.crawlers.compuzone._fetch_price_from_detail", return_value=None),
    ):
        yield

# ── HTML fixture 빌더 ────────────────────────────────────────────────────────


def _item(
    pno: str,
    name: str = "테스트 상품",
    price: str = "450000",
    *,
    with_name: bool = True,
    with_price: bool = True,
) -> str:
    """li.li-obj 한 개짜리 HTML 조각."""
    name_block = f'<a class="prd_info_name">{name}</a>' if with_name else ""
    price_block = f'<div class="prd_price" data-price="{price}"></div>' if with_price else ""
    return f'<li class="li-obj" id="li-pno-{pno}">{name_block}{price_block}</li>'


def _page(*items: str) -> str:
    return f"<html><body><ul>{''.join(items)}</ul></body></html>"


EMPTY_PAGE = _page()


# ── ① 카테고리 목록 경로 ─────────────────────────────────────────────────────


class TestCategoryListPath:
    def test_matched_target_produces_raw_price(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("100001", name="AMD 라이젠 7800X3D", price="450000"))

        with patch.object(crawler, "_fetch_category_html", return_value=page):
            results = crawler.crawl_raw()

        assert len(results) == 1
        raw = results[0]
        assert raw.site == "compuzone"
        assert raw.category == "CPU"
        assert raw.brand == "AMD"
        assert raw.product_name == "AMD 라이젠 7800X3D"
        assert raw.price_text == "450000"
        assert "ProductNo=100001" in raw.url
        assert raw.url.startswith("https://www.compuzone.co.kr/")
        assert raw.crawled_at is not None

    def test_non_matching_product_no_is_ignored(self, make_watch_conn):
        """워치리스트 ProductNo 와 다른 상품은 수집하지 않는다.

        (카테고리 상위 N개를 담던 옛 방식과 달라진 핵심 지점)
        """
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("999999", name="다른 상품"), _item("888888", name="또 다른 상품"))

        with patch.object(crawler, "_fetch_category_html", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_uses_category_medium_div_no(self, make_watch_conn):
        """카테고리에 매핑된 MediumDivNo 로 목록을 요청한다."""
        conn = make_watch_conn([("RTX 5070", "200001", "GPU", "NVIDIA")])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("200001", name="RTX 5070"))

        with patch.object(crawler, "_fetch_category_html", return_value=page) as mock_fetch:
            crawler.crawl_raw()

        assert mock_fetch.call_args[0][0] == CATEGORY_MEDIUM_DIV_NO["GPU"] == "1016"

    def test_unsupported_category_is_skipped(self, make_watch_conn):
        """매핑에 없는 카테고리는 요청 자체를 하지 않는다."""
        conn = make_watch_conn([("4K 모니터", "300001", "MONITOR", "LG")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_category_html") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_empty_watchlist_does_not_fetch(self, make_watch_conn):
        crawler = CompuzoneCrawler(conn=make_watch_conn([]))

        with patch.object(crawler, "_fetch_category_html") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_multiple_targets_each_collected(self, make_watch_conn):
        conn = make_watch_conn([
            ("라이젠 7800X3D", "100001", "CPU", "AMD"),
            ("RTX 5070", "200001", "GPU", "NVIDIA"),
        ])
        crawler = CompuzoneCrawler(conn=conn)
        pages = {
            "1012": _page(_item("100001", name="AMD 라이젠 7800X3D")),
            "1016": _page(_item("200001", name="RTX 5070")),
        }

        with patch.object(crawler, "_fetch_category_html", side_effect=lambda d, page=1: pages[d]):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert {r.category for r in results} == {"CPU", "GPU"}


# ── 페이지네이션 ─────────────────────────────────────────────────────────────


class TestPagination:
    def test_finds_target_on_later_page(self, make_watch_conn):
        """1페이지에 없으면 다음 페이지를 넘겨가며 찾는다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        pages = {
            1: _page(_item("999999")),
            2: _page(_item("888888")),
            3: _page(_item("100001", name="AMD 라이젠 7800X3D", price="450000")),
        }

        with patch.object(
            crawler, "_fetch_category_html", side_effect=lambda d, page=1: pages.get(page)
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].price_text == "450000"
        assert mock_fetch.call_count == 3  # 찾은 페이지에서 멈춘다

    def test_stops_after_max_pages(self, make_watch_conn):
        """끝까지 못 찾으면 MAX_CRAWL_PAGES 에서 멈추고 fallback 으로 넘어간다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_category_html", return_value=_page(_item("999999"))
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        assert mock_fetch.call_count == MAX_CRAWL_PAGES

    def test_empty_page_stops_pagination(self, make_watch_conn):
        """빈 목록을 만나면 더 넘기지 않는다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_category_html", return_value=EMPTY_PAGE
        ) as mock_fetch:
            crawler.crawl_raw()

        assert mock_fetch.call_count == 1

    def test_fetch_failure_stops_pagination(self, make_watch_conn):
        """fetch 가 None 이면 즉시 중단하고 fallback 으로 넘어간다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_category_html", return_value=None) as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        assert mock_fetch.call_count == 1


# ── ②③ fallback 전환 ────────────────────────────────────────────────────────


class TestFallbackChain:
    def _target_row(self):
        return [("라이젠 7800X3D", "100001", "CPU", "AMD")]

    def test_falls_back_to_search_when_not_in_list(self, make_watch_conn):
        """① 실패 → ② 검색 fallback 결과를 사용한다."""
        crawler = CompuzoneCrawler(conn=make_watch_conn(self._target_row()))
        from datetime import UTC, datetime

        found = RawCrawledPrice(
            site="compuzone", category="CPU", product_name="검색으로 찾은 상품",
            price_text="440000", brand="AMD",
            url="https://www.compuzone.co.kr/product/product_detail.htm?ProductNo=100001",
            crawled_at=datetime.now(UTC),
        )

        with (
            patch.object(crawler, "_fetch_category_html", return_value=_page(_item("999999"))),
            patch.object(crawler, "_search_product_price", return_value=found) as mock_search,
            patch("src.crawlers.compuzone._fetch_price_from_detail") as mock_detail,
        ):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].product_name == "검색으로 찾은 상품"
        mock_search.assert_called_once()
        mock_detail.assert_not_called()  # ② 가 성공하면 ③ 은 부르지 않는다

    def test_falls_back_to_detail_page_when_search_fails(self, make_watch_conn):
        """①② 모두 실패 → ③ 상세페이지 fallback 을 쓴다."""
        crawler = CompuzoneCrawler(conn=make_watch_conn(self._target_row()))

        with (
            patch.object(crawler, "_fetch_category_html", return_value=_page(_item("999999"))),
            patch.object(crawler, "_search_product_price", return_value=None),
            patch(
                "src.crawlers.compuzone._fetch_price_from_detail",
                return_value=("상세페이지 상품명", "430000"),
            ) as mock_detail,
        ):
            results = crawler.crawl_raw()

        assert len(results) == 1
        raw = results[0]
        assert raw.product_name == "상세페이지 상품명"
        assert raw.price_text == "430000"
        assert raw.site == "compuzone"
        assert raw.category == "CPU"
        assert raw.brand == "AMD"
        assert "ProductNo=100001" in raw.url
        assert f"MediumDivNo={CATEGORY_MEDIUM_DIV_NO['CPU']}" in raw.url
        mock_detail.assert_called_once_with("100001")

    def test_all_paths_fail_produces_nothing(self, make_watch_conn):
        """①②③ 전부 실패해도 예외 없이 빈 결과를 낸다."""
        crawler = CompuzoneCrawler(conn=make_watch_conn(self._target_row()))

        with patch.object(crawler, "_fetch_category_html", return_value=None):
            results = crawler.crawl_raw()

        assert results == []


# ── 결손 데이터 ──────────────────────────────────────────────────────────────


class TestMalformedItems:
    def _run(self, make_watch_conn, page_html):
        crawler = CompuzoneCrawler(conn=make_watch_conn([("쿼리", "100001", "CPU", "AMD")]))
        with patch.object(crawler, "_fetch_category_html", return_value=page_html):
            return crawler.crawl_raw()

    def test_missing_price_does_not_crash(self, make_watch_conn):
        results = self._run(make_watch_conn, _page(_item("100001", with_price=False)))
        assert results == []

    def test_missing_name_does_not_crash(self, make_watch_conn):
        results = self._run(make_watch_conn, _page(_item("100001", with_name=False)))
        assert results == []

    def test_empty_data_price_does_not_crash(self, make_watch_conn):
        results = self._run(make_watch_conn, _page(_item("100001", price="")))
        assert results == []
