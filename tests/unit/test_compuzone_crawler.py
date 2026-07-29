"""Unit tests for CompuzoneCrawler — inline HTML fixtures, WATCHLIST 기반.

컴퓨존은 단일 경로다(다나와·견적왕과 동일):
  검색(search_list.php GET) → li.li-obj 파싱 → id 의 ProductNo 정확매칭

과거의 3단계 fallback(카테고리 목록 POST → 검색 GET → 상세페이지 정규식)은 제거했다.
- 카테고리 목록은 검색어를 쓰지 않아 추천순 상위 100개에 들어야만 찾을 수 있었고,
  검색 경로가 그 상위집합임을 실측으로 확인했다.
- 상세페이지 경로는 <title> 기반이라 목록/검색과 상품명이 달랐고(용량·옵션이 덧붙음),
  stg_products 자연키가 (site, product_name) 이라 같은 상품이 둘로 갈라졌다.
  게다가 워치리스트 등록 자체가 검색 결과에서 고르는 구조라 도달 가능성도 낮았다.
"""

from unittest.mock import patch

from src.crawlers.compuzone import (
    CATEGORY_MEDIUM_DIV_NO,
    MAX_SEARCH_PAGES,
    CompuzoneCrawler,
)
from tests.unit.conftest import FakeClock

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


# ── 기본 수집 경로 ───────────────────────────────────────────────────────────


class TestCrawlRaw:
    def test_matched_target_produces_raw_price(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("100001", name="AMD 라이젠 7800X3D", price="450000"))

        with patch.object(crawler, "_fetch_search_html", return_value=page):
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

    def test_site_field_is_compuzone_not_korean_name(self, make_watch_conn):
        """stg_watchlist 조회는 site='컴퓨존' 으로 하지만 RawCrawledPrice.site 는 영문이다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html", return_value=_page(_item("100001"))):
            results = crawler.crawl_raw()

        assert results[0].site == "compuzone"
        assert crawler.site_name == "compuzone"

    def test_all_targets_share_one_crawled_at(self, make_watch_conn):
        """한 크롤링 회차의 모든 상품은 같은 crawled_at 을 가져야 한다.

        대상마다 now() 를 부르면 같은 회차인데 시각이 갈린다. 실제로 운영 DB에서
        컴퓨존 한 회차가 4개의 서로 다른 crawled_at 을 만들고 있었다.
        stg_price_history 자연키가 (product_id, crawled_at) 이라 하위 계층의
        시계열 정렬과 일별 집계가 이 값에 직접 의존한다.
        """
        conn = make_watch_conn([
            ("라이젠 7800X3D", "100001", "CPU", "AMD"),
            ("라이젠 9800X3D", "100002", "CPU", "AMD"),
        ])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("100001"), _item("100002"))

        with (
            patch.object(crawler, "_fetch_search_html", return_value=page),
            patch("src.crawlers.compuzone.datetime", FakeClock()),
        ):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert results[0].crawled_at == results[1].crawled_at

    def test_non_matching_product_no_is_ignored(self, make_watch_conn):
        """워치리스트 ProductNo 와 다른 상품은 수집하지 않는다."""
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        page = _page(_item("999999", name="다른 상품"), _item("888888", name="또 다른 상품"))

        with patch.object(crawler, "_fetch_search_html", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_uses_query_and_category_medium_div_no(self, make_watch_conn):
        """검색 시 워치리스트의 query 와 카테고리 매핑 MediumDivNo 를 함께 넘긴다."""
        conn = make_watch_conn([("RTX 5070", "200001", "GPU", "NVIDIA")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=_page(_item("200001"))
        ) as mock_fetch:
            crawler.crawl_raw()

        query_arg, mdno_arg = mock_fetch.call_args[0][0], mock_fetch.call_args[0][1]
        assert query_arg == "RTX 5070"
        assert mdno_arg == CATEGORY_MEDIUM_DIV_NO["GPU"] == "1016"

    def test_unsupported_category_is_skipped(self, make_watch_conn):
        """매핑에 없는 카테고리는 요청 자체를 하지 않는다."""
        conn = make_watch_conn([("4K 모니터", "300001", "MONITOR", "LG")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_empty_watchlist_does_not_fetch(self, make_watch_conn):
        crawler = CompuzoneCrawler(conn=make_watch_conn([]))

        with patch.object(crawler, "_fetch_search_html") as mock_fetch:
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

        with patch.object(
            crawler, "_fetch_search_html", side_effect=lambda q, mdno, page=1: pages[mdno]
        ):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert {r.category for r in results} == {"CPU", "GPU"}

    def test_fetch_failure_skips_only_that_target(self, make_watch_conn):
        """한 대상의 검색이 실패해도 나머지 대상은 계속 수집한다."""
        conn = make_watch_conn([
            ("라이젠 7800X3D", "100001", "CPU", "AMD"),
            ("RTX 5070", "200001", "GPU", "NVIDIA"),
        ])
        crawler = CompuzoneCrawler(conn=conn)

        def fake_fetch(query, mdno, page=1):
            return None if mdno == "1012" else _page(_item("200001", name="RTX 5070"))

        with patch.object(crawler, "_fetch_search_html", side_effect=fake_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].category == "GPU"

    def test_missing_target_is_logged(self, make_watch_conn, caplog):
        """대상을 못 찾으면 어느 상품인지 로그에 남는다.

        fallback 이 없어졌으므로 검색 실패가 곧 그 상품의 수집 실패다.
        조용히 사라지면 워치리스트가 커졌을 때 부분 실패를 알 수 없다.
        """
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html", return_value=_page(_item("999999"))):
            results = crawler.crawl_raw()

        assert results == []
        assert "100001" in caplog.text


# ── 페이지네이션 ─────────────────────────────────────────────────────────────


class TestPagination:
    def test_finds_target_on_later_page(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)
        pages = {
            1: _page(_item("999999")),
            2: _page(_item("100001", name="AMD 라이젠 7800X3D", price="450000")),
        }

        with patch.object(
            crawler, "_fetch_search_html", side_effect=lambda q, m, page=1: pages.get(page)
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].price_text == "450000"
        assert mock_fetch.call_count == 2  # 찾은 페이지에서 멈춘다

    def test_stops_after_max_pages(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=_page(_item("999999"))
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        assert mock_fetch.call_count == MAX_SEARCH_PAGES

    def test_empty_page_stops_pagination(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=EMPTY_PAGE
        ) as mock_fetch:
            crawler.crawl_raw()

        assert mock_fetch.call_count == 1

    def test_fetch_failure_stops_pagination(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "100001", "CPU", "AMD")])
        crawler = CompuzoneCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html", return_value=None) as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        assert mock_fetch.call_count == 1


# ── 결손 데이터 ──────────────────────────────────────────────────────────────


class TestMalformedItems:
    def _run(self, make_watch_conn, page_html):
        crawler = CompuzoneCrawler(conn=make_watch_conn([("쿼리", "100001", "CPU", "AMD")]))
        with patch.object(crawler, "_fetch_search_html", return_value=page_html):
            return crawler.crawl_raw()

    def test_missing_price_does_not_crash(self, make_watch_conn):
        assert self._run(make_watch_conn, _page(_item("100001", with_price=False))) == []

    def test_missing_name_does_not_crash(self, make_watch_conn):
        assert self._run(make_watch_conn, _page(_item("100001", with_name=False))) == []

    def test_empty_data_price_does_not_crash(self, make_watch_conn):
        assert self._run(make_watch_conn, _page(_item("100001", price=""))) == []

    def test_item_without_pno_prefix_is_skipped(self, make_watch_conn):
        """id 가 li-pno- 로 시작하지 않는 항목은 대조 대상이 아니다."""
        crawler = CompuzoneCrawler(conn=make_watch_conn([("쿼리", "100001", "CPU", "AMD")]))
        page = '<html><body><ul><li class="li-obj" id="banner-100001"></li></ul></body></html>'

        with patch.object(crawler, "_fetch_search_html", return_value=page):
            assert crawler.crawl_raw() == []
