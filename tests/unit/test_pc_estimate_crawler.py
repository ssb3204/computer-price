"""Unit tests for PCEstimateCrawler — inline HTML fixtures, WATCHLIST 기반.

견적왕은 fallback 없이 단일 경로다:
  검색(POST) → li.list 파싱 → href 의 pd_no 정확매칭
"""

from unittest.mock import patch

from src.crawlers.pc_estimate import CATEGORY_TO_CATE2, MAX_SEARCH_PAGES, PCEstimateCrawler
from tests.unit.conftest import FakeClock

# ── HTML fixture 빌더 ────────────────────────────────────────────────────────


def _item(
    pd_no: str,
    name: str = "테스트 상품",
    price: str = "450,000원",
    *,
    href: str | None = None,
    with_name: bool = True,
    with_price: bool = True,
) -> str:
    """li.list 한 개짜리 HTML 조각."""
    link = f"/shop/view.html?pd_no={pd_no}" if href is None else href
    name_block = f'<a class="name" href="{link}">{name}</a>' if with_name else ""
    price_block = f'<span class="card">{price}</span>' if with_price else ""
    return f'<li class="list">{name_block}{price_block}</li>'


def _page(*items: str) -> str:
    return f"<html><body><ul>{''.join(items)}</ul></body></html>"


EMPTY_PAGE = _page()


# ── 기본 수집 경로 ───────────────────────────────────────────────────────────


class TestCrawlRaw:
    def test_matched_target_produces_raw_price(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)
        page = _page(_item("1001", name="AMD 라이젠 7 7800X3D", price="450,000원"))

        with patch.object(crawler, "_fetch_search_html", return_value=page):
            results = crawler.crawl_raw()

        assert len(results) == 1
        raw = results[0]
        assert raw.category == "CPU"
        assert raw.brand == "AMD"
        assert raw.product_name == "AMD 라이젠 7 7800X3D"
        assert raw.price_text == "450,000원"
        assert raw.url == "https://kjwwang.com/shop/view.html?pd_no=1001"
        assert raw.crawled_at is not None

    def test_all_targets_share_one_crawled_at(self, make_watch_conn):
        """한 크롤링 회차의 모든 상품은 같은 crawled_at 을 가져야 한다.

        대상·페이지마다 now() 를 부르면 같은 회차인데 시각이 갈린다.
        stg_price_history 자연키가 (product_id, crawled_at) 이라 하위 계층의
        시계열 정렬과 일별 집계가 이 값에 직접 의존한다.
        """
        conn = make_watch_conn([
            ("라이젠 7800X3D", "1001", "CPU", "AMD"),
            ("RTX 5070", "1002", "GPU", "NVIDIA"),
        ])
        crawler = PCEstimateCrawler(conn=conn)
        page = _page(_item("1001"), _item("1002"))

        with (
            patch.object(crawler, "_fetch_search_html", return_value=page),
            patch("src.crawlers.pc_estimate.datetime", FakeClock()),
        ):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert results[0].crawled_at == results[1].crawled_at

    def test_site_field_is_kjwwang_not_korean_name(self, make_watch_conn):
        """저장되는 site 값은 'kjwwang' 이다.

        stg_watchlist 조회는 site='견적왕' 으로 하지만 RawCrawledPrice.site 는
        영문 코드명을 쓴다 — 둘이 달라서 혼동하기 쉬운 지점이라 고정해둔다.
        """
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html", return_value=_page(_item("1001"))):
            results = crawler.crawl_raw()

        assert results[0].site == "kjwwang"
        assert crawler.site_name == "kjwwang"

    def test_non_matching_pd_no_is_ignored(self, make_watch_conn):
        """워치리스트 pd_no 와 다른 상품은 수집하지 않는다.

        (카테고리 상위 N개를 담던 옛 방식과 달라진 핵심 지점)
        """
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)
        page = _page(_item("9999", name="다른 상품"), _item("8888", name="또 다른 상품"))

        with patch.object(crawler, "_fetch_search_html", return_value=page):
            results = crawler.crawl_raw()

        assert results == []

    def test_uses_category_cate2_and_query(self, make_watch_conn):
        """검색 시 워치리스트의 query 와 카테고리 매핑 cate2 를 함께 넘긴다."""
        conn = make_watch_conn([("RTX 5070", "2001", "GPU", "NVIDIA")])
        crawler = PCEstimateCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=_page(_item("2001"))
        ) as mock_fetch:
            crawler.crawl_raw()

        query_arg, cate2_arg = mock_fetch.call_args[0][0], mock_fetch.call_args[0][1]
        assert query_arg == "RTX 5070"
        assert cate2_arg == CATEGORY_TO_CATE2["GPU"] == "12"

    def test_unsupported_category_is_skipped(self, make_watch_conn):
        """매핑에 없는 카테고리는 요청 자체를 하지 않는다."""
        conn = make_watch_conn([("4K 모니터", "5001", "MONITOR", "LG")])
        crawler = PCEstimateCrawler(conn=conn)

        with patch.object(crawler, "_fetch_search_html") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_empty_watchlist_does_not_fetch(self, make_watch_conn):
        crawler = PCEstimateCrawler(conn=make_watch_conn([]))

        with patch.object(crawler, "_fetch_search_html") as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        mock_fetch.assert_not_called()

    def test_multiple_targets_each_collected(self, make_watch_conn):
        conn = make_watch_conn([
            ("라이젠 7800X3D", "1001", "CPU", "AMD"),
            ("RTX 5070", "2001", "GPU", "NVIDIA"),
        ])
        crawler = PCEstimateCrawler(conn=conn)
        pages = {
            "9": _page(_item("1001", name="AMD 라이젠 7800X3D")),
            "12": _page(_item("2001", name="RTX 5070")),
        }

        with patch.object(
            crawler, "_fetch_search_html", side_effect=lambda q, cate2, page=1: pages[cate2]
        ):
            results = crawler.crawl_raw()

        assert len(results) == 2
        assert {r.category for r in results} == {"CPU", "GPU"}

    def test_fetch_failure_skips_only_that_target(self, make_watch_conn):
        """한 대상의 검색이 실패해도 나머지 대상은 계속 수집한다."""
        conn = make_watch_conn([
            ("라이젠 7800X3D", "1001", "CPU", "AMD"),
            ("RTX 5070", "2001", "GPU", "NVIDIA"),
        ])
        crawler = PCEstimateCrawler(conn=conn)

        def fake_fetch(query, cate2, page=1):
            return None if cate2 == "9" else _page(_item("2001", name="RTX 5070"))

        with patch.object(crawler, "_fetch_search_html", side_effect=fake_fetch):
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].category == "GPU"


# ── 페이지네이션 ─────────────────────────────────────────────────────────────


class TestPagination:
    def test_finds_target_on_later_page(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)
        pages = {
            1: _page(_item("9999")),
            2: _page(_item("8888")),
            3: _page(_item("1001", name="AMD 라이젠 7800X3D", price="450,000원")),
        }

        with patch.object(
            crawler, "_fetch_search_html", side_effect=lambda q, c, page=1: pages.get(page)
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert len(results) == 1
        assert results[0].price_text == "450,000원"
        assert mock_fetch.call_count == 3  # 찾은 페이지에서 멈춘다

    def test_stops_after_max_pages(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=_page(_item("9999"))
        ) as mock_fetch:
            results = crawler.crawl_raw()

        assert results == []
        assert mock_fetch.call_count == MAX_SEARCH_PAGES

    def test_empty_page_stops_pagination(self, make_watch_conn):
        conn = make_watch_conn([("라이젠 7800X3D", "1001", "CPU", "AMD")])
        crawler = PCEstimateCrawler(conn=conn)

        with patch.object(
            crawler, "_fetch_search_html", return_value=EMPTY_PAGE
        ) as mock_fetch:
            crawler.crawl_raw()

        assert mock_fetch.call_count == 1


# ── 결손 데이터 ──────────────────────────────────────────────────────────────


class TestMalformedItems:
    def _run(self, make_watch_conn, page_html):
        crawler = PCEstimateCrawler(conn=make_watch_conn([("쿼리", "1001", "CPU", "AMD")]))
        with patch.object(crawler, "_fetch_search_html", return_value=page_html):
            return crawler.crawl_raw()

    def test_missing_name_tag_is_skipped(self, make_watch_conn):
        """a.name 이 없으면 pd_no 를 못 뽑으므로 건너뛴다."""
        assert self._run(make_watch_conn, _page(_item("1001", with_name=False))) == []

    def test_missing_price_does_not_crash(self, make_watch_conn):
        assert self._run(make_watch_conn, _page(_item("1001", with_price=False))) == []

    def test_href_without_pd_no_is_skipped(self, make_watch_conn):
        """href 에 pd_no= 가 없으면 매칭 대상이 아니다."""
        page = _page(_item("1001", href="/shop/view.html?id=1001"))
        assert self._run(make_watch_conn, page) == []

    def test_empty_href_produces_empty_url(self, make_watch_conn):
        """href 가 '/' 로 시작하지 않으면 url 은 빈 문자열이 된다."""
        page = _page(_item("1001", href="shop/view.html?pd_no=1001"))
        results = self._run(make_watch_conn, page)

        assert len(results) == 1
        assert results[0].url == ""
