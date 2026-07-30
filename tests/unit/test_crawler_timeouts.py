"""Unit tests: 크롤러 HTTP 타임아웃 예산.

배경 — 2026-07-28 파이프라인 장애.
컴퓨존이 TCP SYN에 무응답이라 requests가 `timeout=30`을 꽉 채워 기다렸다.
30은 connect와 read 공통값이라 "죽은 호스트에 연결 시도"까지 30초를 썼고,
3단계 fallback × 대상 4건 = 5분이 소요됐다.

무응답 서버는 connect 단계에서 걸린다. connect와 read를 분리해 connect를
짧게 잡으면 실패까지의 시간이 지배적으로 줄어든다.
"""

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.crawlers.base import REQUEST_TIMEOUT, BaseCrawler
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.pc_estimate import PCEstimateCrawler

CRAWLER_DIR = Path(__file__).resolve().parents[2] / "src" / "crawlers"


class TestTimeoutBudget:
    def test_request_timeout_is_connect_read_pair(self):
        """단일 값이 아닌 (connect, read) 쌍이어야 둘을 따로 통제할 수 있다."""
        assert isinstance(REQUEST_TIMEOUT, tuple)
        assert len(REQUEST_TIMEOUT) == 2

    def test_connect_timeout_is_short(self):
        """무응답 호스트를 붙들고 기다리는 시간이 실패 시간을 지배한다."""
        connect_timeout, _read_timeout = REQUEST_TIMEOUT
        assert 0 < connect_timeout <= 10, (
            f"connect 타임아웃이 {connect_timeout}초 — 죽은 호스트에 너무 오래 매달린다"
        )

    def test_read_timeout_allows_slow_responses(self):
        """느린 응답과 무응답은 다르다 — read는 넉넉해야 정상 페이지를 놓치지 않는다."""
        connect_timeout, read_timeout = REQUEST_TIMEOUT
        assert read_timeout >= connect_timeout


class TestCrawlersUseSharedTimeout:
    """각 크롤러의 실제 HTTP 호출이 공유 상수를 넘기는지 검증한다."""

    def test_compuzone_search(self, make_watch_conn):
        crawler = CompuzoneCrawler(conn=make_watch_conn([]))
        crawler._session = MagicMock()

        crawler._fetch_search_html("adata", "1276")

        assert crawler._session.get.call_args.kwargs["timeout"] == REQUEST_TIMEOUT

    def test_pc_estimate_search(self, make_watch_conn):
        crawler = PCEstimateCrawler(conn=make_watch_conn([]))
        crawler._session = MagicMock()
        # 검색 전에 토큰 요청이 먼저 나가므로 파싱 가능한 응답이 필요하다.
        crawler._session.post.return_value.text = '<input id="search_query" value="TOK">'

        crawler._fetch_search_html("7800X3D", "1")

        assert crawler._session.post.call_args.kwargs["timeout"] == REQUEST_TIMEOUT

    def test_base_crawler_retry_fetch(self):
        class _Stub(BaseCrawler):
            @property
            def site_name(self) -> str:
                return "stub"

            def crawl_raw(self):
                return []

        crawler = _Stub()
        crawler._session = MagicMock()

        crawler._fetch_with_retry("https://example.com")

        assert crawler._session.get.call_args.kwargs["timeout"] == REQUEST_TIMEOUT


class TestNoHardcodedLongTimeout:
    """호출 지점이 13곳이라 새로 추가되는 곳을 놓치기 쉽다."""

    @pytest.mark.parametrize(
        "module_name", ["base.py", "compuzone.py", "danawa.py", "pc_estimate.py"]
    )
    def test_no_numeric_timeout_literal(self, module_name):
        """숫자 리터럴을 쓰면 connect/read가 같은 값으로 묶여 원래 문제가 재발한다."""
        source = (CRAWLER_DIR / module_name).read_text(encoding="utf-8")
        literals = re.findall(r"timeout=\d+", source)
        assert literals == [], (
            f"{module_name}에 숫자 타임아웃 {literals}가 있다 — REQUEST_TIMEOUT을 쓸 것"
        )
