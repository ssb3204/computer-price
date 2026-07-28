"""Unit tests: 크롤링 단계의 MySQL 커넥션 수명.

배경 — 2026-07-28 파이프라인 장애.
커넥션 1개를 3개 크롤러 전체에 걸쳐 유지하던 구조에서, 컴퓨존이 무응답이라
5분간 붙들려 있는 동안 유휴 커넥션이 끊겼고, 그 커넥션을 물려받은 견적왕이
(2006) MySQL server has gone away 로 죽었다.

커넥션은 크롤러마다 새로 열려야 한다 — 그래야 각 커넥션의 유일한 사용처인
워치리스트 조회가 항상 "방금 연 커넥션"에서 일어난다.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.common.config import MySQLSettings
from src.pipeline.crawl import crawl_all_sites

CRAWLER_PATCH_TARGETS = (
    ("src.pipeline.crawl.DanawaCrawler", "danawa"),
    ("src.pipeline.crawl.CompuzoneCrawler", "compuzone"),
    ("src.pipeline.crawl.PCEstimateCrawler", "pc_estimate"),
)


class _Recorder:
    """커넥션 개폐와 크롤링 호출을 발생 순서대로 기록한다."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.conns_by_site: dict[str, object] = {}
        self._open_count = 0

    @contextmanager
    def fake_get_connection(self, _settings):
        self._open_count += 1
        conn = f"conn-{self._open_count}"
        self.events.append("open")
        try:
            yield conn
        finally:
            self.events.append("close")

    def make_crawler_factory(self, site_name: str, raw_prices=None):
        """크롤러 클래스 대역. 생성 시 받은 conn과 crawl_raw 호출을 기록한다."""

        def factory(conn):
            self.conns_by_site[site_name] = conn
            crawler = MagicMock()
            crawler.site_name = site_name

            def crawl_raw():
                self.events.append(f"crawl:{site_name}")
                return list(raw_prices or [])

            crawler.crawl_raw.side_effect = crawl_raw
            return crawler

        return factory


def _run_with_recorder(recorder: _Recorder):
    settings = MagicMock(spec=MySQLSettings)
    patches = [
        patch("src.pipeline.crawl.get_connection", recorder.fake_get_connection),
    ]
    patches += [
        patch(target, side_effect=recorder.make_crawler_factory(site))
        for target, site in CRAWLER_PATCH_TARGETS
    ]
    for p in patches:
        p.start()
    try:
        return crawl_all_sites(settings)
    finally:
        for p in patches:
            p.stop()


class TestConnectionLifetime:
    def test_connection_closed_before_next_crawler_starts(self):
        """크롤러마다 커넥션을 열고 닫는다 — 크롤링 중 유휴 커넥션이 남지 않는다."""
        recorder = _Recorder()

        _run_with_recorder(recorder)

        assert recorder.events == [
            "open", "crawl:danawa", "close",
            "open", "crawl:compuzone", "close",
            "open", "crawl:pc_estimate", "close",
        ]

    def test_each_crawler_gets_a_distinct_connection(self):
        """커넥션 객체를 크롤러 간에 재사용하지 않는다."""
        recorder = _Recorder()

        _run_with_recorder(recorder)

        conns = list(recorder.conns_by_site.values())
        assert len(conns) == 3
        assert len(set(conns)) == 3, f"커넥션이 재사용됐다: {conns}"

    def test_connection_opened_once_per_crawler(self):
        """크롤러 수만큼만 커넥션을 연다 — 요청마다 열지 않는다."""
        recorder = _Recorder()

        _run_with_recorder(recorder)

        assert recorder.events.count("open") == 3
        assert recorder.events.count("close") == 3
