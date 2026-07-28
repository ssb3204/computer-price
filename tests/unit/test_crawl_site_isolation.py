"""Unit tests: 사이트 단위 실패 격리.

배경 — 2026-07-28 파이프라인 장애.
견적왕에서 터진 pymysql OperationalError가 crawl_all_sites의 except 튜플
(requests.RequestException, ValueError, TypeError, AttributeError, KeyError)에
걸리지 않아 그대로 전파됐고, run_pipeline이 이를 크롤링 단계 실패로 처리해
exit 1 했다. 그 바람에 이미 수집을 마친 다나와 1건까지 적재되지 못했다.

한 사이트에서 무슨 예외가 나든 나머지 사이트와 이미 모은 데이터는 살아야 한다.
"""

from unittest.mock import MagicMock, patch

import pytest
from pymysql.err import OperationalError

from src.common.config import MySQLSettings
from src.common.models import RawCrawledPrice
from src.pipeline.crawl import crawl_all_sites

DANAWA_RAW = RawCrawledPrice(
    site="danawa", category="CPU", product_name="AMD 라이젠 7800X3D",
    price_text="450,000원", brand="AMD", url="https://example.com/1",
    crawled_at=None,
)


def _crawler(site_name, *, returns=None, raises=None):
    crawler = MagicMock()
    crawler.site_name = site_name
    if raises is not None:
        crawler.crawl_raw.side_effect = raises
    else:
        crawler.crawl_raw.return_value = list(returns or [])
    return crawler


def _run(danawa, compuzone, pc_estimate):
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=MagicMock())
    mock_ctx.__exit__ = MagicMock(return_value=False)

    with (
        patch("src.pipeline.crawl.get_connection", return_value=mock_ctx),
        patch("src.pipeline.crawl.DanawaCrawler", return_value=danawa),
        patch("src.pipeline.crawl.CompuzoneCrawler", return_value=compuzone),
        patch("src.pipeline.crawl.PCEstimateCrawler", return_value=pc_estimate),
    ):
        return crawl_all_sites(MagicMock(spec=MySQLSettings))


class TestSiteFailureIsolation:
    @pytest.mark.parametrize(
        ("exc", "label"),
        [
            (OperationalError(2006, "MySQL server has gone away"), "DB 커넥션 사망"),
            (RuntimeError("예상 못한 오류"), "예상 밖 예외"),
            (KeyError("category"), "기존에 잡던 예외"),
        ],
    )
    def test_failure_in_one_site_does_not_abort_the_run(self, exc, label):
        """어떤 예외든 사이트 단위로 격리된다 — 실패가 전파되면 안 된다."""
        danawa = _crawler("danawa", returns=[DANAWA_RAW])
        compuzone = _crawler("compuzone", raises=exc)
        pc_estimate = _crawler("kjwwang", returns=[])

        all_raw, failures = _run(danawa, compuzone, pc_estimate)

        assert all_raw == [DANAWA_RAW], f"{label}: 이미 수집한 데이터가 유실됐다"
        pc_estimate.crawl_raw.assert_called_once(), f"{label}: 뒤 사이트가 실행되지 않았다"
        assert {f["site_name"] for f in failures} == {"compuzone", "kjwwang"}

    def test_failed_site_records_exception_type(self):
        """Slack 알림이 원인을 구분할 수 있어야 한다."""
        _, failures = _run(
            _crawler("danawa", returns=[DANAWA_RAW]),
            _crawler("compuzone", raises=OperationalError(2006, "gone away")),
            _crawler("kjwwang", returns=[DANAWA_RAW]),
        )

        assert len(failures) == 1
        assert failures[0]["site_name"] == "compuzone"
        assert "OperationalError" in failures[0]["error"]

    def test_all_sites_failing_still_returns_normally(self):
        """전멸해도 예외가 아니라 failures 3건으로 보고한다 (exit 판단은 호출자 몫)."""
        all_raw, failures = _run(
            _crawler("danawa", raises=OperationalError(2006, "gone away")),
            _crawler("compuzone", raises=RuntimeError("boom")),
            _crawler("kjwwang", raises=ValueError("bad")),
        )

        assert all_raw == []
        assert len(failures) == 3
