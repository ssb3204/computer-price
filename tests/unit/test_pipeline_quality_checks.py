"""Unit tests: 파이프라인 데이터 품질 검사."""

from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline.crawl import crawl_all_sites
from src.pipeline.quality import LayerConsistencyResult, _find_cross_site_anomalies, check_layer_consistency


# ── 교차 검증 순수 함수 ──────────────────────────────────────────────────────


class TestFindCrossSiteAnomalies:
    def test_single_site_no_anomaly(self):
        """한 사이트만 있으면 비교 불가 → 이상 없음."""
        rows = [("RTX 5080", "다나와", 1_500_000)]
        assert _find_cross_site_anomalies(rows) == []

    def test_similar_prices_no_anomaly(self):
        """두 사이트 가격 차이가 임계값 미만이면 이상 없음."""
        rows = [
            ("RTX 5080", "다나와",  1_500_000),
            ("RTX 5080", "컴퓨존",  1_520_000),  # ~1.3% 차이
        ]
        assert _find_cross_site_anomalies(rows) == []

    def test_anomaly_detected_above_threshold(self):
        """한 사이트 가격이 30% 이상 튀면 이상으로 감지."""
        rows = [
            ("RTX 5080", "다나와",  1_500_000),
            ("RTX 5080", "컴퓨존",  2_100_000),  # 40% 초과
        ]
        anomalies = _find_cross_site_anomalies(rows)
        assert len(anomalies) >= 1
        assert any(a[0] == "RTX 5080" for a in anomalies)

    def test_custom_threshold(self):
        """임계값을 변경하면 감지 기준도 달라진다."""
        rows = [
            ("RAM DDR5", "다나와",  100_000),
            ("RAM DDR5", "컴퓨존",  115_000),  # 15% 차이
        ]
        # 기본 20%: 이상 없음 (15% < 20%)
        assert _find_cross_site_anomalies(rows) == []
        # 10%로 낮추면 감지
        assert len(_find_cross_site_anomalies(rows, threshold=10.0)) >= 1

    def test_different_products_not_compared(self):
        """다른 제품끼리는 비교하지 않는다."""
        rows = [
            ("RTX 5080", "다나와",  1_500_000),
            ("RX 9070",  "컴퓨존",  800_000),
        ]
        assert _find_cross_site_anomalies(rows) == []

    def test_empty_rows(self):
        assert _find_cross_site_anomalies([]) == []

    def test_three_sites_one_outlier(self):
        """셋 중 하나만 튀어도 감지한다."""
        rows = [
            ("CPU 7800X3D", "다나와",  500_000),
            ("CPU 7800X3D", "컴퓨존",  510_000),
            ("CPU 7800X3D", "견적왕",  800_000),  # 60% 이상 편차
        ]
        anomalies = _find_cross_site_anomalies(rows)
        assert any(a[1] == "견적왕" for a in anomalies)


# ── 건수 0 체크 ──────────────────────────────────────────────────────────────


class TestZeroCountCheck:
    def _make_settings(self):
        from src.common.config import SnowflakeSettings
        return MagicMock(spec=SnowflakeSettings)

    def test_zero_result_added_to_failures(self):
        """크롤러가 0건 반환하면 failures에 추가된다."""
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.__exit__ = MagicMock(return_value=False)

        danawa = MagicMock()
        danawa.crawl_raw.return_value = []
        danawa.site_name = "danawa"

        compuzone = MagicMock()
        compuzone.crawl_raw.return_value = []
        compuzone.site_name = "compuzone"

        pc = MagicMock()
        pc.crawl_raw.return_value = []
        pc.site_name = "pc_estimate"

        with (
            patch("src.pipeline.crawl.get_connection", return_value=mock_ctx),
            patch("src.pipeline.crawl.DanawaCrawler", return_value=danawa),
            patch("src.pipeline.crawl.CompuzoneCrawler", return_value=compuzone),
            patch("src.pipeline.crawl.PCEstimateCrawler", return_value=pc),
        ):
            _, failures = crawl_all_sites(self._make_settings())

        assert len(failures) == 3
        site_names = {f["site_name"] for f in failures}
        assert site_names == {"danawa", "compuzone", "pc_estimate"}

    def test_partial_zero_only_zero_sites_in_failures(self):
        """일부 사이트만 0건일 때 해당 사이트만 failures에 들어간다."""
        from src.common.models import RawCrawledPrice
        from datetime import timezone
        from datetime import datetime

        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=MagicMock())
        mock_ctx.__exit__ = MagicMock(return_value=False)

        dummy = RawCrawledPrice(
            site="danawa", category="CPU", product_name="Test",
            price_text="100,000원", brand="테스트", url="https://example.com",
            crawled_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
        )

        danawa = MagicMock()
        danawa.crawl_raw.return_value = [dummy]
        danawa.site_name = "danawa"

        compuzone = MagicMock()
        compuzone.crawl_raw.return_value = []
        compuzone.site_name = "compuzone"

        pc = MagicMock()
        pc.crawl_raw.return_value = []
        pc.site_name = "pc_estimate"

        with (
            patch("src.pipeline.crawl.get_connection", return_value=mock_ctx),
            patch("src.pipeline.crawl.DanawaCrawler", return_value=danawa),
            patch("src.pipeline.crawl.CompuzoneCrawler", return_value=compuzone),
            patch("src.pipeline.crawl.PCEstimateCrawler", return_value=pc),
        ):
            all_raw, failures = crawl_all_sites(self._make_settings())

        assert len(failures) == 2
        assert len(all_raw) == 1
        failure_sites = {f["site_name"] for f in failures}
        assert "danawa" not in failure_sites


# ── 레이어 정합성 체크 ─────────────────────────────────────────────────────────


def _make_mock_conn(raw_count, staging_count, missing_analytics):
    """check_layer_consistency용 Mock Snowflake 커넥션 생성."""
    cur = MagicMock()
    cur.fetchone.side_effect = [
        (raw_count, staging_count),   # Raw vs Staging 건수
        (missing_analytics,),          # Analytics 누락
    ]
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn


class TestLayerConsistencyResult:
    def test_total_issues_all_ok(self):
        r = LayerConsistencyResult(
            raw_count=32, staging_count=31, drop_count=1,
            drop_rate=3.1, missing_analytics=0,
        )
        assert r.total_issues == 0

    def test_total_issues_high_drop_rate(self):
        r = LayerConsistencyResult(
            raw_count=32, staging_count=20, drop_count=12,
            drop_rate=37.5, missing_analytics=0,
        )
        assert r.total_issues == 1  # 손실률 초과 1건

    def test_total_issues_missing_counts(self):
        r = LayerConsistencyResult(
            raw_count=32, staging_count=32, drop_count=0,
            drop_rate=0.0, missing_analytics=3,
        )
        assert r.total_issues == 3  # Analytics 누락 3건


class TestCheckLayerConsistency:
    def _make_settings(self):
        from src.common.config import SnowflakeSettings
        return MagicMock(spec=SnowflakeSettings)

    def test_normal_case_no_issues(self):
        """정상 케이스: 손실 낮고 누락 없음."""
        mock_conn = _make_mock_conn(32, 31, 0)
        with patch("src.pipeline.quality.get_connection", return_value=mock_conn), \
             patch("src.pipeline.quality._send_slack_message") as mock_slack:
            result = check_layer_consistency(self._make_settings())

        assert result.raw_count == 32
        assert result.staging_count == 31
        assert result.drop_count == 1
        assert abs(result.drop_rate - 3.125) < 0.01
        assert result.missing_analytics == 0
        assert result.total_issues == 0
        mock_slack.assert_not_called()

    def test_high_drop_rate_triggers_slack(self):
        """손실률 10% 초과 시 Slack 알림."""
        mock_conn = _make_mock_conn(32, 10, 0)
        with patch("src.pipeline.quality.get_connection", return_value=mock_conn), \
             patch("src.pipeline.quality._send_slack_message") as mock_slack:
            result = check_layer_consistency(self._make_settings())

        assert result.drop_rate > 10.0
        assert result.total_issues == 1
        mock_slack.assert_called_once()
        assert "손실률" in mock_slack.call_args[0][0]

    def test_missing_analytics_triggers_slack(self):
        """Analytics 누락 상품 있으면 Slack 알림."""
        mock_conn = _make_mock_conn(30, 30, 5)
        with patch("src.pipeline.quality.get_connection", return_value=mock_conn), \
             patch("src.pipeline.quality._send_slack_message") as mock_slack:
            result = check_layer_consistency(self._make_settings())

        assert result.missing_analytics == 5
        assert result.total_issues == 5
        mock_slack.assert_called_once()
        assert "Analytics" in mock_slack.call_args[0][0]

    def test_zero_raw_count_no_division_error(self):
        """Raw 건수가 0이면 drop_rate=0.0으로 처리 (ZeroDivisionError 방지)."""
        mock_conn = _make_mock_conn(0, 0, 0)
        with patch("src.pipeline.quality.get_connection", return_value=mock_conn), \
             patch("src.pipeline.quality._send_slack_message"):
            result = check_layer_consistency(self._make_settings())

        assert result.drop_rate == 0.0
        assert result.total_issues == 0
