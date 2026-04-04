"""Unit tests: 파이프라인 데이터 품질 검사."""

from unittest.mock import MagicMock, patch

import pytest

from run_pipeline import _find_cross_site_anomalies, crawl_all_sites


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
            patch("run_pipeline.get_connection", return_value=mock_ctx),
            patch("run_pipeline.DanawaCrawler", return_value=danawa),
            patch("run_pipeline.CompuzoneCrawler", return_value=compuzone),
            patch("run_pipeline.PCEstimateCrawler", return_value=pc),
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
            patch("run_pipeline.get_connection", return_value=mock_ctx),
            patch("run_pipeline.DanawaCrawler", return_value=danawa),
            patch("run_pipeline.CompuzoneCrawler", return_value=compuzone),
            patch("run_pipeline.PCEstimateCrawler", return_value=pc),
        ):
            all_raw, failures = crawl_all_sites(self._make_settings())

        assert len(failures) == 2
        assert len(all_raw) == 1
        failure_sites = {f["site_name"] for f in failures}
        assert "danawa" not in failure_sites
