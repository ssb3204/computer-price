"""Standalone pipeline runner — Airflow 없이 전체 파이프라인 실행.

GitHub Actions 또는 로컬에서 직접 실행:
    python run_pipeline.py

환경변수 필요:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
    SNOWFLAKE_WAREHOUSE (기본값: COMPUTE_WH)
    SNOWFLAKE_DATABASE  (기본값: COMPUTER_PRICE)
    SLACK_WEBHOOK_URL   (선택, 크롤링 실패 시 알림)
"""

import logging
import sys
import time

from src.common.config import SnowflakeSettings
from src.pipeline.analytics import aggregate_analytics
from src.pipeline.crawl import crawl_all_sites
from src.pipeline.detect import detect_changes
from src.pipeline.load_raw import load_raw
from src.pipeline.observability import PipelineTracker
from src.pipeline.quality import check_cross_site_prices
from src.pipeline.slack import send_slack_failures
from src.pipeline.transform import transform_staging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    logger.info("=== 파이프라인 시작 ===")
    settings = SnowflakeSettings()

    tracker = PipelineTracker(settings)
    tracker.start()

    # Step 1: 크롤링
    t = time.monotonic()
    try:
        all_raw, crawl_failures = crawl_all_sites(settings)
        tracker.record_step("crawl", "SUCCESS", duration_sec=time.monotonic() - t, record_count=len(all_raw))
    except Exception as exc:
        tracker.record_step("crawl", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))
        tracker.finish("FAILED", error_msg=str(exc))
        return 1

    # Step 2: Raw 적재
    t = time.monotonic()
    if all_raw:
        try:
            load_raw(settings, all_raw)
            tracker.record_step("load_raw", "SUCCESS", duration_sec=time.monotonic() - t, record_count=len(all_raw))
        except Exception as exc:
            tracker.record_step("load_raw", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))
            tracker.finish("FAILED", error_msg=str(exc))
            return 1
    else:
        tracker.record_step("load_raw", "SKIPPED", duration_sec=0)

    # Step 3: Staging 변환
    t = time.monotonic()
    try:
        transform_staging(settings)
        tracker.record_step("transform", "SUCCESS", duration_sec=time.monotonic() - t)
    except Exception as exc:
        tracker.record_step("transform", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))
        tracker.finish("FAILED", error_msg=str(exc))
        return 1

    # Step 3.5: 교차 검증 (WARNING만, 파이프라인 계속)
    t = time.monotonic()
    try:
        check_cross_site_prices(settings)
        tracker.record_step("quality", "SUCCESS", duration_sec=time.monotonic() - t)
    except Exception as exc:
        tracker.record_step("quality", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))

    # Step 4: 변경 감지
    t = time.monotonic()
    try:
        detect_changes(settings)
        tracker.record_step("detect", "SUCCESS", duration_sec=time.monotonic() - t)
    except Exception as exc:
        tracker.record_step("detect", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))

    # Step 5: Slack (크롤링 실패만)
    t = time.monotonic()
    try:
        send_slack_failures(crawl_failures)
        tracker.record_step("slack", "SUCCESS", duration_sec=time.monotonic() - t)
    except Exception as exc:
        tracker.record_step("slack", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))

    # Step 6: Analytics 집계
    t = time.monotonic()
    try:
        aggregate_analytics(settings)
        tracker.record_step("analytics", "SUCCESS", duration_sec=time.monotonic() - t)
    except Exception as exc:
        tracker.record_step("analytics", "FAILED", duration_sec=time.monotonic() - t, error_msg=str(exc))

    logger.info("=== 파이프라인 완료 ===")

    # 모든 사이트가 실패하면 FAILED, 일부 실패하면 PARTIAL
    if len(crawl_failures) == 3:
        logger.error("모든 사이트 크롤링 실패 — exit 1")
        tracker.finish("FAILED", error_msg="all 3 sites failed")
        return 1

    final_status = "PARTIAL" if crawl_failures else "SUCCESS"
    tracker.finish(final_status)
    return 0


if __name__ == "__main__":
    sys.exit(main())
