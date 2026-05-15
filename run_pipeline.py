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

from src.common.config import SnowflakeSettings
from src.pipeline.analytics import aggregate_analytics
from src.pipeline.crawl import crawl_all_sites
from src.pipeline.detect import detect_changes
from src.pipeline.load_raw import load_raw
from src.pipeline.quality import check_cross_site_prices, check_layer_consistency
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

    # Step 1: 크롤링
    try:
        all_raw, crawl_failures = crawl_all_sites(settings)
        logger.info("[crawl] SUCCESS — %d건", len(all_raw))
    except Exception as exc:
        logger.error("[crawl] FAILED — %s", exc)
        return 1

    # Step 2: Raw 적재
    if all_raw:
        try:
            load_raw(settings, all_raw)
            logger.info("[load_raw] SUCCESS — %d건", len(all_raw))
        except Exception as exc:
            logger.error("[load_raw] FAILED — %s", exc)
            return 1
    else:
        logger.info("[load_raw] SKIPPED — 수집 데이터 없음")

    # Step 3: Staging 변환
    try:
        count = transform_staging(settings)
        logger.info("[transform] SUCCESS — %d건", count)
    except Exception as exc:
        logger.error("[transform] FAILED — %s", exc)
        return 1

    # Step 3.5: 레이어 정합성 검증
    try:
        result = check_layer_consistency(settings)
        logger.info("[quality] Raw=%d Staging=%d 손실률=%.1f%%",
                    result.raw_count, result.staging_count, result.drop_rate)
        check_cross_site_prices(settings)
    except Exception as exc:
        logger.error("[quality] FAILED — %s", exc)

    # Step 4: 변경 감지
    try:
        alert_count = detect_changes(settings)
        logger.info("[detect] SUCCESS — %d건 알림", alert_count)
    except Exception as exc:
        logger.error("[detect] FAILED — %s", exc)

    # Step 5: Slack (크롤링 실패만)
    try:
        send_slack_failures(crawl_failures)
    except Exception as exc:
        logger.error("[slack] FAILED — %s", exc)

    # Step 6: Analytics 집계
    try:
        aggregate_analytics(settings)
        logger.info("[analytics] SUCCESS")
    except Exception as exc:
        logger.error("[analytics] FAILED — %s", exc)

    logger.info("=== 파이프라인 완료 ===")

    if len(crawl_failures) == 3:
        logger.error("모든 사이트 크롤링 실패 — exit 1")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
