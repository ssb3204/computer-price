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

    # Step 1: 크롤링
    all_raw, crawl_failures = crawl_all_sites(settings)

    # Step 2: Raw 적재
    if all_raw:
        load_raw(settings, all_raw)

    # Step 3: Staging 변환
    transform_staging(settings)

    # Step 3.5: 교차 검증 (WARNING만, 파이프라인 계속)
    check_cross_site_prices(settings)

    # Step 4: 변경 감지
    detect_changes(settings)

    # Step 5: Slack (크롤링 실패만)
    send_slack_failures(crawl_failures)

    # Step 6: Analytics 집계
    aggregate_analytics(settings)

    logger.info("=== 파이프라인 완료 ===")

    # 모든 사이트가 실패하면 비정상 종료
    if len(crawl_failures) == 3:
        logger.error("모든 사이트 크롤링 실패 — exit 1")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
