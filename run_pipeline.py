"""Standalone pipeline runner — Airflow 없이 전체 파이프라인 실행.

GitHub Actions 또는 로컬에서 직접 실행:
    python run_pipeline.py            # 평상시 — Staging 변환은 최근 7일 수집분만
    python run_pipeline.py --full     # 전체 점검 — 창 밖에 남은 미처리 raw 행까지 변환

환경변수 필요 (MYSQL_ 접두사, .env 파일 또는 환경변수로 주입):
    MYSQL_HOST      (기본값: localhost)
    MYSQL_PORT      (기본값: 3306)
    MYSQL_USER      (기본값: price_app)
    MYSQL_PASSWORD  (필수)
    MYSQL_DATABASE  (기본값: computer_price)
    SLACK_WEBHOOK_URL (선택, 크롤링 실패 시 알림)
"""
import argparse
import logging
import sys

from src.common.config import MySQLSettings
from src.pipeline.analytics import aggregate_analytics
from src.pipeline.crawl import crawl_all_sites
from src.pipeline.detect import detect_changes
from src.pipeline.load_raw import load_raw
from src.pipeline.quality import check_cross_site_prices, check_layer_consistency
from src.pipeline.slack import send_slack_failures
from src.pipeline.transform import INCREMENTAL_WINDOW_DAYS, transform_staging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="컴퓨터 가격 파이프라인 실행")
    parser.add_argument(
        "--full",
        action="store_true",
        help=f"Staging 변환을 최근 {INCREMENTAL_WINDOW_DAYS}일 창 없이 미처리 raw "
             "전체에 대해 수행한다. 수집이 며칠 멈춰 창 밖으로 밀려난 행을 회수하는 "
             "용도 (주 1회 권장).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logger.info("=== 파이프라인 시작%s ===", " (전체 점검)" if args.full else "")
    settings = MySQLSettings()

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
        count = transform_staging(settings, full_scan=args.full)
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
