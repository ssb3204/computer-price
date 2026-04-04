"""Step 1: 크롤링 — 3개 사이트에서 Raw 가격 수집."""

import logging
from datetime import datetime, timezone

import requests

from src.common.config import SnowflakeSettings
from src.common.models import RawCrawledPrice
from src.common.snowflake_client import get_connection
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.danawa import DanawaCrawler
from src.crawlers.pc_estimate import PCEstimateCrawler

logger = logging.getLogger(__name__)


def crawl_all_sites(settings: SnowflakeSettings) -> tuple[list[RawCrawledPrice], list[dict]]:
    """3개 사이트를 순서대로 크롤링. 실패한 사이트는 crawl_failures에 기록."""
    all_raw: list[RawCrawledPrice] = []
    crawl_failures: list[dict] = []

    with get_connection(settings) as conn:
        crawlers = [
            DanawaCrawler(conn=conn),
            CompuzoneCrawler(),
            PCEstimateCrawler(),
        ]
        for crawler in crawlers:
            try:
                raw_prices = crawler.crawl_raw()
                all_raw.extend(raw_prices)
                logger.info("[크롤링] %s: %d건", crawler.site_name, len(raw_prices))
                if len(raw_prices) == 0:
                    failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    crawl_failures.append({
                        "site_name": crawler.site_name,
                        "error": "크롤링 결과 0건 — 페이지 구조 변경 의심",
                        "failed_at": failed_at,
                    })
            except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError) as e:
                failed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                crawl_failures.append({
                    "site_name": crawler.site_name,
                    "error": f"{type(e).__name__}: {e}",
                    "failed_at": failed_at,
                })
                logger.exception("[크롤링] %s 실패", crawler.site_name)

    logger.info("[크롤링] 총 %d건 수집 (실패: %d개 사이트)", len(all_raw), len(crawl_failures))
    return all_raw, crawl_failures
