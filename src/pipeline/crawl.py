"""Step 1: 크롤링 — 3개 사이트에서 Raw 가격 수집."""

import logging
from datetime import datetime, timezone

import requests

from src.common.config import MySQLSettings
from src.common.models import RawCrawledPrice
from src.common.mysql_client import get_connection
from src.crawlers.compuzone import CompuzoneCrawler
from src.crawlers.compuzone import crawl_single as compuzone_single
from src.crawlers.danawa import DanawaCrawler
from src.crawlers.danawa import crawl_single as danawa_single
from src.crawlers.pc_estimate import PCEstimateCrawler
from src.crawlers.pc_estimate import crawl_single as pcest_single

logger = logging.getLogger(__name__)


def crawl_all_sites(settings: MySQLSettings) -> tuple[list[RawCrawledPrice], list[dict]]:
    """3개 사이트를 순서대로 크롤링. 실패한 사이트는 crawl_failures에 기록."""
    all_raw: list[RawCrawledPrice] = []
    crawl_failures: list[dict] = []

    with get_connection(settings) as conn:
        crawlers = [
            DanawaCrawler(conn=conn),
            CompuzoneCrawler(conn=conn),
            PCEstimateCrawler(conn=conn),
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


_SINGLE_CRAWL_FN = {
    "다나와": danawa_single,
    "컴퓨존": compuzone_single,
    "견적왕": pcest_single,
}


def crawl_and_load_single(
    settings: MySQLSettings,
    site: str,
    query: str,
    pcode: str,
    category: str,
    brand: str | None,
) -> bool:
    """WATCHLIST 추가 직후 단일 상품을 즉시 크롤링해 파이프라인에 적재.

    백그라운드 스레드에서 호출. 실패해도 WATCHLIST 추가는 유지되며
    다음 스케줄 크롤링에서 재시도된다.
    """
    from src.pipeline.load_raw import load_raw
    from src.pipeline.transform import transform_staging

    crawl_fn = _SINGLE_CRAWL_FN.get(site)
    if crawl_fn is None:
        logger.warning("[즉시 크롤링] 지원하지 않는 사이트: %s", site)
        return False

    try:
        raw = crawl_fn(query, pcode, category, brand)
        if raw is None:
            logger.warning("[즉시 크롤링] 상품 미발견: site=%s pcode=%s", site, pcode)
            return False
        load_raw(settings, [raw])
        transform_staging(settings)
        logger.info("[즉시 크롤링] 완료: site=%s pcode=%s", site, pcode)
        return True
    except Exception:
        logger.exception("[즉시 크롤링] 실패: site=%s pcode=%s", site, pcode)
        return False
