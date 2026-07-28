"""Step 1: 크롤링 — 3개 사이트에서 Raw 가격 수집."""

import logging
from datetime import UTC, datetime

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
    """3개 사이트를 순서대로 크롤링. 실패한 사이트는 crawl_failures에 기록.

    커넥션은 크롤러마다 새로 연다. 커넥션의 유일한 사용처는 crawl_raw() 첫머리의
    워치리스트 조회뿐인데, 하나를 3개 크롤러에 걸쳐 재사용하면 앞선 사이트가
    무응답으로 수 분간 붙들려 있는 동안 유휴 커넥션이 경로 중간에서 끊긴다.
    (2026-07-28 장애: 컴퓨존 5분 타임아웃 → 견적왕이 죽은 커넥션을 물려받아 2006)
    """
    all_raw: list[RawCrawledPrice] = []
    crawl_failures: list[dict] = []

    # 모듈 전역이 아닌 호출 시점에 해석한다 — 테스트가 클래스를 patch할 수 있어야 한다.
    crawler_classes = (DanawaCrawler, CompuzoneCrawler, PCEstimateCrawler)

    for crawler_cls in crawler_classes:
        with get_connection(settings) as conn:
            crawler = crawler_cls(conn=conn)
            try:
                raw_prices = crawler.crawl_raw()
                all_raw.extend(raw_prices)
                logger.info("[크롤링] %s: %d건", crawler.site_name, len(raw_prices))
                if len(raw_prices) == 0:
                    failed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
                    crawl_failures.append({
                        "site_name": crawler.site_name,
                        "error": "크롤링 결과 0건 — 페이지 구조 변경 의심",
                        "failed_at": failed_at,
                    })
            # 사이트 하나의 실패가 나머지 사이트와 이미 수집한 데이터를 죽이면 안 된다.
            # 예외 종류를 좁게 나열하면 목록 밖 예외(2026-07-28: pymysql
            # OperationalError)가 전파돼 파이프라인 전체가 중단된다. 여기서 삼킨
            # 예외는 로그와 crawl_failures(→ Slack 알림)에 남으므로 조용히 묻히지 않는다.
            except Exception as e:
                failed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
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
