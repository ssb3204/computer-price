"""Abstract base crawler with retry logic and rate limiting."""

import logging
import time
from abc import ABC, abstractmethod

import requests

from src.common.models import RawPrice

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

REQUEST_DELAY_SECONDS = 2.0
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2.0


class BaseCrawler(ABC):
    def __init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)
        self._last_request_time: float = 0.0

    @property
    @abstractmethod
    def site_name(self) -> str: ...

    @abstractmethod
    def get_target_urls(self) -> list[str]: ...

    @abstractmethod
    def parse_page(self, html: str, url: str) -> list[RawPrice]: ...

    def crawl(self) -> list[RawPrice]:
        all_prices: list[RawPrice] = []
        urls = self.get_target_urls()

        for url in urls:
            html = self._fetch_with_retry(url)
            if html is None:
                continue
            try:
                prices = self.parse_page(html, url)
                all_prices.extend(prices)
                logger.info("Parsed %d prices from %s", len(prices), url)
            except Exception:
                logger.exception("Failed to parse %s", url)

        logger.info("Crawled %d total prices from %s", len(all_prices), self.site_name)
        return all_prices

    def _fetch_with_retry(self, url: str) -> str | None:
        for attempt in range(MAX_RETRIES):
            self._rate_limit()
            try:
                resp = self._session.get(url, timeout=30)
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp.text
            except requests.RequestException as e:
                wait = RETRY_BACKOFF_BASE ** attempt
                logger.warning(
                    "Request failed (attempt %d/%d) for %s: %s. Retrying in %.1fs",
                    attempt + 1, MAX_RETRIES, url, e, wait,
                )
                time.sleep(wait)

        logger.error("All retries exhausted for %s", url)
        return None

    def _rate_limit(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < REQUEST_DELAY_SECONDS:
            time.sleep(REQUEST_DELAY_SECONDS - elapsed)
        self._last_request_time = time.time()
