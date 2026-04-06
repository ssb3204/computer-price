"""파이프라인 실행 이력 추적 — PIPELINE_RUNS / PIPELINE_STEP_RUNS 기록."""

import logging
from datetime import datetime, timezone

from src.common.config import SnowflakeSettings
from src.common.snowflake_client import get_connection

logger = logging.getLogger(__name__)

_UTC = timezone.utc


class PipelineTracker:
    """파이프라인 전체 실행 및 단계별 결과를 Snowflake에 기록한다.

    사용 예::

        tracker = PipelineTracker(settings)
        tracker.start()

        t = time.monotonic()
        crawl_all_sites(...)
        tracker.record_step("crawl", "SUCCESS", duration_sec=time.monotonic()-t, record_count=847)

        tracker.finish("SUCCESS")
    """

    def __init__(self, settings: SnowflakeSettings) -> None:
        self.run_id = datetime.now(_UTC).strftime("run_%Y%m%d_%H%M%S")
        self._settings = settings
        self._started_at = datetime.now(_UTC)

    def start(self) -> str:
        """PIPELINE_RUNS에 RUNNING 상태로 실행 시작을 기록한다. run_id 반환."""
        with get_connection(self._settings) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO RAW.PIPELINE_RUNS
                    (RUN_ID, STARTED_AT, STATUS)
                VALUES (%s, %s, 'RUNNING')
                """,
                (self.run_id, self._started_at),
            )
        logger.info("pipeline run started: %s", self.run_id)
        return self.run_id

    def record_step(
        self,
        step_name: str,
        status: str,
        *,
        duration_sec: float | None = None,
        record_count: int | None = None,
        error_msg: str | None = None,
    ) -> None:
        """단계 실행 결과를 PIPELINE_STEP_RUNS에 삽입한다.

        Args:
            step_name: crawl / load_raw / transform / quality / detect / slack / analytics
            status: SUCCESS / FAILED / SKIPPED
        """
        now = datetime.now(_UTC)
        started_at = (
            datetime.fromtimestamp(now.timestamp() - (duration_sec or 0), tz=_UTC)
            if duration_sec is not None
            else now
        )
        with get_connection(self._settings) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO RAW.PIPELINE_STEP_RUNS
                    (RUN_ID, STEP_NAME, STARTED_AT, FINISHED_AT, DURATION_SEC,
                     RECORD_COUNT, STATUS, ERROR_MSG)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    self.run_id,
                    step_name,
                    started_at,
                    now,
                    duration_sec,
                    record_count,
                    status,
                    error_msg,
                ),
            )
        logger.info("step [%s] %s (%.1fs, %s rows)", step_name, status, duration_sec or 0, record_count)

    def finish(self, status: str, *, error_msg: str | None = None) -> None:
        """PIPELINE_RUNS를 완료 상태로 업데이트한다.

        Args:
            status: SUCCESS / PARTIAL / FAILED
        """
        finished_at = datetime.now(_UTC)
        duration_sec = (finished_at - self._started_at).total_seconds()
        with get_connection(self._settings) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE RAW.PIPELINE_RUNS
                SET FINISHED_AT  = %s,
                    DURATION_SEC = %s,
                    STATUS       = %s,
                    ERROR_MSG    = %s
                WHERE RUN_ID = %s
                """,
                (finished_at, duration_sec, status, error_msg, self.run_id),
            )
        logger.info("pipeline run finished: %s → %s (%.1fs)", self.run_id, status, duration_sec)
