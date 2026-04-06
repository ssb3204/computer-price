"""Unit tests: PipelineTracker — Snowflake 호출은 mock 처리."""

from unittest.mock import MagicMock, call, patch

import pytest

from src.pipeline.observability import PipelineTracker


@pytest.fixture
def settings():
    s = MagicMock()
    s.account = "test-account"
    return s


@pytest.fixture
def mock_conn():
    """get_connection context manager mock."""
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    return conn, cur


class TestPipelineTrackerRunId:
    def test_run_id_format(self, settings):
        """run_id가 run_YYYYMMDD_HHMMSS 형식인지 확인."""
        tracker = PipelineTracker(settings)
        assert tracker.run_id.startswith("run_")
        parts = tracker.run_id.split("_")
        assert len(parts) == 3
        assert len(parts[1]) == 8   # YYYYMMDD
        assert len(parts[2]) == 6   # HHMMSS

    def test_run_id_unique_per_instance(self, settings):
        """두 인스턴스의 run_id가 다르다 (1초 간격 없이도 문자열 비교)."""
        import time
        t1 = PipelineTracker(settings)
        time.sleep(1.01)  # HHMMSS 단위라 1초 이상 간격 필요
        t2 = PipelineTracker(settings)
        assert t1.run_id != t2.run_id


class TestPipelineTrackerStart:
    def test_start_inserts_running_row(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            returned_id = tracker.start()

        assert returned_id == tracker.run_id
        sql_called = cur.execute.call_args[0][0]
        assert "INSERT INTO RAW.PIPELINE_RUNS" in sql_called
        assert "RUNNING" in sql_called

    def test_start_passes_run_id(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.start()

        args = cur.execute.call_args[0][1]
        assert args[0] == tracker.run_id


class TestPipelineTrackerRecordStep:
    def test_record_step_success(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.record_step("crawl", "SUCCESS", duration_sec=5.0, record_count=200)

        sql_called = cur.execute.call_args[0][0]
        assert "INSERT INTO RAW.PIPELINE_STEP_RUNS" in sql_called

        args = cur.execute.call_args[0][1]
        assert args[0] == tracker.run_id    # RUN_ID
        assert args[1] == "crawl"           # STEP_NAME
        assert args[4] == 5.0              # DURATION_SEC
        assert args[5] == 200              # RECORD_COUNT
        assert args[6] == "SUCCESS"        # STATUS
        assert args[7] is None             # ERROR_MSG

    def test_record_step_failed_with_error(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.record_step("transform", "FAILED", duration_sec=1.0, error_msg="conn error")

        args = cur.execute.call_args[0][1]
        assert args[6] == "FAILED"
        assert args[7] == "conn error"

    def test_record_step_skipped(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.record_step("load_raw", "SKIPPED", duration_sec=0)

        args = cur.execute.call_args[0][1]
        assert args[6] == "SKIPPED"


class TestPipelineTrackerFinish:
    def test_finish_success_updates_row(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.finish("SUCCESS")

        sql_called = cur.execute.call_args[0][0]
        assert "UPDATE RAW.PIPELINE_RUNS" in sql_called
        assert "STATUS" in sql_called

        args = cur.execute.call_args[0][1]
        assert args[2] == "SUCCESS"     # STATUS
        assert args[3] is None          # ERROR_MSG
        assert args[4] == tracker.run_id

    def test_finish_failed_with_error_msg(self, settings, mock_conn):
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            tracker.finish("FAILED", error_msg="fatal error")

        args = cur.execute.call_args[0][1]
        assert args[2] == "FAILED"
        assert args[3] == "fatal error"

    def test_finish_duration_positive(self, settings, mock_conn):
        """finish() 호출 시 DURATION_SEC이 양수여야 한다."""
        import time
        conn, cur = mock_conn
        with patch("src.pipeline.observability.get_connection", return_value=conn):
            tracker = PipelineTracker(settings)
            time.sleep(0.05)
            tracker.finish("SUCCESS")

        args = cur.execute.call_args[0][1]
        duration = args[1]  # DURATION_SEC
        assert duration > 0
