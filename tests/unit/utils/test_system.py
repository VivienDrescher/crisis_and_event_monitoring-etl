"""
Unit tests for src.utils.system module.

Covers:
- Environment utils: load_env
- Logging utils: PrefixedLogger, setup_pipeline_logging, setup_standalone_logging
- Retry utilities: with_retries
- Versioning: get_git_commit
- Time utilities: now_iso, get_date_range
"""

import logging
import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from src.utils.system import (
    PrefixedLogger,
    get_date_range,
    get_git_commit,
    get_log_file_path,
    load_env,
    now_iso,
    setup_pipeline_logging,
    setup_standalone_logging,
    with_retries,
)


class TestEnvironmentUtils:
    def test_load_env_explicit_file(self, tmp_path):
        env_file = tmp_path / ".env.test"
        env_file.write_text("TEST_VAR=42")
        path = load_env(env_file)
        assert path == env_file
        assert os.getenv("TEST_VAR") == "42"

    def test_load_env_nonexistent_file_raises(self):
        with pytest.raises(ValueError, match="Specified env file does not exist"):
            load_env("/nonexistent.env")


class TestPrefixedLogger:
    def test_prefix_added(self, caplog):
        logger = logging.getLogger("prefixed_test")
        prefixed = PrefixedLogger(logger, prefix=">> ")
        with caplog.at_level(logging.INFO):
            prefixed.info("message")
        assert ">> message" in caplog.text


class TestPipelineLogging:
    def test_setup_pipeline_logging_creates_handlers(self, tmp_path):
        run_id = "run1"
        logger = setup_pipeline_logging(run_id, "pipeline", tmp_path, debug=True)
        assert logger.level == logging.DEBUG
        log_path = get_log_file_path(logger)
        assert log_path.parent.exists()
        assert log_path.name == "end2end_pipeline.log"

    def test_setup_standalone_logging_creates_file_handler(self, tmp_path):
        run_id = "run1"
        logging.getLogger("pipeline").handlers.clear()
        logger = setup_standalone_logging(
            run_id, "pipeline", "bronze", "table", tmp_path, debug=True
        )
        log_path = get_log_file_path(logger)
        assert log_path.parent.exists()
        assert log_path.name == f"{run_id}.log"


class TestRetryUtils:
    def test_with_retries_success_first_try(self):
        logger = logging.getLogger("retry_test")
        fn = MagicMock(return_value=42)
        result = with_retries(fn, max_retries=3, backoff=0, logger=logger)
        assert result == 42
        assert fn.call_count == 1

    def test_with_retries_retries_and_succeeds(self):
        logger = logging.getLogger("retry_test")
        calls = [0]

        def fn():
            calls[0] += 1
            if calls[0] < 2:
                raise ValueError("fail")
            return 99

        result = with_retries(fn, max_retries=3, backoff=0, logger=logger)
        assert result == 99
        assert calls[0] == 2

    def test_with_retries_exceeds_retries(self):
        logger = logging.getLogger("retry_test")

        def fn():
            raise ValueError("fail")

        with pytest.raises(ValueError):
            with_retries(fn, max_retries=2, backoff=0, logger=logger)


class TestVersioningUtils:
    def test_get_git_commit_returns_hash_or_unknown(self):
        # If git is available, hash should be 40 chars
        commit = get_git_commit()
        assert isinstance(commit, str)
        assert len(commit) == 40 or commit == "unknown"


class TestTimeUtils:
    def test_now_iso_returns_string(self):
        tz = ZoneInfo("UTC")
        ts = now_iso(tz)
        assert isinstance(ts, str)
        assert "+" in ts or "Z" in ts or "00:00" in ts

    def test_get_date_range_datetime_format(self):
        start = datetime(2026, 2, 16, tzinfo=ZoneInfo("UTC"))
        end = datetime(2026, 2, 18, tzinfo=ZoneInfo("UTC"))
        result = get_date_range(
            start, end, step=timedelta(days=1), output_format="datetime"
        )
        assert all(isinstance(d, datetime) for d in result)
        assert result[0] == start
        assert result[-1] == end

    def test_get_date_range_iso_format(self):
        start = datetime(2026, 2, 16, tzinfo=ZoneInfo("UTC"))
        end = datetime(2026, 2, 16, tzinfo=ZoneInfo("UTC"))
        result = get_date_range(start, end, step=timedelta(days=1), output_format="iso")
        assert result[0] == start.isoformat(sep=" ", timespec="seconds")

    def test_get_date_range_iso_tuple_format(self):
        start = datetime(2026, 2, 16, tzinfo=ZoneInfo("UTC"))
        end = datetime(2026, 2, 16, tzinfo=ZoneInfo("UTC"))
        result = get_date_range(
            start, end, step=timedelta(days=1), output_format="iso_tuple"
        )
        assert result[0] == (start.isoformat(sep=" ", timespec="seconds"),)

    def test_get_date_range_raises_for_naive_dates(self):
        start = datetime(2026, 2, 16)
        end = datetime(2026, 2, 16)
        with pytest.raises(ValueError, match="timezone-aware"):
            get_date_range(start, end)
