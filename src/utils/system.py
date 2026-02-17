from __future__ import annotations

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Literal, Optional, Union
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# --------------------------
# Environment Utils
# --------------------------


def load_env(env_file: str | Path | None = None) -> Path | None:
    """
    Load environment variables from a .env file.

    Resolution order:
        1. Explicit env_file argument
        2. ENV-specific file (.env.local, .env.dev, .env.prod)
        3. Fallback to .env

    Args:
        env_file: Optional explicit path to .env file

    Returns:
        Path to the .env file that was loaded, or None if no file found.
    """

    if env_file:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            return env_path
        else:
            raise ValueError(f"Specified env file does not exist: {env_path}")

    # ENV-specific resolution
    env = os.getenv("ENV", "local").lower()
    candidates = [f".env.{env}", ".env"]

    for candidate in candidates:
        env_path = Path(candidate)
        if env_path.exists():
            load_dotenv(env_path)
            return env_path

    return None


# --------------------------
# Logging Utils
# --------------------------


class PrefixedLogger:
    """Wrap a logger to automatically prepend a prefix to messages."""

    def __init__(self, logger: logging.Logger, prefix: str = "    "):
        self._logger = logger
        self._prefix = prefix

    def debug(self, msg, *args, **kwargs):
        self._logger.debug(f"{self._prefix}{msg}", *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._logger.info(f"{self._prefix}{msg}", *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._logger.warning(f"{self._prefix}{msg}", *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._logger.error(f"{self._prefix}{msg}", *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._logger.exception(f"{self._prefix}{msg}", *args, **kwargs)


def setup_pipeline_logging(
    run_id: str,
    pipeline_name: str,
    base_log_dir: Union[str, Path],
    debug: Optional[bool] = False,
    log_level: Optional[str] = None,
) -> logging.Logger:
    """
    Configure logging for an end-to-end pipeline run.

    Creates a global pipeline log at:
        <base_log_dir>/<run_id>/end2end_pipeline.log

    Args:
        run_id: Unique identifier for this pipeline run.
        pipeline_name: Name of the pipeline (used as root logger name).
        base_log_dir: Base directory where logs should be stored.
        debug: If True, sets logging level to DEBUG; overrides log_level.
        log_level: Optional string log level (e.g., "INFO", "WARNING").

    Returns:
        Configured root logger for the pipeline.
    """

    root = logging.getLogger(pipeline_name)

    # Log Level
    if debug:
        level = logging.DEBUG
    else:
        level_name = (log_level or "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
    root.setLevel(level)

    root.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Console
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    # Global pipeline log
    base_log_dir = Path(base_log_dir) / run_id
    base_log_dir.mkdir(parents=True, exist_ok=True)

    pipeline_handler = logging.FileHandler(base_log_dir / "end2end_pipeline.log")
    pipeline_handler.setFormatter(formatter)
    root.addHandler(pipeline_handler)

    return root


def setup_standalone_logging(
    run_id: str,
    pipeline_name: str,
    layer: str,
    table_name: str,
    base_log_dir: Union[str, Path],
    debug: Optional[bool] = False,
    log_level: Optional[str] = None,
) -> logging.Logger:
    """
    Configure logging for a standalone layer/table script.

    Creates dataset-specific logs at:
        <base_log_dir>/local_runs/<layer>/<table_name>/<run_id>.log

    If logging is already configured (e.g., orchestrator run),
    this function returns a logger without reconfiguring handlers.

    Args:
        run_id: Unique identifier for this run.
        pipeline_name: Name of the overall pipeline.
        layer: Layer name (e.g., bronze, silver, gold).
        table_name: Table or dataset name.
        base_log_dir: Base directory for logs.
        debug: If True, sets logging level to DEBUG; overrides log_level.
        log_level: Optional string log level (e.g., "INFO", "WARNING").

    Returns:
        Configured logger for the specific layer/table.
    """

    root = logging.getLogger(pipeline_name)

    # If root already configured → assume orchestrator run
    if root.handlers:
        return logging.getLogger(f"{pipeline_name}.{layer}.{table_name}")

    logger = logging.getLogger(f"{layer}.{table_name}")

    # Logging level
    if debug:
        level = logging.DEBUG
    else:
        level_name = (log_level or "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

    logger.setLevel(level)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler (per dataset)
    log_dir = Path(base_log_dir) / "local_runs" / layer / table_name
    log_dir.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(log_dir / f"{run_id}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logging.getLogger(f"{layer}.{table_name}")


def get_log_file_path(logger: logging.Logger) -> Path | None:
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            return Path(handler.baseFilename)
    return None


# --------------------------
# Retry Utils
# --------------------------


def with_retries(
    fn: Callable[[], Any],
    *,
    max_retries: int,
    backoff: float,
    logger: logging.Logger,
) -> Any:
    """
    Retry a function up to `max_retries` times with a fixed backoff in seconds.

    Args:
        fn: Function to execute
        max_retries: Maximum number of attempts
        backoff: Seconds to wait between retries
        logger: Logger for warning messages

    Returns:
        The return value of `fn` if successful

    Raises:
        The exception raised by `fn` on the final failed attempt
    """
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt == max_retries:
                raise
            time.sleep(backoff)


# --------------------------
# Versioning Utils
# --------------------------


def get_git_commit() -> str:
    """
    Return the current Git commit hash for the repository.

    Returns:
        str: 40-character SHA of the HEAD commit.
             Returns "unknown" if Git is unavailable or an error occurs.

    Notes:
        Useful for versioning pipelines, adding reproducibility metadata,
        or tracking the exact code used for a data processing run.
    """
    try:
        commit_hash = (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
        return commit_hash
    except Exception:
        return "unknown"


# --------------------------
# Time Utils
# --------------------------


def now_iso(timezone: ZoneInfo) -> str:
    """
    Return the current timestamp in the specified timezone
    as an ISO-8601 formatted string.

    Args:
        tz_name: IANA timezone. Defaults to "UTC".

    Returns:
        ISO-8601 formatted timestamp string including timezone offset.
        Example:
            "2026-01-27T04:12:33.421089+00:00"
    """
    return datetime.now(timezone).isoformat()


def get_date_range(
    start_date: datetime,
    end_date: datetime,
    step: timedelta = timedelta(days=1),
    output_format: Literal["datetime", "iso", "iso_tuple"] = "datetime",
) -> list:
    """
    Generate a date range (inclusive) in the timezone of the start_date.

    Args:
        start_date: timezone-aware datetime
        end_date: timezone-aware datetime
        step: increment between dates (default: 1 day)
        output_format:
            - "datetime"  → List[datetime] in the start_date timezone
            - "iso"       → List[str] ISO format strings in start_date timezone
            - "iso_tuple" → List[Tuple[str]] for SQL parameter binding

    Returns:
        List of values depending on output_format.
    """
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("start_date and end_date must be timezone-aware")

    results = []
    current = start_date

    while current <= end_date:
        if output_format == "datetime":
            results.append(current)
        elif output_format == "iso":
            results.append(current.isoformat(sep=" ", timespec="seconds"))
        elif output_format == "iso_tuple":
            results.append((current.isoformat(sep=" ", timespec="seconds"),))
        else:
            raise ValueError(f"Unknown output_format: {output_format}")

        current += step

    return results
