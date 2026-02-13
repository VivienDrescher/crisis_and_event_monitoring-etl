from __future__ import annotations

import os
import time
import subprocess 
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Union, Callable, Any, List, Literal
import logging
import logging.config 

# --------------------------
# Environment Utils 
# --------------------------

def load_env(env_file: str | Path | None = None, logger: Optional[logging.Logger] = None) -> Path | None:
    """
    Load environment variables from a .env file.

    Resolution order:
        1. Explicit env_file argument
        2. ENV-specific file (.env.local, .env.dev, .env.prod)
        3. Fallback to .env

    Args:
        env_file: Optional explicit path to .env file
        logger: Optional logger for informational messages

    Returns:
        Path to the .env file that was loaded, or None if no file found.
    """
    logger = logger or logging.getLogger(__name__)

    if env_file:
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"[load_env] Loaded environment variables from {env_path}")
            return env_path
        else:
            logger.warning(f"[load_env] Specified env file does not exist: {env_path}")
            return None

    # ENV-specific resolution
    env = os.getenv("ENV", "local").lower()
    candidates = [f".env.{env}", ".env"]

    for candidate in candidates:
        env_path = Path(candidate)
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f"[load_env] Loaded environment variables from {env_path}")
            return env_path

    logger.warning("[load_env] No .env file found. Environment variables may not be loaded.")
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


def setup_logger(
    name: str,
    log_dir: Union[str, Path],
    debug: bool = False,
    log_level: Optional[str] = None,
    log_config: Optional[dict] = None,
) -> Tuple[logging.Logger, Path]:
    """
    Setup a logger with optional file output and configuration.

    Priority for log level:
        1. debug=True      → DEBUG
        2. log_level       → From .env or argument
        3. default         → INFO

    Args:
        name: Logger name (e.g., "pipeline.bronze.gdelt")
        log_dir: Directory where log file will be stored
        debug: Force DEBUG level
        log_level: Optional string level (INFO, WARNING, ERROR, etc.)
        log_config: Optional dict for logging.config.dictConfig

    Returns:
        Tuple of (logger instance, Path to log file)
    """
    log_dir = Path(log_dir)
    os.makedirs(log_dir, exist_ok=True)

    log_file = log_dir / f"{name}.log"

    # Determine effective logging level
    if debug:
        level = logging.DEBUG
    else:
        level_name = (log_level or "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

    # Configure logging
    if log_config:
        # Update file handler and root level
        log_config["handlers"]["file"]["filename"] = str(log_file)
        log_config["root"]["level"] = level
        logging.config.dictConfig(log_config)
    else:
        # Basic default configuration
        logging.basicConfig(
            filename=log_file,
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

    # Create logger instance
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Ensure console output when debugging locally
    if debug:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        ))
        logger.addHandler(console_handler)

    return logger, log_file

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
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL
        ).decode().strip()
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