from __future__ import annotations
import os
import logging
import logging.config
from pathlib import Path
from typing import Optional, Tuple, Union

import logging
from typing import Optional


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