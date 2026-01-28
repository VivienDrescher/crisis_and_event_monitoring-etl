from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional
import logging


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