import os
import logging
import logging.config
from pathlib import Path

import os
import logging
import logging.config
from pathlib import Path

# general
def setup_logger(
    name: str,
    log_dir: str | Path,
    debug: bool = False,
    log_level: str | None = None,
    log_config: dict | None = None,
):
    """
    Setup application logger.

    Priority:
    1. DEBUG=True  -> DEBUG
    2. log_level   -> value from .env
    3. default     -> INFO
    """
    os.makedirs(log_dir, exist_ok=True)
    log_file = Path(log_dir) / f"{name}.log"

    # Resolve effective log level
    if debug:
        level = logging.DEBUG
    else:
        level_name = (log_level or "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)

    if log_config:
        log_config["handlers"]["file"]["filename"] = str(log_file)
        log_config["root"]["level"] = level
        logging.config.dictConfig(log_config)
    else:
        logging.basicConfig(
            filename=log_file,
            level=level,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

    logger = logging.getLogger(name)
    logger.setLevel(level)

    return logger, log_file
