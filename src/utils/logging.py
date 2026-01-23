import os
import logging
import logging.config
from pathlib import Path

def setup_logger(pipeline_name, layer, source_name, log_path, debug=False, log_config=None):
    os.makedirs(log_path, exist_ok=True)
    log_file = Path(log_path) / f"{pipeline_name}_{layer}_{source_name}.log"

    if log_config:
        log_config["handlers"]["file"]["filename"] = str(log_file)
        log_config["root"]["level"] = logging.DEBUG if debug else logging.INFO
        logging.config.dictConfig(log_config)
    else:
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG if debug else logging.INFO,
            format="%(asctime)s [%(levelname)s] %(pipeline_name)s: %(message)s"
        )

    logger = logging.getLogger(f"{pipeline_name}.{layer}.{source_name}")
    if debug:
        logger.setLevel(logging.DEBUG)
    return logger, log_file
