import logging
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src import load_postgres
from src.layer import bronze, gold, silver
from src.utils.io import load_yaml
from src.utils.system import load_dotenv, setup_pipeline_logging


def main():

    # --------------------------
    # Load environment variables
    # --------------------------
    load_dotenv()

    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    LOG_PATH = os.getenv("LOG_PATH", "logs")
    CONFIG_PATH = os.getenv("CONFIG_PATH", "config")

    # --------------------------
    # Load configs
    # --------------------------
    pipeline_config = load_yaml(Path(CONFIG_PATH) / "pipeline.yaml")

    PIPELINE_NAME = pipeline_config["pipeline"]["name"]
    PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")
    timezone = ZoneInfo(PIPELINE_TIMEZONE)

    run_start_time = datetime.now(tz=timezone)
    run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

    # --------------------------
    # Setup logging
    # --------------------------
    setup_pipeline_logging(run_id, PIPELINE_NAME, Path(LOG_PATH), DEBUG, LOG_LEVEL)
    logger = logging.getLogger(PIPELINE_NAME)
    logger.info("Pipeline started")

    # --------------------------
    # Run end2end pipeline
    # --------------------------
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx Layer: BRONZE xxxxxxxxxxxxxxxxxxxxxxxxxx")
    bronze.run()
    # --------------------------
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx Layer: SILVER xxxxxxxxxxxxxxxxxxxxxxxxxx")
    silver.run()
    # --------------------------
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx Layer: GOLD xxxxxxxxxxxxxxxxxxxxxxxxxx")
    gold.run()
    # --------------------------
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxx LOAD POSTGRES xxxxxxxxxxxxxxxxxxxxxxxxxx")
    load_postgres.run()

    logger.info("Pipeline completed succesfully")


if __name__ == "__main__":
    main()
