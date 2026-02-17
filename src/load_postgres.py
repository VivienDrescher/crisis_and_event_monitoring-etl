import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine

from src.utils.io import load_yaml, read_parquet
from src.utils.system import load_env, setup_standalone_logging


def run():

    TASK_NAME = "load_to_postgres"

    # --------------------------
    # Load environment variables
    # --------------------------
    load_env()

    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold"))
    CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config"))
    LOG_PATH = Path(os.getenv("LOG_PATH", "logs"))

    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", 5432)

    # --------------------------
    # Load configs
    # --------------------------
    pipeline_config = load_yaml(Path(CONFIG_PATH) / "pipeline.yaml")
    schemas_config = load_yaml(Path(CONFIG_PATH) / "schemas.yaml")

    PIPELINE_NAME = pipeline_config["pipeline"]["name"]
    PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")
    timezone = ZoneInfo(PIPELINE_TIMEZONE)

    # --------------------------
    # Iterate over all Gold tables
    # --------------------------
    table_names = schemas_config["schemas"]["gold"]
    for table_name in table_names:
        run_start_time = datetime.now(tz=timezone)
        run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

        # --------------------------
        # Setup logging
        # --------------------------
        logger = setup_standalone_logging(
            run_id, PIPELINE_NAME, TASK_NAME, table_name, LOG_PATH, DEBUG, LOG_LEVEL
        )
        logger.info(
            f"--------------------------- Table: {table_name.upper()} ---------------------------"
        )
        logger.info(f"Starting {TASK_NAME} run for table {table_name}")

        # --------------------------
        # Extract schema details
        # --------------------------
        gold_schema = schemas_config["schemas"]["gold"][table_name]
        if not gold_schema.get("enabled", True):
            logger.info(f"{table_name} is disabled. Skipping run for {table_name}.")
            continue

        # --------------------------
        # Load gold table
        # --------------------------
        gold_dir = Path(GOLD_PATH) / table_name
        gold_files = list(gold_dir.glob("*.parquet"))

        if not gold_files:
            logger.info(
                f"[Read Table] No Gold Parquet file found for {gold_files}. Exiting."
            )
            sys.exit(0)

        if len(gold_files) > 1:
            raise ValueError(f"More than 1 Gold Parquet file found in {gold_dir}")

        gold_file = gold_files[-1]

        df = read_parquet(gold_file, logger=logger)

        # --------------------------
        # Load into PostgreSQL, replacing the old table
        # --------------------------
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logger.info(f"Wrote table {table_name} into Postgres,replacing the old table")
