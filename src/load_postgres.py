import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
from sqlalchemy import create_engine

from src.utils.io import read_parquet
from src.utils.system import PrefixedLogger, load_env, setup_logger

TASK_NAME = "load_to_postgres"

# --------------------------
# Load environment variables
# --------------------------
load_env()

ENV = os.getenv("ENV", "local")
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
def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


sources_config = load_yaml(Path(CONFIG_PATH) / "sources.yaml")
pipeline_config = load_yaml(Path(CONFIG_PATH) / "pipeline.yaml")
schemas_config = load_yaml(Path(CONFIG_PATH) / "schemas.yaml")
logging_config = load_yaml(Path(CONFIG_PATH) / "logging.yaml")

PIPELINE_NAME = pipeline_config["pipeline"]["name"]
PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")
timezone = ZoneInfo(PIPELINE_TIMEZONE)

# --------------------------
# CLI args
# --------------------------
parser = argparse.ArgumentParser(description="Gold ingestion")
parser.add_argument(
    "--table", type=str, required=True, help="Name of the gold table, e.g., gdelt_daily"
)
args = parser.parse_args()
table_name = args.table.lower()

run_start_time = datetime.now(tz=timezone)
run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

# --------------------------
# Setup logging
# --------------------------
logger, log_file = setup_logger(
    name=f"{PIPELINE_NAME}.{TASK_NAME}.{table_name}",
    log_dir=LOG_PATH,
    debug=DEBUG,
    log_level=LOG_LEVEL,
    log_config=logging_config,
)
prefixed_logger = PrefixedLogger(logger)
logger.info(f"Starting {TASK_NAME} pipeline for table {table_name}")

# --------------------------
# Prepare directories
# --------------------------
gold_dir = Path(GOLD_PATH) / table_name
gold_dir.mkdir(parents=True, exist_ok=True)

# --------------------------
# Load gold table
# --------------------------
gold_files = list(gold_dir.glob("*.parquet"))

if not gold_files:
    logger.info(f"[Read Table] No Gold Parquet file found for {gold_files}. Exiting.")
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
