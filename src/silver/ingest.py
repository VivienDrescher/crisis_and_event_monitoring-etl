import os
import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
import sys 

from src.utils.env import load_env
from src.utils.logging import setup_logger
from src.utils.io import read_parquet, write_parquet
from src.utils.dates import utc_now_iso
from src.utils.metadata import save_run_metadata
from src.silver.transforms_standard import process_bronze_to_silver

# --------------------------
# Load environment variables
# --------------------------
load_env()

ENV = os.getenv("ENV", "local")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "data/bronze"))
SILVER_PATH = Path(os.getenv("SILVER_PATH", "data/silver"))
CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config"))
LOG_PATH = Path(os.getenv("LOG_PATH", "logs"))

# --------------------------
# CLI args
# --------------------------
parser = argparse.ArgumentParser(description="Silver ingestion")
parser.add_argument("--source", type=str, required=True, help="Name of the source, e.g., gdelt")
args = parser.parse_args()
source_name = args.source.lower()

run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# --------------------------
# Load YAML configs 
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

# --------------------------
# Setup logger 
# --------------------------
logger, log_file = setup_logger(
    name=f"{PIPELINE_NAME}.silver.{source_name}",
    log_dir=LOG_PATH,
    debug=DEBUG,
    log_level=LOG_LEVEL,
    log_config=logging_config
)

# --------------------------
# Validate source + schema
# --------------------------
if source_name not in sources_config["sources"]:
    raise ValueError(f"Source {source_name} not found in sources.yaml")

source = sources_config["sources"][source_name]
if not source.get("enabled", True):
    logger.info(f"Source {source_name} is disabled. Skipping.")
    sys.exit()

if source_name not in schemas_config["schemas"]["silver"]:
    raise ValueError(f"No Silver schema defined for {source_name}")

silver_schema = schemas_config["schemas"]["silver"][source_name]

# --------------------------
# Prepare directories
# --------------------------
bronze_dir = Path(BRONZE_PATH) / source_name
bronze_dir.mkdir(parents=True, exist_ok=True)

silver_dir = SILVER_PATH / source_name
silver_dir.mkdir(parents=True, exist_ok=True)

runs_dir = silver_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
pipeline_start_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
pipeline_end_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --------------------------
# Process files 
# --------------------------
processed_files = []

for bronze_file in bronze_dir.glob("*.parquet"):
    logger.info(f"Processing {bronze_file.name}")

    df = read_parquet(bronze_file)

    df_silver = process_bronze_to_silver(
        df=df,
        source_name=source_name,
        silver_schema=silver_schema,
        bronze_file_name=str(bronze_file),
        run_id=run_id,
        logger=logger
    )

    silver_file = silver_dir / bronze_file.name
    write_parquet(df_silver, silver_file)

    processed_files.append(silver_file.name)

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_id=run_id,
    pipeline_name=PIPELINE_NAME,
    pipeline_timezone=PIPELINE_TIMEZONE,
    layer="silver",
    source_name=source_name,
    pipeline_start_date=pipeline_start_date,
    pipeline_end_date=pipeline_end_date,
    log_file=log_file,
    processed_files=processed_files,
    source_config=source,
    output_dir=silver_dir
)

logger.info(f"Silver ingestion complete: {run_id}, metadata saved to {metadata_file}")