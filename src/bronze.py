import os
import sys
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timedelta, timezone
import time

from dotenv import load_dotenv
from .utils.io import read_tabular_file, write_tabular_file, derive_bronze_parquet_path
from .utils.metadata import add_bronze_metadata, save_run_metadata
from .utils.validation import validate_required_columns
from .utils.download import download_and_extract
from .utils.dates import utc_now_iso
from .utils.logging import setup_logger

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()
ENV = os.getenv("ENV", "local").lower()
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
LOG_PATH = os.getenv("LOG_PATH", "logs")
CONFIG_PATH = os.getenv("CONFIG_PATH", "config")
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_BACKOFF = int(os.getenv("RETRY_BACKOFF", 5))

# --------------------------
# CLI arguments
# --------------------------
parser = argparse.ArgumentParser(description="Bronze ETL ingestion")
parser.add_argument("--source", type=str, required=True, help="Name of the source, e.g., gdelt")
args = parser.parse_args()
source_name = args.source.lower()
run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

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

# --------------------------
# Setup logging
# --------------------------
logger, log_file = setup_logger(pipeline_config["pipeline"]["name"], "bronze", source_name, LOG_PATH, DEBUG, logging_config)

# --------------------------
# Validate source
# --------------------------
if source_name not in sources_config["sources"]:
    raise ValueError(f"Source {source_name} not found in sources.yaml")

source = sources_config["sources"][source_name]
if not source.get("enabled", True):
    logger.info(f"Source {source_name} is disabled. Skipping.")
    sys.exit()

source_type = source.get("type", "manual_drop")
latest_file_only = source.get("latest_file_only", False)
retention_policy = source.get("retention_policy", "append_only")
table_params = source.get("table_params", {})

required_columns = schemas_config["schemas"]["bronze"].get(source_name, {}).get("required_columns", [])

if source_type == "manual_drop":
    logger.info(f"{source_name} is manual_drop. Skipping ingestion.")
    sys.exit()
elif source_type != "automated_download":
    raise NotImplementedError(f"Source type {source_type} not implemented.")

# --------------------------
# Prepare directories
# --------------------------
output_dir = Path(BRONZE_PATH) / source_name
output_dir.mkdir(parents=True, exist_ok=True)
runs_dir = output_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
start_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
end_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --------------------------
# Main download loop
# --------------------------
current_date = end_date
downloaded_files = []
found_latest_file = False

# Backwards iteration over the pipeline date range 
while current_date >= start_date:

    # Exit download loop if the source requires only the latest file and it was downloaded already 
    if latest_file_only and found_latest_file:
        break

    # Determine filename based on filename pattern
    filename_pattern = source["path_template"].get("filename_pattern")
    if source_name == "gdelt":
        filename = filename_pattern.format(year=current_date.year, month=current_date.month, day=current_date.day)
    else:
        raise NotImplementedError(f"Filename pattern logic not defined for {source_name}")

    # Construct relevant url and paths 
    url = f"{source['path_template']['base_url']}/{filename}"
    bronze_temp = output_dir / filename
    bronze_parquet = derive_bronze_parquet_path(bronze_temp)

    # Skip if append_only & file exists
    if retention_policy == "append_only" and bronze_parquet.exists():
        logger.info(f"File exists, skipping: {filename}")
        current_date -= timedelta(days=1)
        found_latest_file = True
        continue

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bronze_temp = download_and_extract(url, bronze_temp, "." + table_params.get("file_type", "csv"), timeout=DOWNLOAD_TIMEOUT)
            df = read_tabular_file(bronze_temp, table_params)
            validate_required_columns(df, required_columns, logger)
            df = add_bronze_metadata(df, source_name, filename, url, run_id)
            write_tabular_file(df, bronze_parquet, write_type="parquet")
            bronze_temp.unlink(missing_ok=True)
            downloaded_files.append(str(bronze_parquet))
            found_latest_file = True
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF if not DEBUG else 1)
            else:
                raise
    current_date -= timedelta(days=1)

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_id,
    PIPELINE_NAME,
    PIPELINE_TIMEZONE,
    source_name,
    start_date,
    end_date,
    log_file,
    downloaded_files,
    source,
    output_dir
)

logger.info(f"Bronze ingestion complete: {run_id}, metadata saved to {metadata_file}")
