import os
import sys
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timedelta, timezone
import time

from src.bronze.io import read_source_table, derive_bronze_output_path
from src.bronze.metadata import add_bronze_metadata
from src.bronze.download_http import download_and_extract
from src.common_utils.env import load_env
from src.common_utils.run_metadata import save_run_metadata
from src.common_utils.schema_validation import validate_required_columns
from src.common_utils.parquet import write_parquet
from src.common_utils.logging import setup_logger, PrefixedLogger

# --------------------------
# Load environment variables
# --------------------------
load_env()

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
bronze_dir = Path(BRONZE_PATH) / source_name
bronze_dir.mkdir(parents=True, exist_ok=True)
runs_dir = bronze_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
pipeline_start_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
pipeline_end_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --------------------------
# Main download loop
# --------------------------
current_date = pipeline_end_date
downloaded_files = []
found_latest_file = False
prefixed_logger = PrefixedLogger(logger)

# Backwards iteration over the pipeline date range 
while current_date >= pipeline_start_date:

    # Exit download loop if the source requires only the latest file and it was downloaded already 
    if latest_file_only and found_latest_file:
        logger.info(f"Exiting download loop. Source only requires latest file.")
        break

    # Determine filename based on filename pattern
    filename_pattern = source["path_template"].get("filename_pattern")
    if source_name == "gdelt":
        filename = filename_pattern.format(year=current_date.year, month=current_date.month, day=current_date.day)
    else:
        raise NotImplementedError(f"Filename pattern logic not defined for {source_name}")
    
    logger.info(f"Processing {filename}")

    # Construct relevant url and paths 
    url = f"{source['path_template']['base_url']}/{filename}"
    bronze_output_path_temp = bronze_dir / filename
    bronze_output_path = derive_bronze_output_path(bronze_output_path_temp)

    # Skip if append_only & file exists
    if retention_policy == "append_only" and bronze_output_path.exists():
        prefixed_logger.info(f"[Append-only retention policy] Skipping {bronze_output_path} -> File already exists")
        current_date -= timedelta(days=1)
        found_latest_file = True
        continue

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            
            # 1. Download file from url and extract if it's a zip 
            bronze_output_path_temp = download_and_extract(
                url=url, 
                target_path=bronze_output_path_temp, 
                expected_suffix = "." + table_params.get("file_type", "csv"), 
                timeout=DOWNLOAD_TIMEOUT, 
                logger=prefixed_logger
            )

            # 2. Read source table into dataframe 
            df = read_source_table(bronze_output_path_temp, table_params, prefixed_logger)

            # 3. Validate required columns 
            validate_required_columns(df, required_columns, prefixed_logger)
            
            # 4. Add Bronze metadata 
            df = add_bronze_metadata(
                df,
                source_name=source_name,
                source_file=filename,
                source_url=url,
                run_id=run_id,
                logger=prefixed_logger,
            )

            # 5. Write to Bronze layer as parquet 
            write_parquet(
                df,
                bronze_output_path,
                safe_write=True,
                logger=prefixed_logger,
            )

            bronze_output_path_temp.unlink(missing_ok=True)
            downloaded_files.append(str(bronze_output_path))
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
    run_id=run_id,
    pipeline_name=PIPELINE_NAME,
    pipeline_timezone=PIPELINE_TIMEZONE,
    layer="bronze",
    source_name=source_name,
    pipeline_start_date=pipeline_start_date,
    pipeline_end_date=pipeline_end_date,
    log_file=log_file,
    processed_files=downloaded_files,
    source_config=source,
    output_dir=bronze_dir
)

logger.info(f"Bronze ingestion complete: {run_id}, metadata saved to {metadata_file}")
