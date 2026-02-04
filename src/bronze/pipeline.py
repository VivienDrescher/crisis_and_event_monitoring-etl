import os
import sys
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timedelta, timezone
import time

from src.bronze.io import derive_bronze_output_path
from src.bronze.download_http import download_and_extract
from src.bronze.file_processing import process_bronze_file
from src.common_utils.env import load_env
from src.common_utils.run_metadata import save_run_metadata
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
prefixed_logger = PrefixedLogger(logger)

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
retention_policy = source.get("retention_policy", "append_only")

bronze_schema = schemas_config["schemas"]["bronze"].get(source_name, {})
source_file_type = bronze_schema.get("file_type", "csv")
reader_params = bronze_schema.get("reader", {})
required_columns = bronze_schema.get("required_columns", [])

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

logger.info(f"Starting bronze pipeline for source {source_name}")

# --------------------------
# Source type: Manual drop 
# --------------------------
if source_type=="manual_drop":
    
    # Process all non-parquet files in folder
    folder_path = bronze_dir
    files_to_process = [f for f in folder_path.iterdir() if f.is_file() and f.suffix.lower() != ".parquet"]

    if not files_to_process:
        logger.info(f"No non-parquet files to process in manual_drop folder: {folder_path}. Exiting")
        sys.exit(0)

    for file_path in files_to_process:
        try:

            logger.info(f"Processing {file_path.name}")
            
            process_bronze_file(
                file_path=file_path,
                source_name=source_name,
                file_type=source_file_type,
                reader_params=reader_params,
                required_columns=required_columns,
                run_id=run_id,
                downloaded_files=downloaded_files,
                logger=prefixed_logger,
            )
        except Exception as e:
            prefixed_logger.warning(f"Failed processing {file_path.name}: {e}")
            if DEBUG:
                raise

# --------------------------
# Source type: Automated download 
# --------------------------
elif source_type=="automated_download":

    skip_download_loop = False 

    # Backwards iteration over the pipeline date range 
    while current_date >= pipeline_start_date:

        # Exit download loop if the source requires only the latest file and it was downloaded already 
        if skip_download_loop:
            logger.info(f"Exiting download loop. All required files processed.")
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
            if retention_policy=="latest_file_only":
                skip_download_loop = True
            continue

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Download + extract
                bronze_output_path_temp = download_and_extract(
                    url=url,
                    target_path=bronze_output_path_temp,
                    expected_suffix="." + source_file_type,
                    timeout=DOWNLOAD_TIMEOUT,
                    logger=prefixed_logger,
                )

                if bronze_output_path_temp is None:
                    prefixed_logger.info(
                        "[File not existent] Skipping processing of non-existent source file."
                    )
                    break

                process_bronze_file(
                    file_path=bronze_output_path_temp,
                    source_name=source_name,
                    file_type=source_file_type,
                    reader_params=reader_params,
                    required_columns=required_columns,
                    run_id=run_id,
                    downloaded_files=downloaded_files,
                    logger=prefixed_logger,
                )
                if retention_policy=="latest_file_only":
                    skip_download_loop = True
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF if not DEBUG else 1)
                else:
                    raise
        current_date -= timedelta(days=1)

# --------------------------
# Source type: Unknown 
# --------------------------
else:
    raise NotImplementedError(f"Source type {source_type} not implemented.") 

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

logger.info(f"Bronze pipeline complete: {run_id}, metadata saved to {metadata_file}")
