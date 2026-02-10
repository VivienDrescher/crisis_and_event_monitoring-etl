import os
import sys
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.bronze.file_processing import process_bronze_file

from src.common_utils.path import replace_path_suffix
from src.common_utils.io import download_file_from_url
from src.common_utils.time import get_date_range
from src.common_utils.env import load_env
from src.common_utils.run_metadata import save_run_metadata
from src.common_utils.retry import with_retries
from src.common_utils.logging import setup_logger, PrefixedLogger

LAYER_NAME = "bronze"

# --------------------------
# Load environment variables
# --------------------------
load_env()

ENV = os.getenv("ENV", "local").lower()
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

LANDING_PATH = os.getenv("LANDING_PATH", "data/landing")
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
LOG_PATH = os.getenv("LOG_PATH", "logs")
CONFIG_PATH = os.getenv("CONFIG_PATH", "config")

DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_BACKOFF = int(os.getenv("RETRY_BACKOFF", 5))

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
# CLI arguments
# --------------------------
parser = argparse.ArgumentParser(description="Bronze ETL ingestion")
parser.add_argument("--table", type=str, required=True, help="Name of bronze table, e.g., gdelt")
args = parser.parse_args()
table_name = args.table.lower()

run_start_time = datetime.now(tz=timezone)
run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

# --------------------------
# Setup logging
# --------------------------
logger, log_file = setup_logger(
    name=f"{PIPELINE_NAME}.silver.{table_name}",
    log_dir=LOG_PATH, 
    debug=DEBUG,
    log_level=LOG_LEVEL,
    log_config=logging_config
)
prefixed_logger = PrefixedLogger(logger)

# --------------------------
# Validate source + schema 
# --------------------------
if table_name not in sources_config["sources"]:
    raise ValueError(f"Source {table_name} not found in sources.yaml")

source_config = sources_config["sources"][table_name]
if not source_config.get("enabled", True):
    logger.info(f"Source {table_name} is disabled. Skipping.")
    sys.exit()
source_type = source_config.get("type")
retention_policy = source_config.get("retention_policy")

bronze_schema = schemas_config["schemas"][LAYER_NAME].get(table_name, {})

# --------------------------
# Prepare directories
# --------------------------
landing_dir = Path(LANDING_PATH) / table_name
landing_dir.mkdir(parents=True, exist_ok=True)

bronze_output_dir = Path(BRONZE_PATH) / table_name
bronze_output_dir.mkdir(parents=True, exist_ok=True)

runs_output_dir = bronze_output_dir / "_runs"
runs_output_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
pipeline_start_date = datetime.strptime(pipeline_config["pipeline"]["execution"].get("start_date"), "%Y-%m-%d").replace(tzinfo=timezone)
pipeline_end_date_str = pipeline_config["pipeline"]["execution"].get("end_date")
if pipeline_end_date_str: 
    pipeline_end_date = datetime.strptime(pipeline_end_date_str, "%Y-%m-%d").replace(tzinfo=timezone)
else:
    pipeline_end_date = datetime.now(tz=timezone)

# --------------------------
# Bronze Processing 
# --------------------------
processed_input_files = []
processed_output_files = []
num_skipped_existent = 0
num_skipped_nonexistent = 0

logger.info(f"Starting bronze pipeline for table {table_name}")

if source_type == "manual_drop":

    input_files = sorted(f for f in landing_dir.iterdir() if f.is_file())

    for input_path in input_files:
        bronze_output_path = replace_path_suffix(bronze_output_dir / input_path.name, ".parquet")

        if retention_policy == "append_only" and bronze_output_path.exists():
            prefixed_logger.info("[Skipping file] append_only and output already exists")
            num_skipped_existent += 1
            continue 

        output = process_bronze_file(
            input_path=input_path,
            output_path=bronze_output_path,
            table_name=table_name,
            source_config=source_config,
            table_schema=bronze_schema,
            run_id=run_id,
            logger=prefixed_logger,
        )

        if output:
            processed_input_files.append(input_path)
            processed_output_files.append(output)

elif source_type == "automated_download":

    filename_pattern = source_config["path_template"]["filename_pattern"]
    base_url = source_config["path_template"]["base_url"]

    file_candidates = []

    for d in get_date_range(pipeline_start_date, pipeline_end_date):
        filename = filename_pattern.format(
            year=d.year,
            month=d.month,
            day=d.day,
        )
        file_candidates.append(
            (f"{base_url}/{filename}", landing_dir / filename)
        )

    logger.info(f"Iterating over {len(file_candidates)} potential input files in the pipeline date range.")

    for file_count, (url, landing_path) in enumerate(file_candidates, start=1):

        logger.info(f"Processing file for date {file_count}/{len(file_candidates)}: {landing_path.name}")

        bronze_output_path = replace_path_suffix(bronze_output_dir / landing_path.name, ".parquet")
        if retention_policy == "append_only" and bronze_output_path.exists():
            prefixed_logger.info("[Skipping file] append_only and output already exists")
            num_skipped_existent += 1
            continue 

        def attempt():
            exists = download_file_from_url(
                url=url,
                target_path=landing_path,
                timeout=DOWNLOAD_TIMEOUT,
                logger=prefixed_logger,
            )
            if not exists:
                prefixed_logger.info("[File not existent] Skipping")
                return None
            
            return process_bronze_file(
                input_path=landing_path,
                output_path=bronze_output_path,
                table_name=table_name,
                source_config=source_config,
                table_schema=bronze_schema,
                run_id=run_id,
                logger=prefixed_logger,
            )

        output = with_retries(
            attempt,
            max_retries=MAX_RETRIES,
            backoff=RETRY_BACKOFF if not DEBUG else 1,
            logger=logger,
        )

        if output:
            processed_input_files.append(landing_path)
            processed_output_files.append(output)
        
        else: 
            num_skipped_nonexistent += 1 

else:
    raise NotImplementedError(f"Source type {source_type} not implemented")

logger.info(f"Pipeline complete. {len(processed_output_files)} files written. {num_skipped_existent} already existing. {num_skipped_nonexistent} nonexistent.")

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_output_dir=runs_output_dir,
    run_id=run_id,
    layer=LAYER_NAME,
    table_name=table_name,
    log_file=log_file,
    pipeline_config=pipeline_config,
    schema_config=schemas_config,
    input_files=processed_input_files,
    output_files=processed_output_files, 
    source_configs=source_config,
    start_time=run_start_time
)

logger.info(f"Saved run metadata: {metadata_file}")