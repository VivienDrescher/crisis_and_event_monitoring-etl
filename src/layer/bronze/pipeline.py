import os
import sys
from pathlib import Path
import yaml
import argparse
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from src.layer.bronze.transforms import process_bronze_file

from src.utils.storage import replace_path_suffix
from src.utils.io import download_file_from_url
from src.utils.system import get_date_range, load_env, with_retries, setup_logger, PrefixedLogger
from src.utils.pipeline import save_run_metadata, load_checkpoint, save_checkpoint, identify_new_files

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
parser = argparse.ArgumentParser(description="Bronze ingestion")
parser.add_argument("--table", type=str, required=True, help="Name of bronze table, e.g., gdelt")
args = parser.parse_args()
table_name = args.table.lower()

run_start_time = datetime.now(tz=timezone)
run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

# --------------------------
# Setup logging
# --------------------------
logger, log_file = setup_logger(
    name=f"{PIPELINE_NAME}.{LAYER_NAME}.{table_name}",
    log_dir=LOG_PATH, 
    debug=DEBUG,
    log_level=LOG_LEVEL,
    log_config=logging_config
)
prefixed_logger = PrefixedLogger(logger)
logger.info(f"Starting {LAYER_NAME} pipeline for table {table_name}")

# --------------------------
# Validate source + schema 
# --------------------------
if table_name not in sources_config["sources"]:
    raise ValueError(f"Source {table_name} not found in sources.yaml")

source_config = sources_config["sources"][table_name]
if not source_config.get("enabled", True):
    logger.info(f"Source {table_name} is disabled. Skipping.")
    sys.exit()

aquisition_method = source_config.get("aquisition_method")
ingestion_mode = source_config.get("ingestion_mode")

if table_name not in schemas_config["schemas"][LAYER_NAME]:
    raise ValueError(f"No {LAYER_NAME} schema defined for {table_name}")

bronze_schema = schemas_config["schemas"][LAYER_NAME][table_name]

# --------------------------
# Prepare directories
# --------------------------
landing_dir = Path(LANDING_PATH) / table_name
landing_dir.mkdir(parents=True, exist_ok=True)

bronze_dir = Path(BRONZE_PATH) / table_name
bronze_dir.mkdir(parents=True, exist_ok=True)

runs_dir = bronze_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

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
# Identification of files to process 
# --------------------------
checkpoint_path = bronze_dir / "_checkpoint.json"
checkpoint_files_old = load_checkpoint(checkpoint_path)

if aquisition_method == "manual_drop":
    # Files already present in landing directory
    landing_files = [f for f in landing_dir.iterdir() if f.is_file()]

    if ingestion_mode in ("latest_snapshot", "append"):
        # Only process files not yet recorded in checkpoint
        file_candidates = identify_new_files(
            files=landing_files,
            checkpoint_files=checkpoint_files_old,
        )

    elif ingestion_mode == "overwrite":
        # Reprocess all landing files
        file_candidates = landing_files

    else: 
        raise ValueError(f"Unknown ingestion_mode {ingestion_mode}")
    
elif aquisition_method == "http_download":
    # Generate potential remote files from date range (newest first)
    filename_pattern = source_config["path_template"]["filename_pattern"]
    base_url = source_config["path_template"]["base_url"]

    file_candidates = []
    for date in reversed(get_date_range(pipeline_start_date, pipeline_end_date)):
        filename = filename_pattern.format(year=date.year, month=date.month, day=date.day)
        file_candidates.append((f"{base_url}/{filename}", landing_dir / filename))

else: 
    raise ValueError(f"Unknown source_type {aquisition_method}")

logger.info(f"Identified {len(file_candidates)} candidate files.")

# --------------------------
# Process files
# --------------------------
processed_input_files =  set()
processed_output_files = set()

num_skipped_existent = 0
num_skipped_nonexistent = 0
found_latest_snapshot = False 

for file_count, candidate in enumerate(file_candidates, start=1):
    
    # Stop early once the newest available snapshot is processed
    if ingestion_mode == "latest_snapshot" and found_latest_snapshot:
        break

    # Unpack candidate depending on acquisition method
    if aquisition_method == "http_download":
        url, input_file = candidate
    else:
        input_file = candidate
        url = None

    logger.info(f"Iterating over candidate file {file_count}/{len(file_candidates)}: {input_file.name}")

    bronze_path = replace_path_suffix(bronze_dir / input_file.name, "parquet")

    # Skip already processed files for append or latest_snapshot
    if ingestion_mode != "overwrite" and bronze_path.exists():
        prefixed_logger.info(f"[Skipping file] {bronze_path.name} already exists. Skipping download")
        num_skipped_existent += 1

        if ingestion_mode == "latest_snapshot":
            found_latest_snapshot = True
            prefixed_logger.info(f"[Latest snapshot] Using file: {input_file.name}")

        continue

    #  Download (if needed) + process Bronze transformation
    def process_file():
        if url:
            exists = download_file_from_url(
                url, 
                input_file, 
                timeout=DOWNLOAD_TIMEOUT, 
                logger=prefixed_logger
            )
            if not exists:
                prefixed_logger.info(f"[File not existent] {input_file.name}")
                return None
            
        return process_bronze_file(
            input_path=input_file,
            output_path=bronze_path,
            table_name=table_name,
            source_config=source_config,
            bronze_schema=bronze_schema,
            run_id=run_id,
            timezone=timezone, 
            logger=prefixed_logger,
        )
    
    output = with_retries(
        process_file,
        max_retries=MAX_RETRIES,
        backoff=RETRY_BACKOFF if not DEBUG else 1,
        logger=logger,
    )

    if output:
        processed_input_files.add(str(input_file))
        processed_output_files.add(output)

        if ingestion_mode == "latest_snapshot":
            found_latest_snapshot = True
            prefixed_logger.info(f"[Latest snapshot] Using file: {input_file.name}")

    else:
        num_skipped_nonexistent += 1

# Update checkpoint file 
if processed_input_files:
    updated_files = checkpoint_files_old | processed_input_files
    save_checkpoint(checkpoint_path, updated_files, timezone)
else:
    logger.info("No new files processed — keeping existing checkpoint unchanged.")

logger.info(
    f"Pipeline complete | "
    f"written={len(processed_output_files)} | "
    f"skipped_existing={num_skipped_existent} | "
    f"skipped_nonexistent={num_skipped_nonexistent} | "
    f"candidates={len(file_candidates)}"
)

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_output_dir=runs_dir,
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