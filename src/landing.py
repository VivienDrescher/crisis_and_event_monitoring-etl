import os
import sys
import zipfile
from pathlib import Path
import yaml
import logging
import logging.config
import requests
from datetime import datetime, timedelta, timezone
import pandas as pd
from dotenv import load_dotenv
import argparse
import time
from .utils.dates import utc_now_iso

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()

# General
ENV = os.getenv("ENV", "local").lower()  # local / dev / prod
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Paths
LANDING_PATH = os.getenv("LANDING_PATH", "data/landing")
LOG_PATH = os.getenv("LOG_PATH", "logs")
CONFIG_PATH = os.getenv("CONFIG_PATH", "config")

# Network
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_BACKOFF = int(os.getenv("RETRY_BACKOFF", 5))

# --------------------------
# Parse command-line arguments
# --------------------------
parser = argparse.ArgumentParser(description="Landing ETL ingestion for any source")
parser.add_argument(
    "--source", type=str, required=True, help="Name of the source, e.g., gdelt"
)
args = parser.parse_args()
source_name = args.source.lower()

# --------------------------
# Generate run ID
# --------------------------
run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# --------------------------
# Load configs
# --------------------------
with open(Path(CONFIG_PATH) / "sources.yaml") as f:
    sources_config = yaml.safe_load(f)

with open(Path(CONFIG_PATH) / "pipeline.yaml") as f:
    pipeline_config = yaml.safe_load(f)

with open(Path(CONFIG_PATH) / "schemas.yaml") as f:
    schemas_config = yaml.safe_load(f)

with open(Path(CONFIG_PATH) / "logging.yaml") as f:
    logging_config = yaml.safe_load(f)

PIPELINE_NAME = pipeline_config["pipeline"]["name"]
PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")

# --------------------------
# Setup logging
# --------------------------
# Prepare log file and ensure directory exists
log_file = Path(LOG_PATH) / f"{PIPELINE_NAME}_landing_{source_name}_{run_id}.log"
os.makedirs(Path(LOG_PATH), exist_ok=True)

# Configure logger
logging_config["handlers"]["file"]["filename"] = str(log_file)
logger = logging.getLogger(f"{pipeline_config['pipeline']['name']}.landing.{source_name}")
effective_log_level = logging.DEBUG if DEBUG else getattr(logging, LOG_LEVEL, logging.INFO)
logging_config["root"]["level"] = effective_log_level
logging.config.dictConfig(logging_config)

# Enable debug mode if needed
if DEBUG:
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled")

# Log start of ingestion run
logger.info(f"Starting Landing ingestion run for source: {source_name}, run_id: {run_id}, ENV={ENV}, DEBUG={DEBUG}")

# --------------------------
# Validate source
# --------------------------
# Verify that source is configured in sources.yaml
if source_name not in sources_config["sources"]:
    logger.error(f"Source {source_name} not found in sources.yaml")
    raise ValueError(f"Source {source_name} not found in sources.yaml")

source = sources_config["sources"][source_name]

# Skip if source is disabled
if not source.get("enabled", True):
    logger.info(f"Source {source_name} is disabled in sources.yaml. Skipping.")
    exit()

# Extract source config parameters
source_type = source.get("type", "manual_drop")
latest_file_only = source.get("latest_file_only", False)
retention_policy = source.get("retention_policy", "append_only")

# Validate source download type
if source_type == "automated_download":
    pass
elif source_type == "manual_drop":
    logger.info(f"{source_name} is a manually dropped source. Skipping ingestion pipeline for this source.")
    sys.exit() 
else: 
    logger.error(f"Source type {source_type} not implemented. Skipping {source_name}.")
    raise NotImplementedError(f"Source type {source_type} not implemented.")

# --------------------------
# Determine pipeline date range
# --------------------------
start_date = datetime.strptime(
    pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d"
).replace(tzinfo=timezone.utc)

end_date = datetime.strptime(
    pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d"
).replace(tzinfo=timezone.utc)

# --------------------------
# Prepare data output directory
# --------------------------
output_dir = Path(LANDING_PATH) / source_name
output_dir.mkdir(parents=True, exist_ok=True)

runs_dir = output_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Download loop (backwards iteration by date)
# --------------------------
current_date = end_date
today = datetime.now(timezone.utc)
downloaded_files = []
found_latest_file = False

while current_date >= start_date:

    # Exit download loop if for this source only the latest file is required and it was downloaded already 
    if latest_file_only and found_latest_file:
        logger.info(
            f"Latest file for source '{source_name}' found for date {current_date.date()}. "
            f"Stopping backward iteration."
        )
        break

    # Build url
    filename_pattern = source["path_template"].get("filename_pattern")

    if source_name == "gdelt":
        filename = filename_pattern.format(
            year=current_date.year,
            month=current_date.month,
            day=current_date.day
        )
    else: 
        logger.error(f"Undefined how to build filename based on filename pattern '{filename_pattern}' for source {source_name}.")
        raise 

    url = f"{source['path_template']['base_url']}/{filename}"

    local_path = output_dir / filename
    if local_path.suffix == ".zip":
        expected_path = local_path.with_suffix("")
    else: 
        expected_path = local_path 

    # Handle retention policy
    if retention_policy == "append_only" and expected_path.exists():
        logger.info(f"File exists (append_only), skipping: {filename}")
        current_date -= timedelta(days=1)
        found_latest_file = True
        continue
    elif retention_policy == "overwrite" and expected_path.exists():
        logger.info(f"Overwriting existing file: {filename}")
    elif retention_policy in ["append_only", "overwrite"]:
        # File does not exist yet → OK to download
        pass
    else:
        logger.error(f"Undefined or unsupported retention policy: '{retention_policy}'")
        raise ValueError(f"Undefined or unsupported retention policy: '{retention_policy}'")

    # Download with retry logic
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Download the file
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()

            # Save ZIP or CSV
            with open(local_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded {local_path}")

            # Handle ZIP files
            if local_path.suffix == ".zip":
                with zipfile.ZipFile(local_path, "r") as z:
                    z.extractall(local_path.parent)
                logger.info(f"Extracted ZIP contents to {local_path.parent}")

                # Remove the ZIP
                local_path.unlink()

                # Point local_path to the extracted CSV
                # Assumes ZIP contained exactly one CSV
                # CSV name is same as ZIP but without .zip
                local_path = local_path.with_suffix("")  # removes .zip → CSV

            # Now local_path points to the CSV (ready to read)
            downloaded_files.append(str(local_path))
            found_latest_file = True

            break  # Successfully downloaded, exit retry loop

        except requests.exceptions.HTTPError as e:
            # Check if 404 → file does not exist, skip retries
            if e.response.status_code == 404:
                logger.warning(f"File not found: {url}, skipping retries")
                break
            else:
                logger.warning(f"Download failed (attempt {attempt}/{MAX_RETRIES}): {e}.")
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_BACKOFF if not DEBUG else 1  # Speedup retries during debugging
                    time.sleep(wait_time)
                else:
                    logger.error(f"Max retries reached for {url}")

        except (requests.ConnectionError, requests.Timeout) as e:
            # Retryable network error
            logger.warning(f"Network error, attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF if not DEBUG else 1 # Speedup retries during debugging 
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached for {url}")
                raise 

        except Exception as e:
            # Catch-all for other unexpected errors
            logger.error(f"Unexpected error downloading {url}: {e}")
            raise

    current_date -= timedelta(days=1)

# --------------------------
# Save run metadata
# --------------------------
run_metadata = {
    "run_id": run_id,
    "timestamp_utc": utc_now_iso(),
    "pipeline": {
        "name": PIPELINE_NAME,
        "timezone": PIPELINE_TIMEZONE,
    },
    "source": source_name,
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "log_file": str(log_file),
    "num_downloaded_files": len(downloaded_files),
    "downloaded_files": downloaded_files,
    "env": {
        "ENV": ENV,
        "DEBUG": DEBUG,
        "LANDING_PATH": LANDING_PATH,
        "LOG_PATH": LOG_PATH,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "MAX_RETRIES": MAX_RETRIES,
        "RETRY_BACKOFF": RETRY_BACKOFF
    },
    "source_config": source
}

metadata_file = runs_dir / f"run_{run_id}.yaml"
with open(metadata_file, "w") as f:
    yaml.dump(run_metadata, f, sort_keys=False)

logger.info(f"Landing ingestion run complete: {run_id}")
logger.info(f"Metadata saved to {metadata_file}")