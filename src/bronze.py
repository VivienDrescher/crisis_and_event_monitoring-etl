import os
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

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()

# General
ENV = os.getenv("ENV", "local").lower()  # local / dev / prod
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Paths
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
LOG_PATH = os.getenv("LOG_PATH", "logs")
CONFIG_PATH = os.getenv("CONFIG_PATH", "config")

# Network
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 60))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_BACKOFF = int(os.getenv("RETRY_BACKOFF", 5))

# --------------------------
# Parse command-line arguments
# --------------------------
parser = argparse.ArgumentParser(description="Bronze ETL ingestion for any source")
parser.add_argument(
    "--source", type=str, required=True, help="Name of the source, e.g., acled or gdelt"
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

# --------------------------
# Setup logging
# --------------------------
log_file = Path(LOG_PATH) / f"pipeline_{source_name}_{run_id}.log"
os.makedirs(Path(LOG_PATH), exist_ok=True)
logging_config["handlers"]["file"]["filename"] = str(log_file)
logger = logging.getLogger(f"{pipeline_config['pipeline']['name']}.bronze.{source_name}")

effective_log_level = logging.DEBUG if DEBUG else getattr(logging, LOG_LEVEL, logging.INFO)
logging_config["root"]["level"] = effective_log_level
logging.config.dictConfig(logging_config)

if DEBUG:
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled")

logger.info(
    f"Starting Bronze ingestion run for source: {source_name}, run_id: {run_id}, ENV={ENV}, DEBUG={DEBUG}"
)

# --------------------------
# Validate source
# --------------------------
if source_name not in sources_config["sources"]:
    logger.error(f"Source {source_name} not found in sources.yaml")
    raise ValueError(f"Source {source_name} not found in sources.yaml")

source = sources_config["sources"][source_name]

# Skip if source is disabled
if not source.get("enabled", True):
    logger.info(f"Source {source_name} is disabled in sources.yaml. Skipping.")
    exit()

# --------------------------
# Source config parameters
# --------------------------
source_type = source.get("type", "csv_download")
single_file_only = source.get("single_file_only", False)
retention_policy = source.get("retention_policy", "append_only")

# Required columns for Bronze validation
required_columns = schemas_config["schemas"]["bronze"].get(source_name, {}).get("required_columns", [])

# --------------------------
# Determine date range
# --------------------------
start_date = datetime.strptime(
    pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d"
).replace(tzinfo=timezone.utc)

end_date = datetime.strptime(
    pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d"
).replace(tzinfo=timezone.utc)

# --------------------------
# Prepare output directory
# --------------------------
output_dir = Path(BRONZE_PATH) / source_name
output_dir.mkdir(parents=True, exist_ok=True)

runs_dir = output_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

downloaded_files = []

# --------------------------
# Download loop
# --------------------------
current_date = end_date
today = datetime.now(timezone.utc)
found_single_file = False

while current_date >= start_date:

    if single_file_only and found_single_file:
        logger.info(
            f"Latest file for source '{source_name}' found for date {current_date.date()}. "
            f"Stopping backward iteration."
        )
        break

    # --------------------------
    # Build folder (if any)
    # --------------------------
    folder_pattern = source["path_template"].get("folder_pattern")
    if folder_pattern:
        folder = folder_pattern.format(
            year=current_date.year,
            month=current_date.month
        )
    else:
        folder = None

    # --------------------------
    # Build filename
    # --------------------------
    filename_pattern = source["path_template"]["filename_pattern"]

    if "{month_abbr}" in filename_pattern:
        # ACLED-style filename with month abbreviation
        month_abbr_format = source["path_template"]["date_format"]["month_abbr"]
        month_abbr = current_date.strftime(month_abbr_format)
        filename = filename_pattern.format(
            day=current_date.day,
            month=current_date.month,
            month_abbr=month_abbr,
            year=current_date.year
        )
    else:
        # Generic pattern (GDELT YYYYMMDD)
        filename = filename_pattern.format(
            year=current_date.year,
            month=current_date.month,
            day=current_date.day
        )

    # --------------------------
    # Build URL
    # --------------------------
    if folder:
        url = f"{source['path_template']['base_url']}/{folder}/{filename}"
    else:
        url = f"{source['path_template']['base_url']}/{filename}"

    local_path = output_dir / filename

    # Handle retention policy
    if retention_policy == "append_only" and local_path.exists():
        logger.info(f"File exists (append_only), skipping: {filename}")
        current_date -= timedelta(days=1)
        found_single_file = True
        continue
    elif retention_policy == "overwrite" and local_path.exists():
        logger.info(f"Overwriting existing file: {filename}")
    elif retention_policy in ["append_only", "overwrite"]:
        # File does not exist yet → OK to download
        pass
    else:
        logger.error(f"Undefined or unsupported retention policy: '{retention_policy}'")
        raise ValueError(f"Undefined or unsupported retention policy: '{retention_policy}'")

    # --------------------------
    # Download file (only CSV / Excel supported now)
    # --------------------------
    if source_type != "csv_download":
        logger.error(f"Source type {source_type} not implemented. Skipping {source_name}.")
        raise NotImplementedError(f"Source type {source_type} not implemented.")

    # --------------------------
    # Download with retry logic
    # --------------------------
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Saved to {local_path}")
            downloaded_files.append(str(local_path))
            found_single_file = True

            # Column validation
            if required_columns:
                try:
                    df = pd.read_csv(local_path, nrows=10)
                    logger.info(df.columns)
                    missing_cols = [c for c in required_columns if c not in df.columns]
                    if missing_cols:
                        logger.warning(f"Missing columns in {filename}: {missing_cols}")
                except Exception as e:
                    logger.error(f"Failed to read CSV {filename}: {e}")

            # Debug sleep for development so logs remain readable
            if DEBUG:
                time.sleep(0.5)

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
                wait_time = RETRY_BACKOFF if not DEBUG else 1
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached for {url}")
                raise  # Optionally fail the pipeline

        except Exception as e:
            # Catch-all for other unexpected errors
            logger.error(f"Unexpected error downloading {url}: {e}")
            raise

    current_date -= timedelta(days=1)


# --------------------------
# Save run metadata
# --------------------------
PIPELINE_NAME = pipeline_config["pipeline"]["name"]
PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")

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
        "BRONZE_PATH": BRONZE_PATH,
        "LOG_PATH": LOG_PATH,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "MAX_RETRIES": MAX_RETRIES,
        "RETRY_BACKOFF": RETRY_BACKOFF
    },
    "source_config": source  # directly dump the dict from sources.yaml
}

metadata_file = runs_dir / f"run_{run_id}.yaml"
with open(metadata_file, "w") as f:
    yaml.dump(run_metadata, f, sort_keys=False)

logger.info(f"Bronze ingestion run complete: {run_id}")
logger.info(f"Metadata saved to {metadata_file}")
