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


def read_tabular_file(file_path, table_params=None, nrows=None):
    """
    Read a CSV, TSV, Excel, or other tabular file using parameters from table_params.

    Args:
        file_path (Path or str): Path to the file
        table_params (dict, optional): Parameters from sources.yaml
        nrows (int, optional): Number of rows to read

    Returns:
        pd.DataFrame
    """
    if table_params is None:
        table_params = {}

    # Determine file type (from table_params or file extension)
    file_type = table_params.get("file_type", file_path.suffix.lower().lstrip(".")).lower()

    # Copy table_params to avoid modifying the original dict
    params = dict(table_params)
    params.pop("file_type", None)  # remove file_type before passing to pandas
    if nrows is not None:
        params["nrows"] = nrows

    if file_type == "csv":
        # Set dtype=str by default to avoid mixed-type warnings, unless overridden
        params.setdefault("dtype", str)
        # Also set low_memory=False to avoid chunk inference issues
        params.setdefault("low_memory", False)
        return pd.read_csv(file_path, **params)

    elif file_type in ("xlsx", "xls"):
        return pd.read_excel(file_path, **params)

    elif file_type == "parquet":
        return pd.read_parquet(file_path, **params)

    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")
    
def write_tabular_file(
    df: pd.DataFrame,
    file_path: Path,
    write_type: str | None = None,
    write_params: dict | None = None,
    safe_write: bool = True,
):
    """
    Write a DataFrame to disk based on file type.

    Args:
        df (pd.DataFrame): DataFrame to write
        file_path (Path): Target file path
        write_type (str, optional): csv, xlsx, parquet. Defaults to file suffix.
        write_params (dict, optional): Parameters forwarded to pandas writer
        safe_write (bool): Write via temp file + atomic replace
    """
    write_params = write_params or {}
    write_type = (write_type or file_path.suffix.lstrip(".")).lower()

    # Temporary path for safe writes
    target_path = file_path
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp") if safe_write else file_path

    if write_type == "csv":
        df.to_csv(
            tmp_path,
            index=False,
            **write_params,
        )

    elif write_type in ("xlsx", "xls"):
        df.to_excel(
            tmp_path,
            index=False,
            **write_params,
        )

    elif write_type == "parquet":
        df.to_parquet(
            tmp_path,
            index=False,
            **write_params,
        )

    else:
        raise ValueError(f"Unsupported write type: {write_type}")

    # Atomic replace
    if safe_write:
        tmp_path.replace(target_path)

def derive_bronze_parquet_path(source_path: Path) -> Path:
    """
    Derive a clean bronze Parquet path from a source file path.
    Removes all known data suffixes.
    """
    name = source_path.name.lower()

    for suffix in [".zip", ".csv", ".tsv", ".xlsx", ".xls"]:
        if name.endswith(suffix):
            source_path = source_path.with_suffix("")
            name = source_path.name.lower()

    return source_path.with_suffix(".parquet")
    
def validate_required_columns(df, required_columns):

    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        logger.error(f"Missing columns: {missing_cols}")
        raise 
    else: 
        logger.info(f"Column validation successfull.")

def add_bronze_metadata(
    df,
    source,
    source_file,
    source_url=None,
    run_id=None,
):
    df = df.copy()

    df["_ingested_at"] = utc_now_iso()
    df["_source"] = source
    df["_source_file"] = source_file

    if source_url:
        df["_source_url"] = source_url
    if run_id:
        df["_run_id"] = run_id

    return df

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
log_file = Path(LOG_PATH) / f"{PIPELINE_NAME}_bronze_{source_name}_{run_id}.log"
os.makedirs(Path(LOG_PATH), exist_ok=True)

# Configure logger
logging_config["handlers"]["file"]["filename"] = str(log_file)
logger = logging.getLogger(f"{pipeline_config['pipeline']['name']}.bronze.{source_name}")
effective_log_level = logging.DEBUG if DEBUG else getattr(logging, LOG_LEVEL, logging.INFO)
logging_config["root"]["level"] = effective_log_level
logging.config.dictConfig(logging_config)

# Enable debug mode if needed
if DEBUG:
    logger.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled")

# Log start of ingestion run
logger.info(f"Starting Bronze ingestion run for source: {source_name}, run_id: {run_id}, ENV={ENV}, DEBUG={DEBUG}")

# --------------------------
# Validate source
# --------------------------
# Verify that source is configured in sources.yaml
if source_name not in sources_config["sources"]:
    logger.error(f"Source {source_name} not found in sources.yaml")
    raise ValueError(f"Source {source_name} not found in sources.yaml")

source = sources_config["sources"][source_name]
table_params = source.get("table_params", {})

# Skip if source is disabled
if not source.get("enabled", True):
    logger.info(f"Source {source_name} is disabled in sources.yaml. Skipping.")
    exit()

# Extract source config parameters
source_type = source.get("type", "manual_drop")
latest_file_only = source.get("latest_file_only", False)
retention_policy = source.get("retention_policy", "append_only")

# Required columns for Bronze validation
required_columns = schemas_config["schemas"]["bronze"].get(source_name, {}).get("required_columns", [])

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
output_dir = Path(BRONZE_PATH) / source_name
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

    bronze_path_temp = output_dir / filename
    bronze_path_parquet = derive_bronze_parquet_path(bronze_path_temp)

    # Handle retention policy
    if retention_policy == "append_only" and bronze_path_parquet.exists():
        logger.info(f"File exists (append_only), skipping: {filename}")
        current_date -= timedelta(days=1)
        found_latest_file = True
        continue
    elif retention_policy == "overwrite" and bronze_path_parquet.exists():
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

            with open(bronze_path_temp, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded {bronze_path_temp}")

            file_extension = "." + table_params.get("file_type", "csv")

            # If ZIP → extract and remove ZIP
            if bronze_path_temp.suffix.lower() == ".zip":
                with zipfile.ZipFile(bronze_path_temp, "r") as z:
                    z.extractall(bronze_path_temp.parent)
                logger.info(f"Extracted ZIP contents to {bronze_path_temp.parent}")
                bronze_path_temp.unlink()

                # Pick the extracted file (only the one with expected extension)
                extracted_files = [
                    p for p in bronze_path_temp.parent.iterdir()
                    if p.is_file() and p.suffix.lower() == file_extension
                ]

                if len(extracted_files) != 1:
                    raise RuntimeError(
                        f"Expected 1 {file_extension} file in ZIP, found {len(extracted_files)}: "
                        f"{[p.name for p in extracted_files]}"
                    )

                bronze_path_temp = extracted_files[0]  # now points to extracted file

            # Read the extracted / downloaded file
            df = read_tabular_file(bronze_path_temp, table_params)

            # Validate + enrich
            validate_required_columns(df, required_columns)
            df = add_bronze_metadata(df, source_name, filename, url, run_id)

            # Write Parquet (final bronze artifact)
            write_tabular_file(df, bronze_path_parquet, write_type="parquet")

            # Remove the temporary source file (CSV/XLSX)
            bronze_path_temp.unlink()

            downloaded_files.append(str(bronze_path_parquet))
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
        "BRONZE_PATH": BRONZE_PATH,
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

logger.info(f"Bronze ingestion run complete: {run_id}")
logger.info(f"Metadata saved to {metadata_file}")