import os
import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
import sys
import pandas as pd  
import shutil

from src.common_utils.env import load_env
from src.common_utils.logging import setup_logger, PrefixedLogger
from src.common_utils.parquet import read_parquet, write_parquet
from src.common_utils.checkpoints import load_checkpoint, save_checkpoint, identify_new_bronze_files
from src.common_utils.partitions import derive_partition_columns
from src.common_utils.run_metadata import save_run_metadata
from src.silver.transforms.standard import process_bronze_to_silver
from src.common_utils.parquet_partition import read_parquet_partition, write_parquet_partition


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
parser.add_argument("--table", type=str, required=True, help="Name of the table, e.g., gdelt")
args = parser.parse_args()
table_name = args.table.lower()

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

source = sources_config["sources"][table_name]
if not source.get("enabled", True):
    logger.info(f"Source {table_name} is disabled. Skipping.")
    sys.exit()

retention_policy = source.get("retention_policy", "append_only")

if table_name not in schemas_config["schemas"]["silver"]:
    raise ValueError(f"No Silver schema defined for {table_name}")

silver_schema = schemas_config["schemas"]["silver"][table_name]

# --------------------------
# Prepare directories
# --------------------------
bronze_dir = Path(BRONZE_PATH) / table_name
bronze_dir.mkdir(parents=True, exist_ok=True)

silver_dir = SILVER_PATH / table_name
silver_dir.mkdir(parents=True, exist_ok=True)

runs_dir = silver_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
pipeline_start_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
pipeline_end_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --------------------------
# Collect Bronze files
# --------------------------
bronze_files = sorted(bronze_dir.glob("*.parquet"), key=lambda f: f.stat().st_mtime)

if not bronze_files:
    logger.info("No Bronze Parquet files to process. Exiting.")
    sys.exit(0)

processed_files = []
written_files = []

logger.info(f"Starting silver pipeline for source {table_name}")

# --------------------------
# Retention Policy: Latest file only  
# --------------------------
if retention_policy=="latest_file_only":
    bronze_file = bronze_files[-1]
    
    logger.info("Retention policy: latest_file_only")

    # Delete previous Silver data
    for path in silver_dir.glob("*"):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.suffix == ".parquet":
            path.unlink()
    logger.info(f"Deleted all existing Silver data")

    logger.info(f"Processing latest file only: {bronze_file.name}")

    # Process the latest Bronze file
    processed_files.append(bronze_file.name)
    df = read_parquet(bronze_file, logger=prefixed_logger)
    df_silver = process_bronze_to_silver(
        df=df,
        table_name=table_name,
        silver_schema=silver_schema,
        bronze_file_name=str(bronze_file),
        silver_run_id=run_id,
        logger=prefixed_logger
    )

    silver_file = silver_dir / bronze_file.name
    write_parquet(df_silver, silver_file, logger=prefixed_logger)
    written_files.append(silver_file.name)

# --------------------------
# Retention policy: Overwrite  
# --------------------------
elif retention_policy=="overwrite":

    logger.info("Retention policy: overwrite")

    # Delete previous Silver data
    for path in silver_dir.glob("*"):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.suffix == ".parquet":
            path.unlink()
    logger.info(f"[overwrite] Deleted all existing Silver data")

    # Validate schema requirements 
    column_schema = silver_schema.get("columns")
    primary_key = [col for col, spec in column_schema.items() if spec.get("primary_key", False)]
    record_timestamp = silver_schema.get("record_timestamp")
    partition_key  = [col for col, spec in column_schema.items() if spec.get("partition_key", False)]

    if not primary_key or not record_timestamp or not partition_key:
        raise ValueError(
            "Overwrite sources must define "
            "'primary_key', 'record_timestamp', and 'partition_key' in silver schema"
        )

    logger.info(
        f"[overwrite] Recomputing all Silver data from Bronze using "
        f"primary_key={primary_key}, "
        f"record_timestamp={record_timestamp}, "
        f"partition_key={partition_key}"
    )

    # Process all Bronze files
    df_all = []

    for bronze_file in bronze_files:
        prefixed_logger.info(f"Processing Bronze file: {bronze_file.name}")

        df = read_parquet(bronze_file, logger=prefixed_logger)
        df_silver = process_bronze_to_silver(
            df=df,
            table_name=table_name,
            silver_schema=silver_schema,
            bronze_file_name=str(bronze_file),
            silver_run_id=run_id,
            logger=prefixed_logger
        )
        df_all.append(df_silver)
        processed_files.append(bronze_file)

    if not df_all:
        logger.info("No data produced from Bronze files. Exiting")
        sys.exit()
    
    df_combined = pd.concat(df_all, ignore_index=True)

    # Deduplicate globally based on the record_timestamp
    df_combined = df_combined.sort_values(by=record_timestamp, ascending=True)
    df_combined = df_combined.drop_duplicates(subset=primary_key, keep="last")

    # Write Silver partitions
    written_files = []

    df_combined = derive_partition_columns(df_combined, partition_key)
    partition_groups = (
        df_combined
        .groupby(partition_key, dropna=False)
        .groups
    )

    logger.info(f"Writing {len(list(partition_groups.keys()))} paritions...")

    for partition_values in partition_groups.keys():
        # partition_values is a tuple (even if length 1)
        logger.info(f"Processing parition {partition_values}")

        # Ensure parition_values is iterable even when there is only one parition key 
        if not isinstance(partition_values, tuple):
            partition_values = [partition_values]

        # Extract only records for the current parition values from the new silver dataframe 
        mask = True
        for col, val in zip(partition_key, partition_values):
            mask &= df_combined[col] == val
        df_partition = df_combined[mask]

        partition_path = write_parquet_partition(
            df=df_partition,
            base_dir=silver_dir,
            partition_key=partition_key,
            partition_values=partition_values,
            file_prefix=table_name,
            logger=prefixed_logger,
        )

        written_files.append(partition_path.name)

    logger.info(
        f"[Full Silver overwrite complete "
        f"({len(written_files)} partition files written)"
    )

# --------------------------
# Retention policy: Append-only   
# --------------------------
elif retention_policy=="append_only":

    logger.info("Retention policy: append_only")

    # Validate schema requirements 
    column_schema = silver_schema.get("columns")
    primary_key = [col for col, spec in column_schema.items() if spec.get("primary_key", False)]
    record_timestamp = silver_schema.get("record_timestamp")
    partition_key = [col for col, spec in column_schema.items() if spec.get("partition_key", False)]

    if not primary_key or not record_timestamp or not partition_key:
        raise ValueError(
            "Append-only sources must define "
            "'primary_key', 'record_timestamp', and 'partition_key' in silver schema"
        )

    logger.info(
        f"[overwrite] Recomputing all Silver data from Bronze using "
        f"primary_key={primary_key}, "
        f"record_timestamp={record_timestamp}, "
        f"partition_key={partition_key}"
    )

    # Identify new Bronze files
    checkpoint_path = silver_dir / "_checkpoint.json"
    checkpoint_files = load_checkpoint(checkpoint_path)
    new_bronze_files = identify_new_bronze_files(
        bronze_files=bronze_files,
        processed_files=checkpoint_files,
    )

    if not new_bronze_files:
        logger.info("No new Bronze files to process. Exiting")
        sys.exit(0)

    logger.info(f"Processing {len(new_bronze_files)} new Bronze files")

    # Process new Bronze data → Silver
    df_new_all = []

    for bronze_file in new_bronze_files:
        df = read_parquet(bronze_file, logger=prefixed_logger)
        df_silver = process_bronze_to_silver(
            df=df,
            table_name=table_name,
            silver_schema=silver_schema,
            bronze_file_name=str(bronze_file),
            silver_run_id=run_id,
            logger=prefixed_logger,
        )
        df_new_all.append(df_silver)
        checkpoint_files.add(bronze_file.name)
        processed_files.append(bronze_file.name)

    df_new = pd.concat(df_new_all, ignore_index=True)

    # Derive affected silver partitions
    df_new = derive_partition_columns(df_new, partition_key)

    partition_groups = (
        df_new
        .groupby(partition_key, dropna=False)
        .groups
    )

    logger.info(f"Partitions affected by new data: {len(list(partition_groups.keys()))}")

    # Partition-aware merge
    for partition_values in partition_groups.keys():

        logger.info(f"Processing {partition_values}")

        if not isinstance(partition_values, tuple):
            partition_values = [partition_values]

        # Extract only records for the current parition values from the new silver dataframe 
        mask = True
        for col, val in zip(partition_key, partition_values):
            mask &= df_new[col] == val
        df_new_part = df_new[mask]

        # Read the relevant silver partition for the current parition value 
        df_existing = read_parquet_partition(
            silver_dir,
            partition_key,
            partition_values,
            prefixed_logger,
        )

        # Merge the new data with the previous data and keep only the latest record in case of duplicates 
        df_merged = pd.concat([df_existing, df_new_part], ignore_index=True)
        df_merged = (
            df_merged
            .sort_values(by=record_timestamp)
            .drop_duplicates(subset=primary_key, keep="last")
        )

        # Overwrite silver partition with the latest data 
        partition_path = write_parquet_partition(
            df=df_merged,
            base_dir=silver_dir,
            partition_key=partition_key,
            partition_values=partition_values,
            file_prefix=table_name,
            logger=prefixed_logger,
        )
        written_files.append(partition_path.name)

    save_checkpoint(checkpoint_path, processed_files)
    logger.info(f"[overwrite] Incorporated {len(new_bronze_files)} new Files form Bronze into {len(list(partition_groups.keys()))} existing silver partitions")

# --------------------------
# Retention policy: Unknown  
# --------------------------
else: 
    raise ValueError(f"Retention policy {retention_policy} not implemented")

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_id=run_id,
    pipeline_name=PIPELINE_NAME,
    pipeline_timezone=PIPELINE_TIMEZONE,
    layer="silver",
    input_name=table_name,
    pipeline_start_date=pipeline_start_date,
    pipeline_end_date=pipeline_end_date,
    log_file=log_file,
    processed_files=processed_files,
    source_config=source,
    output_dir=silver_dir
)

logger.info(f"Silver pipeline complete: {run_id}, metadata saved to {metadata_file}")