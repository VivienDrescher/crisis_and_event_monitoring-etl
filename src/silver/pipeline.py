import os
import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
import sys
from zoneinfo import ZoneInfo

from src.silver.transforms.standard import transform_bronze_files_to_silver

from src.common_utils.env import load_env
from src.common_utils.logging import setup_logger, PrefixedLogger
from src.common_utils.dataframe import deduplicate
from src.common_utils.parquet import write_parquet
from src.common_utils.checkpoints import load_checkpoint, save_checkpoint, identify_new_files
from src.common_utils.run_metadata import save_run_metadata
from src.common_utils.parquet_parition import write_partitioned_parquet
from src.common_utils.file_system import clear_data_dir

LAYER_NAME = "silver"

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
# CLI args
# --------------------------
parser = argparse.ArgumentParser(description="Silver ingestion")
parser.add_argument("--table", type=str, required=True, help="Name of the table, e.g., gdelt")
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

ingestion_mode = source_config.get("ingestion_mode")

if table_name not in schemas_config["schemas"]["silver"]:
    raise ValueError(f"No {LAYER_NAME} schema defined for {table_name}")

silver_schema = schemas_config["schemas"][LAYER_NAME][table_name]

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
# Silver processing 
# --------------------------
processed_output_files = set()

bronze_files = sorted(bronze_dir.glob("*.parquet"), key=lambda f: f.stat().st_mtime)
if not bronze_files:
    logger.info("No Bronze files found. Exiting.")
    sys.exit(0)

# Schema and partition config
column_schema = silver_schema.get("columns", {})
primary_keys = [col for col, spec in column_schema.items() if spec.get("primary_key", False)]
record_timestamp = silver_schema.get("record_timestamp")
partition_keys = [col for col, spec in column_schema.items() if spec.get("partition_key", False)]

checkpoint_path = silver_dir / "_checkpoint.json"
checkpoint_files_old = load_checkpoint(checkpoint_path)

# MODE: latest_snapshot
if ingestion_mode == "latest_snapshot":
    clear_data_dir(silver_dir, prefixed_logger)
    checkpoint_files_old = set()

    latest_file = bronze_files[-1]
    logger.info(f"Processing latest Bronze file only: {latest_file.name}")

    df, processed_input_files = transform_bronze_files_to_silver(
        [latest_file], table_name, silver_schema, run_id, prefixed_logger
    )

    if df is not None:
        output_file = silver_dir / latest_file.name
        write_parquet(df, output_file, logger=prefixed_logger)
        processed_output_files.add(str(output_file))

# MODE: overwrite (full rebuild)
elif ingestion_mode == "overwrite":
    clear_data_dir(silver_dir, prefixed_logger)
    checkpoint_files_old = set()

    logger.info(f"Processing {len(bronze_files)} Bronze files to Silver.")
    df, processed_input_files = transform_bronze_files_to_silver(
        bronze_files, table_name, silver_schema, run_id, prefixed_logger
    )

    if df is None:
        logger.info("No Bronze data found. Exiting.")
        sys.exit(0)

    # Global deduplication 
    df = deduplicate(df, primary_keys, record_timestamp, prefixed_logger)

    logger.info(f"Writing the processed Broze files to Silver partitions.")    
    processed_output_files = write_partitioned_parquet(df, partition_keys, silver_dir, logger=prefixed_logger)

# MODE: append (incremental)
elif ingestion_mode == "append":
    new_bronze_files = identify_new_files(
        files=bronze_files,
        checkpoint_files=checkpoint_files_old,
    )

    if not new_bronze_files:
        logger.info("No new Bronze files to process. Exiting.")
        sys.exit(0)

    logger.info(f"Processing {len(bronze_files)} Bronze files to Silver.")
    df_new, processed_input_files = transform_bronze_files_to_silver(
        new_bronze_files, table_name, silver_schema, run_id, prefixed_logger
    )
    if df_new is None:
        logger.info("No Bronze data found. Exiting.")
        sys.exit(0)

    # Partition-aware merge write 
    logger.info(f"Merging the processed Bronze files into the existing Silver partitions.")  
    processed_output_files = write_partitioned_parquet(
        df_new, 
        partition_keys, 
        silver_dir, 
        True, 
        primary_keys, 
        record_timestamp, 
        prefixed_logger
        )    

# Unknown mode
else:
    raise ValueError(f"Ingestion mode {ingestion_mode} not implemented")

# Update checkpoint file 
if processed_input_files:
    updated_files = checkpoint_files_old | processed_input_files
    save_checkpoint(checkpoint_path, updated_files)
else:
    logger.info("No new files processed — keeping existing checkpoint unchanged.")

logger.info(
    f"Pipeline complete | "
    f"written={len(processed_output_files)} | "
    f"processed_input_files={len(processed_input_files)}"
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