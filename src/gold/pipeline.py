import os
import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
import sys
import pandas as pd  
from zoneinfo import ZoneInfo

from src.gold.transforms.standard import process_silver_to_gold

from src.common_utils.env import load_env
from src.common_utils.logging import setup_logger, PrefixedLogger
from src.common_utils.time import get_date_range
from src.common_utils.parquet import read_parquet, write_parquet
from src.common_utils.run_metadata import save_run_metadata
from src.common_utils.parquet_parition import read_parquet_partition, build_partition_path


LAYER_NAME = "gold"

# --------------------------
# Load environment variables
# --------------------------
load_env()

ENV = os.getenv("ENV", "local")
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

SILVER_PATH = Path(os.getenv("SILVER_PATH", "data/silver"))
GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold"))
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
parser = argparse.ArgumentParser(description="Gold ingestion")
parser.add_argument("--table", type=str, required=True, help="Name of the gold table, e.g., gdelt_daily")
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
# Validate table + schema
# --------------------------
if table_name not in schemas_config["schemas"][LAYER_NAME]:
    raise ValueError(f"No Gold schema defined for {table_name}")

gold_schema = schemas_config["schemas"][LAYER_NAME][table_name]
gold_column_schema = gold_schema.get("columns", {})

# --------------------------
# Prepare directories
# --------------------------
gold_dir = Path(GOLD_PATH) / table_name
gold_dir.mkdir(parents=True, exist_ok=True)

runs_dir = gold_dir / "_runs"
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
# Gold processing
# --------------------------

dfs_input = {}  # will store all loaded input tables required to compute this gold table
source_configs = {} # will store the configs for all input source tables 
processed_input_files = set()
processed_output_files = set()

# Determine all required inputs to calculate the gold table 
inputs = gold_schema.get("inputs")
logger.info(f"{len(inputs)} input tables required to determine {table_name}.")

for input, config in inputs.items():

    # Validate source + schema
    input_name = config.get("table_name")
    layer_name = config.get("layer")

    logger.info(f"Reading input table {input_name}.")

    # Input table layer: Silver 
    if layer_name == "silver":

        if input_name not in sources_config["sources"]:
            raise ValueError(f"Source {input_name} not found in sources.yaml")
        
        silver_dir = Path(SILVER_PATH) / input_name

        # Determine silver layout based on source ingestion mode 
        source_config = sources_config["sources"][input_name]
        ingestion_mode = source_config.get("ingestion_mode")

        if ingestion_mode == "latest_snapshot":
            silver_layout = "snapshot"
        else:
            silver_layout = "partitioned"

        source_configs[input_name] = source_config

        # Silver layout: snapshot (process only one silver file)
        if silver_layout == "snapshot":
            silver_input_files = list(silver_dir.glob("*.parquet"))

            if not silver_input_files:
                logger.info(f"[Read Table] No Silver Parquet file found for {input_name}. Exiting.")
                sys.exit(0)

            if len(silver_input_files) > 1:
                raise ValueError(f"More than 1 Silver Parquet file found for {input_name}")

            prefixed_logger.info(f"Reading 1/1 parquet files from {silver_dir}")
            silver_input_file = silver_input_files[-1]
            df_silver = read_parquet(silver_input_file, logger=prefixed_logger)
            dfs_input[input_name] = df_silver
            processed_input_files.add(str(silver_input_file))

        # Silver layout: paritioned (process all silver partitions within pipeline range)
        else:

            # Get only parition values within pipeline range 
            silver_column_schema = schemas_config["schemas"]["silver"][input_name].get("columns")
            silver_partition_keys = [col for col, spec in silver_column_schema.items() if spec.get("partition_key", False)]
            candidate_partition_values = get_date_range(pipeline_start_date, pipeline_end_date, output_format="iso_tuple")

            prefixed_logger.info(f"[Read Table] Reading {len(candidate_partition_values)} candidate partition files from {silver_dir}")

            # Read all silver partitions within pipeline range 
            df_silver_all = []
            for candidate_partition_value in candidate_partition_values:
                partition_dir = build_partition_path(
                    silver_dir,
                    silver_partition_keys,
                    candidate_partition_value
                )
                df_partition, files_red = read_parquet_partition(partition_dir, logger=prefixed_logger)
                df_silver_all.append(df_partition)
                processed_input_files.update(files_red)

            if df_silver_all:
                df_silver = pd.concat(df_silver_all, ignore_index=True)
                dfs_input[input_name] = df_silver
            else:
                prefixed_logger.info(f"[Read Table] No Silver partitions found for {input_name}. Exiting")
                sys.exit(0)

    # Input table layer: Gold 
    elif layer_name == "gold":
        gold_input_dir = Path(GOLD_PATH) / input_name
        gold_input_files = sorted(gold_input_dir.glob("*.parquet"), key=lambda f: f.stat().st_mtime)

        if not gold_input_files:
            prefixed_logger.info(f"[Read Table] No Gold file found for {input_name}. Exiting.")
            sys.exit(0)

        # Read the latest gold table 
        prefixed_logger.info(f"[Read Table] Reading the latest file from {gold_input_dir}")
        gold_input_file = gold_input_files[-1]
        df_gold = read_parquet(gold_input_file, logger=prefixed_logger)
        dfs_input[input_name] = df_gold
        processed_input_files.add(str(gold_input_file))

    else:
        raise ValueError(f"Unknown layer {layer_name}")

# Perform the gold transformations on the inputs  
df_gold = process_silver_to_gold(
    dfs=dfs_input,
    gold_table_name=table_name,
    gold_column_schema=gold_column_schema,
    logger=prefixed_logger,
)

# Write gold as Parquet safely
gold_output_path = gold_dir / f"{run_id}.parquet"
write_parquet(df_gold, gold_output_path, logger=logger)
processed_output_files.add(str(gold_output_path))

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
    source_configs=sources_config,
    start_time=run_start_time
)

logger.info(f"Saved run metadata: {metadata_file}")