import os
import argparse
import yaml
from pathlib import Path
from datetime import datetime, timezone
import sys
import pandas as pd  

from src.common_utils.env import load_env
from src.common_utils.time import date_range_utc
from src.common_utils.logging import setup_logger, PrefixedLogger
from src.common_utils.parquet import read_parquet, write_parquet
from src.common_utils.run_metadata import save_run_metadata
from src.common_utils.parquet_partition import read_parquet_partition
from src.gold.transforms.standard import process_silver_to_gold

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
# CLI args
# --------------------------
parser = argparse.ArgumentParser(description="Gold ingestion")
parser.add_argument("--table", type=str, required=True, help="Name of the table, e.g., gdelt_daily")
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
    name=f"{PIPELINE_NAME}.gold.{table_name}",
    log_dir=LOG_PATH,
    debug=DEBUG,
    log_level=LOG_LEVEL,
    log_config=logging_config
)
prefixed_logger = PrefixedLogger(logger)

# --------------------------
# Validate table + schema
# --------------------------
if table_name not in schemas_config["schemas"]["gold"]:
    raise ValueError(f"No Gold schema defined for {table_name}")

gold_output_schema = schemas_config["schemas"]["gold"][table_name]
gold_output_column_schema = gold_output_schema.get("columns", {})

# --------------------------
# Prepare directories
# --------------------------
gold_output_dir = Path(GOLD_PATH) / table_name
gold_output_dir.mkdir(parents=True, exist_ok=True)

runs_dir = gold_output_dir / "_runs"
runs_dir.mkdir(exist_ok=True)

# --------------------------
# Pipeline date range
# --------------------------
pipeline_start_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
pipeline_end_date = datetime.strptime(pipeline_config["pipeline"]["execution"]["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

# --------------------------
# Gold layer processing
# --------------------------

logger.info(f"Starting gold pipeline for table {table_name}")

dfs_input = {}  # will store all loaded input tables required to compute this gold table
input_names = []
source_configs = {}
inputs = gold_output_schema.get("inputs")

logger.info(f"{len(inputs)} input tables required to determine {table_name}.")

for input, config in inputs.items():

    # --------------------------
    # Validate source + schema
    # --------------------------
    input_name = config.get("table_name")
    input_names.append(input_name)
    layer_name = config.get("layer")

    logger.info(f"Reading input table {input_name}.")

    # --------------------------
    # Silver layer input 
    # --------------------------
    if layer_name == "silver":

        if input_name not in sources_config["sources"]:
            raise ValueError(f"Source {input_name} not found in sources.yaml")

        source_config = sources_config["sources"][input_name]
        source_configs["input_name"] = source_config
        source_retention_policy = source_config.get("retention_policy")
        silver_input_dir = Path(SILVER_PATH) / input_name

        # --------------------------
        # Retention policy: Latest File Only
        # --------------------------
        if source_retention_policy == "latest_file_only":
            silver_input_files = list(silver_input_dir.glob("*.parquet"))

            if not silver_input_files:
                logger.info(f"[Read Table] No Silver Parquet file found for {input_name}. Exiting.")
                sys.exit(0)

            if len(silver_input_files) > 1:
                raise ValueError(f"More than 1 Silver Parquet file found for {input_name}")

            prefixed_logger.info(f"[Read Table] Reading 1 parquet file from {silver_input_dir}")
            df_silver = read_parquet(silver_input_files[-1], logger=prefixed_logger)
            dfs_input[input_name] = df_silver

        # --------------------------
        # Retention policy: Overwrite or Append-only
        # --------------------------
        elif source_retention_policy in ("overwrite", "append_only"):
            silver_column_schema = schemas_config["schemas"]["silver"][input_name].get("columns")
            silver_partition_key = [col for col, spec in silver_column_schema.items() if spec.get("partition_key", False)]

            partition_values = date_range_utc(pipeline_start_date, pipeline_end_date)
            prefixed_logger.info(f"[Read Table] Reading {len(partition_values)} partition files from {silver_input_dir}")

            df_silver_all = []
            for partition_value in partition_values:
                df_partition = read_parquet_partition(
                    base_dir=silver_input_dir,
                    partition_key=silver_partition_key,
                    partition_values=partition_value,
                    logger=prefixed_logger,
                )
                df_silver_all.append(df_partition)

            if df_silver_all:
                df_silver = pd.concat(df_silver_all, ignore_index=True)
                dfs_input[input_name] = df_silver
            else:
                prefixed_logger.info(f"[Read Table] No Silver partitions found for {input_name}. Exiting")
                sys.exit(0)

        else:
            raise ValueError(f"Unknown retention policy {source_retention_policy} for {input_name}")

    # --------------------------
    # Gold layer input
    # --------------------------
    elif layer_name == "gold":
        gold_input_dir = Path(GOLD_PATH) / input_name
        gold_files = sorted(gold_input_dir.glob("*.parquet"), key=lambda f: f.stat().st_mtime)

        if not gold_files:
            prefixed_logger.info(f"[Read Table] No Gold file found for {input_name}. Exiting.")
            sys.exit(0)

        # Load the latest gold table 
        prefixed_logger.info(f"[Read Table] Reading the latest file from {gold_input_dir}")
        gold_file = gold_files[-1]
        df_gold = read_parquet(gold_file, logger=prefixed_logger)
        dfs_input[input_name] = df_gold

    else:
        raise ValueError(f"Unknown layer {layer_name}")

# Perform the gold transformations on the inputs  

df_gold = process_silver_to_gold(
    dfs=dfs_input,
    gold_table_name=table_name,
    gold_column_schema=gold_output_column_schema,
    logger=prefixed_logger,
)

# Write gold as Parquet safely
gold_output_path = gold_output_dir / f"{run_id}.parquet"
write_parquet(df_gold, gold_output_path, safe_write=True, logger=logger)

# --------------------------
# Save run metadata
# --------------------------
metadata_file = save_run_metadata(
    run_id=run_id,
    pipeline_name=PIPELINE_NAME,
    pipeline_timezone=PIPELINE_TIMEZONE,
    layer="gold",
    input_name= ', '.join(input_names),  
    pipeline_start_date=pipeline_start_date,
    pipeline_end_date=pipeline_end_date,
    log_file=log_file,
    processed_files=[gold_output_path],
    source_config=source_configs, 
    output_dir=gold_output_dir 
)

logger.info(f"Gold pipeline complete: {run_id}, metadata saved to {metadata_file}")