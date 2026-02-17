import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from src.data_quality_checks import check_record_timestamp
from src.layer.gold.transforms.common import process_silver_to_gold
from src.utils.dataframe import get_record_timestamp_column
from src.utils.io import (
    build_partition_path,
    load_yaml,
    read_parquet,
    read_parquet_partition,
    write_parquet,
)
from src.utils.run import save_run_metadata
from src.utils.storage import clear_data_dir
from src.utils.system import (
    PrefixedLogger,
    get_date_range,
    get_log_file_path,
    load_env,
    setup_standalone_logging,
)


def run():
    LAYER_NAME = "gold"

    # --------------------------
    # Load environment variables
    # --------------------------
    load_env()

    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    SILVER_PATH = Path(os.getenv("SILVER_PATH", "data/silver"))
    GOLD_PATH = Path(os.getenv("GOLD_PATH", "data/gold"))
    CONFIG_PATH = Path(os.getenv("CONFIG_PATH", "config"))
    LOG_PATH = Path(os.getenv("LOG_PATH", "logs"))

    # --------------------------
    # Load configs
    # --------------------------
    sources_config = load_yaml(Path(CONFIG_PATH) / "sources.yaml")
    pipeline_config = load_yaml(Path(CONFIG_PATH) / "pipeline.yaml")
    schemas_config = load_yaml(Path(CONFIG_PATH) / "schemas.yaml")

    # --------------------------
    # Determine pipeline details
    # --------------------------
    PIPELINE_NAME = pipeline_config["pipeline"]["name"]
    PIPELINE_TIMEZONE = pipeline_config.get("timezone", "UTC")
    timezone = ZoneInfo(PIPELINE_TIMEZONE)

    pipeline_start_date = datetime.strptime(
        pipeline_config["pipeline"]["execution"].get("start_date"), "%Y-%m-%d"
    ).replace(tzinfo=timezone)
    pipeline_end_date_str = pipeline_config["pipeline"]["execution"].get("end_date")
    if pipeline_end_date_str:
        pipeline_end_date = datetime.strptime(
            pipeline_end_date_str, "%Y-%m-%d"
        ).replace(tzinfo=timezone)
    else:
        pipeline_end_date = datetime.now(tz=timezone)

    run_start_time = datetime.now(tz=timezone)
    run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

    # --------------------------
    # Iterate over all Silver tables
    # --------------------------
    table_names = schemas_config["schemas"][LAYER_NAME]
    for table_name in table_names:
        run_start_time = datetime.now(tz=timezone)
        run_id = run_start_time.strftime("%Y%m%d_%H%M%S")

        # --------------------------
        # Setup logging
        # --------------------------
        logger = setup_standalone_logging(
            run_id, PIPELINE_NAME, LAYER_NAME, table_name, LOG_PATH, DEBUG, LOG_LEVEL
        )
        log_file = get_log_file_path(logger)
        prefixed_logger = PrefixedLogger(logger)
        logger.info(
            f"--------------------------- Table: {table_name.upper()} ---------------------------"
        )
        logger.info(f"Starting {LAYER_NAME} run for table {table_name}")

        # --------------------------
        # Extract schema details
        # --------------------------
        gold_schema = schemas_config["schemas"][LAYER_NAME][table_name]
        if not gold_schema.get("enabled", True):
            logger.info(f"{table_name} is disabled. Skipping run for {table_name}.")
            continue
        gold_column_schema = gold_schema.get("columns", {})

        # --------------------------
        # Prepare directories
        # --------------------------
        gold_dir = Path(GOLD_PATH) / table_name
        gold_dir.mkdir(parents=True, exist_ok=True)

        runs_dir = gold_dir / "_runs"
        runs_dir.mkdir(exist_ok=True)

        # --------------------------
        # Gold processing
        # --------------------------

        dfs_input = {}  # will store all loaded input tables required to compute this gold table
        source_configs = {}  # will store the configs for all input source tables
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
                silver_column_schema = schemas_config["schemas"]["silver"][
                    input_name
                ].get("columns")

                # Silver layout: snapshot (process only one silver file)
                if silver_layout == "snapshot":
                    silver_input_files = list(silver_dir.glob("*.parquet"))

                    if not silver_input_files:
                        logger.info(
                            f"[Read Table] No Silver Parquet file found for {input_name}. Exiting."
                        )
                        sys.exit(0)

                    if len(silver_input_files) > 1:
                        raise ValueError(
                            f"More than 1 Silver Parquet file found for {input_name}"
                        )

                    silver_input_file = silver_input_files[-1]
                    df_silver = read_parquet(silver_input_file, logger=prefixed_logger)

                    # Filter for data within pipeline range
                    record_timestamp = get_record_timestamp_column(silver_column_schema)
                    check_record_timestamp(df_silver, record_timestamp, logger)

                    df_silver_filtered = df_silver[
                        (df_silver[record_timestamp] >= pipeline_start_date)
                        & (df_silver[record_timestamp] <= pipeline_end_date)
                    ]
                    logger.info(
                        f"[Pipeline range filter] Filtered {len(df_silver_filtered)}/{len(df_silver)} rows "
                        f"using '{record_timestamp}' between {pipeline_start_date} and {pipeline_end_date}."
                    )

                    dfs_input[input_name] = df_silver_filtered
                    processed_input_files.add(str(silver_input_file))

                # Silver layout: paritioned (process all silver partitions within pipeline range)
                else:
                    # Get only parition values within pipeline range
                    silver_partition_keys = [
                        col
                        for col, spec in silver_column_schema.items()
                        if spec.get("partition_key", False)
                    ]
                    candidate_partition_values = get_date_range(
                        pipeline_start_date,
                        pipeline_end_date,
                        output_format="iso_tuple",
                    )

                    prefixed_logger.info(
                        f"[Read Table] Reading {len(candidate_partition_values)} candidate partition files from {silver_dir}"
                    )

                    # Read all silver partitions within pipeline range
                    df_silver_all = []
                    for candidate_partition_value in candidate_partition_values:
                        partition_dir = build_partition_path(
                            silver_dir, silver_partition_keys, candidate_partition_value
                        )
                        df_partition, files_red = read_parquet_partition(
                            partition_dir, logger=prefixed_logger
                        )
                        df_silver_all.append(df_partition)
                        processed_input_files.update(files_red)

                    if df_silver_all:
                        df_silver = pd.concat(df_silver_all, ignore_index=True)
                        dfs_input[input_name] = df_silver
                    else:
                        prefixed_logger.info(
                            f"[Read Table] No Silver partitions found for {input_name}. Exiting"
                        )
                        sys.exit(0)

            # Input table layer: Gold
            elif layer_name == "gold":
                gold_input_dir = Path(GOLD_PATH) / input_name
                gold_input_files = list(gold_input_dir.glob("*.parquet"))

                if not gold_input_files:
                    prefixed_logger.info(
                        f"[Read Table] No Gold file found for {input_name}. Exiting."
                    )
                    sys.exit(0)

                if len(gold_input_files) > 1:
                    raise ValueError(
                        f"More than 1 Gold Parquet file found for {input_name}"
                    )

                # Read the latest gold table
                prefixed_logger.info(
                    f"[Read Table] Reading the report file from {gold_input_dir}"
                )
                gold_input_file = gold_input_files[-1]
                df_gold = read_parquet(gold_input_file, logger=prefixed_logger)
                dfs_input[input_name] = df_gold
                processed_input_files.add(str(gold_input_file))

            else:
                raise ValueError(f"Unknown layer {layer_name}")

        # Perform the gold transformations on the inputs
        logger.info("Preforming silver to gold transformations. ")
        df_gold = process_silver_to_gold(
            dfs=dfs_input,
            gold_table_name=table_name,
            gold_column_schema=gold_column_schema,
            logger=prefixed_logger,
        )

        # Drop outdated reports form the output directory
        clear_data_dir(gold_dir, logger)

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
            start_time=run_start_time,
        )

        logger.info(f"Saved run metadata: {metadata_file}")


if __name__ == "__main__":
    run()
