import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.layer.silver.transforms.common import process_bronze_to_silver
from src.utils.dataframe import deduplicate
from src.utils.io import load_yaml, write_parquet, write_partitioned_parquet
from src.utils.run import (
    identify_new_files,
    load_checkpoint,
    save_checkpoint,
    save_run_metadata,
)
from src.utils.storage import clear_data_dir
from src.utils.system import (
    PrefixedLogger,
    get_log_file_path,
    load_env,
    setup_standalone_logging,
)


def run():
    LAYER_NAME = "silver"

    # --------------------------
    # Load environment variables
    # --------------------------
    load_env()

    DEBUG = os.getenv("DEBUG", "false").lower() == "true"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    BRONZE_PATH = Path(os.getenv("BRONZE_PATH", "data/bronze"))
    SILVER_PATH = Path(os.getenv("SILVER_PATH", "data/silver"))
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

    # --------------------------
    # Iterate over all Bronze tables
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
        # Extract source and schema details
        # --------------------------
        if table_name not in sources_config["sources"]:
            raise ValueError(f"Source {table_name} not found in sources.yaml")

        source_config = sources_config["sources"][table_name]
        if not source_config.get("enabled", True):
            logger.info(
                f"Source {table_name} is disabled. Skipping run for {table_name}."
            )
            continue

        ingestion_mode = source_config.get("ingestion_mode")

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

        bronze_files = sorted(
            bronze_dir.glob("*.parquet"), key=lambda f: f.stat().st_mtime
        )
        if not bronze_files:
            logger.info("No Bronze files found. Terminating run for {table_name}.")
            continue

        # Schema and partition config
        column_schema = silver_schema.get("columns", {})
        primary_keys = [
            col for col, spec in column_schema.items() if spec.get("primary_key", False)
        ]
        record_timestamps = [
            col
            for col, spec in column_schema.items()
            if spec.get("record_timestamp", False)
        ]
        partition_keys = [
            col
            for col, spec in column_schema.items()
            if spec.get("partition_key", False)
        ]
        if len(record_timestamps) > 1:
            raise ValueError(
                f"More than 1 Silver record timestamp file found for {table_name}"
            )
        record_timestamp = record_timestamps[-1]

        checkpoint_path = silver_dir / "_checkpoint.json"
        checkpoint_files_old = load_checkpoint(checkpoint_path)

        # MODE: latest_snapshot
        if ingestion_mode == "latest_snapshot":
            clear_data_dir(silver_dir, logger)
            checkpoint_files_old = set()

            latest_file = bronze_files[-1]
            logger.info(f"Processing latest Bronze file only: {latest_file.name}")

            df, processed_input_files = process_bronze_to_silver(
                [latest_file],
                table_name,
                silver_schema,
                run_id,
                timezone,
                prefixed_logger,
            )

            if df is not None:
                output_file = silver_dir / latest_file.name
                write_parquet(df, output_file, logger=logger)
                processed_output_files.add(str(output_file))

        # MODE: overwrite (full rebuild)
        elif ingestion_mode == "overwrite":
            clear_data_dir(silver_dir, prefixed_logger)
            checkpoint_files_old = set()

            logger.info(f"Processing {len(bronze_files)} Bronze files to Silver.")
            df, processed_input_files = process_bronze_to_silver(
                bronze_files,
                table_name,
                silver_schema,
                run_id,
                timezone,
                prefixed_logger,
            )

            if df is None:
                logger.info("No Bronze data found. Terminating run for {table_name}")
                continue

            # Global deduplication
            df = deduplicate(df, primary_keys, logger=prefixed_logger)

            logger.info("Writing the processed Broze files to Silver partitions.")
            processed_output_files = write_partitioned_parquet(
                df, partition_keys, silver_dir, logger=prefixed_logger
            )

        # MODE: append (incremental)
        elif ingestion_mode == "append":
            new_bronze_files = identify_new_files(
                files=bronze_files,
                checkpoint_files=checkpoint_files_old,
            )

            if not new_bronze_files:
                logger.info(
                    "No new Bronze files to process. Terminating run for {table_name}."
                )
                continue

            logger.info(f"Processing {len(bronze_files)} Bronze files to Silver.")
            df_new, processed_input_files = process_bronze_to_silver(
                new_bronze_files,
                table_name,
                silver_schema,
                run_id,
                timezone,
                prefixed_logger,
            )
            if df_new is None:
                logger.info("No Bronze data found. Terminating run for {table_name}")
                continue

            # Partition-aware merge write
            logger.info(
                "Merging the processed Bronze files into the existing Silver partitions."
            )
            processed_output_files = write_partitioned_parquet(
                df_new,
                partition_keys,
                silver_dir,
                True,
                primary_keys,
                record_timestamp,
                prefixed_logger,
            )

        # Unknown mode
        else:
            raise ValueError(f"Ingestion mode {ingestion_mode} not implemented")

        # Update checkpoint file
        if processed_input_files:
            updated_files = checkpoint_files_old | processed_input_files
            save_checkpoint(checkpoint_path, updated_files, timezone)
        else:
            logger.info(
                "No new files processed — keeping existing checkpoint unchanged."
            )

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
            start_time=run_start_time,
        )

        logger.info(f"Saved run metadata: {metadata_file}")


if __name__ == "__main__":
    run()
