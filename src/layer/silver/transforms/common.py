from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from src.layer.silver.metadata import add_silver_metadata
from src.layer.silver.transforms.custom_registry import SILVER_DATASET_CUSTOM_TRANSFORMS
from src.utils.data_quality_checks import (
    check_column_types,
    check_non_nullable_columns,
    check_primary_key_uniqueness,
    check_primary_keys_exist,
    check_required_columns,
)
from src.utils.dataframe import (
    apply_column_renames,
    cast_to_schema,
    deduplicate,
    normalize_strings,
)
from src.utils.io import read_parquet


def process_bronze_to_silver(
    files: Iterable[Path],
    table_name: str,
    silver_schema: Dict,
    run_id: str,
    timezone: ZoneInfo,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, Set[str]]:
    """
    Apply all Silver layer transformations to Bronze files for a given table.

    Steps:
        1. Rename columns
        2. Apply source-specific transformations
        3. Normalize string columns
        4. Parse date/datetime columns
        5. Enforce schema dtypes
        6. Add Silver metadata
        7. Validate required columns and NOT NULLs
        8. Deduplicate

    Args:
        files: Iterable of Bronze file paths
        table_name: Name of the Silver table to produce
        silver_schema: Dict defining Silver column types, nullable, primary keys, etc.
        run_id: Current Silver pipeline run ID
        timezone: Timezone to use for metadata
        logger: Optional logger for informational messages

    Returns:
        Tuple containing:
            - DataFrame with Silver transformations applied
            - Set of processed input file paths
    """

    logger = logger or logging.getLogger(__name__)

    processed_input_files = set()
    dfs = []

    for num_file, bronze_file in enumerate(files, start=1):
        logger.info(
            f"Processing Bronze file {num_file}/{len(files)}: {bronze_file.name}"
        )

        df = read_parquet(bronze_file, logger=logger)

        entry = SILVER_DATASET_CUSTOM_TRANSFORMS.get(table_name)
        if not entry:
            raise ValueError(
                f"[process_bronze_to_silver] No silver transform registered for dataset '{table_name}'"
            )

        transform_fn = entry["function"]

        silver_column_schema = silver_schema.get("columns")
        schema_dtypes = {
            col: spec["type"]
            for col, spec in silver_column_schema.items()
            if "type" in spec
        }
        required_cols = [col for col, _ in silver_column_schema.items()]
        non_nullable_cols = [
            col
            for col, spec in silver_column_schema.items()
            if spec.get("nullable") is False
        ]
        primary_keys = [
            col
            for col, spec in silver_column_schema.items()
            if spec.get("primary_key", False)
        ]

        # Extract bronze run metdata relevant for silver layer
        bronze_run_id = df["_run_id"].iloc[0] if "_run_id" in df.columns else None
        bronze_ingested_at = (
            df["_bronze_ingested_at"].iloc[0]
            if "_bronze_ingested_at" in df.columns
            else None
        )

        # Rename columns
        df = apply_column_renames(df, silver_column_schema)

        # Apply custom transformation
        df = transform_fn(df, logger)

        # Normalize strings
        df = normalize_strings(df, logger)

        # Deduplicate
        check_primary_keys_exist(df, primary_keys, logger)
        df = deduplicate(df, primary_keys, logger=logger)

        # Enforce silver schema (types + drop extra columns)
        df = cast_to_schema(df, schema_dtypes, logger)

        # Add Silver metadata
        df = add_silver_metadata(
            df,
            bronze_run_id=bronze_run_id,
            bronze_ingested_at=bronze_ingested_at,
            silver_run_id=run_id,
            timezone=timezone,
            logger=logger,
        )

        #  Data Quality Checks
        check_required_columns(df, required_cols, logger)
        check_non_nullable_columns(df, non_nullable_cols, logger)
        check_primary_key_uniqueness(df, primary_keys, logger)
        check_column_types(df, schema_dtypes, logger)

        dfs.append(df)
        processed_input_files.add(str(bronze_file))

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True), processed_input_files
