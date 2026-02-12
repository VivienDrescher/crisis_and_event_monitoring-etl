from __future__ import annotations

from typing import Iterable, Optional, Dict
from pathlib import Path 
import pandas as pd
import logging

from src.silver.transforms.registry import SILVER_DATASET_CUSTOM_TRANSFORMS
from src.silver.metadata import add_silver_metadata
from src.common_utils.schema_validation import enforce_schema, validate_required_columns, validate_columns_not_null
from src.common_utils.parquet import read_parquet

# -------------------------------
# Helper / reusable transformations
# -------------------------------

def deduplicate(
    df: pd.DataFrame,
    primary_keys: Iterable[str],
    record_timestamp: Optional[str],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Ensure only one record per primary key based on the latest timestamp.

    Keeps the row with the maximum timestamp per primary key.

    Args:
        df: Input DataFrame
        primary_key: Iterable of column names defining the primary key
        record_timestamp: Column used to determine latest record
        logger: Optional logger

    Returns:
        Deduplicated DataFrame (copy)
    """
    logger = logger or logging.getLogger(__name__)

    # Validate primary keys 
    if not primary_keys:
        logger.warning("[deduplicate] No primary key provided; skipping deduplication")
        return df

    valid_primary_keys = [c for c in primary_keys if c in df.columns]
    invalid_primary_keys = [c for c in primary_keys if c not in df.columns]
    if invalid_primary_keys:
        logger.warning("[deduplicate] Bronze data is missing primary key columns; skipping deduplication")
        return df

    # Validate timestamp column 
    if not record_timestamp or record_timestamp not in df.columns:
        logger.warning("[deduplicate] No valid timestamp column found; skipping deduplication")
        return df 

    if not pd.api.types.is_datetime64_any_dtype(df[record_timestamp]):
        df[record_timestamp] = pd.to_datetime(df[record_timestamp], errors="coerce")

    # Keep row with max timestamp per primary key ---
    index = df.groupby(valid_primary_keys)[record_timestamp].idxmax()
    df_deduped = df.loc[index].copy()

    if len(df) != len(df_deduped):
        logger.info(
            f"[deduplicate] Reduced {len(df)} → {len(df_deduped)} rows "
            f"keeping latest per {valid_primary_keys} "
            f"based on '{record_timestamp}'."
        )

    return df_deduped


def normalize_strings(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Strip leading and trailing whitespace from all string columns in the DataFrame.

    Notes:
        - Only applies to columns with dtype 'string'.
        - Returns a copy; does not mutate in place.
        - Logs debug info if a logger is provided.

    Args:
        df: Input DataFrame
        logger: Optional logger for debug messages

    Returns:
        pd.DataFrame with all string columns stripped of leading/trailing whitespace
    """
    df = df.copy()
    string_cols = df.select_dtypes(include="string").columns.tolist()

    if logger:
        logger.debug(f"[normalize_strings] Normalizing string columns: {string_cols}")

    for col in string_cols:
        df[col] = df[col].str.strip()

    return df


def apply_column_renames(
    df: pd.DataFrame,
    columns_schema: dict,
) -> pd.DataFrame:
    """
    Rename columns according to a provided mapping.

    Notes:
        - Columns in rename_map that do not exist in df are ignored.
        - Returns a copy; original df is not mutated.

    Args:
        df: Input DataFrame
        columns_schema: Silver schema definition including source column names

    Returns:
        pd.DataFrame with columns renamed
    """
    rename_map = {
        spec["source"]: col_name
        for col_name, spec in columns_schema.items()
        if "source" in spec
    }
    return df.rename(columns=rename_map)


# -------------------------------
# Full Silver pipeline
# -------------------------------
def transform_bronze_files_to_silver(
    files, #?
    table_name: str, 
    silver_schema: Dict,
    run_id: str,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Apply all Silver layer transformations to a DataFrame in canonical order.

    Steps:
        1. Rename columns
        2. Apply source-specific transformations
        3. Normalize string columns
        4. Parse date/datetime columns
        5. Enforce schema dtypes
        6. Add Silver metadata
        7. Validate required columns and NOT NULLs
        8. Deduplicate
    """
    
    logger = logger or logging.getLogger(__name__)

    processed_input_files = set()
    dfs = []

    for num_file, bronze_file in enumerate(files, start=1):
        logger.info(f"Processing Bronze file {num_file}/{len(files)}: {bronze_file.name}")

        df = read_parquet(bronze_file, logger=logger)

        entry = SILVER_DATASET_CUSTOM_TRANSFORMS.get(table_name) 
        if not entry:
            raise ValueError(
                f"[process_bronze_to_silver] No silver transform registered for dataset '{table_name}'"
            )

        transform_fn = entry["function"]

        silver_column_schema = silver_schema.get("columns")
        schema_dtypes = { col: spec["type"] for col, spec in silver_column_schema.items() if "type" in spec}
        required_cols = [col for col, spec in silver_column_schema.items() if spec.get("nullable") is False]
        primary_keys = [col for col, spec in silver_column_schema.items() if spec.get("primary_key", False)]
        record_timestamp = silver_schema.get("record_timestamp")

        # Extract bronze run metdata relevant for silver layer 
        bronze_run_id = df["_run_id"].iloc[0] if "_run_id" in df.columns else None
        bronze_ingested_at = df["_bronze_ingested_at"].iloc[0] if "_bronze_ingested_at" in df.columns else None

        # Rename columns
        df = apply_column_renames(df, silver_column_schema)

        # Apply custom transformation
        df = transform_fn(df, logger)

        # Normalize strings
        df = normalize_strings(df, logger)

        # Deduplicate
        df = deduplicate(df, primary_keys, record_timestamp, logger)

        # Enforce silver schema (types + drop extra columns)
        df = enforce_schema(df, schema_dtypes, logger)

        # Add Silver metadata
        df = add_silver_metadata(
            df,
            bronze_run_id=bronze_run_id,
            bronze_ingested_at = bronze_ingested_at,
            silver_run_id=run_id,
            logger=logger
        )

        #  Validate schema
        validate_required_columns(df, required_cols, logger)
        validate_columns_not_null(df, required_cols, logger)

        dfs.append(df)
        processed_input_files.add(str(bronze_file))

    if not dfs:
        return None

    return pd.concat(dfs, ignore_index=True), processed_input_files 