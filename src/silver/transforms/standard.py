from __future__ import annotations

from typing import Iterable, Optional, Dict, Tuple
import pandas as pd
import logging

from src.silver.transforms.custom import CUSTOM_TRANSFORMS
from src.silver.metadata import add_silver_metadata
from src.common_utils.schema_validation import validate_required_columns_not_null, validate_required_columns


# -------------------------------
# Helper / reusable transformations
# -------------------------------

def deduplicate(
    df: pd.DataFrame,
    primary_key: Optional[Iterable[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Drop duplicate rows based on the primary key columns.

    Args:
        df: Input DataFrame
        primary_key: Iterable of column names defining the primary key
        logger: Optional logger

    Returns:
        Deduplicated DataFrame (copy)
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()
    
    if not primary_key:
        logger.warning("[deduplicate] No primary key provided; skipping deduplication")
        return df

    valid_keys = [c for c in primary_key if c in df.columns]
    missing_keys = [c for c in primary_key if c not in df.columns]

    if missing_keys:
        logger.warning(f"[deduplicate] Primary key columns missing from DataFrame: {missing_keys}")
    if not valid_keys:
        logger.warning("[deduplicate] No valid primary key columns found; skipping deduplication")
        return df

    df_deduped = df.drop_duplicates(subset=valid_keys, keep="last")
    logger.info(f"[deduplicate] Dropped {len(df) - len(df_deduped)} duplicate rows using keys {valid_keys}")
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
    rename_map: Optional[Dict[str, str]] = None,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Rename columns according to a provided mapping.

    Notes:
        - Columns in rename_map that do not exist in df are ignored.
        - Returns a copy; original df is not mutated.
        - Logs a warning for any missing columns if a logger is provided.

    Args:
        df: Input DataFrame
        rename_map: Dictionary mapping old column names to new column names
        logger: Optional logger for warnings

    Returns:
        pd.DataFrame with columns renamed
    """
    if not rename_map:
        return df

    missing = [c for c in rename_map if c not in df.columns]
    if missing and logger:
        logger.warning(f"[apply_column_renames] Rename map contains missing columns: {missing}")

    return df.rename(columns=rename_map)


def enforce_schema(
    df: pd.DataFrame,
    schema_dtypes: Dict[str, str],
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Enforce a schema on a DataFrame by casting column types and dropping extra columns.

    Notes:
        - Only keeps columns present in schema_dtypes.
        - Safely parses datetime columns to UTC-aware datetime64[ns, UTC].
        - Logs warnings for missing columns if a logger is provided.
        - Raises ValueError if casting fails.

    Args:
        df: Input DataFrame
        schema_dtypes: Dictionary mapping column names to desired dtypes
        logger: Optional logger for warnings

    Returns
        pd.DataFrame with columns casted to schema types
    """
    df = df.loc[:, df.columns.intersection(schema_dtypes.keys())].copy()

    for col, dtype in schema_dtypes.items():
        if col not in df.columns:
            if logger:
                logger.warning(f"[enforce_schema] Column '{col}' not found; skipping cast")
            continue

        try:
            if "datetime" in str(dtype).lower():
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            raise ValueError(f"[enforce_schema] Failed casting column '{col}' to {dtype}") from e
    
    logger.info(f"[enforce_schema] Enforced datatypes and dropped columns missing a dtype specification")

    return df


def apply_custom_transform(
    df: pd.DataFrame,
    source_name: str,
    logger: Optional[logging.Logger] = None
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Apply a source-specific custom transformation if registered.

    Args:
        df: Input DataFrame
        source_name: Source key to look up in CUSTOM_TRANSFORMS
        logger: Optional logger to pass to the custom transform

    Returns:
        Tuple of:
            - Transformed DataFrame
            - Name of the custom transform applied (or None if no transform)
    """
    logger = logger or logging.getLogger(__name__)
    entry = CUSTOM_TRANSFORMS.get(source_name)

    if not entry:
        logger.debug(f"[apply_custom_transform] No custom transform registered for source '{source_name}'")
        return df, None

    transform_func = entry["function"]
    transform_name = entry["name"]

    # Pass the logger if the function accepts it
    try:
        df = transform_func(df, logger=logger)
    except Exception as e:
        logger.exception(f"[apply_custom_transform] Failed to apply custom transform '{transform_name}'")
        raise

    return df, transform_name


# -------------------------------
# Full Silver pipeline
# -------------------------------

def process_bronze_to_silver(
    df: pd.DataFrame,
    source_name: str,
    silver_schema: Dict,
    silver_run_id: str,
    bronze_file_name: str,
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

    df = df.copy()

    bronze_run_id = df["_run_id"].iloc[0] if "_run_id" in df.columns else None
    bronze_ingested_at = df["_bronze_ingested_at"].iloc[0] if "_bronze_ingested_at" in df.columns else None

    # 1. Rename
    df = apply_column_renames(df, silver_schema.get("rename_columns"), logger)

    # 2. Custom transformation
    df, transform_custom_name = apply_custom_transform(df, source_name, logger)

    # 3. Normalize strings
    df = normalize_strings(df, logger)

    # 4. Enforce schema
    df = enforce_schema(df, silver_schema.get("dtypes"), logger)

    # 5. Add Silver metadata
    df = add_silver_metadata(
        df,
        source_name=source_name,
        bronze_file=bronze_file_name,
        bronze_run_id=bronze_run_id,
        bronze_ingested_at = bronze_ingested_at,
        silver_run_id=silver_run_id,
        transform_standard_name=silver_schema.get("transform_name"),
        transform_custom_name=transform_custom_name,
        logger=logger
    )

    # 7. Validate schema
    required_cols = silver_schema.get("required_columns", [])
    validate_required_columns(df, required_cols, logger)
    validate_required_columns_not_null(df, required_cols, logger)

    # 8. Deduplicate
    primary_key = silver_schema.get("primary_key")
    if primary_key:
        df = deduplicate(df, primary_key, logger)

    return df