from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional

import pandas as pd


def deduplicate(
    df: pd.DataFrame,
    primary_keys: Iterable[str],
    version_timestamp: Optional[str] = "_bronze_ingested_at",
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Ensure only one record per primary key based on the latest timestamp.

    Keeps the row with the maximum record_timestamp per primary key.

    Args:
        df: Input DataFrame
        primary_key: Iterable of column names defining the primary key
        version_timestamp: Column used to determine latest record
        logger: Optional logger

    Returns:
        Deduplicated DataFrame (copy)
    """
    logger = logger or logging.getLogger(__name__)

    # Validate timestamp column
    if not version_timestamp or version_timestamp not in df.columns:
        logger.warning(
            "[deduplicate] No valid timestamp column found; skipping deduplication"
        )
        return df

    if not pd.api.types.is_datetime64_any_dtype(df[version_timestamp]):
        df[version_timestamp] = pd.to_datetime(df[version_timestamp], errors="coerce")

    # Keep row with max timestamp per primary key ---
    index = df.groupby(primary_keys)[version_timestamp].idxmax()
    df_deduped = df.loc[index].copy()

    if len(df) != len(df_deduped):
        logger.info(
            f"[deduplicate] Reduced {len(df)} → {len(df_deduped)} rows "
            f"keeping latest per {primary_keys} "
            f"based on '{version_timestamp}'."
        )

    return df_deduped


def normalize_strings(
    df: pd.DataFrame, logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Strip leading and trailing whitespace from all string columns in the DataFrame.

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
        columns_schema: Schema definition including source column names

    Returns:
        pd.DataFrame with columns renamed
    """
    rename_map = {
        spec["source"]: col_name
        for col_name, spec in columns_schema.items()
        if "source" in spec
    }
    return df.rename(columns=rename_map)


def cast_to_schema(
    df: pd.DataFrame,
    schema_dtypes: Dict[str, str],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Transform DataFrame to enforce schema:
    - Drops columns not in schema
    - Casts columns to the expected types

    Args:
        df: Input DataFrame
        schema_dtypes: Dictionary of column -> target dtype
        logger: Optional logger

    Returns:
        DataFrame with columns cast to target types
    """
    logger = logger or logging.getLogger(__name__)

    # Drop extra columns not defined in schema
    columns_to_keep = df.columns.intersection(schema_dtypes.keys())
    dropped_columns = [col for col in df.columns if col not in columns_to_keep]
    df = df.loc[:, columns_to_keep].copy()

    if dropped_columns:
        logger.info(
            f"[cast_to_schema] Dropped {len(dropped_columns)} columns not present in schema: {dropped_columns}"
        )

    # Cast columns to target types
    for col, dtype in schema_dtypes.items():
        if col not in df.columns:
            logger.warning(
                f"[cast_to_schema] Column '{col}' missing in DataFrame. Filling with NaN/None"
            )
            df[col] = pd.NA

        try:
            if "datetime" in str(dtype).lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        except Exception:
            logger.error(
                f"[cast_to_schema] Failed casting column '{col}' to {dtype}. Filling with NaN/None"
            )
            df[col] = pd.NA

    return df


def get_record_timestamp_column(
    column_schema: Dict[str, Dict[str, any]],
) -> Optional[str]:
    """
    Identify the column in the schema marked as the record timestamp.

    Args:
        column_schema: Dictionary of column_name -> column_spec from schema.yaml

    Returns:
        The column name that is marked as record_timestamp.

    Raises:
        ValueError: If more than one column is marked as record_timestamp.
    """
    record_timestamps = [
        col for col, spec in column_schema.items() if spec.get("record_timestamp")
    ]

    if not record_timestamps:
        return None
    elif len(record_timestamps) > 1:
        raise ValueError(
            f"More than one record_timestamp found in schema: {record_timestamps}"
        )

    return record_timestamps[0]
