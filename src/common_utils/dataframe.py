from __future__ import annotations

from typing import Iterable, Optional
import pandas as pd
import logging


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