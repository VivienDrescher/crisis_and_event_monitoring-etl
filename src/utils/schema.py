from __future__ import annotations

import logging
from typing import Dict, List, Optional

import pandas as pd


def enforce_schema(
    df: pd.DataFrame,
    schema_dtypes: Dict[str, str],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Enforce a schema on a DataFrame by casting column types and dropping extra columns.

    Notes:
        - Only keeps columns present in schema_dtypes.
        - Safely parses datetime columns to timezone-aware datetime64[ns, tz].
        - Logs warnings for missing columns if a logger is provided.
        - Raises ValueError if casting fails.

    Args:
        df: Input DataFrame
        schema_dtypes: Dictionary mapping column names to desired dtypes
        logger: Optional logger for warnings

    Returns
        pd.DataFrame with columns casted to schema types
    """
    logger = logger or logging.getLogger(__name__)

    # Drop any extra columns in df that are not in schema_dtypes
    columns_to_keep = df.columns.intersection(schema_dtypes.keys())
    dropped_columns = [col for col in df.columns if col not in columns_to_keep]
    df = df.loc[:, columns_to_keep].copy()

    for col, dtype in schema_dtypes.items():
        # Log a warning if column is missing in df
        if col not in df.columns:
            raise ValueError(
                f"[enforce_schema] Column '{col}' specified in schema.yaml not found in dataframe."
            )

        # Convert to the dtype specified in schemas.yaml
        try:
            if "datetime" in str(dtype).lower():
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            raise ValueError(
                f"[enforce_schema] Failed casting column '{col}' to {dtype}"
            ) from e

    if dropped_columns:
        logger.info(
            f"[enforce_schema] Enforced datatypes. Dropped columns not in schema: {dropped_columns}"
        )
    else:
        logger.info("[enforce_schema] Enforced datatypes. No columns were dropped.")

    return df


def validate_required_columns(
    df: pd.DataFrame, required_columns: List[str], logger: logging.Logger
) -> None:
    """
    Ensure that all specified columns are present in the DataFrame.

    Args:
        df: DataFrame to validate
        required_columns: List of columns that must exist
        logger: Logger for messages

    Raises:
        ValueError: If any required column is missing
    """
    logger = logger or logging.getLogger(__name__)

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"[validate_required_columns] Missing required columns: {missing}"
        )

    logger.info("[validate_required_columns] Column validation successful")


def validate_columns_not_null(
    df: pd.DataFrame, non_nullable_columns: List[str], logger: logging.Logger
) -> None:
    """
    Ensure that the non-nullable columns do not contain null values.

    Args:
        df: DataFrame to validate
        non_nullable_columns: List of columns to check for nulls
        logger: Logger for messages

    Raises:
        ValueError: If any required column contains nulls
    """
    logger = logger or logging.getLogger(__name__)

    nulls = df[non_nullable_columns].isna().any()
    failing = nulls[nulls].index.tolist()
    if failing:
        raise ValueError(
            f"[ validate_columns_not_null] Null values found in required columns: {failing}"
        )

    logger.info("[validate_columns_not_null] Non-NULL validation successful")


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
