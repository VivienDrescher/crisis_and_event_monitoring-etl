from __future__ import annotations

import pandas as pd
from typing import List, Optional, Dict
import logging


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
    
    logger.info(f"[enforce_schema] Enforced datatypes and dropped columns missing a specification in schema.yaml")

    return df


def validate_required_columns(
    df: pd.DataFrame,
    required_columns: List[str],
    logger: logging.Logger
) -> None:
    """
    Ensure that all required columns are present in the DataFrame.

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
        logger.error(f"[validate_required_columns] Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

    logger.info(f"[validate_required_columns] Column validation successful")


def validate_required_columns_not_null(
    df: pd.DataFrame,
    required_columns: List[str],
    logger: logging.Logger
) -> None:
    """
    Ensure that required columns do not contain null values.

    Args:
        df: DataFrame to validate
        required_columns: List of columns to check for nulls
        logger: Logger for messages

    Raises:
        ValueError: If any required column contains nulls
    """
    logger = logger or logging.getLogger(__name__)

    nulls = df[required_columns].isna().any()
    failing = nulls[nulls].index.tolist()
    if failing:
        logger.error(f"[validate_not_null] Null values found in required columns: {failing}")
        raise ValueError(f"Null values found in required columns: {failing}")

    logger.info(f"[validate_not_null] Non-NULL validation successful")

    
