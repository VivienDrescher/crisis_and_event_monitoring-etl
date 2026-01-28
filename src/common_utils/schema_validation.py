from __future__ import annotations

import pandas as pd
from typing import List
import logging


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

    
