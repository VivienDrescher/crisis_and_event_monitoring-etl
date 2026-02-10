from __future__ import annotations

from typing import Callable, Optional
import pandas as pd
import logging


# --------------------------
# ACLED-specific transform
# --------------------------
def transform_acled_month_start(
    df: pd.DataFrame,
    month_col: str = "MONTH",
    year_col: str = "YEAR",
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Convert ACLED 'MONTH' and 'YEAR' columns into a single
    'month_start_date' datetime column representing the first day of the month.

    Args:
        df: Input DataFrame
        month_col: Name of the month column (e.g., 'Jan', 'Feb')
        year_col: Name of the year column
        logger: Optional logger

    Returns:
        DataFrame with new column 'month_start_date'
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    if month_col not in df.columns or year_col not in df.columns:
        logger.warning(f"[custom_transform_acled] Columns {month_col}/{year_col} missing; skipping transformation")
        return df

    try:
        # Convert month abbreviations to integer month (1-12)
        month_numbers = pd.to_datetime(df[month_col], format="%B", errors="coerce").dt.month
        df["month_start_date"] = pd.to_datetime(dict(year=df[year_col], month=month_numbers, day=1), errors="coerce")
        num_missing = df["month_start_date"].isna().sum()
        if num_missing > 0:
            logger.warning(f"[custom_transform_acled] {num_missing} rows could not be converted to 'month_start_date'")
        else:
            logger.info("[custom_transform_acled] 'month_start_date' successfully created")
    except Exception as e:
        logger.exception("[custom_transform_acled] Failed to create 'month_start_date'")
        raise

    return df


# --------------------------
# GDELT-specific transform
# --------------------------
def transform_gdelt(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Apply GDELT-specific transformations.

    Currently:
      - Converts `event_date` from YYYYMMDD string/int to datetime[ns, UTC]

    Args:
        df: Input DataFrame
        logger: Optional logger

    Returns:
        Transformed DataFrame
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    if "event_date" not in df.columns:
        logger.warning("[custom_transform_gdelt] Column 'event_date' not found; skipping transformation")
        return df

    try:
        df["event_date"] = pd.to_datetime(
            df["event_date"].astype(str),
            format="%Y%m%d",
            errors="coerce",  # invalid values → NaT
            utc=True
        )
        num_missing = df["event_date"].isna().sum()
        if num_missing > 0:
            logger.warning(f"[custom_transform_gdelt] {num_missing} 'event_date' values could not be parsed and became NaT")
        else:
            logger.info("[custom_transform_gdelt] 'event_date' successfully converted to datetime")
    except Exception as e:
        logger.exception("[custom_transform_gdelt] Failed to transform 'event_date'")
        raise

    return df


# --------------------------
# Map of custom transforms
# --------------------------
CUSTOM_TRANSFORMS: dict[str, dict[str, Callable]] = {
    "acled": {
        "name": "acled_custom_transforms",
        "function": transform_acled_month_start,
    },
    "gdelt": {
        "name": "gdelt_custom_transforms",
        "function": transform_gdelt,
    }
}