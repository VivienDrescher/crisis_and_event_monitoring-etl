from __future__ import annotations

from typing import Optional
import pandas as pd
import logging


def build(
    df: pd.DataFrame,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Convert ACLED 'MONTH' and 'YEAR' columns into a single
    'month_start_date' datetime column representing the first day of the month.

    Args:
        df: Input DataFrame
        logger: Optional logger for informational messages

    Returns:
        DataFrame with new column 'month_start_date'
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    month_col = "MONTH"
    year_col = "YEAR"
    
    if month_col not in df.columns or year_col not in df.columns:
        logger.warning(f"[custom_transform_acled] Columns {month_col}/{year_col} missing; skipping transformation")
        return df

    try:
        # Convert month abbreviations to integer month (1-12)
        month_numbers = pd.to_datetime(df[month_col], format="%B", errors="coerce").dt.month
        df["month_start_date"] = pd.to_datetime(dict(year=df[year_col], month=month_numbers, day=1), errors="coerce", utc=True)
        num_missing = df["month_start_date"].isna().sum()
        if num_missing > 0:
            logger.warning(f"[custom_transform_acled] {num_missing} rows could not be converted to 'month_start_date'")
        else:
            logger.info("[custom_transform_acled] 'month_start_date' successfully created")
    except Exception as e:
        logger.exception("[custom_transform_acled] Failed to create 'month_start_date'")
        raise

    return df