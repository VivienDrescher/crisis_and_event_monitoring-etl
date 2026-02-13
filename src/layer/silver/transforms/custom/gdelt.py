from __future__ import annotations

import logging
from typing import Optional

import pandas as pd


def build(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Apply GDELT-specific transformations.

    Currently:
      - Converts `event_date` from YYYYMMDD string/int to datetime[ns, UTC]

    Args:
        df: Input DataFrame
        logger: Optional logger for informational messages

    Returns:
        Transformed DataFrame
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    if "event_date" not in df.columns:
        logger.warning(
            "[custom_transform_gdelt] Column 'event_date' not found; skipping transformation"
        )
        return df

    try:
        df["event_date"] = pd.to_datetime(
            df["event_date"].astype(str),
            format="%Y%m%d",
            errors="coerce",  # invalid values → NaT
            utc=True,
        )
        num_missing = df["event_date"].isna().sum()
        if num_missing > 0:
            logger.warning(
                f"[custom_transform_gdelt] {num_missing} 'event_date' values could not be parsed and became NaT"
            )
        else:
            logger.info(
                "[custom_transform_gdelt] 'event_date' successfully converted to datetime"
            )
    except Exception:
        logger.exception("[custom_transform_gdelt] Failed to transform 'event_date'")
        raise

    return df
