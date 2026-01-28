from __future__ import annotations

from typing import Callable, Optional
import pandas as pd
import logging

def transform_gdelt(df: pd.DataFrame, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    """
    Apply GDELT-specific transformations.

    Currently:
      - Converts `event_at` from YYYYMMDD string/int to datetime[ns, UTC]

    Args:
        df: Input DataFrame
        logger: Optional logger

    Returns:
        Transformed DataFrame
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    if "event_at" not in df.columns:
        logger.warning("[transform_gdelt] Column 'event_at' not found; skipping transformation")
        return df

    try:
        df["event_at"] = pd.to_datetime(
            df["event_at"].astype(str),
            format="%Y%m%d",
            errors="coerce",  # invalid values → NaT
            utc=True
        )
        num_missing = df["event_at"].isna().sum()
        if num_missing > 0:
            logger.warning(f"[transform_gdelt] {num_missing} 'event_at' values could not be parsed and became NaT")
        else:
            logger.info("[transform_gdelt] 'event_at' successfully converted to datetime")
    except Exception as e:
        logger.exception("[transform_gdelt] Failed to transform 'event_at'")
        raise

    return df


CUSTOM_TRANSFORMS: dict[str, dict[str, Callable]] = {
    "gdelt": {
        "name": "gdelt_custom_transforms_v1",
        "function": transform_gdelt,
    }
}
