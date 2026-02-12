from __future__ import annotations

import pandas as pd
import logging
from typing import Optional

from src.common_utils.time import utc_now_iso


def add_bronze_metadata(
    df: pd.DataFrame,
    source_name: str,
    source_file: str,
    bronze_run_id: str,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add metadata to a Bronze DataFrame.

    Args:
        df: Input DataFrame
        source_name: Name of the source
        source_file: File name being ingested
        run_id: Bronze pipeline run ID
        logger: Optional logger for informational messages

    Returns:
        DataFrame enriched with Bronze metadata
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    df["_bronze_ingested_at"] = utc_now_iso()
    df["_source_name"] = source_name
    df["_source_file"] = source_file
    df["_bronze_run_id"] = bronze_run_id

    logger.info(f"[add_bronze_metadata] Bronze metadata added")

    return df