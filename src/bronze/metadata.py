from __future__ import annotations

import pandas as pd
import logging
from typing import Optional

from src.common_utils.time import utc_now_iso


def add_bronze_metadata(
    df: pd.DataFrame,
    source_name: str,
    source_file: str,
    run_id: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add metadata to a Bronze DataFrame.

    Notes:
        - Bronze stores raw ingestion data.
        - All timestamps are in UTC ISO format.
        - Code version is always added for traceability.

    Args:
        df: Input DataFrame
        source_name: Name of the source
        source_file: File name being ingested
        run_id: Optional ingestion run ID
        logger: Optional logger for informational messages

    Returns:
        DataFrame enriched with Bronze metadata
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    df["_bronze_ingested_at"] = utc_now_iso()
    df["_source_name"] = source_name
    df["_source_file"] = source_file
    if run_id:
        df["_run_id"] = run_id

    logger.info(f"[add_bronze_metadata] Bronze metadata added")

    return df