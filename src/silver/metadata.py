from __future__ import annotations

import pandas as pd
import logging 
from typing import Optional
from datetime import datetime

from src.common_utils.time import utc_now_iso


def add_silver_metadata(
    df: pd.DataFrame,
    bronze_run_id: str,
    bronze_ingested_at: datetime,
    silver_run_id: str,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add metadata to a Silver DataFrame.

    Args:
        df: Silver DataFrame
        bronze_run_id: Bronze ingestion run ID
        bronze_ingested_at: Bronze ingestion timestamp 
        silver_run_id: Silver ingestion run ID
        logger: Optional logger for informational messages
        
    Returns:
        DataFrame enriched with Silver metadata
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    df["_silver_ingested_at"] = utc_now_iso()
    df["_silver_run_id"] = silver_run_id
    df["_bronze_ingested_at"] = bronze_ingested_at
    df["_bronze_run_id"] = bronze_run_id

    logger.info(f"[add_silver_metadata] Silver metadata added")

    return df