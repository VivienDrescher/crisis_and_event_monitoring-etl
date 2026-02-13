from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd

from src.utils.system import now_iso


def add_silver_metadata(
    df: pd.DataFrame,
    bronze_run_id: str,
    bronze_ingested_at: datetime,
    silver_run_id: str,
    timezone: ZoneInfo,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add metadata to a Silver DataFrame.

    Args:
        df: Silver DataFrame
        bronze_run_id: Bronze ingestion run ID
        bronze_ingested_at: Bronze ingestion timestamp
        silver_run_id: Silver ingestion run ID

        logger: Optional logger for informational mtimezone: Timezone to use for the _silver_ingested_at timestamp (ZoneInfo)essages

    Returns:
        DataFrame enriched with Silver metadata
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    df["_silver_ingested_at"] = now_iso(timezone)
    df["_silver_run_id"] = silver_run_id
    df["_bronze_ingested_at"] = bronze_ingested_at
    df["_bronze_run_id"] = bronze_run_id

    logger.info("[add_silver_metadata] Silver metadata added")

    return df
