from __future__ import annotations

import pandas as pd
import logging 
from typing import Optional
from datetime import datetime

from src.common_utils.time import utc_now_iso
from src.common_utils.version import get_git_commit


def add_silver_metadata(
    df: pd.DataFrame,
    source_name: str,
    bronze_file: str,
    bronze_run_id: str,
    bronze_ingested_at: datetime,
    silver_run_id: str,
    transform_standard_name: Optional[str] = None,
    transform_custom_name: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add metadata to a Silver DataFrame.

    Silver metadata captures:
        - Source traceability
        - Bronze provenance
        - Transformation applied
        - Code version

    Args:
        df: Silver DataFrame
        source_name: Source name (e.g., "gdelt")
        bronze_file: Bronze file name that was processed
        bronze_run_id: Bronze ingestion run ID
        bronze_ingested_at: Bronze ingestion timestamp 
        silver_run_id: Silver ingestion run ID
        transform_standard_name: Name of standard transformation applied
        transform_custom_name: Name of any custom transformation applied
        logger: Optional logger for informational messages
        
    Returns:
        DataFrame enriched with Silver metadata
    """
    logger = logger or logging.getLogger(__name__)
    df = df.copy()

    df["_silver_ingested_at"] = utc_now_iso()
    df["_source"] = source_name
    df["_bronze_file"] = bronze_file
    df["_bronze_run_id"] = bronze_run_id
    df["_bronze_ingested_at"] = bronze_ingested_at
    df["_silver_run_id"] = silver_run_id

    if transform_standard_name:
        df["_transform_standard_name"] = transform_standard_name
    if transform_custom_name:
        df["_transform_custom_name"] = transform_custom_name

    # Track the exact git commit for reproducibility
    df["_code_version"] = get_git_commit()

    logger.info(f"[add_silver_metadata] Silver metadata added")

    return df