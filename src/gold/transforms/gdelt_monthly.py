from __future__ import annotations

import logging
import pandas as pd
from typing import Dict 

def build(
    dfs: Dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Build monthly GDELT metrics from one or more silver tables.

    Args:
        dfs: dictionary of silver DataFrames, e.g. {"gdelt": df_gdelt}
        logger: logger

    Returns:
        DataFrame with monthly GDELT metrics 
    """
    
    logger.info("[gdelt_monthly] Building monthly GDEDLT metrics")

    # Extract the required input tables
    if "gdelt" not in dfs:
        raise ValueError("[monthly_gdelt_metrics] Input 'gdelt' table not found in dfs")
    df_gdelt = dfs["gdelt"].copy()

    # Derive month start date
    df_gdelt["event_date"] = pd.to_datetime(df_gdelt["event_date"], utc=True)
    df_gdelt["month_start_date"] = (
    pd.to_datetime(df_gdelt["event_date"], utc=True)
    .dt.tz_convert("UTC")
    .dt.normalize()
    - pd.to_timedelta(
        pd.to_datetime(df_gdelt["event_date"], utc=True).dt.day - 1, unit="D"
    )
)

    # Aggregate metrics 
    gdelt_monthly = (
        df_gdelt.groupby("month_start_date", as_index=False)
        .agg(
            total_events=("event_id", "nunique"),
            avg_tone=("avg_tone", "mean"),
            total_mentions=("num_mentions", "sum"),
            total_articles=("num_articles", "sum"),
        )
    )
    
    logger.info("[gdelt_monthly] Produced %s monthly rows", len(gdelt_monthly))

    return gdelt_monthly