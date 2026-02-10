from __future__ import annotations

import logging
import pandas as pd
from typing import Dict 

def build(
    dfs: Dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Build daily GDELT metrics from one or more silver tables.

    Args:
        dfs: dictionary of silver DataFrames, e.g. {"gdelt": df_gdelt}
        logger: logger

    Returns:
        DataFrame with daily GDELT metrics 
    """
    
    logger.info("[gdelt_daily] Building daily GDEDLT metrics")

    # Extract the required input tables
    if "gdelt" not in dfs:
        raise ValueError("[daily_gdelt] Input 'gdelt' table not found in dfs")
    df_gdelt = dfs["gdelt"].copy()

    # Aggregate metrics 
    gdelt_daily = (
        df_gdelt.groupby("event_date", as_index=False)
            .agg(
                total_events=("event_id", "nunique"),
                avg_tone=("avg_tone", "mean"),
                total_mentions=("num_mentions", "sum"),
                total_articles=("num_articles", "sum")
            )
    )

    logger.info("[gdelt_daily] Produced %s monthly rows", len(gdelt_daily))

    return gdelt_daily
