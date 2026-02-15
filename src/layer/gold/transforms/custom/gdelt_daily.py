from __future__ import annotations

import logging
from typing import Dict

import pandas as pd


def build(
    dfs: Dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Build daily GDELT metrics from one or more silver tables.

    Args:
        dfs: Dictionary of silver DataFrames, e.g. {"gdelt": df_gdelt}
        logger: Logger for informational messages

    Returns:
        DataFrame with daily GDELT metrics
    """

    logger.info("[gdelt_daily] Building daily GDEDLT metrics")

    # Extract the required input tables
    if "gdelt" not in dfs:
        raise ValueError("[daily_gdelt] Input 'gdelt' table not found in dfs")
    df_gdelt = dfs["gdelt"].copy()

    # Aggregate metrics
    gdelt_daily = df_gdelt.groupby("event_date", as_index=False).agg(
        total_events=("event_id", "nunique"),
        avg_tone=("avg_tone", "mean"),
        total_mentions=("num_mentions", "sum"),
        total_articles=("num_articles", "sum"),
    )

    return gdelt_daily
