from __future__ import annotations

import logging
from typing import Dict

import pandas as pd


def build(
    dfs: Dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Build monthly ACLED metrics from one or more silver tables.

    Args:
        dfs: Dictionary of silver DataFrames, e.g. {"acled": df_acled}
        logger: Logger for informational messages

    Returns:
        DataFrame with monthly metrics
    """
    logger.info("[acled_monthly] Building monthly ACLED metrics")

    # Extract the required input tables
    if "acled" not in dfs:
        raise ValueError("[monthly_acled] Input 'acled' table not found in dfs")
    df_acled = dfs["acled"].copy()

    # Aggregate metrics
    acled_monthly = df_acled.groupby("month_start_date", as_index=False).agg(
        total_events=("num_events", "sum"),
    )

    return acled_monthly
