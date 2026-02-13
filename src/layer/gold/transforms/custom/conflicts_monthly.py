from __future__ import annotations

import logging
import pandas as pd
from typing import Dict 


def build(
    dfs: Dict[str, pd.DataFrame],
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Build monthly conflict report by joining GDELT and ACLED monthly metrics.

    Args:
        dfs: Dictionary of silver DataFrames, e.g. {"gdelt_monthly": df_gdelt, "acled_monthly": df_acled}
        logger: Logger for informational messages

    Returns:
        DataFrame with monthly conflict metrics
    """

    # Extract the required input tables
    if "acled_monthly" not in dfs:
        raise ValueError("[conflicts_monthly_report] Input 'acled_monthly' table not found in dfs")
    if "gdelt_monthly" not in dfs:
        raise ValueError("[conflicts_monthly_report] Input 'gdelt_monthly' table not found in dfs")
    gdelt_monthly = dfs["gdelt_monthly"].copy()
    acled_monthly = dfs["acled_monthly"].copy()

    # Ensure datetime type for join key
    gdelt_monthly["month_start_date"] = pd.to_datetime(
        gdelt_monthly["month_start_date"]
    )
    acled_monthly["month_start_date"] = pd.to_datetime(
        acled_monthly["month_start_date"], utc=True
    )

    # Rename metrics to avoid collisions and match schema
    gdelt_monthly = gdelt_monthly.rename(
        columns={
            "total_events": "gdelt_total_events",
            "avg_tone": "gdelt_avg_tone",
        }
    )
    acled_monthly = acled_monthly.rename(
        columns={
            "total_events": "acled_total_events",
        }
    )

    # Full outer join
    conflicts_monthly = gdelt_monthly.merge(
        acled_monthly,
        on="month_start_date",
        how="outer",
    )

    logger.info("[conflicts_monthly] Produced %s rows", len(conflicts_monthly))

    return conflicts_monthly