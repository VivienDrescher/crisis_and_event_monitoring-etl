from __future__ import annotations

from typing import Optional, Dict
import pandas as pd
import logging

from src.layer.gold.transforms.custom_registry import GOLD_DATASET_TRANSFORMS
from src.utils.schema_validation import enforce_schema, validate_required_columns, validate_columns_not_null


def process_silver_to_gold(
    dfs: Dict[str, pd.DataFrame],
    gold_table_name: str,
    gold_column_schema: Dict,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Orchestrate full Silver → Gold processing for one gold table.

    Args:
        dfs: Dictionary of silver DataFrames, keyed by table name
        gold_table_name: Name of the gold dataset to produce
        gold_column_schema: Dict defining gold column types, nullable, etc.
        logger: Optional logger for informational messages

    Returns:
        Gold DataFrame after transformations and validation
    """
    logger = logger or logging.getLogger(__name__)

    # Retrieve the gold transform function for this dataset
    entry = GOLD_DATASET_TRANSFORMS.get(gold_table_name)
    if not entry:
        raise ValueError(
            f"[process_silver_to_gold] No gold transform registered for dataset '{gold_table_name}'"
        )

    transform_fn = entry["function"]

    # Perform dataset-specific transformations (handles multiple input dfs)
    df_gold = transform_fn(dfs, logger)

    # Enforce gold schema (types + drop extra columns)
    schema_dtypes = {col: spec["type"] for col, spec in gold_column_schema.items() if "type" in spec}
    df_gold = enforce_schema(df_gold, schema_dtypes, logger)

    # Validate required columns and NOT NULL constraints
    required_cols = [col for col, spec in gold_column_schema.items() if spec.get("nullable") is False]
    validate_required_columns(df_gold, required_cols, logger)
    validate_columns_not_null(df_gold, required_cols, logger)

    return df_gold
