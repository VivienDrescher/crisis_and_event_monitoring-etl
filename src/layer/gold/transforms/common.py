from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd

from src.layer.gold.transforms.custom_registry import GOLD_DATASET_TRANSFORMS
from src.utils.data_quality_checks import (
    check_column_types,
    check_non_nullable_columns,
    check_primary_key_uniqueness,
    check_primary_keys_exist,
    check_required_columns,
)
from src.utils.dataframe import cast_to_schema


def process_silver_to_gold(
    dfs: Dict[str, pd.DataFrame],
    gold_table_name: str,
    gold_column_schema: Dict,
    logger: Optional[logging.Logger] = None,
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
    schema_dtypes = {
        col: spec["type"] for col, spec in gold_column_schema.items() if "type" in spec
    }
    df_gold = cast_to_schema(df_gold, schema_dtypes, logger)

    # Validate required columns and NOT NULL constraints
    required_cols = [col for col, _ in gold_column_schema.items()]
    non_nullable_cols = [
        col for col, spec in gold_column_schema.items() if spec.get("nullable") is False
    ]
    primary_keys = [
        col
        for col, spec in gold_column_schema.items()
        if spec.get("primary_key", False)
    ]

    #  Data Quality Checks
    check_required_columns(df_gold, required_cols, logger)
    check_non_nullable_columns(df_gold, non_nullable_cols, logger)
    check_primary_keys_exist(df_gold, primary_keys, logger)
    check_primary_key_uniqueness(df_gold, primary_keys, logger)
    check_column_types(df_gold, schema_dtypes, logger)

    return df_gold
