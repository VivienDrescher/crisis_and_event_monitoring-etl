from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Optional
import logging
import pandas as pd

from src.common_utils.parquet import write_parquet
from src.common_utils.dataframe import deduplicate


def build_partition_path(
    base_dir: Path,
    partition_keys: List[str],
    partition_values: Tuple[str, ...],
) -> Path:
    """
    Build a Hive-style partition path.

    Example:
        partition_key    = ["event_date", "country"]
        partition_values = ("2024-01-01", "DE")

        -> base_dir/event_date=2024-01-01/country=DE
    """

    # Avoids issues with mixed dtypes (datetime, int, etc.) when building directory paths.
    partition_keys = list(str(parition_key) for parition_key in partition_keys)
    partition_values = tuple(str(parition_value) for parition_value in partition_values)

    if len(partition_keys) != len(partition_values):
        raise ValueError(
            "[build_parition_path] "
            "Partition key/value length mismatch: "
            f"{len(partition_keys)} keys vs {len(partition_values)} values"
        )

    path = base_dir
    for key, value in zip(partition_keys, partition_values):
        path = path / f"{key}={value}"

    return path


def read_parquet_partition(
    partition_dir: Path,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read all Parquet files from a parition directory 

    Returns an empty DataFrame if the partition does not exist.
    """
    logger = logger or logging.getLogger(__name__)

    # Verify the directory exists -> return empty df otherwise 
    if not partition_dir.exists():
        logger.info(f"[read_partition] Parition folder {partition_dir} not existing.")
        return pd.DataFrame(), []

    # Read all files from the parition folder
    parition_files = list(partition_dir.glob("*.parquet"))
    if not parition_files:
        return pd.DataFrame(), []
    dfs = [pd.read_parquet(f) for f in parition_files]
    logger.info(f"[read_partition] Red Parition folder {partition_dir}.")

    return pd.concat(dfs, ignore_index=True), parition_files


def write_partitioned_parquet(
    df: pd.DataFrame,
    partition_keys: list[str],
    output_dir: Path,
    is_merge: bool = False,
    primary_keys: Optional[list[str]] = None,
    record_timestamp: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> set[str]:
    """
    Write a DataFrame to partitioned Parquet files.

    Args:
        df: DataFrame to write
        partition_keys: Columns to partition by
        output_dir: Base output directory
        is_merge: If True, merge with existing partition data and deduplicate
        primary_keys: Columns to use for deduplication (required if is_merge=True)
        record_timestamp: Timestamp column for deduplication (required if is_merge=True)
        logger: Optional logger

    Returns:
        Set of output file paths written
    """
    logger = logger or logging.getLogger(__name__)
    files_written = set()

    if df.empty:
        logger.info("Empty DataFrame provided, nothing to write.")
        return files_written

    grouped = df.groupby(partition_keys, dropna=False)

    for partition_values, df_partition in grouped:
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)

        partition_dir = build_partition_path(
            output_dir,
            partition_keys,
            tuple(str(v) for v in partition_values),
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        if is_merge:
            if primary_keys is None or record_timestamp is None:
                raise ValueError("primary_keys and record_timestamp must be provided for merge mode")
            df_existing, _ = read_parquet_partition(partition_dir, logger)
            if df_existing is not None and not df_existing.empty:
                df_partition = pd.concat([df_existing, df_partition], ignore_index=True)
                df_partition = deduplicate(df_partition, primary_keys, record_timestamp, logger)

        output_file = partition_dir / "part.parquet"
        write_parquet(df_partition, output_file, logger=logger)
        files_written.add(str(output_file))

    return files_written


