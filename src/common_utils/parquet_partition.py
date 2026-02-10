from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple, Optional
import logging
import uuid
import pandas as pd

from src.common_utils.parquet import write_parquet


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
    base_dir: Path,
    partition_keys: List[str],
    partition_values: Tuple[str, ...],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read all Parquet files belonging to a specific partition based on the parition keys and values.

    Returns an empty DataFrame if the partition does not exist.
    """
    logger = logger or logging.getLogger(__name__)

    # Determine partition directory  
    partition_path = build_partition_path(
        base_dir, partition_keys, partition_values
    )

    # Verify the directory exists -> return empty df otherwise 
    if not partition_path.exists():
        logger.info(f"[read_parquet_partition] No existing partition: {partition_path}")
        return pd.DataFrame()

    # Read all files from the parition folder
    files = list(partition_path.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]

    return pd.concat(dfs, ignore_index=True)


def write_parquet_partition(
    df: pd.DataFrame,
    base_dir: Path,
    partition_keys: List[str],
    partition_values: Iterable[str],
    file_name: str,
    logger: Optional[logging.Logger] = None,
) -> List[Path]:
    """
    Safely write a single partition to a Parquet file based on the partition keys and values.

    Uses atomic write semantics:
    - write to a temp file
    - fsync
    - atomic rename
    """
    logger = logger or logging.getLogger(__name__)

    # Determin partition directory  
    partition_path = build_partition_path(
        base_dir, partition_keys, partition_values
    )
    
    # Determine output paths 
    partition_path.mkdir(parents=True, exist_ok=True)
    output_file = partition_path / f"{file_name}.parquet"
    tmp_file = partition_path / f".{file_name}.{uuid.uuid4().hex}.tmp"

    # Safe write 
    write_parquet(df, tmp_file, logger=logger)
    tmp_file.replace(output_file)
    
    logger.info(f"[write_parquet_partition] Wrote {len(df)} rows to {partition_path}")

    return output_file