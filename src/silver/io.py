from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple, Optional
import logging
import uuid

import pandas as pd

from src.common_utils.parquet import write_parquet
from src.common_utils.partitions import build_partition_path


def read_silver_partition(
    silver_dir: Path,
    partition_key: List[str],
    partition_values: Tuple[str, ...],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read all Parquet files belonging to a specific Silver partition.

    Returns an empty DataFrame if the partition does not exist.
    """
    logger = logger or logging.getLogger(__name__)

    partition_path = build_partition_path(
        silver_dir, partition_key, partition_values
    )

    if not partition_path.exists():
        logger.info(f"[partition] No existing Silver partition: {partition_path}")
        return pd.DataFrame()

    files = list(partition_path.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)


def write_silver_partition(
    df: pd.DataFrame,
    silver_dir: Path,
    partition_key: List[str],
    partition_values: Iterable[str],
    file_prefix: str,
    logger: Optional[logging.Logger] = None,
) -> List[Path]:
    """
    Safely write a single Silver partition.

    Uses atomic write semantics:
    - write to a temp file
    - fsync
    - atomic rename
    """
    logger = logger or logging.getLogger(__name__)

    partition_values = tuple(str(v) for v in partition_values)

    partition_path = build_partition_path(
        silver_dir, partition_key, partition_values
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    output_file = partition_path / f"{file_prefix}.parquet"
    tmp_file = partition_path / f".{file_prefix}.{uuid.uuid4().hex}.tmp"

    logger.info(
        f"[partition] Writing {len(df)} rows to {partition_path}"
    )

    write_parquet(df, tmp_file, logger=logger)

    # Atomic replace
    tmp_file.replace(output_file)

    logger.info(f"[partition] Commit complete: {output_file}")

    return output_file