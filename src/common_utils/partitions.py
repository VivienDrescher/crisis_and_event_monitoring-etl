from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd


def build_partition_path(
    base_dir: Path,
    partition_key: List[str],
    partition_values: Tuple[str, ...],
) -> Path:
    """
    Build a Hive-style partition path.

    Example:
        partition_key    = ["event_date", "country"]
        partition_values = ("2024-01-01", "DE")

        -> base_dir/event_date=2024-01-01/country=DE
    """
    if len(partition_key) != len(partition_values):
        raise ValueError(
            "Partition key/value length mismatch: "
            f"{len(partition_key)} keys vs {len(partition_values)} values"
        )

    path = base_dir
    for key, value in zip(partition_key, partition_values):
        path = path / f"{key}={value}"

    return path


def derive_partition_columns(
    df: pd.DataFrame,
    partition_key: List[str],
) -> pd.DataFrame:
    """
    Ensure partition columns exist and normalize them to strings.

    This avoids issues with mixed dtypes (datetime, int, etc.)
    when building directory paths.
    """
    df = df.copy()

    for col in partition_key:
        if col not in df.columns:
            raise ValueError(f"Partition column '{col}' not found in DataFrame")

        df[col] = df[col].astype(str)

    return df