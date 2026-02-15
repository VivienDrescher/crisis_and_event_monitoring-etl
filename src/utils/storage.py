from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import List, Optional, Tuple, Union


def clear_data_dir(
    data_dir: Path,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Delete all files and subdirectories in the given data directory.

    Args:
        data_dir: Path to the directory to clear
        logger: Optional logger for informational messages
    """

    logger = logger or logging.getLogger(__name__)

    for path in data_dir.glob("*"):
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    logger.info("[Clear directory] Cleared existing data directory")


def replace_path_suffix(
    path: Union[str, Path],
    new_suffix: str,
) -> Path:
    """
    Replace all suffixes of a file path with a new suffix.

    Examples:
        data.csv            -> data.parquet
        data.csv.gz         -> data.parquet
        data.xlsx.zip       -> data.parquet
        data.parquet.snappy -> data.parquet

    Args:
        path: Input file path
        new_suffix: New suffix including leading dot (e.g. ".parquet")

    Returns:
        Path with suffix replaced
    """
    path = Path(path)

    # Strip all suffixes
    base_name = path.stem
    for _ in path.suffixes[1:]:
        base_name = Path(base_name).stem

    if not new_suffix.startswith("."):
        new_suffix = "." + new_suffix

    return path.with_name(base_name + new_suffix)


def build_partition_path(
    base_dir: Path,
    partition_keys: List[str],
    partition_values: Tuple[str, ...],
) -> Path:
    """
    Build a Hive-style partition path.

    Example:
        partition_keys    = ["event_date", "country"]
        partition_values = ("2024-01-01", "DE")
        Resulting path: base_dir/event_date=2024-01-01/country=DE

    Args:
        base_dir: Base directory path
        partition_keys: List of partition column names
        partition_values: Tuple of partition values corresponding to keys

    Returns:
        Full Path object representing the Hive-style partition path

    Raises:
        ValueError: If the number of keys and values do not match
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
