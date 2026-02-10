from __future__ import annotations

from pathlib import Path
from typing import Union


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