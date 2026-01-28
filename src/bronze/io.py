from __future__ import annotations

from pathlib import Path
from typing import Optional, Union
import pandas as pd
import logging


def read_source_table(
    file_path: Union[str, Path],
    table_params: dict | None = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read a CSV, Excel, or Parquet file using table_params from sources.yaml.

    Args:
        file_path: Path to the file
        table_params: Optional table parameters (may include file_type, dtype, etc.)
        nrows: Number of rows to read
        logger: Optional logger

    Returns:
        pd.DataFrame
    """
    logger = logger or logging.getLogger(__name__)
    table_params = table_params or {}

    file_path = Path(file_path)
    file_type = table_params.get("file_type", file_path.suffix.lower().lstrip(".")).lower()

    params = dict(table_params)
    params.pop("file_type", None)

    logger.info(f"[read_source_table] Reading {file_path} as {file_type}")

    if file_type == "csv":
        params.setdefault("dtype", str)
        params.setdefault("low_memory", False)
        return pd.read_csv(file_path, **params)
    elif file_type in ("xlsx", "xls"):
        return pd.read_excel(file_path, **params)
    elif file_type == "parquet":
        return pd.read_parquet(file_path, **params)
    else:
        raise ValueError(f"[read_source_table] Unsupported file type: {file_type}")


def derive_bronze_output_path(source_path: Union[str, Path], output_suffix=".parquet") -> Path:
    """
    Derive clean Bronze output path by removing known source suffixes and adding the output suffix.

    Args:
        source_path: Original downloaded source file path

    Returns:
        Path to output file with suffix 
    """
    path = Path(source_path)
    for suffix in [".zip", ".csv", ".tsv", ".xlsx", ".xls", ".parquet"]:
        if path.suffix.lower() == suffix:
            path = path.with_suffix("")
    return path.with_suffix(output_suffix)