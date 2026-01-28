from __future__ import annotations

from pathlib import Path
from typing import Optional, Union
import pandas as pd
import logging


def read_parquet(
    file_path: Union[str, Path],
    columns: list[str] | None = None,
    filters=None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read a Parquet file into a DataFrame.

    Args:
        file_path: Parquet file path
        columns: Optional subset of columns
        filters: Optional pyarrow-style filters
        logger: Optional logger

    Returns:
        pd.DataFrame
    """
    logger = logger or logging.getLogger(__name__)
    df = pd.read_parquet(file_path, columns=columns, filters=filters)
    logger.info(f"[read_parquet] Read {len(df)} rows with {len(df.columns)} columns")
    return df


def write_parquet(
    df: pd.DataFrame,
    file_path: Union[str, Path],
    write_params: dict | None = None,
    safe_write: bool = True,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Write a DataFrame to Parquet with optional atomic safe write.

    Args:
        df: DataFrame to write
        file_path: Target Parquet path
        write_params: Optional pandas.to_parquet parameters
        safe_write: Use temp file + atomic replace
        logger: Optional logger
    """
    logger = logger or logging.getLogger(__name__)
    write_params = write_params or {}
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp") if safe_write else file_path

    logger.info(f"[write_parquet] Writing {len(df)} rows to {tmp_path}")
    df.to_parquet(tmp_path, index=False, **write_params)

    if safe_write:
        tmp_path.replace(file_path)
        logger.info(f"[write_parquet] Safe write complete: {file_path}")
