from __future__ import annotations

from pathlib import Path
from typing import Optional, Union
import pandas as pd
import uuid
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

    file_path = Path(file_path)
    df = pd.read_parquet(file_path, columns=columns, filters=filters)

    logger.info(f"[read_parquet] Read {len(df)} rows with {len(df.columns)} columns from {file_path}")
    return df


def write_parquet(
    df: pd.DataFrame,
    file_path: Union[str, Path],
    write_params: dict = {},
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Write a DataFrame to Parquet with optional atomic safe write.

    Args:
        df: DataFrame to write
        file_path: Target Parquet path
        write_params: Optional pandas.to_parquet parameters
        logger: Optional logger
    """
    logger = logger or logging.getLogger(__name__)
    
    # Determine output paths 
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(file_path.suffix + f".{uuid.uuid4().hex}.tmp")

    # Safe write 
    df.to_parquet(tmp_path, index=False, **write_params)
    tmp_path.replace(file_path)

    logger.info(f"[write_parquet] Safe wrote {len(df)} rows to {file_path}")
