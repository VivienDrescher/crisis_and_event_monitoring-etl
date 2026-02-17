from __future__ import annotations

import logging
import uuid
import zipfile
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

import pandas as pd
import requests
import yaml

from src.utils.dataframe import deduplicate
from src.utils.storage import build_partition_path

# --------------------------
# General I/O Utilities
# --------------------------


def load_yaml(path: str) -> Any:
    """
    Load a YAML file from a local path.

    Args:
        path: Path to the YAML file to load.

    Returns:
        Parsed YAML content as a Python object (dict, list, etc.).
    """
    with open(path) as f:
        return yaml.safe_load(f)


def download_file_from_url(
    url: str,
    target_path: Path,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Download a file from a URL.

    Args:
        url: Remote file URL
        target_path: Local path where file will be saved
        timeout: Timeout for HTTP request (seconds)
        logger: Optional logger. Defaults to module logger.

    Returns:
        Returns False if the file does not exist at the URL (404).
    """
    logger = logger or logging.getLogger(__name__)

    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        logger.info(
            "[download_and_extract] File not found at URL (404). Skipping download."
        )
        return False
    response.raise_for_status()

    target_path.write_bytes(response.content)
    logger.info("[download_and_extract_file] Download complete.")

    return True


# --------------------------
# Tabular File Readers
# --------------------------


def read_tabular_file(
    file_path: Union[str, Path],
    file_type: str,
    compression: Optional[str] = None,
    reader_params: dict | None = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Read a CSV, Parquet, or Excel file (optionally compressed).

    Args:
        file_path: Path to the file
        file_type: "csv", "parquet", "xlsx", or "xls"
        compression: Optional compression type ("zip", "gzip", etc.)
        reader_params: Optional dictionary of reader parameters
        logger: Optional logger

    Returns:
        pd.DataFrame

    Raises:
        ValueError: if the file_type is unsupported
    """
    logger = logger or logging.getLogger(__name__)
    reader_params = reader_params or {}

    file_path = Path(file_path)
    logger.info(f"[read_source_table] Reading {file_path} as {file_type}")

    file_type = file_type.lower()

    # --- CSV ---
    if file_type == "csv":
        reader_params.setdefault("dtype", str)
        reader_params.setdefault("low_memory", False)
        return pd.read_csv(file_path, compression=compression, **reader_params)

    # --- Parquet ---
    elif file_type == "parquet":
        if compression:
            reader_params.setdefault("compression", compression)
        return pd.read_parquet(file_path, **reader_params)

    # --- Excel ---
    elif file_type in ("xlsx", "xls"):
        engine = "openpyxl" if file_type == "xlsx" else "xlrd"

        if compression == "zip":
            with zipfile.ZipFile(file_path, "r") as z:
                excel_files = [
                    f for f in z.namelist() if f.lower().endswith((".xlsx", ".xls"))
                ]
                if not excel_files:
                    raise ValueError(f"No Excel file found in ZIP: {file_path}")

                temp_dir = file_path.parent / f".tmp_extract_{file_path.stem}"
                temp_dir.mkdir(exist_ok=True)

                file_path = temp_dir / Path(excel_files[0]).name
                z.extract(excel_files[0], path=temp_dir)

                logger.info(
                    f"[read_pipeline_source] Extracted {excel_files[0]} from ZIP"
                )

        return pd.read_excel(file_path, engine=engine, **reader_params)

    else:
        raise ValueError(f"[read_source_table] Unsupported file type: {file_type}")


# --------------------------
# Parquet Read/Write Utilities
# --------------------------


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
        logger: Optional logger for informational messages

    Returns:
        pd.DataFrame
    """
    logger = logger or logging.getLogger(__name__)

    file_path = Path(file_path)
    df = pd.read_parquet(file_path, columns=columns, filters=filters)

    logger.info(
        f"[read_parquet] Read {len(df)} rows with {len(df.columns)} columns from {file_path}"
    )
    return df


def write_parquet(
    df: pd.DataFrame,
    file_path: Union[str, Path],
    write_params: dict | None = None,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Write a DataFrame to Parquet with an atomic safe write.

    Args:
        df: DataFrame to write
        file_path: Target Parquet path
        write_params: Optional pandas.to_parquet parameters
        logger: Optional logger for informational messages
    """
    logger = logger or logging.getLogger(__name__)

    # Determine output paths
    file_path = Path(file_path)
    tmp_path = file_path.with_suffix(file_path.suffix + f".{uuid.uuid4().hex}.tmp")

    # Safe write
    df.to_parquet(tmp_path, index=False, **(write_params or {}))
    tmp_path.replace(file_path)

    logger.info(f"[write_parquet] Safe wrote {len(df)} rows to {file_path}")


# --------------------------
# Partitioned Parquet Utilities
# --------------------------


def read_parquet_partition(
    partition_dir: Path,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, List[Path]]:
    """
    Read all Parquet files from a partition directory.

    Returns an empty DataFrame and empty list if the partition does not exist or has no files.

    Args:
        partition_dir: Path to the partition directory
        logger: Optional logger for informational messages

    Returns:
        Tuple containing:
            - Concatenated DataFrame of all Parquet files
            - List of Parquet file paths read
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
    Write a DataFrame to partitioned Parquet files in Hive-style directories.

    Args:
        df: DataFrame to write
        partition_keys: Columns to partition by
        output_dir: Base output directory
        is_merge: If True, merge with existing partition data and deduplicate
        primary_keys: Columns to use for deduplication (required if is_merge=True)
        record_timestamp: Timestamp column for deduplication (required if is_merge=True)
        logger: Optional logger for informational messages

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
                raise ValueError(
                    "primary_keys and record_timestamp must be provided for merge mode"
                )
            df_existing, _ = read_parquet_partition(partition_dir, logger)
            if df_existing is not None and not df_existing.empty:
                df_partition = pd.concat([df_existing, df_partition], ignore_index=True)
                df_partition = deduplicate(
                    df_partition, primary_keys, record_timestamp, logger
                )

        output_file = partition_dir / "part.parquet"
        write_parquet(df_partition, output_file, logger=logger)
        files_written.add(str(output_file))

    return files_written
