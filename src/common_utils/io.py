from __future__ import annotations

from pathlib import Path
from typing import Optional, Union
import pandas as pd
import logging
import zipfile
import requests


def download_file_from_url(
    url: str,
    target_path: Path,
    timeout: int = 60,
    logger: Optional[logging.Logger] = None
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
            f"[download_and_extract] File not found at URL (404). Skipping download: {url}"
        )
        return False
    response.raise_for_status()

    target_path.write_bytes(response.content)
    logger.info(f"[download_and_extract_file] Download complete: {target_path}")

    return True


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
                    f for f in z.namelist()
                    if f.lower().endswith((".xlsx", ".xls"))
                ]
                if not excel_files:
                    raise ValueError(f"No Excel file found in ZIP: {file_path}")

                temp_dir = file_path.parent / f".tmp_extract_{file_path.stem}"
                temp_dir.mkdir(exist_ok=True)

                file_path = temp_dir / Path(excel_files[0]).name
                z.extract(excel_files[0], path=temp_dir)

                logger.info(f"[read_pipeline_source] Extracted {excel_files[0]} from ZIP")
    
        return pd.read_excel(file_path, engine=engine, **reader_params)

    else:
        raise ValueError(f"[read_source_table] Unsupported file type: {file_type}")