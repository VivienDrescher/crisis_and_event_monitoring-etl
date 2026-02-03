from __future__ import annotations

from pathlib import Path
import logging
from typing import Optional, List

from src.bronze.io import derive_bronze_output_path, read_source_table
from src.bronze.metadata import add_bronze_metadata
from src.common_utils.schema_validation import validate_required_columns
from src.common_utils.parquet import write_parquet


def process_bronze_file(
    file_path: Path,
    source_name: str,
    file_type: str,
    reader_params: dict,
    required_columns: list[str],
    run_id: str,
    downloaded_files: List[str],
    logger: Optional[logging.Logger] = None,
) -> Path:
    """
    Process a single Bronze source file:
      1. Read source table
      2. Validate required columns
      3. Add Bronze metadata
      4. Write as Parquet (safe write)
      5. Remove original file
      6. Append final parquet path to downloaded_files

    Args:
        file_path: Path to input file
        source_name: Name of the source (e.g., 'gdelt')
        file_type: Type of the source files (e.g. csv)
        reader_params: Source-specific read parameters
        required_columns: List of required columns
        run_id: Current run ID
        downloaded_files: List to append processed parquet file path
        logger: Optional logger

    Returns:
        Path to the written Parquet file
    """
    logger = logger or logging.getLogger(__name__)

    # 1. Read table
    df = read_source_table(file_path, file_type, reader_params, logger)

    # 2. Validate columns
    validate_required_columns(df, required_columns, logger)

    # 3. Add Bronze metadata
    df = add_bronze_metadata(
        df,
        source_name=source_name,
        source_file=file_path.name,
        source_url=None,  # optional for manual_drop
        run_id=run_id,
        logger=logger,
    )

    # 4. Determine target Parquet path
    bronze_output_path = derive_bronze_output_path(file_path)

    # 5. Write as Parquet safely
    write_parquet(df, bronze_output_path, safe_write=True, logger=logger)

    # 6. Remove original file
    file_path.unlink(missing_ok=True)
    logger.info(f"Removed original file {file_path}")

    # 7. Track processed files
    downloaded_files.append(str(bronze_output_path))

    return bronze_output_path