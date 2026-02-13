from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

from src.layer.bronze.metadata import add_bronze_metadata
from src.utils.io import read_tabular_file, write_parquet
from src.utils.schema_validation import validate_required_columns


def process_bronze_file(
    input_path: Path,
    output_path: Path,
    table_name: str,
    source_config: Dict,
    bronze_schema: Dict,
    run_id: str,
    timezone: ZoneInfo,
    logger: Optional[logging.Logger] = None,
) -> Path:
    """
    Process a single Bronze source file:

    1. Read the source file (CSV, Parquet, Excel, etc.)
    2. Validate that required columns exist
    3. Add Bronze metadata columns
    4. Write to Parquet safely

    Args:
        input_path: Path to the source file
        output_path: Target Parquet output path
        table_name: Name of the table (e.g., 'gdelt')
        source_config: Dict with source file config (file_type, compression, reader params)
        bronze_schema: Dict with schema info (required columns, etc.)
        run_id: Current pipeline run ID
        timezone: Timezone to use for metadata
        logger: Optional logger for informational messages

    Returns:
        Path to the written Parquet file
    """
    logger = logger or logging.getLogger(__name__)

    file_type = source_config.get("file_type", "csv")
    compression = source_config.get("compression")
    reader_params = source_config.get("reader", {})

    # Read source file
    df = read_tabular_file(input_path, file_type, compression, reader_params, logger)

    # Validate required columns
    required_columns = bronze_schema.get("required_columns", [])
    validate_required_columns(df, required_columns, logger)

    # Add Bronze metadata
    df = add_bronze_metadata(
        df,
        source_name=table_name,
        source_file=input_path.name,
        bronze_run_id=run_id,
        timezone=timezone,
        logger=logger,
    )

    # Write Parquet safely
    write_parquet(df, output_path, logger=logger)

    return output_path
