from __future__ import annotations

import yaml
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from src.common_utils.time import utc_now_iso


def save_run_metadata(
    run_id: str,
    pipeline_name: str,
    pipeline_timezone: str,
    layer: str,
    source_name: str,
    pipeline_start_date: pd.Timestamp,
    pipeline_end_date: pd.Timestamp,
    log_file: Path,
    processed_files: List[str],
    source_config: Dict[str, Any],
    output_dir: Path
) -> Path:
    """
    Save run-level metadata to a YAML file.

    Args:
        run_id: Unique run ID
        pipeline_name: Name of pipeline
        pipeline_timezone: Timezone of pipeline
        layer: 'bronze' or 'silver'
        source_name: Source being processed
        pipeline_start_date: Execution start date
        pipeline_end_date: Execution end date
        log_file: Path to log file
        processed_files: List of processed files in this run
        source_config: Dict of source configuration
        output_dir: Base directory to store run metadata

    Returns:
        Path to YAML file saved
    """
    runs_dir = output_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    metadata_file = runs_dir / f"run_{run_id}.yaml"
    run_metadata = {
        "run_id": run_id,
        "timestamp_utc": utc_now_iso(),
        "pipeline": {"name": pipeline_name, "timezone": pipeline_timezone},
        "layer": layer,
        "source": source_name,
        "pipeline_start_date": pipeline_start_date.isoformat(),
        "pipeline_end_date": pipeline_end_date.isoformat(),
        "log_file": str(log_file),
        "num_processed_files": len(processed_files),
        "processed_files": processed_files,
        "source_config": source_config,
    }

    with open(metadata_file, "w") as f:
        yaml.dump(run_metadata, f, sort_keys=False)

    return metadata_file