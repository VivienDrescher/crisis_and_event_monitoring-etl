from __future__ import annotations

import yaml
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone

from src.common_utils.version import get_git_commit


def save_run_metadata(
    run_output_dir: Path, 
    run_id: str,
    layer: str,
    table_name: str,
    log_file: Path,
    pipeline_config: Dict[str, Any],
    schema_config: Dict[str, Any],
    input_files: List[str],
    output_files: List[str],
    source_configs: Dict[str, Any],
    start_time: datetime,
    end_time: datetime | None = None,
) -> Path:
    """
    Save run-level metadata for a pipeline execution to a YAML file.

    Args:
        run_output_dir: Base directory where the run metadata YAML will be saved.
        run_id: Unique identifier for this pipeline run.
        layer: Data layer being processed (e.g., 'bronze', 'silver', 'gold').
        table_name: Name of the table being processed or written.
        log_file: Path to the log file for this run.
        pipeline_config: Dictionary of pipeline configuration used in this run.
        schema_config: Dictionary of schema configuration used in this run.
        input_files: List of input file paths processed in this run.
        output_files: List of output file paths written in this run.
        source_configs: Dictionary of source configurations used in this run.
        start_time: Timestamp when the run started.
        end_time: Timestamp when the run ended. If None, will use current UTC time.

    Returns:
        Path: Full path to the saved YAML metadata file.
    """
    runs_dir = run_output_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    if end_time is None:
        end_time = datetime.now(timezone.utc)

    duration_seconds = (end_time - start_time).total_seconds()

    metadata_file = runs_dir / f"run_{run_id}.yaml"
    run_metadata = {
        "run_id": run_id,
        "git_commit": get_git_commit(), 
        "layer": layer,
        "table": table_name,
        "log_file": str(log_file),
        "num_processed_input_files": len(input_files),
        "processed_input_files": input_files,
        "num_processed_output_files": len(output_files),
        "processed_output_files": output_files,
        "pipeline_config": pipeline_config,
        "schema_config": schema_config,
        "source_configs": source_configs,
        "start_time": start_time,
        "end_time": end_time,
        "duration_seconds": duration_seconds,
    }

    with open(metadata_file, "w") as f:
        yaml.dump(run_metadata, f, sort_keys=False)

    return metadata_file