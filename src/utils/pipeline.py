from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import List, Set
from zoneinfo import ZoneInfo

import yaml

from src.utils.system import get_git_commit, now_iso

# --------------------------
# Checkointing Utils
# --------------------------


def load_checkpoint(checkpoint_path: Path) -> Set[str]:
    """
    Load a checkpoint file containing already processed filenames.

    Args:
        checkpoint_path: Path to the checkpoint JSON file

    Returns:
        Set of processed filenames
    """
    if not checkpoint_path.exists():
        return set()

    with open(checkpoint_path) as f:
        data = json.load(f)

    return set(data.get("checkpoint_files", []))


def save_checkpoint(
    checkpoint_path: Path,
    processed_files: Set[str],
    timezone: ZoneInfo,
) -> None:
    """
    Persist the set of processed filenames to disk as a JSON file.

    Args:
        checkpoint_path: Path to save the checkpoint file
        processed_files: Set of filenames that have been processed
        timezone: Timezone to use for update timestamp of the checkpoint file
    """
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "checkpoint_files": sorted(processed_files),
        "updated_at": now_iso(timezone),
    }

    with open(checkpoint_path, "w") as f:
        json.dump(payload, f, indent=2)


def identify_new_files(
    files: List[Path],
    checkpoint_files: Set[str],
) -> List[Path]:
    """
    Identify files that have not yet been processed.

    Args:
        files: List of file paths to check
        checkpoint_files: Set of filenames that have already been processed

    Returns:
        List of file paths that are new (not in checkpoint_files)
    """
    return [f for f in files if str(f) not in checkpoint_files]


# --------------------------
# Metadata Utils
# --------------------------


def save_run_metadata(
    run_output_dir: Path,
    run_id: str,
    layer: str,
    table_name: str,
    log_file: Path,
    pipeline_config: dict,
    schema_config: dict,
    input_files: list[str],
    output_files: list[str],
    source_configs: dict,
    start_time: datetime,
    end_time: datetime | None = None,
) -> Path:
    """
    Save run-level metadata for a pipeline execution to a YAML file.

    Timestamps are stored in the timezone of `start_time`.

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
        start_time: Timestamp when the run started (timezone-aware).
        end_time: Timestamp when the run ended. If None, will use current time in start_time timezone.

    Returns:
        Path: Full path to the saved YAML metadata file.
    """
    if start_time.tzinfo is None:
        raise ValueError("start_time must be timezone-aware")

    runs_dir = run_output_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    # Use start_time timezone for end_time if not provided
    if end_time is None:
        end_time = datetime.now(start_time.tzinfo)
    elif end_time.tzinfo is None:
        # convert naive end_time to start_time timezone
        end_time = end_time.replace(tzinfo=start_time.tzinfo)

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
        "start_time": start_time.isoformat(sep=" ", timespec="seconds"),
        "end_time": end_time.isoformat(sep=" ", timespec="seconds"),
        "duration_seconds": duration_seconds,
    }

    with open(metadata_file, "w") as f:
        yaml.dump(run_metadata, f, sort_keys=False)

    return metadata_file
