from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Set
import json


def load_checkpoint(checkpoint_path: Path) -> Set[str]:
    """
    Load a checkpoint file containing already processed Bronze filenames.
    """
    if not checkpoint_path.exists():
        return set()

    with open(checkpoint_path) as f:
        data = json.load(f)

    return set(data.get("processed_bronze_files", []))


def save_checkpoint(
    checkpoint_path: Path,
    processed_files: Set[str],
) -> None:
    """
    Persist the set of processed Bronze filenames to disk.
    """
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "processed_bronze_files": sorted(processed_files),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(checkpoint_path, "w") as f:
        json.dump(payload, f, indent=2)


def identify_new_bronze_files(
    bronze_files: List[Path],
    processed_files: Set[str],
) -> List[Path]:
    """
    Identify Bronze files that have not yet been processed.
    """
    return [
        f for f in bronze_files
        if f.name not in processed_files
    ]