from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Set
import json


def load_checkpoint(checkpoint_path: Path) -> Set[str]:
    """
    Load a checkpoint file containing already processed filenames.
    """
    if not checkpoint_path.exists():
        return set()

    with open(checkpoint_path) as f:
        data = json.load(f)

    return set(data.get("checkpoint_files", []))


def save_checkpoint(
    checkpoint_path: Path,
    processed_files: Set[str],
) -> None:
    """
    Persist the set of processed filenames to disk.
    """
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "processed_files": sorted(processed_files),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(checkpoint_path, "w") as f:
        json.dump(payload, f, indent=2)


def identify_new_files(
    files: List[Path],
    checkpoint_files: Set[str],
) -> List[Path]:
    """
    Identify files that have not yet been processed.
    """
    return [
        f for f in files
        if f.name not in checkpoint_files
    ]