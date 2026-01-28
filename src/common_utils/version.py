from __future__ import annotations

import subprocess


def get_git_commit() -> str:
    """
    Return the current Git commit hash for the repository.

    Returns:
        str: 40-character SHA of the HEAD commit.
             Returns "unknown" if Git is unavailable or an error occurs.

    Notes:
        Useful for versioning pipelines, adding reproducibility metadata,
        or tracking the exact code used for a data processing run.
    """
    try:
        commit_hash = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        return commit_hash
    except Exception:
        return "unknown"