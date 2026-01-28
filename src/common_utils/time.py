from __future__ import annotations

from datetime import datetime, timezone


def utc_now_iso() -> str:
    """
    Return the current UTC timestamp as an ISO-8601 string.

    Example:
        '2026-01-27T04:12:33.421089+00:00'

    Used for ingestion / transformation metadata where a
    timezone-safe, human-readable format is required.
    """
    return datetime.now(timezone.utc).isoformat()