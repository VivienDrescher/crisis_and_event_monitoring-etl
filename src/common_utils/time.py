from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List, Literal


def utc_now_iso() -> str:
    """
    Return the current UTC timestamp as an ISO-8601 string.

    Example:
        '2026-01-27T04:12:33.421089+00:00'

    Used for ingestion / transformation metadata where a
    timezone-safe, human-readable format is required.
    """
    return datetime.now(timezone.utc).isoformat()


def get_date_range(
    start_date: datetime,
    end_date: datetime,
    step: timedelta = timedelta(days=1),
    output_format: Literal["datetime", "iso", "iso_tuple"] = "datetime",
) -> List:
    """
    Generate a UTC-normalized date range (inclusive).

    Args:
        start_date: timezone-aware datetime
        end_date: timezone-aware datetime
        step: increment (default: 1 day)
        output_format:
            - "datetime"  → List[datetime]
            - "iso"       → List[str] (UTC ISO format)
            - "iso_tuple" → List[Tuple[str]] (for SQL parameter binding)

    Returns:
        List of values depending on output format.
    """
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("start_date and end_date must be timezone-aware")

    results = []
    current = start_date

    while current <= end_date:
        dt_utc = current.astimezone(timezone.utc)

        if output_format == "datetime":
            results.append(dt_utc)
        elif output_format == "iso":
            results.append(dt_utc.isoformat(sep=" ", timespec="seconds"))
        elif output_format == "iso_tuple":
            results.append((dt_utc.isoformat(sep=" ", timespec="seconds"),))
        else:
            raise ValueError(f"Unknown output mode: {output}")

        current += step

    return results