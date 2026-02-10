from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List


def utc_now_iso() -> str:
    """
    Return the current UTC timestamp as an ISO-8601 string.

    Example:
        '2026-01-27T04:12:33.421089+00:00'

    Used for ingestion / transformation metadata where a
    timezone-safe, human-readable format is required.
    """
    return datetime.now(timezone.utc).isoformat()


def date_range_utc(start_date, end_date):
    date_range = []
    current = start_date

    while current <= end_date:
        dt = current.astimezone(timezone.utc)
        date_range.append((dt.isoformat(sep=" ", timespec="seconds"),))
        current += timedelta(days=1)

    return date_range


def date_range(
    start_date: datetime,
    end_date: datetime,
    step: timedelta = timedelta(days=1),
) -> List[datetime]:
    """
    Generate a list of timezone-aware UTC datetimes between two dates (inclusive).
    The returned datetimes are normalized to UTC.

    Args:
        start_date: Start datetime (must be timezone-aware)
        end_date: End datetime (must be timezone-aware)
        step: Time delta between values (default: 1 day)

    Returns:
        List of UTC-aware datetime objects, inclusive of start and end date

    Raises:
        ValueError: If start_date or end_date are not timezone-aware
    """
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("start_date and end_date must be timezone-aware")

    dates: List[datetime] = []
    current = start_date

    while current <= end_date:
        dates.append(current.astimezone(timezone.utc))
        current += step

    return dates