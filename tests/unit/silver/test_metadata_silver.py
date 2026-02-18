"""
Unit tests for Silver metadata enrichment.

Tests that:
- Metadata columns are added
- Existing columns remain unchanged
- Timestamps are correctly generated
"""

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.layer.silver.metadata import add_silver_metadata


def test_add_silver_metadata(monkeypatch):
    # Sample input DataFrame
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # Test inputs
    bronze_run_id = "bronze_123"
    bronze_ingested_at = datetime(2026, 2, 16, 12, 0, 0)
    silver_run_id = "silver_456"
    timezone = ZoneInfo("UTC")

    # Patch now_iso in the module where it's used
    fixed_time = "2026-02-16T12:00:00+00:00"
    monkeypatch.setattr("src.layer.silver.metadata.now_iso", lambda tz: fixed_time)

    # Use a null logger
    logger = logging.getLogger("test_logger")
    logger.addHandler(logging.NullHandler())

    # Call the function
    result = add_silver_metadata(
        df, bronze_run_id, bronze_ingested_at, silver_run_id, timezone, logger
    )

    # Original columns unchanged
    pd.testing.assert_frame_equal(result[["col1", "col2"]], df)

    # Check metadata columns exist
    for col in [
        "_silver_ingested_at",
        "_silver_run_id",
        "_bronze_ingested_at",
        "_bronze_run_id",
    ]:
        assert col in result.columns

    # Check values
    assert all(result["_silver_ingested_at"] == fixed_time)
    assert all(result["_silver_run_id"] == silver_run_id)
    assert all(result["_bronze_run_id"] == bronze_run_id)
    assert all(result["_bronze_ingested_at"] == bronze_ingested_at)
