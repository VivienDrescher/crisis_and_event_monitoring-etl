"""
Unit tests for Bronze metadata enrichment.

These tests verify that:
- Bronze metadata columns are added correctly
- Existing DataFrame columns remain unchanged
- The ingestion timestamp is generated correctly
- Metadata values match the provided inputs
"""

import logging
from zoneinfo import ZoneInfo

import pandas as pd

from src.layer.bronze.metadata import add_bronze_metadata


def test_add_bronze_metadata(monkeypatch):
    # Sample input DataFrame
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

    # Test inputs
    source_name = "test_source"
    source_file = "file.csv"
    bronze_run_id = "run_123"
    timezone = ZoneInfo("UTC")

    # Patch now_iso to return a fixed timestamp for reproducibility
    fixed_time = "2026-02-16T12:00:00+00:00"
    monkeypatch.setattr("src.layer.bronze.metadata.now_iso", lambda tz: fixed_time)

    # Use a test logger
    logger = logging.getLogger("test_logger")
    logger.addHandler(logging.NullHandler())  # prevent output in test

    # Call the function
    result = add_bronze_metadata(
        df, source_name, source_file, bronze_run_id, timezone, logger
    )

    # Check that original columns are unchanged
    pd.testing.assert_frame_equal(result[["col1", "col2"]], df)

    # Check that metadata columns are added
    assert "_bronze_ingested_at" in result.columns
    assert "_source_name" in result.columns
    assert "_source_file" in result.columns
    assert "_bronze_run_id" in result.columns

    # Check values
    assert all(result["_bronze_ingested_at"] == fixed_time)
    assert all(result["_source_name"] == source_name)
    assert all(result["_source_file"] == source_file)
    assert all(result["_bronze_run_id"] == bronze_run_id)
