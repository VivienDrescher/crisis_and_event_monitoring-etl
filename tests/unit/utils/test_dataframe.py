"""
Unit tests for src.utils.dataframe module.

Covers:
- deduplicate
- normalize_strings
- apply_column_renames
"""

import logging
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from src.utils.dataframe import apply_column_renames, deduplicate, normalize_strings


class TestDeduplicate:
    """Tests for the deduplicate function."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_deduplicate_basic(self):
        # Input with duplicates
        df = pd.DataFrame(
            {
                "id": [1, 1, 2],
                "_bronze_ingested_at": [
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 1),
                ],
                "value": [10, 20, 30],
            }
        )

        result = deduplicate(df, primary_keys=["id"], logger=self.logger)
        # Only the latest per id kept
        expected = pd.DataFrame(
            {
                "id": [1, 2],
                "_bronze_ingested_at": [
                    datetime(2026, 1, 2),
                    datetime(2026, 1, 1),
                ],
                "value": [20, 30],
            }
        ).reset_index(drop=True)

        assert_frame_equal(result.reset_index(drop=True), expected)

    def test_no_primary_keys(self):
        df = pd.DataFrame({"id": [1, 2], "_bronze_ingested_at": [1, 2]})
        result = deduplicate(df, primary_keys=[], logger=self.logger)
        assert_frame_equal(result, df)

    def test_missing_timestamp_column(self):
        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        result = deduplicate(df, primary_keys=["id"], logger=self.logger)
        # Should return original df since timestamp column missing
        assert_frame_equal(result, df)


class TestNormalizeStrings:
    """Tests for the normalize_strings function."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_normalize_basic(self):
        df = pd.DataFrame(
            {"col1": pd.Series(["  a ", "b  "], dtype="string"), "col2": [1, 2]}
        )
        result = normalize_strings(df, logger=self.logger)
        expected = pd.DataFrame(
            {"col1": pd.Series(["a", "b"], dtype="string"), "col2": [1, 2]}
        )
        assert_frame_equal(result, expected)

    def test_no_string_columns(self):
        df = pd.DataFrame({"col1": [1, 2]})
        result = normalize_strings(df, logger=self.logger)
        assert_frame_equal(result, df)


class TestApplyColumnRenames:
    """Tests for the apply_column_renames function."""

    def test_basic_rename(self):
        df = pd.DataFrame({"old_col": [1, 2], "keep_col": [3, 4]})
        schema = {"new_col": {"source": "old_col"}}
        result = apply_column_renames(df, schema)
        expected = pd.DataFrame({"new_col": [1, 2], "keep_col": [3, 4]})
        assert_frame_equal(result, expected)

    def test_missing_source_column(self):
        df = pd.DataFrame({"keep_col": [1, 2]})
        schema = {"new_col": {"source": "nonexistent"}}
        result = apply_column_renames(df, schema)
        # Should leave df unchanged
        assert_frame_equal(result, df)
