"""
Unit tests for src.utils.dataframe module.

Covers:
- deduplicate
- normalize_strings
- apply_column_renames
- cast_to_schema
- get_record_timestamp_column
"""

import logging
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from src.utils.dataframe import (
    apply_column_renames,
    cast_to_schema,
    deduplicate,
    get_record_timestamp_column,
    normalize_strings,
)


class TestDeduplicate:
    """Tests for the deduplicate function."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_deduplicate_basic(self):
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
        expected = pd.DataFrame(
            {
                "id": [1, 2],
                "_bronze_ingested_at": [datetime(2026, 1, 2), datetime(2026, 1, 1)],
                "value": [20, 30],
            }
        ).reset_index(drop=True)
        assert_frame_equal(result.reset_index(drop=True), expected)


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
        assert_frame_equal(result, df)


class TestCastToSchema:
    """Tests for cast_to_schema function."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_cast_basic(self):
        df = pd.DataFrame(
            {
                "int_col": ["1", "2", "3"],
                "float_col": ["1.5", "2.5", "3.5"],
                "datetime_col": ["2026-01-01", "2026-01-02", "2026-01-03"],
                "extra_col": [9, 9, 9],
            }
        )
        schema_dtypes = {
            "int_col": "int",
            "float_col": "float",
            "datetime_col": "datetime",
            "missing_col": "str",
        }
        result = cast_to_schema(df, schema_dtypes, logger=self.logger)
        # Check dtypes
        assert pd.api.types.is_integer_dtype(result["int_col"])
        assert pd.api.types.is_float_dtype(result["float_col"])
        assert pd.api.types.is_datetime64_any_dtype(result["datetime_col"])
        # Check missing column added
        assert "missing_col" in result.columns
        # Check extra column dropped
        assert "extra_col" not in result.columns


class TestGetRecordTimestampColumn:
    """Tests for get_record_timestamp_column function."""

    def test_no_timestamp(self):
        schema = {"col1": {}, "col2": {"record_timestamp": False}}
        assert get_record_timestamp_column(schema) is None

    def test_single_timestamp(self):
        schema = {"col1": {}, "col2": {"record_timestamp": True}}
        assert get_record_timestamp_column(schema) == "col2"

    def test_multiple_timestamps(self):
        schema = {
            "col1": {"record_timestamp": True},
            "col2": {"record_timestamp": True},
        }
        try:
            get_record_timestamp_column(schema)
        except ValueError as e:
            assert "More than one record_timestamp" in str(e)
        else:
            assert False, "Expected ValueError for multiple record_timestamp columns"
