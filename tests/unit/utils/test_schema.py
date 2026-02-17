"""
Unit tests for src.utils.schema module.

Covers:
- enforce_schema
- validate_required_columns
- validate_columns_not_null
- get_record_timestamp_column

Tests verify correct dtype enforcement, dropping of extra columns,
detection of missing columns, null checks, and timestamp column retrieval.
"""

import logging

import pandas as pd
import pytest

from src.utils.schema import (
    enforce_schema,
    get_record_timestamp_column,
    validate_columns_not_null,
    validate_required_columns,
)


class TestSchemaUtils:
    def test_enforce_schema_basic(self):
        """Should cast columns to schema dtypes and drop extra columns."""
        df = pd.DataFrame(
            {
                "col1": ["1", "2"],
                "col2": ["2026-02-16", "2026-02-17"],
                "extra": [10, 20],
            }
        )

        schema = {
            "col1": "int",
            "col2": "datetime64[ns]",
        }

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = enforce_schema(df, schema, logger)

        # Columns casted correctly
        assert result["col1"].dtype == "int64"
        assert pd.api.types.is_datetime64_any_dtype(result["col2"])
        # Extra column dropped
        assert "extra" not in result.columns

    def test_enforce_schema_missing_column_raises(self):
        """Should raise ValueError if a schema column is missing in the DataFrame."""
        df = pd.DataFrame({"col1": [1, 2]})
        schema = {"col1": "int", "col2": "str"}  # col2 missing

        with pytest.raises(
            ValueError, match="Column 'col2' specified in schema.yaml not found"
        ):
            enforce_schema(df, schema)

    def test_validate_required_columns_success(self):
        """No exception if all required columns exist."""
        df = pd.DataFrame({"a": [1], "b": [2]})
        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())
        validate_required_columns(df, ["a", "b"], logger)

    def test_validate_required_columns_missing_raises(self):
        """Raises ValueError if required columns are missing."""
        df = pd.DataFrame({"a": [1]})
        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_required_columns(df, ["a", "b"], logger)

    def test_validate_columns_not_null_success(self):
        """No exception if non-nullable columns contain no nulls."""
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())
        validate_columns_not_null(df, ["a", "b"], logger)

    def test_validate_columns_not_null_with_nulls_raises(self):
        """Raises ValueError if any non-nullable column contains nulls."""
        df = pd.DataFrame({"a": [1, None], "b": ["x", "y"]})
        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())
        with pytest.raises(ValueError, match="Null values found in required columns"):
            validate_columns_not_null(df, ["a", "b"], logger)

    def test_get_record_timestamp_column_none(self):
        """Returns None if no column is marked as record_timestamp."""
        schema = {"col1": {}, "col2": {}}
        assert get_record_timestamp_column(schema) is None

    def test_get_record_timestamp_column_single(self):
        """Returns column name marked as record_timestamp."""
        schema = {"col1": {"record_timestamp": True}, "col2": {}}
        assert get_record_timestamp_column(schema) == "col1"

    def test_get_record_timestamp_column_multiple_raises(self):
        """Raises ValueError if more than one column marked as record_timestamp."""
        schema = {
            "col1": {"record_timestamp": True},
            "col2": {"record_timestamp": True},
        }
        with pytest.raises(ValueError, match="More than one record_timestamp found"):
            get_record_timestamp_column(schema)
