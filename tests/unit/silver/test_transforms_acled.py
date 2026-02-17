"""
Unit tests for the ACLED custom Silver transformation.

The `build` function is responsible for:
    - Creating a `month_start_date` column
    - Converting MONTH (full month name) + YEAR into
        a timezone-aware UTC datetime representing
        the first day of the month.

These tests validate:
    - Correct month/year conversion
    - Behavior when required columns are missing
    - Handling of invalid month values
"""

import logging

import pandas as pd
from pandas.testing import assert_frame_equal

from src.layer.silver.transforms.custom.acled import build


class TestCustomACLEDTransform:
    def test_valid_month_and_year_creates_month_start_date(self):
        df = pd.DataFrame(
            {
                "MONTH": ["January", "February", "March"],
                "YEAR": [2026, 2026, 2026],
                "value": [10, 20, 30],
            }
        )

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(df, logger)

        # Original columns preserved
        assert all(col in result.columns for col in ["MONTH", "YEAR", "value"])

        # New column created
        assert "month_start_date" in result.columns

        expected_dates = pd.to_datetime(
            ["2026-01-01", "2026-02-01", "2026-03-01"],
            utc=True,
        )

        assert all(result["month_start_date"] == expected_dates)

    def test_missing_required_columns_returns_original_df(self):
        df = pd.DataFrame({"MONTH": ["January", "February"]})

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(df, logger)

        assert_frame_equal(result, df)

    def test_invalid_month_results_in_nat(self):
        df = pd.DataFrame(
            {
                "MONTH": ["Foo", "Bar"],
                "YEAR": [2026, 2026],
            }
        )

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(df, logger)

        assert "month_start_date" in result.columns
        assert result["month_start_date"].isna().all()
