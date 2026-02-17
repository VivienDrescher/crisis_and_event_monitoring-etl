"""
Unit tests for ACLED monthly gold aggregation.

These tests verify that:
- Monthly metrics are correctly aggregated from the ACLED silver table
- The function raises an error when the required input table is missing
"""

import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.layer.gold.transforms.custom.acled_monthly import build


class TestACLEDMonthlyBuild:
    """
    Tests for the ACLED monthly aggregation logic.
    """

    def test_build_aggregates_monthly_events_correctly(self):
        df_acled = pd.DataFrame(
            {
                "month_start_date": pd.to_datetime(
                    ["2026-01-01", "2026-01-01", "2026-02-01"],
                    utc=True,
                ),
                "num_events": [5, 7, 3],
            }
        )

        dfs = {"acled": df_acled}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(dfs, logger)

        expected = pd.DataFrame(
            {
                "month_start_date": pd.to_datetime(
                    ["2026-01-01", "2026-02-01"],
                    utc=True,
                ),
                "total_events": [12, 3],
            }
        )

        assert_frame_equal(
            result.sort_values("month_start_date").reset_index(drop=True),
            expected.sort_values("month_start_date").reset_index(drop=True),
        )

    def test_build_raises_error_if_acled_missing(self):
        dfs = {}  # missing required table

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        with pytest.raises(ValueError, match="Input 'acled' table not found"):
            build(dfs, logger)
