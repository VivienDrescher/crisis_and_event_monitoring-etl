"""
Unit tests for GDELT monthly metrics builder.

These tests verify that:
- Monthly aggregation works correctly
- 'month_start_date' is correctly derived as the first day of each month
- Metrics (unique counts, sums, averages) are computed correctly
- Missing input table raises a ValueError
"""

import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.layer.gold.transforms.custom.gdelt_monthly import build


class TestGdeltMonthlyBuild:
    """
    Tests for monthly GDELT metrics aggregation.
    """

    def test_successful_monthly_aggregation(self):
        # Sample input: events spanning Feb and Mar
        df_gdelt = pd.DataFrame(
            {
                "event_date": ["2026-02-16", "2026-02-20", "2026-03-05"],
                "event_id": [1, 2, 3],
                "avg_tone": [0.5, 1.5, -0.5],
                "num_mentions": [2, 3, 1],
                "num_articles": [1, 2, 1],
            }
        )

        dfs = {"gdelt": df_gdelt}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(dfs, logger)

        # Expected month_start_date: first day of the month
        expected = pd.DataFrame(
            {
                "month_start_date": pd.to_datetime(["2026-02-01", "2026-03-01"]),
                "total_events": [2, 1],  # unique event_ids
                "avg_tone": [1.0, -0.5],  # mean
                "total_mentions": [5, 1],  # sum
                "total_articles": [3, 1],  # sum
            }
        )

        # Sort for consistent comparison
        result_sorted = result.sort_values("month_start_date").reset_index(drop=True)
        expected_sorted = expected.sort_values("month_start_date").reset_index(
            drop=True
        )

        assert_frame_equal(result_sorted, expected_sorted)

    def test_missing_gdelt_table_raises_error(self):
        dfs = {}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        with pytest.raises(ValueError, match="gdelt"):
            build(dfs, logger)
