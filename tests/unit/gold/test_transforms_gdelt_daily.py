"""
Unit tests for GDELT daily metrics builder.

These tests verify that:
- Daily aggregation works correctly
- Metrics are correctly calculated (unique counts, sums, averages)
- Missing input table raises a ValueError
"""

import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.layer.gold.transforms.custom.gdelt_daily import build


class TestGdeltDailyBuild:
    """
    Tests for daily GDELT metrics aggregation.
    """

    def test_successful_aggregation(self):
        # Sample input
        df_gdelt = pd.DataFrame(
            {
                "event_date": ["2026-02-16", "2026-02-16", "2026-02-17"],
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

        # Expected aggregation
        expected = pd.DataFrame(
            {
                "event_date": pd.to_datetime(["2026-02-16", "2026-02-17"]),
                "total_events": [2, 1],  # unique event_ids
                "avg_tone": [1.0, -0.5],  # mean
                "total_mentions": [5, 1],  # sum
                "total_articles": [3, 1],  # sum
            }
        )

        result_sorted = result.sort_values("event_date").reset_index(drop=True)
        expected_sorted = expected.sort_values("event_date").reset_index(drop=True)

        assert_frame_equal(result_sorted, expected_sorted)

    def test_missing_gdelt_raises_error(self):
        dfs = {}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        with pytest.raises(ValueError, match="gdelt"):
            build(dfs, logger)
