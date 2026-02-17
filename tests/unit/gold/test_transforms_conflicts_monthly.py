"""
Unit tests for conflicts monthly gold aggregation.

These tests verify that:
- GDELT and ACLED monthly metrics are correctly joined
- Columns are properly renamed to avoid collisions
- A full outer join is performed
- Errors are raised when required inputs are missing
"""

import logging

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.layer.gold.transforms.custom.conflicts_monthly import build


class TestConflictsMonthlyBuild:
    """
    Tests for the monthly conflict report builder.
    """

    def test_successful_full_outer_join(self):
        gdelt = pd.DataFrame(
            {
                "month_start_date": ["2026-01-01", "2026-02-01"],
                "total_events": [100, 200],
                "avg_tone": [0.1, 0.2],
            }
        )

        acled = pd.DataFrame(
            {
                "month_start_date": ["2026-02-01", "2026-03-01"],
                "total_events": [5, 7],
            }
        )

        dfs = {
            "gdelt_monthly": gdelt,
            "acled_monthly": acled,
        }

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        result = build(dfs, logger)

        expected = pd.DataFrame(
            {
                "month_start_date": pd.to_datetime(
                    ["2026-01-01", "2026-02-01", "2026-03-01"]
                ),
                "gdelt_total_events": [100.0, 200.0, None],
                "gdelt_avg_tone": [0.1, 0.2, None],
                "acled_total_events": [None, 5.0, 7.0],
            }
        )

        result_sorted = result.sort_values("month_start_date").reset_index(drop=True)
        expected_sorted = expected.sort_values("month_start_date").reset_index(
            drop=True
        )

        assert_frame_equal(result_sorted, expected_sorted)

    def test_missing_acled_raises_error(self):
        dfs = {"gdelt_monthly": pd.DataFrame()}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        with pytest.raises(ValueError, match="acled_monthly"):
            build(dfs, logger)

    def test_missing_gdelt_raises_error(self):
        dfs = {"acled_monthly": pd.DataFrame()}

        logger = logging.getLogger("test_logger")
        logger.addHandler(logging.NullHandler())

        with pytest.raises(ValueError, match="gdelt_monthly"):
            build(dfs, logger)
