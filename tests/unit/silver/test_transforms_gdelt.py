"""
Unit tests for the GDELT custom silver transformation.

These tests verify:
- Conversion of YYYYMMDD values to UTC datetime
- Handling of invalid dates (coerce to NaT)
- Behavior when 'event_date' column is missing
"""

import pandas as pd

from src.layer.silver.transforms.custom.gdelt import build


class TestGdeltBuild:
    def test_converts_valid_event_date_to_utc_datetime(self):
        """Valid YYYYMMDD values should be converted to UTC datetime."""
        df = pd.DataFrame({"event_date": [20240115, "20240201"]})

        result = build(df)

        # assert pd.api.types.is_datetime64tz_dtype(result["event_date"])
        assert isinstance(result["event_date"].dtype, pd.DatetimeTZDtype)
        assert str(result["event_date"].dtype) == "datetime64[ns, UTC]"

        assert result.loc[0, "event_date"] == pd.Timestamp("2024-01-15", tz="UTC")
        assert result.loc[1, "event_date"] == pd.Timestamp("2024-02-01", tz="UTC")

    def test_invalid_event_date_becomes_nat(self):
        """Invalid YYYYMMDD values should be coerced to NaT."""
        df = pd.DataFrame({"event_date": ["20240115", "invalid_date"]})

        result = build(df)

        assert pd.isna(result.loc[1, "event_date"])
        assert result.loc[0, "event_date"] == pd.Timestamp("2024-01-15", tz="UTC")

    def test_missing_event_date_column_returns_unchanged_df(self):
        """If 'event_date' is missing, dataframe should remain unchanged."""
        df = pd.DataFrame({"some_other_column": [1, 2, 3]})

        result = build(df)

        pd.testing.assert_frame_equal(result, df)
