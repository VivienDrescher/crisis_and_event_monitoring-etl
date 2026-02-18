import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from src.utils.data_quality_checks import (
    check_column_types,
    check_data_recency,
    check_date_range_coverage,
    check_extra_columns,
    check_no_future_dates,
    check_non_nullable_columns,
    check_not_negative,
    check_partition_keys_exist,
    check_primary_key_uniqueness,
    check_primary_keys_exist,
    check_record_timestamp,
    check_required_columns,
    check_string_format,
    check_valid_categories,
    check_value_range,
)

# -----------------------------------------
# Fixtures
# -----------------------------------------


@pytest.fixture
def logger():
    return logging.getLogger("test_logger")


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "category": ["A", "B", "A"],
            "user_id": ["user_001", "user_002", "user_003"],
            "timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03"]
            ).tz_localize("UTC"),
        }
    )


# -----------------------------------------
# Critical schema checks
# -----------------------------------------


def test_required_columns_pass(sample_df, logger):
    check_required_columns(sample_df, ["id", "value"], logger)


def test_required_columns_fail(sample_df, logger):
    with pytest.raises(ValueError):
        check_required_columns(sample_df, ["missing_col"], logger)


def test_non_nullable_columns_fail(sample_df, logger):
    df = sample_df.copy()
    df.loc[0, "value"] = None
    with pytest.raises(ValueError):
        check_non_nullable_columns(df, ["value"], logger)


def test_primary_key_uniqueness_fail(sample_df, logger):
    df = sample_df.copy()
    df.loc[2, "id"] = 1
    with pytest.raises(ValueError):
        check_primary_key_uniqueness(df, ["id"], logger)


def test_primary_keys_exist_fail(sample_df, logger):
    with pytest.raises(ValueError):
        check_primary_keys_exist(sample_df, ["missing_pk"], logger)


def test_partition_keys_exist_fail(sample_df, logger):
    with pytest.raises(ValueError):
        check_partition_keys_exist(sample_df, ["missing_partition"], logger)


# -----------------------------------------
# Type & schema monitoring (warnings)
# -----------------------------------------


def test_check_column_types_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_column_types(sample_df, {"id": "float"}, logger)

    assert "Column type mismatches detected" in caplog.text


def test_check_extra_columns_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_extra_columns(sample_df, ["id"], logger)

    assert "Extra columns found" in caplog.text


def test_check_record_timestamp_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_record_timestamp(sample_df, "missing_timestamp", logger)

    assert "Record timestamp column" in caplog.text


# -----------------------------------------
# Content checks (warn only)
# -----------------------------------------


def test_check_value_range_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_value_range(sample_df, "value", 15, 25, logger)

    assert "out of range" in caplog.text


def test_check_valid_categories_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_valid_categories(sample_df, "category", ["A"], logger)

    assert "invalid categories" in caplog.text


def test_check_string_format_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    check_string_format(sample_df, "user_id", r"user_\d{4}", logger)

    assert "not matching format" in caplog.text


def test_check_not_negative_warns(sample_df, logger, caplog):
    df = sample_df.copy()
    df.loc[0, "value"] = -5

    caplog.set_level(logging.WARNING)

    check_not_negative(df, "value", logger)

    assert "negative values" in caplog.text


# -----------------------------------------
# Temporal checks
# -----------------------------------------


def test_check_date_range_coverage_warns(sample_df, logger, caplog):
    caplog.set_level(logging.WARNING)

    # Missing 2024-01-04
    check_date_range_coverage(
        sample_df,
        "timestamp",
        datetime(2024, 1, 1),
        datetime(2024, 1, 4),
        logger,
    )

    assert "Missing dates" in caplog.text


def test_check_no_future_dates_warns(sample_df, logger, caplog):
    df = sample_df.copy()
    df.loc[0, "timestamp"] = datetime.now(timezone.utc) + timedelta(days=1)

    caplog.set_level(logging.WARNING)

    check_no_future_dates(df, "timestamp", logger)

    assert "future timestamps" in caplog.text


def test_check_data_recency_warns(sample_df, logger, caplog):
    df = sample_df.copy()

    caplog.set_level(logging.WARNING)

    check_data_recency(
        df,
        "timestamp",
        allowed_interval=timedelta(days=1),
        logger=logger,
    )

    assert "stale" in caplog.text
