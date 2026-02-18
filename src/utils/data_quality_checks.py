from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, List

import pandas as pd


# --------------------------
# Schema checks (critical)
# --------------------------
def check_required_columns(
    df: pd.DataFrame, required_columns: List[str], logger: logging.Logger
) -> None:
    """
    Ensure that all specified columns are present in the DataFrame.

    Args:
        df: DataFrame to validate
        required_columns: List of columns that must exist
        logger: Logger for messages

    Raises:
        ValueError: If any required column is missing
    """
    logger = logger or logging.getLogger(__name__)

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"[DQ] Missing required columns: {missing}")
    logger.info("[DQ] Required columns check passed")


def check_non_nullable_columns(
    df: pd.DataFrame, non_nullable_columns: List[str], logger: logging.Logger
) -> None:
    """
    Ensure that the non-nullable columns do not contain null values.

    Args:
        df: DataFrame to validate
        non_nullable_columns: List of columns to check for nulls
        logger: Logger for messages

    Raises:
        ValueError: If any required column contains nulls
    """
    logger = logger or logging.getLogger(__name__)

    nulls = df[non_nullable_columns].isna().any()
    failing = nulls[nulls].index.tolist()
    if failing:
        raise ValueError(f"[DQ] Non-nullable columns contain nulls: {failing}")
    logger.info("[DQ] Non-nullable columns check passed")


def check_primary_key_uniqueness(
    df: pd.DataFrame, primary_keys: List[str], logger: logging.Logger
) -> None:
    """
    Ensure that the combination of primary key columns is unique.

    Args:
        df: DataFrame to validate.
        primary_keys: List of columns defining the primary key.
        logger: Logger for messages.

    Raises:
        ValueError: If duplicate primary key combinations are found.
    """
    logger = logger or logging.getLogger(__name__)

    duplicates = df[df.duplicated(subset=primary_keys, keep=False)]
    if not duplicates.empty:
        raise ValueError(f"[DQ] Duplicate primary key values found:\n{duplicates}")
    logger.info("[DQ] Primary key uniqueness check passed")


def check_column_types(df: pd.DataFrame, schema_dtypes: dict, logger: logging.Logger):
    """
    Verify that DataFrame columns have the expected data types as defined in the schema.

    Notes:
        - Logs a warning for any column whose type does not match the expected type.
        - Does not modify the DataFrame.
        - Supports basic dtype categories: datetime, int, float, object/string.

    Args:
        df: The DataFrame to check.
        schema_dtypes: Dictionary mapping column names to expected data types (e.g., {"col1": "int", "col2": "datetime"}).
        logger: Logger object to record warnings or info messages.

    Returns:
        None. Logs results of the check.
    """
    mismatches = {}
    for col, expected_type in schema_dtypes.items():
        if col not in df.columns:
            continue
        actual_type = str(df[col].dtype)
        if "datetime" in str(
            expected_type
        ).lower() and not pd.api.types.is_datetime64_any_dtype(df[col]):
            mismatches[col] = f"expected datetime, got {actual_type}"
        elif "int" in str(expected_type).lower() and not pd.api.types.is_integer_dtype(
            df[col]
        ):
            mismatches[col] = f"expected int, got {actual_type}"
        elif "float" in str(expected_type).lower() and not pd.api.types.is_float_dtype(
            df[col]
        ):
            mismatches[col] = f"expected float, got {actual_type}"
        elif "object" in str(
            expected_type
        ).lower() and not pd.api.types.is_object_dtype(df[col]):
            mismatches[col] = f"expected object/string, got {actual_type}"

    if mismatches:
        logger.warning(f"[DQ] Column type mismatches detected: {mismatches}")
    else:
        logger.info("[DQ] All column types match schema")


def check_extra_columns(df: pd.DataFrame, schema_columns: list, logger: logging.Logger):
    """
    Identify columns in the DataFrame that are not defined in the schema.

    Notes:
        - Useful for monitoring unexpected upstream changes.
        - Logs a warning if extra columns are found, info if none are present.

    Args:
        df: The DataFrame to check.
        schema_columns: List of expected schema column names.
        logger: Logger object to record warnings or info messages.

    Returns:
        None. Logs results of the check.
    """
    extras = [c for c in df.columns if c not in schema_columns]
    if extras:
        logger.warning(f"[DQ] Extra columns found not defined in schema: {extras}")
    else:
        logger.info("[DQ] No extra columns found")


def check_record_timestamp(
    df: pd.DataFrame, timestamp_col: str, logger: logging.Logger
):
    """
    Verify the presence and non-nullness of the record timestamp column.

    Notes:
        - Checks that the column exists in the DataFrame.
        - Counts null values in the timestamp column and logs a warning if any are found.
        - Does not modify the DataFrame.

    Args:
        df: The DataFrame to check.
        timestamp_col: Name of the column used as the record timestamp.
        logger: Logger object to record warnings or info messages.

    Returns:
        None. Logs results of the check.
    """
    if timestamp_col not in df.columns:
        logger.warning(f"[DQ] Record timestamp column '{timestamp_col}' missing")
        return
    null_count = df[timestamp_col].isna().sum()
    if null_count > 0:
        logger.warning(
            f"[DQ] Record timestamp column '{timestamp_col}' has {null_count} nulls"
        )
    else:
        logger.info("[DQ] Record timestamp column check passed")


def check_primary_keys_exist(
    df: pd.DataFrame,
    primary_keys: list[str],
    logger: logging.Logger,
) -> None:
    """
    Ensure that all specified primary key columns exist in the DataFrame.

    Notes:
        - Critical DQ check for deduplication.
        - Raises an error if any primary key column is missing.

    Args:
        df: DataFrame to validate.
        primary_keys: List of primary key column names.
        logger: Logger object for info messages.

    Raises:
        ValueError: If any primary key column is missing.
    """
    logger = logger or logging.getLogger(__name__)

    missing = [k for k in primary_keys if k not in df.columns]
    if missing:
        raise ValueError(f"[DQ] Missing primary key columns: {missing}")

    logger.info(f"[DQ] All primary key columns exist: {primary_keys}")


def check_partition_keys_exist(
    df: pd.DataFrame,
    partition_keys: list[str],
    logger: logging.Logger,
) -> None:
    """
    Ensure that all specified partition key columns exist in the DataFrame for layers that define partitioning.

    Notes:
        - Raises an error if any partition key column is missing in those layers.

    Args:
        df: DataFrame to validate.
        partition_keys: List of partition key column names.
        logger: Logger object for info messages.

    Raises:
        ValueError: If any partition key column is missing in a partitioned layer.
    """
    logger = logger or logging.getLogger(__name__)

    missing = [k for k in partition_keys if k not in df.columns]
    if missing:
        raise ValueError(f"[DQ] Missing partition key columns': {missing}")

    logger.info(f"[DQ] All partition key columns exist: {partition_keys}")
    return


# --------------------------
# Content checks (warn only)
# --------------------------
def check_value_range(
    df: pd.DataFrame,
    column: str,
    min_value: float,
    max_value: float,
    logger: logging.Logger,
) -> None:
    """
    Check that values in a numeric column fall within a specified range.

    Logs a warning for any values outside the range.

    Args:
        df: DataFrame to validate.
        column: Name of the numeric column.
        min_val: Minimum acceptable value.
        max_val: Maximum acceptable value.
        logger: Logger for warnings.
    """
    logger = logger or logging.getLogger(__name__)

    out_of_range = df[(df[column] < min_value) | (df[column] > max_value)]
    if not out_of_range.empty:
        logger.warning(
            f"[DQ] Column '{column}' has {len(out_of_range)} values out of range ({min_value}-{max_value})"
        )
    else:
        logger.info(f"[DQ] Column '{column}' value range check passed")


def check_valid_categories(
    df: pd.DataFrame, column: str, valid_categories: List[Any], logger: logging.Logger
) -> None:
    """
    Check that all values in a categorical column are in a list of valid categories.

    Logs a warning for any invalid values.

    Args:
        df: DataFrame to validate.
        column: Name of the categorical column.
        valid_categories: List of allowed values.
        logger: Logger for warnings.
    """

    logger = logger or logging.getLogger(__name__)
    invalid = df[~df[column].isin(valid_categories)]
    if not invalid.empty:
        logger.warning(f"[DQ] Column '{column}' has {len(invalid)} invalid categories")
    else:
        logger.info(f"[DQ] Column '{column}' valid categories check passed")


def check_string_format(
    df: pd.DataFrame, column: str, expected_format: str, logger: logging.Logger
) -> None:
    """
    Check that all values in a string column match a given format (regex).

    Args:
        df: DataFrame to check
        column: Name of the column
        expected_format: Regex pattern (use raw string `r"..."`) that values must match
        logger: Logger for warnings / info

    Notes:
        - Uses pandas str.match under the hood.
        - Values that do not match the pattern are logged as warnings.
        - NaN values are ignored in the match.

    Example:
        # Must start with 'user_' followed by exactly 3 digits
        check_string_format(df, "user_id", r"user_\\d{3}", logger)
    """
    logger = logger or logging.getLogger(__name__)

    # simple example: check prefix / suffix / regex could be implemented
    invalid = df[~df[column].str.match(expected_format, na=False)]
    if not invalid.empty:
        logger.warning(
            f"[DQ] Column '{column}' has {len(invalid)} values not matching format '{expected_format}'"
        )
    else:
        logger.info(f"[DQ] Column '{column}' string format check passed")


def check_not_negative(df: pd.DataFrame, column: str, logger: logging.Logger) -> None:
    """
    Check that all values in a numeric column are non-negative.

    Logs a warning for negative values.

    Args:
        df: DataFrame to validate.
        column: Name of the numeric column.
        logger: Logger for warnings.
    """
    logger = logger or logging.getLogger(__name__)

    negative = df[df[column] < 0]
    if not negative.empty:
        logger.warning(f"[DQ] Column '{column}' has {len(negative)} negative values")
    else:
        logger.info(f"[DQ] Column '{column}' non-negative check passed")


# --------------------------
# Temporal DQ checks (warn only)
# --------------------------
def check_date_range_coverage(
    df: pd.DataFrame,
    timestamp_col: str,
    expected_start: datetime,
    expected_end: datetime,
    logger: logging.Logger,
) -> None:
    """
    Check that the timestamp column covers all dates in the expected range (inclusive).

    Args:
        df: DataFrame to check
        timestamp_col: Name of the timestamp column
        expected_start: Start of expected date range (inclusive)
        expected_end: End of expected date range (inclusive)
        logger: Logger for warnings / info

    Notes:
        - Only considers the date part (ignores time of day)
        - Logs a warning if any dates are missing in the middle of the range
    """
    logger = logger or logging.getLogger(__name__)

    # Extract date only
    actual_dates = pd.to_datetime(df[timestamp_col]).dt.normalize().unique()
    actual_dates_set = set(actual_dates)

    # Build full expected date range
    expected_dates = pd.date_range(expected_start, expected_end, freq="D")
    expected_dates_set = set(expected_dates)

    missing_dates = sorted(expected_dates_set - actual_dates_set)

    if missing_dates:
        logger.warning(
            f"[DQ] Timestamp coverage incomplete. Missing dates: {missing_dates}"
        )
    else:
        logger.info(
            f"[DQ] Timestamp coverage check passed: {expected_start.date()} -> {expected_end.date()}"
        )


def check_no_future_dates(
    df: pd.DataFrame, timestamp_col: str, logger: logging.Logger
) -> None:
    """
    Ensure that no values in a timestamp or date column are in the future.

    Logs a warning for any future dates.

    Args:
        df: DataFrame to validate.
        column: Name of the timestamp/date column.
        logger: Logger for warnings.
    """
    logger = logger or logging.getLogger(__name__)

    future_dates = df[df[timestamp_col] > datetime.now(df[timestamp_col].dt.tz)]
    if not future_dates.empty:
        logger.warning(
            f"[DQ] Column '{timestamp_col}' has {len(future_dates)} future timestamps"
        )
    else:
        logger.info(f"[DQ] Column '{timestamp_col}' future date check passed")


def check_data_recency(
    df: pd.DataFrame,
    timestamp_col: str,
    allowed_interval: timedelta,
    logger: logging.Logger,
) -> None:
    """
    Ensure that the latest timestamp in the column is within an allowed interval from now.

    Logs a warning if the most recent data is too old.

    Args:
        df: DataFrame to validate.
        column: Name of the timestamp column.
        allowed_interval: Maximum allowed age of the latest data (timedelta).
        logger: Logger for warnings.
    """
    logger = logger or logging.getLogger(__name__)
    latest = df[timestamp_col].max()
    if latest < datetime.now(df[timestamp_col].dt.tz) - allowed_interval:
        logger.warning(
            f"[DQ] Data in '{timestamp_col}' is stale. Latest {timestamp_col}: {latest}"
        )
    else:
        logger.info(f"[DQ] Data recency check passed. Latest {timestamp_col}: {latest}")
