import pandas as pd

from src.utils.schema import apply_column_renames, enforce_schema
from src.utils.strings import normalize_strings
from src.utils.dates import parse_dates
from src.silver.transforms_custom import CUSTOM_TRANSFORMS
from src.utils.deduplicate import deduplicate
from src.utils.validation import validate_not_null, validate_required_columns
from src.utils.metadata import add_silver_metadata


def apply_custom_transform(df: pd.DataFrame, source_name: str):
    entry = CUSTOM_TRANSFORMS.get(source_name)

    if not entry:
        return df, None

    df = entry["function"](df)
    return df, entry["name"]


def process_bronze_to_silver(df, source_name, silver_schema, run_id, bronze_file_name, logger):
    """
    Apply all Silver transformations in canonical order.
    """

    # Read metadata from bronze that is kept in silver
    bronze_run_id = df["_run_id"].iloc[0]
 
    # 1. Rename → canonical schema
    df = apply_column_renames(df, silver_schema.get("rename_columns"), logger)

    # 2. Source-specific logic
    df, transform_custom_name = apply_custom_transform(df, source_name)

    # 3. Normalize strings
    df = normalize_strings(df)

    # 4. Parse event timestamps
    event_col = silver_schema.get("event_time_column")
    if event_col:
        df = parse_dates(df, [event_col])

    # 5. Enforce types
    df = enforce_schema(df, silver_schema.get("dtypes"), logger)
 
    # 6. Add Silver metadata
    df = add_silver_metadata(
        df,
        source_name=source_name,
        bronze_file=bronze_file_name,
        bronze_run_id=bronze_run_id,
        silver_run_id=run_id,
        transform_standard_name=silver_schema["transform_name"],
        transform_custom_name=transform_custom_name
    )

    # 7. Validate schema
    required = silver_schema.get("required_columns", [])
    validate_required_columns(df, required, logger)
    validate_not_null(df, required, logger)

    # 8. Deduplicate
    pk = silver_schema.get("primary_key")
    if pk:
        df = deduplicate(df, pk)

    return df

