import pandas as pd  

def apply_column_renames(
    df: pd.DataFrame,
    rename_map: dict | None,
    logger=None,
) -> pd.DataFrame:
    if not rename_map:
        return df

    missing = [c for c in rename_map if c not in df.columns]
    if missing and logger:
        logger.warning(f"Rename map contains missing columns: {missing}")

    return df.rename(columns=rename_map)

def enforce_schema(df: pd.DataFrame, schema_dtypes: dict, logger=None) -> pd.DataFrame:
    """
    Cast DataFrame columns to schema dtypes and drop any columns not in schema.
    
    Args:
        df: Input DataFrame
        schema_dtypes: dict mapping column -> dtype
        logger: optional logger
    
    Returns:
        pd.DataFrame with columns casted and extra columns removed
    """
    allowed_cols = list(schema_dtypes.keys())

    # Only keep allowed columns 
    df = df.loc[:, df.columns.intersection(allowed_cols)].copy()
    
    # Cast remaining columns
    for col, dtype in schema_dtypes.items():
        if col not in df.columns:
            if logger:
                logger.warning(f"Column {col} not found in DataFrame, skipping cast")
            continue

        try:
            if "datetime" in str(dtype):
                # Only parse if column is not already datetime
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
            else:
                # Safe numeric / string casting
                df[col] = df[col].astype(dtype)
        except Exception as e:
            raise ValueError(f"Failed casting column '{col}' to {dtype}") from e

    return df
