# bronze & silver 
def validate_required_columns(df, required_columns, logger):
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")
    logger.info("Column validation successful")

# silver 
def validate_not_null(df, required_columns, logger):
    nulls = df[required_columns].isna().any()
    failing = nulls[nulls].index.tolist()
    if failing:
        logger.error(f"Null values found in required columns: {failing}")
        raise ValueError(f"Null values found in required columns: {failing}")
    logger.info("Non-NULL validation successfull")
    
