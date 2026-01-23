def validate_required_columns(df, required_columns, logger):
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        logger.error(f"Missing columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")
    logger.info("Column validation successful")
