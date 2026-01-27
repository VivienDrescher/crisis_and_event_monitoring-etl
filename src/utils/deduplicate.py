# silver 
# Only to be applied when source guarantees PR uniqueness or duplicates are ingestion artifacts
def deduplicate(df, primary_key: list[str]):
    if not primary_key:
        return df
    return df.drop_duplicates(subset=primary_key)
