import pandas as pd
from datetime import datetime, timezone

# general
def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

# silver 
def parse_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df