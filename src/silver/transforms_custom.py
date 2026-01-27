from typing import Callable
import pandas as pd

def transform_gdelt(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Example: derive event_at from int YYYYMMDD
    if "event_at" in df.columns:
        df["event_at"] = pd.to_datetime(
            df["event_at"].astype(str),
            format="%Y%m%d",
            errors="coerce",
            utc=True
        )

    return df

CUSTOM_TRANSFORMS: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {
    "gdelt": {
        "name": "gdelt_custom_transforms_v1", 
        "function": transform_gdelt,
    }
}
