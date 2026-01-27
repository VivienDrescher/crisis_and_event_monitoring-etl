import pandas as pd 

# silver 
def normalize_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.select_dtypes(include="string"):
        df[col] = df[col].str.strip()
    return df