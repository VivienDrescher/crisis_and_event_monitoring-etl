from pathlib import Path
import pandas as pd

def read_tabular_file(file_path, table_params=None, nrows=None):
    table_params = table_params or {}
    file_type = table_params.get("file_type", file_path.suffix.lower().lstrip(".")).lower()
    params = dict(table_params)
    params.pop("file_type", None)
    if nrows:
        params["nrows"] = nrows

    if file_type == "csv":
        params.setdefault("dtype", str)
        params.setdefault("low_memory", False)
        return pd.read_csv(file_path, **params)
    elif file_type in ("xlsx", "xls"):
        return pd.read_excel(file_path, **params)
    elif file_type == "parquet":
        return pd.read_parquet(file_path, **params)
    else:
        raise ValueError(f"Unsupported file type: {file_path.suffix}")


def write_tabular_file(df, file_path: Path, write_type: str | None = None, write_params: dict | None = None, safe_write: bool = True):
    write_params = write_params or {}
    write_type = (write_type or file_path.suffix.lstrip(".")).lower()
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp") if safe_write else file_path

    if write_type == "csv":
        df.to_csv(tmp_path, index=False, **write_params)
    elif write_type in ("xlsx", "xls"):
        df.to_excel(tmp_path, index=False, **write_params)
    elif write_type == "parquet":
        df.to_parquet(tmp_path, index=False, **write_params)
    else:
        raise ValueError(f"Unsupported write type: {write_type}")

    if safe_write:
        tmp_path.replace(file_path)


def derive_bronze_parquet_path(source_path: Path) -> Path:
    path = source_path
    for suffix in [".zip", ".csv", ".tsv", ".xlsx", ".xls"]:
        if path.suffix.lower() == suffix:
            path = path.with_suffix("")
    return path.with_suffix(".parquet")
