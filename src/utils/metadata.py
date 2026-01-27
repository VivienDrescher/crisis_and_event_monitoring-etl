import yaml 
import pandas as pd
from pathlib import Path
from src.utils.dates import utc_now_iso
from src.utils.version import get_git_commit


# bronze 
def add_bronze_metadata(df, source_name, source_file, source_url=None, run_id=None):
    df = df.copy()
    df["_ingested_at"] = utc_now_iso()
    df["_source"] = source_name
    df["_source_file"] = source_file
    if source_url:
        df["_source_url"] = source_url
    if run_id:
        df["_run_id"] = run_id

    df["_code_version"] = get_git_commit()

    return df

# silver 
def add_silver_metadata(
    df: pd.DataFrame,
    source_name: str,
    bronze_file:str, 
    bronze_run_id: str,
    silver_run_id: str,
    transform_standard_name: str | None = None,
    transform_custom_name: str | None= None,
) -> pd.DataFrame:
    """
    Add metadata columns to a Silver DataFrame.

    Args:
        df (pd.DataFrame): Input Silver DataFrame
        source_name (str): Name of the source (e.g., 'gdelt')
        bronze_file (str): Bronze file name 
        bronze_run_id (str): Bronze ingestion run ID
        silver_run_id (str): Silver ingestion run ID
        transform_name (str, optional): Name of transformation function or pipeline

    Returns:
        pd.DataFrame: DataFrame enriched with Silver metadata
    """
    df = df.copy()
    
    df["_silver_ingested_at"] = utc_now_iso()
    df["_source"] = source_name
    df["_bronze_file"] = bronze_file
    df["_silver_run_id"] = silver_run_id
    df["_bronze_run_id"] = bronze_run_id
    

    if transform_standard_name:
        df["_transform_standard_name"] = transform_standard_name
    if transform_custom_name: 
        df["_transform_custom_name"] = transform_custom_name

    df["_code_version"] = get_git_commit()

    return df

# general 
def save_run_metadata(
    run_id,
    pipeline_name,
    pipeline_timezone,
    layer, 
    source_name,
    pipeline_start_date,
    pipeline_end_date,
    log_file,
    processed_files,
    source_config,
    output_dir: Path
):
    runs_dir = output_dir / "_runs"
    runs_dir.mkdir(exist_ok=True)

    metadata_file = runs_dir / f"run_{run_id}.yaml"
    run_metadata = {
        "run_id": run_id,
        "timestamp_utc": utc_now_iso(),
        "pipeline": {"name": pipeline_name, "timezone": pipeline_timezone},
        "layer": layer,
        "source": source_name,
        "pipeline_start_date": pipeline_start_date.isoformat(),
        "pipeline_end_date": pipeline_end_date.isoformat(),
        "log_file": str(log_file),
        "num_processed_files": len(processed_files),
        "processed_files": processed_files,
        "source_config": source_config,
    }

    with open(metadata_file, "w") as f:
        yaml.dump(run_metadata, f, sort_keys=False)

    return metadata_file