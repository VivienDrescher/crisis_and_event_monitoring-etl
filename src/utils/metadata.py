import yaml 
from pathlib import Path
from .dates import utc_now_iso

def add_bronze_metadata(df, source, source_file, source_url=None, run_id=None):
    df = df.copy()
    df["_ingested_at"] = utc_now_iso()
    df["_source"] = source
    df["_source_file"] = source_file
    if source_url:
        df["_source_url"] = source_url
    if run_id:
        df["_run_id"] = run_id
    return df

def save_run_metadata(
    run_id,
    pipeline_name,
    pipeline_timezone,
    source_name,
    start_date,
    end_date,
    log_file,
    downloaded_files,
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
        "source": source_name,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "log_file": str(log_file),
        "num_downloaded_files": len(downloaded_files),
        "downloaded_files": downloaded_files,
        "source_config": source_config,
    }

    with open(metadata_file, "w") as f:
        yaml.dump(run_metadata, f, sort_keys=False)

    return metadata_file