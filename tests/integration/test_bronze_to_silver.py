import pandas as pd

from src.layer.silver.run import process_bronze_to_silver


def test_bronze_to_silver(tmp_path):
    # Create a sample bronze parquet
    df_bronze = pd.DataFrame(
        {
            "event_id": [1, 2],
            "num_mentions": [5, 10],
            "_run_id": ["run_123", "run_123"],
            "_bronze_ingested_at": ["2026-01-01", "2026-01-01"],
        }
    )
    bronze_file = tmp_path / "bronze.parquet"
    df_bronze.to_parquet(bronze_file)

    result, processed = process_bronze_to_silver(
        files=[bronze_file],
        table_name="gdelt",
        silver_schema={
            "columns": {
                "event_id": {"type": "int64", "nullable": False, "primary_key": True},
                "num_mentions": {"type": "int64", "nullable": False},
            },
            "record_timestamp": None,
        },
        run_id="run_123",
    )

    assert "num_mentions" in result.columns
    assert "_silver_run_id" in result.columns
    assert result.shape[0] == 2
