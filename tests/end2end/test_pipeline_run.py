import os
import subprocess
import sys


def test_full_pipeline(tmp_path):
    # Map all pipeline paths to temporary test directories
    env = {
        "DEBUG": "True",
        "LOG_LEVEL": "DEBUG",
        "DATA_ROOT": str(tmp_path / "data"),
        "LANDING_PATH": str(tmp_path / "data" / "landing"),
        "BRONZE_PATH": str(tmp_path / "data" / "bronze"),
        "SILVER_PATH": str(tmp_path / "data" / "silver"),
        "GOLD_PATH": str(tmp_path / "data" / "gold"),
        "LOG_PATH": str(tmp_path / "logs"),
    }

    result = subprocess.run(
        [sys.executable, "-m", "src.pipeline_orchestrator"],
        check=True,
        env={**os.environ, **env},
    )
    assert result.returncode == 0

    # TODO (Verify output exists, data content, .....)
