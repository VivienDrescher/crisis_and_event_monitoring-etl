"""
Unit tests for src.utils.run module.

Covers:
- load_checkpoint
- save_checkpoint
- identify_new_files
- save_run_metadata

These tests ensure proper checkpoint persistence, new file detection,
and run metadata saving without touching real Git or system time.
"""

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import yaml

from src.utils.run import (
    identify_new_files,
    load_checkpoint,
    save_checkpoint,
    save_run_metadata,
)


class TestCheckpointingUtils:
    def test_load_checkpoint_nonexistent(self, tmp_path):
        """Should return empty set if checkpoint file does not exist."""
        path = tmp_path / "checkpoint.json"
        result = load_checkpoint(path)
        assert result == set()

    def test_load_checkpoint_existing(self, tmp_path):
        """Should return set of checkpoint filenames from JSON file."""
        path = tmp_path / "checkpoint.json"
        data = {"checkpoint_files": ["file1.csv", "file2.csv"]}
        path.write_text(json.dumps(data))

        result = load_checkpoint(path)
        assert result == {"file1.csv", "file2.csv"}

    def test_save_checkpoint_and_load(self, tmp_path, monkeypatch):
        """Should save a checkpoint and be able to reload it."""
        path = tmp_path / "checkpoint.json"
        processed_files = {"fileA.csv", "fileB.csv"}
        timezone = ZoneInfo("UTC")

        # Patch now_iso to fixed timestamp
        monkeypatch.setattr(
            "src.utils.run.now_iso", lambda tz: "2026-02-16T12:00:00+00:00"
        )
        save_checkpoint(path, processed_files, timezone)

        loaded = load_checkpoint(path)
        assert loaded == processed_files

        # Check that updated_at exists
        content = json.loads(path.read_text())
        assert content["updated_at"] == "2026-02-16T12:00:00+00:00"

    def test_identify_new_files(self, tmp_path):
        """Should return only files not present in checkpoint."""
        file1 = tmp_path / "a.csv"
        file2 = tmp_path / "b.csv"
        files = [file1, file2]
        checkpoint_files = {str(file1)}

        new_files = identify_new_files(files, checkpoint_files)
        assert new_files == [file2]

    def test_save_run_metadata_creates_yaml(self, tmp_path, monkeypatch):
        """Should save pipeline run metadata as YAML with correct keys."""
        run_output_dir = tmp_path
        run_id = "run_123"
        layer = "bronze"
        table_name = "table_xyz"
        log_file = tmp_path / "log.txt"
        pipeline_config = {"param": 1}
        schema_config = {"field": "value"}
        input_files = ["in1.csv", "in2.csv"]
        output_files = ["out1.parquet"]
        source_configs = {"source": {"file_type": "csv"}}
        start_time = datetime(2026, 2, 16, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        # Patch get_git_commit to avoid relying on local git
        monkeypatch.setattr("src.utils.run.get_git_commit", lambda: "dummycommit")

        metadata_file = save_run_metadata(
            run_output_dir,
            run_id,
            layer,
            table_name,
            log_file,
            pipeline_config,
            schema_config,
            input_files,
            output_files,
            source_configs,
            start_time,
            end_time=start_time + timedelta(seconds=60),
        )

        assert metadata_file.exists()

        content = yaml.safe_load(metadata_file.read_text())
        assert content["run_id"] == run_id
        assert content["git_commit"] == "dummycommit"
        assert content["layer"] == layer
        assert content["table"] == table_name
        assert content["num_processed_input_files"] == len(input_files)
        assert content["num_processed_output_files"] == len(output_files)
        assert content["duration_seconds"] == 60
