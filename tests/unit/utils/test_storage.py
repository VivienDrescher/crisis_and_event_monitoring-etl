"""
Unit tests for src.utils.storage module.

Covers:
- clear_data_dir: deleting files and directories
- replace_path_suffix: replacing all suffixes with a new one
- build_partition_path: building Hive-style partition paths

Tests use pytest tmp_path fixture to avoid modifying real files.
"""

import pytest

from src.utils.storage import build_partition_path, clear_data_dir, replace_path_suffix


class TestStorageUtils:
    def test_clear_data_dir_removes_files_and_dirs(self, tmp_path):
        """All files and subdirectories should be deleted."""
        # Setup: create files and subdirs
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.csv"
        file1.write_text("hello")
        file2.write_text("world")
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested")

        # Call function
        clear_data_dir(tmp_path)

        # All contents removed
        assert list(tmp_path.iterdir()) == []

    @pytest.mark.parametrize(
        "input_path, new_suffix, expected",
        [
            ("data.csv", ".parquet", "data.parquet"),
            ("data.csv.gz", ".parquet", "data.parquet"),
            ("folder/data.xlsx.zip", ".parquet", "folder/data.parquet"),
            ("file.parquet.snappy", ".parquet", "file.parquet"),
            ("file.txt", "csv", "file.csv"),  # test missing leading dot
        ],
    )
    def test_replace_path_suffix(self, input_path, new_suffix, expected):
        """Should replace all suffixes with the new suffix."""
        result = replace_path_suffix(input_path, new_suffix)
        assert str(result) == expected

    def test_build_partition_path_basic(self, tmp_path):
        """Builds correct Hive-style path."""
        base_dir = tmp_path
        keys = ["year", "month"]
        values = ("2026", "02")
        result = build_partition_path(base_dir, keys, values)
        expected = base_dir / "year=2026" / "month=02"
        assert result == expected

    def test_build_partition_path_mismatched_lengths_raises(self, tmp_path):
        """Should raise ValueError if keys and values length mismatch."""
        base_dir = tmp_path
        keys = ["year", "month"]
        values = ("2026",)
        with pytest.raises(ValueError, match="Partition key/value length mismatch"):
            build_partition_path(base_dir, keys, values)

    def test_build_partition_path_converts_values_to_str(self, tmp_path):
        """Ensure numeric or datetime values are converted to string."""
        base_dir = tmp_path
        keys = ["year", "day"]
        values = (2026, 16)  # ints
        result = build_partition_path(base_dir, keys, values)
        expected = tmp_path / "year=2026" / "day=16"
        assert result == expected
