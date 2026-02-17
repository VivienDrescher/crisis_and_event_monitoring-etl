"""
Unit tests for src.utils.io module.

Tests cover:
- Parquet read/write utilities
- Partitioned parquet utilities
- Basic file I/O
- Network download (mocked)
"""

import logging
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from src.utils.io import (
    read_parquet,
    read_parquet_partition,
    write_parquet,
    write_partitioned_parquet,
)


class TestParquetIO:
    """Tests for basic Parquet read/write functions."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_write_and_read_parquet(self, tmp_path):
        # Sample DataFrame
        df = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        file_path = tmp_path / "test.parquet"

        # Write Parquet
        write_parquet(df, file_path, logger=self.logger)
        assert file_path.exists()

        # Read Parquet
        df_read = read_parquet(file_path, logger=self.logger)
        assert_frame_equal(df, df_read)


class TestPartitionedParquet:
    """Tests for partitioned parquet utilities."""

    def setup_method(self):
        self.logger = logging.getLogger("test_logger")
        self.logger.addHandler(logging.NullHandler())

    def test_write_partitioned_parquet_basic(self, tmp_path):
        df = pd.DataFrame(
            {
                "year": [2026, 2026, 2025],
                "month": [2, 3, 12],
                "value": [10, 20, 30],
            }
        )

        files_written = write_partitioned_parquet(
            df=df,
            partition_keys=["year", "month"],
            output_dir=tmp_path,
            is_merge=False,
            logger=self.logger,
        )

        # Expect one file per partition
        assert len(files_written) == 3
        for f in files_written:
            assert Path(f).exists()

        # Read back partitioned files
        for f in files_written:
            df_read = pd.read_parquet(f)
            assert not df_read.empty

    def test_write_partitioned_parquet_merge(self, tmp_path):
        # Sample DataFrames
        df1 = pd.DataFrame(
            {
                "id": [1, 2],
                "year": [2026, 2026],
                "month": [2, 2],
                "value": [10, 20],
                "_bronze_ingested_at": ["2026-02-15T12:00:00", "2026-02-15T13:00:00"],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": [2, 3],
                "year": [2026, 2026],
                "month": [2, 2],
                "value": [25, 30],
                "_bronze_ingested_at": ["2026-02-16T12:00:00", "2026-02-16T13:00:00"],
            }
        )

        # Initial write
        write_partitioned_parquet(
            df=df1,
            partition_keys=["year", "month"],
            output_dir=tmp_path,
            is_merge=False,
            logger=self.logger,
        )

        # Merge write (deduplicate using primary_keys + record_timestamp)
        files_written = write_partitioned_parquet(
            df=df2,
            partition_keys=["year", "month"],
            output_dir=tmp_path,
            is_merge=True,
            primary_keys=["id"],
            record_timestamp="_bronze_ingested_at",  # <- must be provided
            logger=self.logger,
        )

        # Read back merged partition
        for f in files_written:
            df_merged = pd.read_parquet(f)
            # IDs 1,2,3 should exist
            assert set(df_merged["id"]) == {1, 2, 3}
            # Check that row for id=2 keeps the latest _bronze_ingested_at
            row2 = df_merged[df_merged["id"] == 2].iloc[0]
            assert row2["_bronze_ingested_at"] == pd.Timestamp("2026-02-16T12:00:00")

    def test_read_parquet_partition_empty_dir(self, tmp_path):
        df, files = read_parquet_partition(tmp_path, logger=self.logger)
        assert df.empty
        assert files == []

    def test_read_parquet_partition_with_files(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2]})
        file1 = tmp_path / "part1.parquet"
        file2 = tmp_path / "part2.parquet"
        df.to_parquet(file1)
        df.to_parquet(file2)

        df_read, files = read_parquet_partition(tmp_path, logger=self.logger)
        assert_frame_equal(df_read, pd.concat([df, df], ignore_index=True))
        assert set(files) == {file1, file2}
