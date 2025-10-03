from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from jqsys.storage.bronze import BronzeStorage


class TestBronzeStorage:
    def test_init_creates_base_directory(self, tmp_path):
        base_path = tmp_path / "bronze_test"
        storage = BronzeStorage(base_path)

        assert storage.base_path == base_path
        assert base_path.exists()
        assert base_path.is_dir()

    def test_init_with_existing_directory(self, tmp_path):
        base_path = tmp_path / "existing_bronze"
        base_path.mkdir(parents=True)

        storage = BronzeStorage(base_path)
        assert storage.base_path == base_path

    def test_store_raw_response_basic(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        sample_data = [
            {"Code": "1301", "Date": "2024-01-15", "Close": 100.0},
            {"Code": "1332", "Date": "2024-01-15", "Close": 200.0},
        ]
        date = datetime(2024, 1, 15)

        file_path = storage.store_raw_response(endpoint="daily_quotes", data=sample_data, date=date)

        expected_path = tmp_path / "bronze" / "daily_quotes" / "date=2024-01-15" / "data.parquet"
        assert file_path == expected_path
        assert file_path.exists()

        # Verify data was stored correctly
        df = pl.read_parquet(file_path)
        assert len(df) == 2
        assert "Code" in df.columns
        assert "_endpoint" in df.columns
        assert "_partition_date" in df.columns
        assert "_ingested_at" in df.columns

    def test_store_raw_response_with_metadata(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        sample_data = [{"Code": "1301", "Close": 100.0}]
        date = datetime(2024, 1, 15)
        metadata = {"source": "test", "record_count": 1}

        file_path = storage.store_raw_response(
            endpoint="daily_quotes", data=sample_data, date=date, metadata=metadata
        )

        df = pl.read_parquet(file_path)
        assert "_metadata" in df.columns
        stored_metadata = json.loads(df["_metadata"][0])
        assert stored_metadata == metadata

    def test_store_empty_data(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")
        date = datetime(2024, 1, 15)

        with patch("jqsys.storage.bronze.logger") as mock_logger:
            file_path = storage.store_raw_response(endpoint="daily_quotes", data=[], date=date)

            expected_path = (
                tmp_path / "bronze" / "daily_quotes" / "date=2024-01-15" / "empty.parquet"
            )
            assert file_path == expected_path
            mock_logger.warning.assert_called_once()

    def test_store_raw_response_exception_handling(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Test with data structure that will cause issues
        date = datetime(2024, 1, 15)

        with patch("jqsys.storage.bronze.pl.DataFrame") as mock_df:
            mock_df.side_effect = Exception("DataFrame creation failed")

            with patch("jqsys.storage.bronze.logger") as mock_logger:
                with pytest.raises(Exception):
                    storage.store_raw_response(
                        endpoint="daily_quotes", data=[{"Code": "1301", "Close": 100.0}], date=date
                    )
                mock_logger.error.assert_called()

    def test_read_raw_data_single_date(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store test data
        sample_data = [
            {"Code": "1301", "Date": "2024-01-15", "Close": 100.0},
            {"Code": "1332", "Date": "2024-01-15", "Close": 200.0},
        ]
        date = datetime(2024, 1, 15)
        storage.store_raw_response("daily_quotes", sample_data, date)

        # Read it back
        df = storage.read_raw_data("daily_quotes", date=date)

        assert len(df) == 2
        assert df.filter(pl.col("Code") == "1301").height == 1
        assert df.filter(pl.col("Code") == "1332").height == 1

    def test_read_raw_data_date_range(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16), datetime(2024, 1, 17)]
        for i, date in enumerate(dates):
            sample_data = [{"Code": "1301", "Close": 100.0 + i}]
            storage.store_raw_response("daily_quotes", sample_data, date)

        # Read date range
        df = storage.read_raw_data(
            "daily_quotes", date_range=(datetime(2024, 1, 15), datetime(2024, 1, 16))
        )

        assert len(df) == 2  # Should include 15th and 16th only

    def test_read_raw_data_all_dates(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16)]
        for date in dates:
            sample_data = [{"Code": "1301", "Close": 100.0}]
            storage.store_raw_response("daily_quotes", sample_data, date)

        # Read all data (no date filter)
        df = storage.read_raw_data("daily_quotes")

        assert len(df) == 2

    def test_read_raw_data_nonexistent_endpoint(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        with pytest.raises(FileNotFoundError, match="No data found for endpoint"):
            storage.read_raw_data("nonexistent_endpoint")

    def test_read_raw_data_date_and_range_error(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        with pytest.raises(ValueError, match="Cannot specify both date and date_range"):
            storage.read_raw_data(
                "daily_quotes",
                date=datetime(2024, 1, 15),
                date_range=(datetime(2024, 1, 15), datetime(2024, 1, 16)),
            )

    def test_read_raw_data_no_matching_files(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Create endpoint directory but no data files
        endpoint_dir = tmp_path / "bronze" / "daily_quotes"
        endpoint_dir.mkdir(parents=True)

        df = storage.read_raw_data("daily_quotes")
        assert df.is_empty()

    def test_list_available_dates(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 17), datetime(2024, 1, 16)]
        for date in dates:
            sample_data = [{"Code": "1301", "Close": 100.0}]
            storage.store_raw_response("daily_quotes", sample_data, date)

        available_dates = storage.list_available_dates("daily_quotes")

        # Should be sorted ascending
        expected_dates = sorted(dates)
        assert available_dates == expected_dates

    def test_list_available_dates_nonexistent_endpoint(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        dates = storage.list_available_dates("nonexistent_endpoint")
        assert dates == []

    def test_list_available_dates_invalid_directory_names(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Create some invalid directory names
        endpoint_dir = tmp_path / "bronze" / "daily_quotes"
        endpoint_dir.mkdir(parents=True)

        # Create valid and invalid date directories
        (endpoint_dir / "date=2024-01-15").mkdir()
        (endpoint_dir / "date=2024-01-15" / "data.parquet").write_text("dummy")

        (endpoint_dir / "invalid_dir").mkdir()
        (endpoint_dir / "date=invalid-date").mkdir()

        dates = storage.list_available_dates("daily_quotes")

        assert len(dates) == 1
        assert dates[0] == datetime(2024, 1, 15)

    def test_get_storage_stats(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store some test data
        sample_data = [{"Code": "1301", "Close": 100.0}]
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16)]

        for date in dates:
            storage.store_raw_response("daily_quotes", sample_data, date)
            storage.store_raw_response("listed_info", sample_data, date)

        stats = storage.get_storage_stats()

        assert "endpoints" in stats
        assert "total_files" in stats
        assert "total_size_mb" in stats

        assert "daily_quotes" in stats["endpoints"]
        assert "listed_info" in stats["endpoints"]

        # Each endpoint should have 2 dates
        assert stats["endpoints"]["daily_quotes"]["dates"] == 2
        assert stats["endpoints"]["listed_info"]["dates"] == 2

        # Total should be 4 files
        assert stats["total_files"] == 4

    def test_get_storage_stats_empty_storage(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        stats = storage.get_storage_stats()

        assert stats == {"endpoints": {}, "total_files": 0, "total_size_mb": 0}

    def test_read_raw_data_invalid_date_directories(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Create endpoint with invalid date directories
        endpoint_dir = tmp_path / "bronze" / "daily_quotes"
        endpoint_dir.mkdir(parents=True)

        # Create invalid date directory without actual parquet file
        invalid_dir = endpoint_dir / "date=invalid-format"
        invalid_dir.mkdir()

        with patch("jqsys.storage.bronze.logger"):
            df = storage.read_raw_data("daily_quotes")

            assert df.is_empty()
            # Warning is called from list_available_dates when processing invalid directory names

    def test_store_and_read_large_dataset(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Create larger dataset
        sample_data = []
        for i in range(1000):
            sample_data.append(
                {
                    "Code": f"130{i % 10}",
                    "Date": "2024-01-15",
                    "Close": 100.0 + i,
                    "Volume": 1000 + i,
                }
            )

        date = datetime(2024, 1, 15)
        storage.store_raw_response("daily_quotes", sample_data, date)

        # Read it back
        df = storage.read_raw_data("daily_quotes", date=date)

        assert len(df) == 1000
        assert df.filter(pl.col("Code") == "1300").height == 100  # Every 10th record

    def test_multiple_files_concatenation(self, tmp_path):
        storage = BronzeStorage(tmp_path / "bronze")

        # Store data across multiple dates - each date gets both codes in one call
        codes = ["1301", "1332"]
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16), datetime(2024, 1, 17)]

        for date in dates:
            # Store both codes for each date in a single response
            sample_data = []
            for code in codes:
                sample_data.append(
                    {"Code": code, "Date": date.strftime("%Y-%m-%d"), "Close": 100.0}
                )
            storage.store_raw_response("daily_quotes", sample_data, date)

        # Read all data
        df = storage.read_raw_data("daily_quotes")

        # Should have 6 records total (2 codes × 3 dates)
        assert len(df) == 6

        # Check unique codes and dates
        unique_codes = df["Code"].unique().sort()
        assert unique_codes.to_list() == ["1301", "1332"]

    def test_pathlib_path_initialization(self, tmp_path):
        # Test initialization with pathlib.Path object
        base_path = Path(tmp_path) / "bronze_pathlib"
        storage = BronzeStorage(base_path)

        assert storage.base_path == base_path
        assert base_path.exists()

    def test_string_path_initialization(self, tmp_path):
        # Test initialization with string path
        base_path_str = str(tmp_path / "bronze_string")
        storage = BronzeStorage(base_path_str)

        assert storage.base_path == Path(base_path_str)
        assert storage.base_path.exists()
