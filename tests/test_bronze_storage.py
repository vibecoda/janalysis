from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import patch

import polars as pl
import pytest

from jqsys.storage.backends.filesystem_backend import FilesystemBackend
from jqsys.storage.blob import BlobStorage
from jqsys.storage.bronze import BronzeStorage


@pytest.fixture
def blob_storage(tmp_path):
    """Create a temporary filesystem-based BlobStorage for testing."""
    backend = FilesystemBackend(base_path=str(tmp_path / "test_blobs"))
    return BlobStorage(backend=backend)


class TestBronzeStorage:
    def test_init_creates_base_directory(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        assert bronze.storage == blob_storage
        assert bronze.add_metadata_columns is False

    def test_init_with_existing_directory(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage, add_metadata_columns=True)

        assert bronze.storage == blob_storage
        assert bronze.add_metadata_columns is True

    def test_store_raw_response_basic(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        sample_data = [
            {"Code": "1301", "Date": "2024-01-15", "Close": 100.0},
            {"Code": "1332", "Date": "2024-01-15", "Close": 200.0},
        ]
        date = datetime(2024, 1, 15)

        blob_key = bronze.store_raw_response(endpoint="daily_quotes", data=sample_data, date=date)

        expected_key = "daily_quotes/2024-01-15/data.parquet"
        assert blob_key == expected_key
        assert blob_storage.exists(blob_key)

        # Verify data was stored correctly
        blob_data = blob_storage.get(blob_key)
        from io import BytesIO

        df = pl.read_parquet(BytesIO(blob_data))
        assert len(df) == 2
        assert "Code" in df.columns
        assert "_endpoint" not in df.columns  # metadata columns off by default

    def test_store_raw_response_with_metadata(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage, add_metadata_columns=True)

        sample_data = [{"Code": "1301", "Close": 100.0}]
        date = datetime(2024, 1, 15)
        metadata = {"source": "test", "record_count": 1}

        blob_key = bronze.store_raw_response(
            endpoint="daily_quotes", data=sample_data, date=date, metadata=metadata
        )

        blob_data = blob_storage.get(blob_key)
        from io import BytesIO

        df = pl.read_parquet(BytesIO(blob_data))
        assert "_endpoint" in df.columns
        assert "_partition_date" in df.columns
        assert "_ingested_at" in df.columns
        assert "_metadata" in df.columns
        stored_metadata = json.loads(df["_metadata"][0])
        assert stored_metadata == metadata

    def test_store_empty_data(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)
        date = datetime(2024, 1, 15)

        with patch("jqsys.storage.bronze.logger") as mock_logger:
            blob_key = bronze.store_raw_response(endpoint="daily_quotes", data=[], date=date)

            expected_key = "daily_quotes/2024-01-15/data.parquet"
            assert blob_key == expected_key
            assert blob_storage.exists(blob_key)
            mock_logger.warning.assert_called_once()

    def test_store_raw_response_exception_handling(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Test with data structure that will cause issues
        date = datetime(2024, 1, 15)

        # Mock DataFrame.write_parquet to raise an exception
        with patch("jqsys.storage.bronze.pl.DataFrame.write_parquet") as mock_write:
            mock_write.side_effect = Exception("Write failed")

            with patch("jqsys.storage.bronze.logger") as mock_logger:
                with pytest.raises(Exception):
                    bronze.store_raw_response(
                        endpoint="daily_quotes", data=[{"Code": "1301", "Close": 100.0}], date=date
                    )
                mock_logger.error.assert_called()

    def test_read_raw_data_single_date(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store test data
        sample_data = [
            {"Code": "1301", "Date": "2024-01-15", "Close": 100.0},
            {"Code": "1332", "Date": "2024-01-15", "Close": 200.0},
        ]
        date = datetime(2024, 1, 15)
        bronze.store_raw_response("daily_quotes", sample_data, date)

        # Read it back
        df = bronze.read_raw_data("daily_quotes", date=date)

        assert len(df) == 2
        assert df.filter(pl.col("Code") == "1301").height == 1
        assert df.filter(pl.col("Code") == "1332").height == 1

    def test_read_raw_data_date_range(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16), datetime(2024, 1, 17)]
        for i, date in enumerate(dates):
            sample_data = [{"Code": "1301", "Close": 100.0 + i}]
            bronze.store_raw_response("daily_quotes", sample_data, date)

        # Read date range
        df = bronze.read_raw_data(
            "daily_quotes", date_range=(datetime(2024, 1, 15), datetime(2024, 1, 16))
        )

        assert len(df) == 2  # Should include 15th and 16th only

    def test_read_raw_data_all_dates(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16)]
        for date in dates:
            sample_data = [{"Code": "1301", "Close": 100.0}]
            bronze.store_raw_response("daily_quotes", sample_data, date)

        # Read all data (no date filter)
        df = bronze.read_raw_data("daily_quotes")

        assert len(df) == 2

    def test_read_raw_data_nonexistent_endpoint(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # No data stored, should return empty DataFrame
        df = bronze.read_raw_data("nonexistent_endpoint")
        assert df.is_empty()

    def test_read_raw_data_date_and_range_error(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        with pytest.raises(ValueError, match="Cannot specify both date and date_range"):
            bronze.read_raw_data(
                "daily_quotes",
                date=datetime(2024, 1, 15),
                date_range=(datetime(2024, 1, 15), datetime(2024, 1, 16)),
            )

    def test_read_raw_data_no_matching_files(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # No data stored
        df = bronze.read_raw_data("daily_quotes")
        assert df.is_empty()

    def test_list_available_dates(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 17), datetime(2024, 1, 16)]
        for date in dates:
            sample_data = [{"Code": "1301", "Close": 100.0}]
            bronze.store_raw_response("daily_quotes", sample_data, date)

        available_dates = bronze.list_available_dates("daily_quotes")

        # Should be sorted ascending
        expected_dates = sorted(dates)
        assert available_dates == expected_dates

    def test_list_available_dates_nonexistent_endpoint(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        dates = bronze.list_available_dates("nonexistent_endpoint")
        assert dates == []

    def test_list_available_dates_invalid_directory_names(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store valid data
        sample_data = [{"Code": "1301", "Close": 100.0}]
        bronze.store_raw_response("daily_quotes", sample_data, datetime(2024, 1, 15))

        # Store some invalid blobs (not ending with data.parquet)
        blob_storage.put("daily_quotes/2024-01-16/invalid.txt", b"invalid")
        blob_storage.put("daily_quotes/invalid-date/data.parquet", b"invalid")

        dates = bronze.list_available_dates("daily_quotes")

        # Should only include valid date
        assert len(dates) == 1
        assert dates[0] == datetime(2024, 1, 15)

    def test_get_storage_stats(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store some test data
        sample_data = [{"Code": "1301", "Close": 100.0}]
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16)]

        for date in dates:
            bronze.store_raw_response("daily_quotes", sample_data, date)
            bronze.store_raw_response("listed_info", sample_data, date)

        stats = bronze.get_storage_stats()

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

    def test_get_storage_stats_empty_storage(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        stats = bronze.get_storage_stats()

        assert stats == {"endpoints": {}, "total_files": 0, "total_size_mb": 0}

    def test_read_raw_data_invalid_date_directories(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

        # Store some invalid blobs
        blob_storage.put("daily_quotes/invalid-format/data.parquet", b"invalid")

        with patch("jqsys.storage.bronze.logger"):
            df = bronze.read_raw_data("daily_quotes")

            assert df.is_empty()

    def test_store_and_read_large_dataset(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

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
        bronze.store_raw_response("daily_quotes", sample_data, date)

        # Read it back
        df = bronze.read_raw_data("daily_quotes", date=date)

        assert len(df) == 1000
        assert df.filter(pl.col("Code") == "1300").height == 100  # Every 10th record

    def test_multiple_files_concatenation(self, blob_storage):
        bronze = BronzeStorage(storage=blob_storage)

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
            bronze.store_raw_response("daily_quotes", sample_data, date)

        # Read all data
        df = bronze.read_raw_data("daily_quotes")

        # Should have 6 records total (2 codes × 3 dates)
        assert len(df) == 6

        # Check unique codes and dates
        unique_codes = df["Code"].unique().sort()
        assert unique_codes.to_list() == ["1301", "1332"]

    def test_pathlib_path_initialization(self, tmp_path):
        # Test initialization with BlobStorage using filesystem backend
        backend = FilesystemBackend(base_path=str(tmp_path / "bronze"))
        blob_storage = BlobStorage(backend=backend)
        bronze = BronzeStorage(storage=blob_storage)

        assert bronze.storage == blob_storage

    def test_string_path_initialization(self, tmp_path):
        # Test initialization with BlobStorage using filesystem backend
        base_path_str = str(tmp_path / "bronze_string")
        backend = FilesystemBackend(base_path=base_path_str)
        blob_storage = BlobStorage(backend=backend)
        bronze = BronzeStorage(storage=blob_storage)

        assert bronze.storage == blob_storage
