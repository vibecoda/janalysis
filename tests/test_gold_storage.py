from __future__ import annotations

from datetime import date
from io import BytesIO
from unittest.mock import Mock

import polars as pl
import pytest

from jqsys.core.storage.backends.filesystem_backend import FilesystemBackend
from jqsys.core.storage.blob import BlobStorage
from jqsys.data.layers.gold import GoldStorage
from jqsys.data.layers.silver import SilverStorage


class TestGoldStorage:
    @pytest.fixture
    def mock_blob_storage(self, tmp_path):
        """Create a temporary filesystem-based BlobStorage for testing."""
        backend = FilesystemBackend(str(tmp_path / "gold"))
        return BlobStorage(backend)

    @pytest.fixture
    def mock_silver_storage(self, tmp_path):
        """Create a temporary silver storage for testing."""
        backend = FilesystemBackend(str(tmp_path / "silver"))
        silver_blob_storage = BlobStorage(backend)
        return SilverStorage(silver_blob_storage)

    @pytest.fixture
    def sample_silver_data(self):
        """Sample normalized silver data for multiple stocks across multiple dates."""
        return {
            date(2024, 1, 15): pl.DataFrame(
                [
                    {
                        "code": "1301",
                        "date": date(2024, 1, 15),
                        "open": 101.0,
                        "high": 105.0,
                        "low": 98.0,
                        "close": 102.0,
                        "volume": 100000,
                        "turnover_value": 10200000.0,
                        "adjustment_factor": 1.0,
                        "adj_close": 102.0,
                        "processed_at": "2024-01-15T10:00:00",
                    },
                    {
                        "code": "1332",
                        "date": date(2024, 1, 15),
                        "open": 201.0,
                        "high": 210.0,
                        "low": 198.0,
                        "close": 205.0,
                        "volume": 50000,
                        "turnover_value": 10250000.0,
                        "adjustment_factor": 1.1,
                        "adj_close": 225.5,
                        "processed_at": "2024-01-15T10:00:00",
                    },
                ]
            ),
            date(2024, 1, 16): pl.DataFrame(
                [
                    {
                        "code": "1301",
                        "date": date(2024, 1, 16),
                        "open": 103.0,
                        "high": 107.0,
                        "low": 102.0,
                        "close": 106.0,
                        "volume": 120000,
                        "turnover_value": 12600000.0,
                        "adjustment_factor": 1.0,
                        "adj_close": 106.0,
                        "processed_at": "2024-01-16T10:00:00",
                    },
                    {
                        "code": "1332",
                        "date": date(2024, 1, 16),
                        "open": 206.0,
                        "high": 215.0,
                        "low": 205.0,
                        "close": 210.0,
                        "volume": 60000,
                        "turnover_value": 12600000.0,
                        "adjustment_factor": 1.1,
                        "adj_close": 231.0,
                        "processed_at": "2024-01-16T10:00:00",
                    },
                ]
            ),
        }

    def test_init_creates_storage(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        assert storage.storage == mock_blob_storage
        assert storage.silver == mock_silver_storage

    def test_init_without_silver_storage(self, mock_blob_storage):
        storage = GoldStorage(mock_blob_storage)

        assert storage.storage == mock_blob_storage
        assert isinstance(storage.silver, SilverStorage)

    def test_transform_daily_prices_success(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Mock silver storage to return sample data
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )

        # Transform data
        stats = storage.transform_daily_prices()

        assert stats["dates_processed"] == 2
        assert stats["stocks_updated"] == 2  # 1301 and 1332
        assert stats["records_written"] == 4  # 2 stocks × 2 dates

        # Verify stock 1301 has data
        stock_1301_key = storage._get_gold_key("1301")
        assert storage.storage.exists(stock_1301_key)

        # Read and verify
        blob_data = storage.storage.get(stock_1301_key)
        df = pl.read_parquet(BytesIO(blob_data))
        assert len(df) == 2  # Two dates
        assert df["code"][0] == "1301"

    def test_transform_daily_prices_with_date_range(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Mock silver storage
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )

        # Transform only first date
        stats = storage.transform_daily_prices(
            start_date=date(2024, 1, 15), end_date=date(2024, 1, 15)
        )

        assert stats["dates_processed"] == 1
        assert stats["stocks_updated"] == 2
        assert stats["records_written"] == 2  # 2 stocks × 1 date

    def test_transform_daily_prices_empty_silver(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Mock empty silver storage
        mock_silver_storage.list_available_dates = Mock(return_value=[])

        stats = storage.transform_daily_prices()

        assert stats["dates_processed"] == 0
        assert stats["stocks_updated"] == 0
        assert stats["records_written"] == 0

    def test_transform_daily_prices_incremental_update(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Mock silver storage
        all_dates = list(sample_silver_data.keys())
        mock_silver_storage.list_available_dates = Mock(return_value=all_dates)
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )

        # First transformation - process first date
        stats1 = storage.transform_daily_prices(start_date=all_dates[0], end_date=all_dates[0])
        assert stats1["dates_processed"] == 1

        # Read stock 1301 data
        df1 = storage.read_stock_prices("1301")
        assert len(df1) == 1  # Only first date

        # Second transformation - process second date
        stats2 = storage.transform_daily_prices(start_date=all_dates[1], end_date=all_dates[1])
        assert stats2["dates_processed"] == 1

        # Read stock 1301 data again
        df2 = storage.read_stock_prices("1301")
        assert len(df2) == 2  # Both dates now

    def test_transform_daily_prices_with_force_refresh(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup mocks
        all_dates = list(sample_silver_data.keys())
        mock_silver_storage.list_available_dates = Mock(return_value=all_dates)
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )

        # First transformation
        storage.transform_daily_prices(start_date=all_dates[0], end_date=all_dates[0])

        # Modify sample data for same date
        modified_data = sample_silver_data[all_dates[0]].clone()
        modified_data = modified_data.with_columns(pl.lit(999.0).alias("close"))

        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: modified_data
            if start == all_dates[0]
            else pl.DataFrame()
        )

        # Transform again with force_refresh
        storage.transform_daily_prices(
            start_date=all_dates[0], end_date=all_dates[0], force_refresh=True
        )

        # Verify data was updated
        df = storage.read_stock_prices("1301")
        assert df.filter(pl.col("date") == all_dates[0])["close"][0] == 999.0

    def test_read_stock_prices_basic(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # Read stock data
        df = storage.read_stock_prices("1301")

        assert len(df) == 2
        assert all(df["code"] == "1301")
        assert "open" in df.columns
        assert "close" in df.columns

    def test_read_stock_prices_with_date_filter(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # Read with date filter
        df = storage.read_stock_prices("1301", start_date=date(2024, 1, 16))

        assert len(df) == 1
        assert df["date"][0] == date(2024, 1, 16)

    def test_read_stock_prices_with_column_selection(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # Read with column selection
        df = storage.read_stock_prices("1301", columns=["close", "volume"])

        # Should include requested columns plus date and code
        assert set(df.columns) == {"code", "date", "close", "volume"}

    def test_read_stock_prices_nonexistent_stock(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        df = storage.read_stock_prices("9999")

        assert df.is_empty()

    def test_list_available_stocks(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # List stocks
        stocks = storage.list_available_stocks()

        assert len(stocks) == 2
        assert "1301" in stocks
        assert "1332" in stocks
        assert stocks == sorted(stocks)  # Should be sorted

    def test_list_available_stocks_empty(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        stocks = storage.list_available_stocks()

        assert len(stocks) == 0

    def test_get_storage_stats(self, mock_blob_storage, mock_silver_storage, sample_silver_data):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # Get stats
        stats = storage.get_storage_stats()

        assert "stocks" in stats
        assert "1301" in stats["stocks"]
        assert "1332" in stats["stocks"]
        assert stats["stocks"]["1301"]["records"] == 2
        assert stats["stocks"]["1332"]["records"] == 2
        assert stats["total_files"] == 2
        assert stats["total_records"] == 4
        assert stats["total_size_mb"] > 0

    def test_get_storage_stats_filtered_by_stock(
        self, mock_blob_storage, mock_silver_storage, sample_silver_data
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Setup and transform
        mock_silver_storage.list_available_dates = Mock(
            return_value=list(sample_silver_data.keys())
        )
        mock_silver_storage.read_daily_prices = Mock(
            side_effect=lambda start, end: sample_silver_data.get(start, pl.DataFrame())
        )
        storage.transform_daily_prices()

        # Get stats for single stock
        stats = storage.get_storage_stats(stock="1301")

        assert len(stats["stocks"]) == 1
        assert "1301" in stats["stocks"]
        assert "1332" not in stats["stocks"]

    def test_get_gold_key(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        key = storage._get_gold_key("1301")

        assert key == "daily_prices/1301/data.parquet"

    def test_deduplication_keeps_last(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Create duplicate data for same date
        duplicate_data = {
            date(2024, 1, 15): pl.DataFrame(
                [
                    {
                        "code": "1301",
                        "date": date(2024, 1, 15),
                        "open": 100.0,
                        "high": 105.0,
                        "low": 98.0,
                        "close": 102.0,
                        "volume": 100000,
                        "turnover_value": 10200000.0,
                        "adjustment_factor": 1.0,
                        "adj_close": 102.0,
                        "processed_at": "2024-01-15T10:00:00",
                    }
                ]
            )
        }

        # First transformation
        mock_silver_storage.list_available_dates = Mock(return_value=[date(2024, 1, 15)])
        mock_silver_storage.read_daily_prices = Mock(return_value=duplicate_data[date(2024, 1, 15)])
        storage.transform_daily_prices()

        # Create updated data for same date
        updated_data = duplicate_data[date(2024, 1, 15)].clone()
        updated_data = updated_data.with_columns(
            [
                pl.lit(999.0).alias("close"),
                pl.lit("2024-01-15T12:00:00").alias("processed_at"),
            ]
        )

        mock_silver_storage.read_daily_prices = Mock(return_value=updated_data)

        # Transform again with force refresh
        storage.transform_daily_prices(force_refresh=True)

        # Verify latest data is kept
        df = storage.read_stock_prices("1301")
        assert len(df) == 1  # Only one row per date
        assert df["close"][0] == 999.0  # Latest value

    def test_atomic_write(self, mock_blob_storage, mock_silver_storage):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Create test data
        test_df = pl.DataFrame(
            [
                {
                    "code": "1301",
                    "date": date(2024, 1, 15),
                    "open": 100.0,
                    "high": 105.0,
                    "low": 98.0,
                    "close": 102.0,
                    "volume": 100000,
                    "turnover_value": 10200000.0,
                    "adjustment_factor": 1.0,
                    "adj_close": 102.0,
                    "processed_at": "2024-01-15T10:00:00",
                }
            ]
        )

        blob_key = "daily_prices/1301/data.parquet"

        # Write atomically
        storage._write_atomic(blob_key, test_df)

        # Verify data was written
        assert storage.storage.exists(blob_key)

        # Verify temp file was cleaned up
        temp_key = f"{blob_key}.tmp"
        assert not storage.storage.exists(temp_key)

    def test_transform_handles_missing_dates_gracefully(
        self, mock_blob_storage, mock_silver_storage
    ):
        storage = GoldStorage(mock_blob_storage, mock_silver_storage)

        # Mock silver with dates but empty data
        mock_silver_storage.list_available_dates = Mock(return_value=[date(2024, 1, 15)])
        mock_silver_storage.read_daily_prices = Mock(return_value=pl.DataFrame())

        stats = storage.transform_daily_prices()

        # Should handle gracefully
        assert stats["dates_processed"] == 0
        assert stats["stocks_updated"] == 0
