from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch

import polars as pl
import pytest

from jqsys.storage.bronze import BronzeStorage
from jqsys.storage.silver import SilverStorage


class TestSilverStorage:
    @pytest.fixture
    def mock_bronze_storage(self, tmp_path):
        bronze = BronzeStorage(tmp_path / "bronze")
        return bronze

    @pytest.fixture
    def sample_raw_data(self):
        return [
            {
                "Code": "1301",
                "Date": "2024-01-15",
                "Open": 101.0,
                "High": 105.0,
                "Low": 98.0,
                "Close": 102.0,
                "Volume": 100000,
                "TurnoverValue": 10200000.0,
                "AdjustmentFactor": 1.0,
                "AdjustmentClose": 102.0,
                "_endpoint": "daily_quotes",
                "_partition_date": "2024-01-15",
                "_ingested_at": "2024-01-15T10:00:00"
            },
            {
                "Code": "1332",
                "Date": "2024-01-15",
                "Open": 201.0,
                "High": 210.0,
                "Low": 198.0,
                "Close": 205.0,
                "Volume": 50000,
                "TurnoverValue": 10250000.0,
                "AdjustmentFactor": 1.1,
                "AdjustmentClose": 225.5,
                "_endpoint": "daily_quotes",
                "_partition_date": "2024-01-15",
                "_ingested_at": "2024-01-15T10:00:00"
            }
        ]

    def test_init_creates_base_directory(self, tmp_path, mock_bronze_storage):
        base_path = tmp_path / "silver_test"
        storage = SilverStorage(base_path, mock_bronze_storage)
        
        assert storage.base_path == base_path
        assert base_path.exists()
        assert base_path.is_dir()
        assert storage.bronze == mock_bronze_storage

    def test_init_without_bronze_storage(self, tmp_path):
        base_path = tmp_path / "silver_test"
        storage = SilverStorage(base_path)
        
        assert storage.base_path == base_path
        assert isinstance(storage.bronze, BronzeStorage)

    def test_normalize_daily_quotes_success(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Mock bronze storage to return sample data
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        file_path = storage.normalize_daily_quotes(test_date)
        
        assert file_path is not None
        assert file_path.exists()
        
        # Verify normalized data structure
        df = pl.read_parquet(file_path)
        
        # Check expected columns exist
        expected_columns = [
            "code", "date", "open", "high", "low", "close", "volume",
            "turnover_value", "adjustment_factor", "adj_close", "processed_at"
        ]
        for col in expected_columns:
            assert col in df.columns
        
        # Check data types
        assert df["code"].dtype == pl.Utf8
        assert df["date"].dtype == pl.Date
        assert df["close"].dtype == pl.Float64
        assert df["volume"].dtype == pl.Int64
        
        # Check data content
        assert len(df) == 2
        assert df.filter(pl.col("code") == "1301").height == 1
        assert df.filter(pl.col("code") == "1332").height == 1

    def test_normalize_daily_quotes_empty_data(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Mock bronze storage to return empty data
        mock_bronze_storage.read_raw_data = Mock(return_value=pl.DataFrame())
        
        with patch("jqsys.storage.silver.logger") as mock_logger:
            result = storage.normalize_daily_quotes(test_date)
            
            assert result is None
            mock_logger.warning.assert_called_once()

    def test_normalize_daily_quotes_already_exists(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # First normalization
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        file_path1 = storage.normalize_daily_quotes(test_date)
        
        # Second normalization (should skip)
        with patch("jqsys.storage.silver.logger") as mock_logger:
            file_path2 = storage.normalize_daily_quotes(test_date)
            
            assert file_path1 == file_path2
            mock_logger.info.assert_called_with(
                "Daily quotes already normalized for 2024-01-15"
            )

    def test_normalize_daily_quotes_force_refresh(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # First normalization
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        file_path1 = storage.normalize_daily_quotes(test_date)
        original_mtime = file_path1.stat().st_mtime
        
        # Force refresh
        file_path2 = storage.normalize_daily_quotes(test_date, force_refresh=True)
        
        assert file_path1 == file_path2
        # File should have been updated
        assert file_path2.stat().st_mtime >= original_mtime

    def test_normalize_daily_quotes_schema_transformation(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Test data with missing adjustment close
        raw_data = [{
            "Code": "1301",
            "Date": "2024-01-15",
            "Open": 100.0,
            "High": 105.0,
            "Low": 98.0,
            "Close": 102.0,
            "Volume": 100000,
            "TurnoverValue": 10200000.0,
            "AdjustmentFactor": 1.1,
            "AdjustmentClose": None,
            "_endpoint": "daily_quotes",
            "_partition_date": "2024-01-15",
            "_ingested_at": "2024-01-15T10:00:00"
        }]
        
        mock_raw_df = pl.DataFrame(raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        file_path = storage.normalize_daily_quotes(test_date)
        df = pl.read_parquet(file_path)
        
        # Should calculate adj_close from close * adjustment_factor
        expected_adj_close = 102.0 * 1.1
        actual_adj_close = df["adj_close"][0]
        assert actual_adj_close == pytest.approx(expected_adj_close)

    def test_validate_daily_quotes_missing_columns(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # DataFrame missing required columns
        invalid_df = pl.DataFrame({
            "code": ["1301"],
            "date": [date(2024, 1, 15)],
            # Missing open, high, low, close, volume
        })
        
        with pytest.raises(ValueError, match="Missing required columns"):
            storage._validate_daily_quotes(invalid_df, datetime(2024, 1, 15))

    def test_validate_daily_quotes_null_values(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # DataFrame with null values in critical columns
        invalid_df = pl.DataFrame({
            "code": [None, "1332"],
            "date": [date(2024, 1, 15), date(2024, 1, 15)],
            "open": [100.0, 200.0],
            "high": [105.0, 210.0],
            "low": [98.0, 198.0],
            "close": [102.0, None],
            "volume": [100000, 50000]
        })
        
        with pytest.raises(ValueError, match="Found .* null"):
            storage._validate_daily_quotes(invalid_df, datetime(2024, 1, 15))

    def test_validate_daily_quotes_negative_prices(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # DataFrame with non-positive close prices
        invalid_df = pl.DataFrame({
            "code": ["1301"],
            "date": [date(2024, 1, 15)],
            "open": [100.0],
            "high": [105.0],
            "low": [98.0],
            "close": [-10.0],  # Invalid negative price
            "volume": [100000]
        })
        
        with pytest.raises(ValueError, match="Found non-positive close prices"):
            storage._validate_daily_quotes(invalid_df, datetime(2024, 1, 15))

    def test_validate_daily_quotes_invalid_ohlc_relationships(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # DataFrame with invalid OHLC relationships
        invalid_df = pl.DataFrame({
            "code": ["1301"],
            "date": [date(2024, 1, 15)],
            "open": [100.0],
            "high": [95.0],  # High < Low (invalid)
            "low": [98.0],
            "close": [102.0],
            "volume": [100000]
        })
        
        with pytest.raises(ValueError, match="invalid OHLC relationships"):
            storage._validate_daily_quotes(invalid_df, datetime(2024, 1, 15))

    def test_validate_daily_quotes_very_high_prices_warning(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # DataFrame with very high prices (should warn but not fail)
        high_price_df = pl.DataFrame({
            "code": ["1301"],
            "date": [date(2024, 1, 15)],
            "open": [1500000.0],
            "high": [1600000.0],
            "low": [1400000.0],
            "close": [1500000.0],  # Very high price
            "volume": [100000]
        })
        
        with patch("jqsys.storage.silver.logger") as mock_logger:
            # Should not raise exception, just warn
            storage._validate_daily_quotes(high_price_df, datetime(2024, 1, 15))
            mock_logger.warning.assert_called()

    def test_get_silver_path(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        test_date = datetime(2024, 1, 15)
        path = storage._get_silver_path("daily_prices", test_date)
        
        expected_path = tmp_path / "silver" / "daily_prices" / "year=2024" / "month=01" / "day=15.parquet"
        assert path == expected_path

    def test_read_daily_prices_single_date(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # First normalize some data
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        storage.normalize_daily_quotes(test_date)
        
        # Read it back
        df = storage.read_daily_prices(test_date, test_date)
        
        assert len(df) == 2
        assert "code" in df.columns
        assert "date" in df.columns
        assert "close" in df.columns

    def test_read_daily_prices_date_range(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # Normalize data for multiple dates
        dates = [datetime(2024, 1, 15), datetime(2024, 1, 16)]
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        for test_date in dates:
            storage.normalize_daily_quotes(test_date)
        
        # Read date range
        df = storage.read_daily_prices(
            datetime(2024, 1, 15), 
            datetime(2024, 1, 16)
        )
        
        # Should have data from both dates
        assert len(df) == 4  # 2 codes × 2 dates

    def test_read_daily_prices_with_code_filter(self, tmp_path, mock_bronze_storage, sample_raw_data):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Normalize data
        mock_raw_df = pl.DataFrame(sample_raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        storage.normalize_daily_quotes(test_date)
        
        # Read with code filter
        df = storage.read_daily_prices(
            test_date, 
            test_date, 
            codes=["1301"]
        )
        
        assert len(df) == 1
        assert df["code"][0] == "1301"

    def test_read_daily_prices_no_data(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # Try to read data that doesn't exist
        df = storage.read_daily_prices(
            datetime(2024, 1, 15),
            datetime(2024, 1, 15)
        )
        
        assert df.is_empty()

    def test_read_daily_prices_exception_handling(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # Create a corrupted file
        file_path = storage._get_silver_path("daily_prices", datetime(2024, 1, 15))
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text("corrupted parquet data")
        
        with pytest.raises(Exception):
            storage.read_daily_prices(
                datetime(2024, 1, 15),
                datetime(2024, 1, 15)
            )

    def test_date_increment_logic(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        
        # Test the date increment logic in read_daily_prices
        # This tests the date loop that increments current_date
        start_date = datetime(2024, 1, 29)  # End of month
        end_date = datetime(2024, 1, 31)    # Stay within same month for simplicity
        
        # Should not crash on date arithmetic
        df = storage.read_daily_prices(start_date, end_date)
        assert df.is_empty()  # No data, but should not error

    def test_normalize_daily_quotes_exception_handling(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Mock bronze storage to raise an exception
        mock_bronze_storage.read_raw_data = Mock(side_effect=Exception("Bronze read error"))
        
        with patch("jqsys.storage.silver.logger") as mock_logger:
            with pytest.raises(Exception):
                storage.normalize_daily_quotes(test_date)
            mock_logger.error.assert_called()

    def test_normalize_daily_quotes_schema_with_nulls(self, tmp_path, mock_bronze_storage):
        storage = SilverStorage(tmp_path / "silver", mock_bronze_storage)
        test_date = datetime(2024, 1, 15)
        
        # Test data with null values in optional fields
        raw_data = [{
            "Code": "1301",
            "Date": "2024-01-15",
            "Open": 100.0,
            "High": 105.0,
            "Low": 98.0,
            "Close": 102.0,
            "Volume": 100000,
            "TurnoverValue": None,  # Null optional field
            "AdjustmentFactor": None,  # Null optional field
            "AdjustmentClose": None,  # Null optional field
            "_endpoint": "daily_quotes",
            "_partition_date": "2024-01-15",
            "_ingested_at": "2024-01-15T10:00:00"
        }]
        
        mock_raw_df = pl.DataFrame(raw_data)
        mock_bronze_storage.read_raw_data = Mock(return_value=mock_raw_df)
        
        file_path = storage.normalize_daily_quotes(test_date)
        df = pl.read_parquet(file_path)
        
        # Should handle nulls gracefully and calculate adj_close as close * 1.0
        assert df["adj_close"][0] == 102.0  # close * 1.0 when adjustment_factor is null

    def test_pathlib_path_initialization(self, tmp_path):
        base_path = Path(tmp_path) / "silver_pathlib"
        storage = SilverStorage(base_path)
        
        assert storage.base_path == base_path
        assert base_path.exists()

    def test_string_path_initialization(self, tmp_path):
        base_path_str = str(tmp_path / "silver_string")
        storage = SilverStorage(base_path_str)
        
        assert storage.base_path == Path(base_path_str)
        assert storage.base_path.exists()