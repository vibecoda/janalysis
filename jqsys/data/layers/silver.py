"""Silver layer: Normalized timeseries data with data quality validation.

Transforms bronze layer raw data into clean, normalized timeseries tables
optimized for financial analysis and feature engineering.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Any

import polars as pl

from jqsys.core.storage.blob import BlobStorage
from jqsys.data.layers.bronze import BronzeStorage

logger = logging.getLogger(__name__)


class SilverStorage:
    """Manages silver layer storage for normalized timeseries data."""

    def __init__(
        self, storage: BlobStorage | None = None, bronze_storage: BronzeStorage | None = None
    ):
        """Initialize silver storage.

        Args:
            storage: BlobStorage instance to use. If None, uses backend from SILVER_BACKEND
                env var (defaults to "demo.silver" for filesystem storage).
            bronze_storage: Bronze storage instance for reading raw data
        """
        if storage is None:
            backend_name = os.getenv("SILVER_BACKEND", "demo.silver")
            storage = BlobStorage.from_name(backend_name)

        self.storage = storage
        self.bronze = bronze_storage or BronzeStorage()

    def normalize_daily_quotes(self, date: date, force_refresh: bool = False) -> str | None:
        """Normalize daily quotes data from bronze to silver layer.

        Args:
            date: Date to process
            force_refresh: Whether to reprocess existing data

        Returns:
            Blob key of normalized file or None if no data
        """
        # Check if already processed
        blob_key = self._get_silver_key("daily_prices", date)
        if self.storage.exists(blob_key) and not force_refresh:
            logger.info(f"Daily quotes already normalized for {date.isoformat()}")
            return blob_key

        try:
            # Read raw data from bronze (bronze layer uses datetime)
            raw_df = self.bronze.read_raw_data(
                "daily_quotes", date=datetime.combine(date, datetime.min.time())
            )
            if raw_df.is_empty():
                logger.warning(f"No raw daily quotes data for {date.isoformat()}")
                return None

            # Normalize the data
            normalized_df = self._normalize_daily_quotes_schema(raw_df, date)

            # Validate data quality
            self._validate_daily_quotes(normalized_df, date)

            # Store normalized data in blob storage
            buffer = BytesIO()
            normalized_df.write_parquet(buffer, compression="snappy", use_pyarrow=True)
            buffer.seek(0)
            self.storage.put(blob_key, buffer.read(), content_type="application/parquet")

            logger.info(
                f"Normalized {len(normalized_df)} daily quotes records for {date.isoformat()}"
            )
            return blob_key

        except Exception as e:
            logger.error(f"Failed to normalize daily quotes for {date.isoformat()}: {e}")
            raise

    def _normalize_daily_quotes_schema(
        self, raw_df: pl.DataFrame, processing_date: date
    ) -> pl.DataFrame:
        """Transform raw daily quotes to normalized schema.

        Args:
            raw_df: Raw DataFrame from bronze layer
            processing_date: Processing date (not used in schema transformation)

        Returns:
            Normalized DataFrame
        """
        # Expected J-Quants daily quotes schema transformation
        # This is a basic normalization - adjust based on actual API response structure

        normalized = raw_df.select(
            [
                # Core identification
                pl.col("Code").alias("code"),
                pl.col("Date").str.strptime(pl.Date, format="%Y-%m-%d").alias("date"),
                # OHLCV data
                pl.col("Open").cast(pl.Float64).alias("open"),
                pl.col("High").cast(pl.Float64).alias("high"),
                pl.col("Low").cast(pl.Float64).alias("low"),
                pl.col("Close").cast(pl.Float64).alias("close"),
                pl.col("Volume").cast(pl.Int64).alias("volume"),
                # Additional fields if available
                pl.col("TurnoverValue").cast(pl.Float64, strict=False).alias("turnover_value"),
                pl.col("AdjustmentFactor")
                .cast(pl.Float64, strict=False)
                .alias("adjustment_factor"),
                pl.col("AdjustmentClose").cast(pl.Float64, strict=False).alias("adj_close"),
                # Metadata
                pl.lit(datetime.now().isoformat()).alias("processed_at"),
            ]
        ).filter(
            # Remove any records with null core data
            pl.col("code").is_not_null()
            & pl.col("date").is_not_null()
            & pl.col("close").is_not_null()
        )

        # Calculate adjusted close if not provided
        normalized = normalized.with_columns(
            [
                pl.when(pl.col("adj_close").is_null())
                .then(pl.col("close") * pl.col("adjustment_factor").fill_null(1.0))
                .otherwise(pl.col("adj_close"))
                .alias("adj_close")
            ]
        )

        return normalized.sort(["code", "date"])

    def _validate_daily_quotes(self, df: pl.DataFrame, expected_date: date) -> None:
        """Validate daily quotes data quality.

        Args:
            df: Normalized DataFrame to validate
            expected_date: Expected date (not used in validation)

        Raises:
            ValueError: If data quality issues are found
        """
        # Check for required columns
        required_cols = ["code", "date", "open", "high", "low", "close", "volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

        # Check for null values in critical columns
        null_counts = df.select(
            [
                pl.col("code").null_count().alias("code_nulls"),
                pl.col("close").null_count().alias("close_nulls"),
            ]
        ).row(0)

        if null_counts[0] > 0:
            raise ValueError(f"Found {null_counts[0]} null codes")
        if null_counts[1] > 0:
            raise ValueError(f"Found {null_counts[1]} null close prices")

        # Check for reasonable price ranges (basic sanity check)
        price_stats = df.select(
            [pl.col("close").min().alias("min_close"), pl.col("close").max().alias("max_close")]
        ).row(0)

        if price_stats[0] <= 0:
            raise ValueError(f"Found non-positive close prices: min={price_stats[0]}")
        if price_stats[1] > 1000000:  # 1M yen seems unreasonable for individual stock
            logger.warning(f"Found very high close price: max={price_stats[1]}")

        # Validate OHLC relationships
        invalid_ohlc = df.filter(
            (pl.col("high") < pl.col("low"))
            | (pl.col("high") < pl.col("open"))
            | (pl.col("high") < pl.col("close"))
            | (pl.col("low") > pl.col("open"))
            | (pl.col("low") > pl.col("close"))
        ).height

        if invalid_ohlc > 0:
            raise ValueError(f"Found {invalid_ohlc} records with invalid OHLC relationships")

        logger.info(f"Data quality validation passed for {len(df)} records")

    def _get_silver_key(self, table: str, date: date) -> str:
        """Get blob key for silver layer table file.

        Args:
            table: Table name (e.g., 'daily_prices', 'fundamentals')
            date: Date for partitioning

        Returns:
            Blob key for silver layer file
        """
        date_str = date.isoformat()
        return f"{table}/{date_str}/data.parquet"

    def read_daily_prices(
        self, start_date: date, end_date: date, codes: list[str] | None = None
    ) -> pl.DataFrame:
        """Read normalized daily prices from silver layer.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            codes: Optional list of stock codes to filter

        Returns:
            DataFrame with daily prices
        """
        # Find all relevant blob keys
        keys_to_read = []
        current_date = start_date

        while current_date <= end_date:
            blob_key = self._get_silver_key("daily_prices", current_date)
            if self.storage.exists(blob_key):
                keys_to_read.append(blob_key)
            current_date = current_date + timedelta(days=1)

        if not keys_to_read:
            return pl.DataFrame()

        # Read and concatenate
        try:
            dataframes = []
            for key in keys_to_read:
                blob_data = self.storage.get(key)
                df = pl.read_parquet(BytesIO(blob_data))
                dataframes.append(df)

            df = dataframes[0] if len(dataframes) == 1 else pl.concat(dataframes)

            # Apply filters
            df = df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))

            if codes:
                df = df.filter(pl.col("code").is_in(codes))

            return df.sort(["date", "code"])

        except Exception as e:
            logger.error(f"Failed to read daily prices: {e}")
            raise

    def list_available_dates(self, table: str) -> list[date]:
        """List all available dates for a table.

        Args:
            table: Table name (e.g., 'daily_prices')

        Returns:
            List of available dates, sorted ascending
        """
        dates = []

        # List all blobs with table prefix
        for blob in self.storage.list(prefix=f"{table}/"):
            # Parse date from key: table/YYYY-MM-DD/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                date_str = parts[1]
                date_obj = date.fromisoformat(date_str)
                dates.append(date_obj)

            except (ValueError, IndexError):
                continue

        return sorted(dates)

    def get_storage_stats(self, table: str | None = None) -> dict[str, Any]:
        """Get storage statistics for silver layer.

        Args:
            table: Optional table name to filter stats. If None, returns stats for all tables.

        Returns:
            Dictionary with storage statistics
        """
        stats = {"tables": {}, "total_files": 0, "total_size_mb": 0}

        # List all blobs (optionally filtered by table)
        prefix = f"{table}/" if table else None
        for blob in self.storage.list(prefix=prefix):
            # Parse table from key: table/YYYY-MM-DD/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                table_name = parts[0]

                # Initialize table stats if needed
                if table_name not in stats["tables"]:
                    stats["tables"][table_name] = {"dates": 0, "files": 0, "size_mb": 0}

                # Update stats
                stats["tables"][table_name]["dates"] += 1
                stats["tables"][table_name]["files"] += 1
                stats["tables"][table_name]["size_mb"] += blob.size / (1024 * 1024)
                stats["total_files"] += 1
                stats["total_size_mb"] += blob.size / (1024 * 1024)

            except (ValueError, IndexError):
                continue

        # Round size to 2 decimal places
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        for table_stats in stats["tables"].values():
            table_stats["size_mb"] = round(table_stats["size_mb"], 2)

        return stats
