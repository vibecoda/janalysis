"""Gold layer: Stock-centric optimized data for analysis and trading.

Transforms silver layer date-partitioned data into stock-partitioned format
optimized for single-stock queries and analysis.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from io import BytesIO
from typing import Any

import polars as pl

from jqsys.core.storage.blob import BlobStorage
from jqsys.data.layers.silver import SilverStorage

logger = logging.getLogger(__name__)


class GoldStorage:
    """Manages gold layer storage for stock-centric daily prices."""

    def __init__(
        self, storage: BlobStorage | None = None, silver_storage: SilverStorage | None = None
    ):
        """Initialize gold storage.

        Args:
            storage: BlobStorage instance to use. If None, uses backend from GOLD_BACKEND
                env var (defaults to "gold_fs" for filesystem storage).
            silver_storage: Silver storage instance for reading normalized data
        """
        if storage is None:
            backend_name = os.getenv("GOLD_BACKEND", "gold_fs")
            storage = BlobStorage.from_name(backend_name)

        self.storage = storage
        self.silver = silver_storage or SilverStorage()

    def transform_daily_prices(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        """Transform silver layer daily prices to gold layer stock-centric format.

        Strategy: Read all silver data at once, then write each stock file once.
        This minimizes I/O operations and is more efficient for bulk transformations.

        Args:
            start_date: Start date for transformation (inclusive). If None, process all available dates.
            end_date: End date for transformation (inclusive). If None, use latest available date.
            force_refresh: If True, overwrite existing data even if date already exists

        Returns:
            Dictionary with transformation statistics
        """
        # Determine date range
        available_dates = self.silver.list_available_dates("daily_prices")
        if not available_dates:
            logger.warning("No data available in silver layer")
            return {"dates_processed": 0, "stocks_updated": 0, "records_written": 0}

        if start_date is None:
            start_date = available_dates[0]
        if end_date is None:
            end_date = available_dates[-1]

        # Filter to dates in range
        dates_to_process = [d for d in available_dates if start_date <= d <= end_date]

        if not dates_to_process:
            logger.warning(f"No silver data in range {start_date} to {end_date}")
            return {"dates_processed": 0, "stocks_updated": 0, "records_written": 0}

        logger.info(
            f"Transforming {len(dates_to_process)} dates from silver to gold layer: "
            f"{dates_to_process[0]} to {dates_to_process[-1]}"
        )

        # Read all silver data at once
        logger.info(f"Reading silver data for range {start_date} to {end_date}")
        silver_df = self.silver.read_daily_prices(start_date, end_date)

        if silver_df.is_empty():
            logger.warning(f"No silver data in range {start_date} to {end_date}")
            return {"dates_processed": 0, "stocks_updated": 0, "records_written": 0}

        # Get unique stock codes across all dates
        stock_codes = silver_df["code"].unique().to_list()
        logger.info(f"Found {len(stock_codes)} unique stocks across {len(dates_to_process)} dates")

        # Get unique dates that have data
        dates_with_data = set(silver_df["date"].unique().to_list())

        stats = {"stocks_updated": 0, "records_written": 0}

        # Process each stock once
        for stock_code in stock_codes:
            try:
                # Filter all data for this stock
                stock_data = silver_df.filter(pl.col("code") == stock_code)

                # Update gold file for this stock (one write per stock)
                self._update_stock_data(stock_code, stock_data, force_refresh)

                stats["stocks_updated"] += 1
                stats["records_written"] += len(stock_data)

            except Exception as e:
                logger.error(f"Failed to update stock {stock_code}: {e}")
                continue

        # Calculate dates processed
        stats["dates_processed"] = len(dates_with_data)

        logger.info(
            f"Transformation complete: {stats['dates_processed']} dates, "
            f"{stats['stocks_updated']} stocks updated, "
            f"{stats['records_written']} records written"
        )

        return stats

    def _update_stock_data(self, stock_code: str, new_data: pl.DataFrame, force_refresh: bool):
        """Update gold file for a specific stock with new data.

        Merges new data with existing data, ensuring one row per date.

        Args:
            stock_code: Stock code to update
            new_data: New data to merge (must contain 'date' column)
            force_refresh: If True, always write; if False, skip if dates already exist
        """
        gold_key = self._get_gold_key(stock_code)

        # Read existing data if it exists
        if self.storage.exists(gold_key):
            existing_blob = self.storage.get(gold_key)
            existing_df = pl.read_parquet(BytesIO(existing_blob))

            if not force_refresh:
                # Check if any dates already exist
                new_dates = set(new_data["date"].to_list())
                existing_dates = set(existing_df["date"].to_list())
                dates_to_skip = new_dates & existing_dates

                if dates_to_skip:
                    logger.debug(f"Skipping {len(dates_to_skip)} existing dates for {stock_code}")
                    # Filter out dates that already exist
                    new_data = new_data.filter(~pl.col("date").is_in(list(dates_to_skip)))

                    if new_data.is_empty():
                        logger.debug(f"No new data to add for {stock_code}")
                        return

            # Merge: concatenate and deduplicate
            merged_df = pl.concat([existing_df, new_data])

            # Deduplicate: keep last occurrence of each date (assumes newer is better)
            # This handles force_refresh case where we replace existing dates
            merged_df = merged_df.unique(subset=["date"], keep="last")

        else:
            # No existing data, use new data as-is
            merged_df = new_data

        # Sort by date
        merged_df = merged_df.sort("date")

        # Write atomically using temp file
        self._write_atomic(gold_key, merged_df)

    def _write_atomic(self, blob_key: str, df: pl.DataFrame):
        """Write DataFrame to blob storage atomically.

        Uses temp file + rename pattern for atomicity.

        Args:
            blob_key: Target blob key
            df: DataFrame to write
        """
        # Write to temp key
        temp_key = f"{blob_key}.tmp"

        buffer = BytesIO()
        df.write_parquet(buffer, compression="snappy", use_pyarrow=True)
        buffer.seek(0)

        self.storage.put(temp_key, buffer.read(), content_type="application/parquet")

        # Rename to final key (atomic on most backends)
        # For blob storage, we'll implement this as copy + delete
        self.storage.put(blob_key, self.storage.get(temp_key), content_type="application/parquet")
        self.storage.delete(temp_key)

    def _get_gold_key(self, stock_code: str) -> str:
        """Get blob key for stock's gold layer file.

        Args:
            stock_code: Stock code

        Returns:
            Blob key for gold layer file
        """
        return f"daily_prices/{stock_code}/data.parquet"

    def read_stock_prices(
        self,
        code: str,
        start_date: date | None = None,
        end_date: date | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """Read daily prices for a specific stock from gold layer.

        Args:
            code: Stock code
            start_date: Start date filter (inclusive)
            end_date: End date filter (inclusive)
            columns: List of columns to select. If None, returns all columns.

        Returns:
            DataFrame with stock's daily prices
        """
        gold_key = self._get_gold_key(code)

        if not self.storage.exists(gold_key):
            logger.warning(f"No gold data for stock {code}")
            return pl.DataFrame()

        try:
            blob_data = self.storage.get(gold_key)
            df = pl.read_parquet(BytesIO(blob_data))

            # Apply date filters
            if start_date is not None:
                df = df.filter(pl.col("date") >= start_date)
            if end_date is not None:
                df = df.filter(pl.col("date") <= end_date)

            # Apply column selection
            if columns is not None:
                # Ensure we include date and code columns
                cols_to_select = list(set(columns + ["date", "code"]))
                df = df.select(cols_to_select)

            return df.sort("date")

        except Exception as e:
            logger.error(f"Failed to read stock prices for {code}: {e}")
            raise

    def list_available_stocks(self) -> list[str]:
        """List all stocks with data in gold layer.

        Returns:
            Sorted list of stock codes
        """
        stocks = []

        # List all blobs with daily_prices prefix
        for blob in self.storage.list(prefix="daily_prices/"):
            # Parse stock code from key: daily_prices/{code}/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                stock_code = parts[1]
                stocks.append(stock_code)

            except (ValueError, IndexError):
                continue

        return sorted(stocks)

    def get_storage_stats(self, stock: str | None = None) -> dict[str, Any]:
        """Get storage statistics for gold layer.

        Args:
            stock: Optional stock code to filter stats. If None, returns stats for all stocks.

        Returns:
            Dictionary with storage statistics
        """
        stats = {"stocks": {}, "total_files": 0, "total_size_mb": 0, "total_records": 0}

        # List all blobs (optionally filtered by stock)
        prefix = f"daily_prices/{stock}/" if stock else "daily_prices/"

        for blob in self.storage.list(prefix=prefix):
            # Parse stock code from key: daily_prices/{code}/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                stock_code = parts[1]

                # Initialize stock stats if needed
                if stock_code not in stats["stocks"]:
                    stats["stocks"][stock_code] = {"files": 0, "size_mb": 0, "records": 0}

                # Get record count by reading the file
                try:
                    blob_data = self.storage.get(blob.key)
                    df = pl.read_parquet(BytesIO(blob_data))
                    record_count = len(df)
                except Exception:
                    record_count = 0

                # Update stats
                stats["stocks"][stock_code]["files"] += 1
                stats["stocks"][stock_code]["size_mb"] += blob.size / (1024 * 1024)
                stats["stocks"][stock_code]["records"] += record_count
                stats["total_files"] += 1
                stats["total_size_mb"] += blob.size / (1024 * 1024)
                stats["total_records"] += record_count

            except (ValueError, IndexError):
                continue

        # Round sizes
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        for stock_stats in stats["stocks"].values():
            stock_stats["size_mb"] = round(stock_stats["size_mb"], 2)

        return stats
