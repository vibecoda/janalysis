"""Bronze layer: Raw data storage for J-Quants API responses.

Handles storage of raw API responses as partitioned Parquet files with
minimal processing - primarily for data lineage and reprocessing capabilities.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from io import BytesIO
from typing import Any

import polars as pl

from jqsys.core.storage.blob import BlobStorage

logger = logging.getLogger(__name__)


class BronzeStorage:
    """Manages bronze layer storage for raw J-Quants API data."""

    def __init__(self, storage: BlobStorage | None = None, add_metadata_columns: bool = False):
        """Initialize bronze storage.

        Args:
            storage: BlobStorage instance to use. If None, uses backend from BRONZE_BACKEND
                env var (defaults to "bronze_fs" for filesystem storage).
            add_metadata_columns: If True, adds _endpoint, _partition_date, _ingested_at, _metadata columns
        """
        if storage is None:
            backend_name = os.getenv("BRONZE_BACKEND", "bronze_fs")
            storage = BlobStorage.from_name(backend_name)

        self.storage = storage
        self.add_metadata_columns = add_metadata_columns

    def store_raw_response(
        self,
        endpoint: str,
        data: list[dict[str, Any]],
        date: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """Store raw API response data.

        Args:
            endpoint: J-Quants endpoint name (e.g., 'daily_quotes', 'listed_info')
            data: Raw response data from API
            date: Date of the data (used for partitioning)
            metadata: Optional metadata about the request/response

        Returns:
            Blob key of stored file
        """
        # Create blob key: endpoint/YYYY-MM-DD/data.parquet
        date_str = date.strftime("%Y-%m-%d")
        blob_key = f"{endpoint}/{date_str}/data.parquet"

        # Convert to Polars DataFrame for efficient storage
        if not data:
            logger.warning(f"No data to store for {endpoint} on {date_str}")
            # Create empty DataFrame with no columns
            df = pl.DataFrame()
        else:
            df = pl.DataFrame(data)

        try:
            # Add metadata columns if requested
            if self.add_metadata_columns:
                df = df.with_columns(
                    [
                        pl.lit(endpoint).alias("_endpoint"),
                        pl.lit(date_str).alias("_partition_date"),
                        pl.lit(datetime.now().isoformat()).alias("_ingested_at"),
                    ]
                )

                # Add optional metadata as JSON column
                if metadata:
                    df = df.with_columns(pl.lit(json.dumps(metadata)).alias("_metadata"))

            # Serialize to Parquet in memory
            buffer = BytesIO()
            df.write_parquet(buffer, compression="snappy", use_pyarrow=True)
            buffer.seek(0)

            # Store in blob storage
            self.storage.put(blob_key, buffer.read(), content_type="application/parquet")

            logger.info(f"Stored {len(df)} records to {blob_key}")
            return blob_key

        except Exception as e:
            logger.error(f"Failed to store {endpoint} data for {date_str}: {e}")
            raise

    def read_raw_data(
        self,
        endpoint: str,
        date: datetime | None = None,
        date_range: tuple[datetime, datetime] | None = None,
    ) -> pl.DataFrame:
        """Read raw data from bronze layer.

        Args:
            endpoint: J-Quants endpoint name
            date: Specific date to read (mutually exclusive with date_range)
            date_range: Tuple of (start_date, end_date) for range query

        Returns:
            Polars DataFrame with raw data
        """
        if date and date_range:
            raise ValueError("Cannot specify both date and date_range")

        # Build list of blob keys to read
        keys_to_read = []

        if date:
            # Single date
            date_str = date.strftime("%Y-%m-%d")
            blob_key = f"{endpoint}/{date_str}/data.parquet"
            if self.storage.exists(blob_key):
                keys_to_read.append(blob_key)
        else:
            # List all blobs for the endpoint
            for blob in self.storage.list(prefix=f"{endpoint}/"):
                # Parse date from key: endpoint/YYYY-MM-DD/data.parquet
                if not blob.key.endswith("/data.parquet"):
                    continue

                try:
                    parts = blob.key.split("/")
                    if len(parts) != 3:
                        continue

                    date_str = parts[1]
                    blob_date = datetime.strptime(date_str, "%Y-%m-%d")

                    # Filter by date range if specified
                    if date_range:
                        start_date, end_date = date_range
                        if not (start_date <= blob_date <= end_date):
                            continue

                    keys_to_read.append(blob.key)

                except (ValueError, IndexError):
                    logger.warning(f"Skipping invalid blob key: {blob.key}")
                    continue

        if not keys_to_read:
            return pl.DataFrame()

        # Read and concatenate all blobs
        try:
            dataframes = []
            for key in keys_to_read:
                blob_data = self.storage.get(key)
                df = pl.read_parquet(BytesIO(blob_data))
                dataframes.append(df)

            if len(dataframes) == 1:
                return dataframes[0]
            else:
                return pl.concat(dataframes)

        except Exception as e:
            logger.error(f"Failed to read {endpoint} data: {e}")
            raise

    def list_available_dates(self, endpoint: str) -> list[datetime]:
        """List all available dates for an endpoint.

        Args:
            endpoint: J-Quants endpoint name

        Returns:
            List of available dates, sorted ascending
        """
        dates = []

        # List all blobs with endpoint prefix
        for blob in self.storage.list(prefix=f"{endpoint}/"):
            # Parse date from key: endpoint/YYYY-MM-DD/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                date_str = parts[1]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                dates.append(date_obj)

            except (ValueError, IndexError):
                continue

        return sorted(dates)

    def get_storage_stats(self) -> dict[str, Any]:
        """Get storage statistics for bronze layer.

        Returns:
            Dictionary with storage statistics
        """
        stats = {"endpoints": {}, "total_files": 0, "total_size_mb": 0}

        # List all blobs
        for blob in self.storage.list():
            # Parse endpoint from key: endpoint/YYYY-MM-DD/data.parquet
            if not blob.key.endswith("/data.parquet"):
                continue

            try:
                parts = blob.key.split("/")
                if len(parts) != 3:
                    continue

                endpoint = parts[0]

                # Initialize endpoint stats if needed
                if endpoint not in stats["endpoints"]:
                    stats["endpoints"][endpoint] = {"dates": 0, "files": 0, "size_mb": 0}

                # Update stats
                stats["endpoints"][endpoint]["dates"] += 1
                stats["endpoints"][endpoint]["files"] += 1
                stats["endpoints"][endpoint]["size_mb"] += blob.size / (1024 * 1024)
                stats["total_files"] += 1
                stats["total_size_mb"] += blob.size / (1024 * 1024)

            except (ValueError, IndexError):
                continue

        # Round size to 2 decimal places
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        for endpoint_stats in stats["endpoints"].values():
            endpoint_stats["size_mb"] = round(endpoint_stats["size_mb"], 2)

        return stats
