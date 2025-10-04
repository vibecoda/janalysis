"""Data ingestion functions for J-Quants API data."""

from __future__ import annotations

import logging
from datetime import datetime

from jqsys.data.client import JQuantsClient
from jqsys.data.layers.bronze import BronzeStorage

logger = logging.getLogger(__name__)


def ingest_daily_quotes(
    client: JQuantsClient,
    bronze: BronzeStorage,
    dates: list[datetime],
    force: bool = False,
) -> int:
    """Ingest daily quotes for specified dates.

    Args:
        client: Authenticated JQuantsClient instance
        bronze: BronzeStorage instance for storing raw data
        dates: List of dates to process
        force: Force re-ingestion even if data already exists

    Returns:
        Total number of records ingested
    """
    logger.info(f"Processing {len(dates)} dates")

    # Process each date
    total_records = 0
    for date in dates:
        logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}")

        # Check if already processed (unless force)
        if not force:
            existing_dates = bronze.list_available_dates("daily_quotes")
            if date in existing_dates:
                logger.info(f"Data already exists for {date.strftime('%Y-%m-%d')}, skipping")
                continue

        try:
            # Fetch data from J-Quants API
            logger.info("Fetching data from J-Quants API...")
            params = {"date": date.strftime("%Y%m%d")}  # J-Quants expects YYYYMMDD format
            data = client.get_paginated(
                "/v1/prices/daily_quotes", data_key="daily_quotes", params=params
            )

            if not data:
                logger.warning(f"No data returned for {date.strftime('%Y-%m-%d')}")
                continue

            logger.info(f"Fetched {len(data)} records from API")
            total_records += len(data)

            # Store in bronze layer
            blob_key = bronze.store_raw_response(
                endpoint="daily_quotes",
                data=data,
                date=date,
                metadata={
                    "api_call": "/v1/prices/daily_quotes",
                    "date_param": date.strftime("%Y%m%d"),
                    "record_count": len(data),
                },
            )
            logger.info(f"Stored raw data: {blob_key}")

        except Exception as e:
            logger.error(f"Failed to process {date.strftime('%Y-%m-%d')}: {e}")
            continue

    logger.info(f"Ingestion completed. Total records ingested: {total_records}")

    # Print storage statistics
    stats = bronze.get_storage_stats()
    logger.info("Bronze storage statistics:")
    logger.info(f"  Endpoints: {stats['endpoint_count']}")
    logger.info(f"  Total dates: {stats['total_dates']}")
    logger.info(f"  Total files: {stats['total_files']}")

    return total_records
