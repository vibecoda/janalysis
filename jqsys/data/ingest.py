"""Data ingestion functions for J-Quants API data."""

from __future__ import annotations

import logging
from datetime import date, datetime

from jqsys.data.client import JQuantsClient
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.silver import SilverStorage

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
    for processing_date in dates:
        logger.info(f"Processing date: {processing_date.strftime('%Y-%m-%d')}")

        # Check if already processed (unless force)
        if not force:
            existing_dates = bronze.list_available_dates("daily_quotes")
            if processing_date in existing_dates:
                logger.info(
                    f"Data already exists for {processing_date.strftime('%Y-%m-%d')}, skipping"
                )
                continue

        try:
            # Fetch data from J-Quants API
            logger.info("Fetching data from J-Quants API...")
            params = {
                "date": processing_date.strftime("%Y%m%d")
            }  # J-Quants expects YYYYMMDD format
            data = client.get_paginated(
                "/v1/prices/daily_quotes", data_key="daily_quotes", params=params
            )

            if not data:
                logger.warning(f"No data returned for {processing_date.strftime('%Y-%m-%d')}")
                continue

            logger.info(f"Fetched {len(data)} records from API")
            total_records += len(data)

            # Store in bronze layer
            blob_key = bronze.store_raw_response(
                endpoint="daily_quotes",
                data=data,
                date=processing_date,
                metadata={
                    "api_call": "/v1/prices/daily_quotes",
                    "date_param": processing_date.strftime("%Y%m%d"),
                    "record_count": len(data),
                },
            )
            logger.info(f"Stored raw data: {blob_key}")

        except Exception as e:
            logger.error(f"Failed to process {processing_date.strftime('%Y-%m-%d')}: {e}")
            continue

    logger.info(f"Ingestion completed. Total records ingested: {total_records}")

    # Print storage statistics
    stats = bronze.get_storage_stats()
    logger.info("Bronze storage statistics:")
    for endpoint, info in stats.get("endpoints", {}).items():
        logger.info(
            f"Endpoint '{endpoint}': {info['dates']} dates, "
            f"{info['files']} files, {info['size_mb']} MB"
        )
    logger.info(
        f"Total files: {stats.get('total_files', 0)}, "
        f"Total size: {stats.get('total_size_mb', 0)} MB"
    )
    return total_records


def normalize_daily_quotes(
    silver: SilverStorage,
    dates: list[date],
    force: bool = False,
) -> int:
    """Normalize daily quotes from bronze to silver layer for specified dates.

    Args:
        silver: SilverStorage instance for storing normalized data
        dates: List of dates to process
        force: Force re-normalization even if data already exists

    Returns:
        Total number of dates successfully normalized
    """
    logger.info(f"Normalizing {len(dates)} dates from bronze to silver layer")

    # Process each date
    successful_dates = 0
    for processing_date in dates:
        logger.info(f"Normalizing date: {processing_date.isoformat()}")

        try:
            # Normalize the data (will skip if already exists unless force=True)
            blob_key = silver.normalize_daily_quotes(processing_date, force_refresh=force)

            if blob_key:
                successful_dates += 1
                logger.info(f"Successfully normalized: {blob_key}")
            else:
                logger.warning(f"No data to normalize for {processing_date.isoformat()}")

        except Exception as e:
            logger.error(f"Failed to normalize {processing_date.isoformat()}: {e}")
            continue

    logger.info(f"Normalization completed. Successfully normalized {successful_dates} dates")

    # Print storage statistics
    stats = silver.get_storage_stats()
    logger.info("Silver storage statistics:")
    for table, info in stats.get("tables", {}).items():
        logger.info(
            f"Table '{table}': {info['dates']} dates, {info['files']} files, {info['size_mb']} MB"
        )
    logger.info(
        f"Total files: {stats.get('total_files', 0)}, "
        f"Total size: {stats.get('total_size_mb', 0)} MB"
    )
    return successful_dates
