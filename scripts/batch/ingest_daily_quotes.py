#!/usr/bin/env python
"""Ingest daily quotes data using the new DuckDB + Polars storage system."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

from jqsys.data.client import JQuantsClient
from jqsys.data.ingest import ingest_daily_quotes
from jqsys.data.layers.bronze import BronzeStorage

logger = logging.getLogger(__name__)


def get_next_weekday(date: datetime) -> datetime:
    """Get the next weekday (Monday-Friday) from the given date.

    Args:
        date: Starting date

    Returns:
        Next weekday date (or same date if already a weekday)
    """
    # Monday=0, Sunday=6
    while date.weekday() >= 5:  # Saturday=5, Sunday=6
        date += timedelta(days=1)
    return date


def get_default_date_range(bronze: BronzeStorage) -> tuple[datetime, datetime]:
    """Determine default date range for ingestion.

    Logic:
    - If data exists, start from day after latest date
    - If no data exists, start from 1 year ago
    - Ensure start date is a weekday
    - End date is 2 weeks after start date

    Args:
        bronze: BronzeStorage instance to check for existing data

    Returns:
        Tuple of (from_date, to_date)
    """
    # Check for existing data
    existing_dates = bronze.list_available_dates("daily_quotes")

    if existing_dates:
        # Start from day after latest date
        latest_date = existing_dates[-1]  # list_available_dates returns sorted list
        from_date = latest_date + timedelta(days=1)
        logger.info(f"Latest data found: {latest_date.strftime('%Y-%m-%d')}")
    else:
        # No data exists, start from 1 year ago
        from_date = datetime.now() - timedelta(days=365)
        logger.info("No existing data found, starting from 1 year ago")

    # Ensure from_date is a weekday
    from_date = get_next_weekday(from_date)

    # End date is 2 weeks later
    to_date = from_date + timedelta(weeks=2)

    logger.info(
        f"Default date range: {from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}"
    )

    return from_date, to_date


def main() -> int:
    """Main ingestion workflow."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Ingest J-Quants daily quotes data")
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date for range ingestion (YYYYMMDD). Defaults to day after latest stored date (or 1 year ago if no data).",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date for range ingestion (YYYYMMDD). Defaults to 2 weeks after from_date.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion even if data already exists",
    )

    args = parser.parse_args()
    client = JQuantsClient.from_env()
    bronze = BronzeStorage()

    try:
        # Determine date range
        if args.from_date and args.to_date:
            # Both dates provided
            start_date = datetime.strptime(args.from_date, "%Y%m%d")
            end_date = datetime.strptime(args.to_date, "%Y%m%d")
        elif args.from_date:
            # Only from_date provided, use 2 weeks later as end
            start_date = datetime.strptime(args.from_date, "%Y%m%d")
            end_date = start_date + timedelta(weeks=2)
        elif args.to_date:
            # Only to_date provided, calculate from_date based on latest data
            default_from, _ = get_default_date_range(bronze)
            start_date = default_from
            end_date = datetime.strptime(args.to_date, "%Y%m%d")
        else:
            # No dates provided, use smart defaults
            start_date, end_date = get_default_date_range(bronze)

        # Build list of dates to process
        dates_to_process = []
        current_date = start_date
        while current_date <= end_date:
            dates_to_process.append(current_date)
            current_date += timedelta(days=1)

        # Call the ingestion function
        ingest_daily_quotes(
            client=client,
            bronze=bronze,
            dates=dates_to_process,
            force=args.force,
        )

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1


if __name__ == "__main__":
    main()
