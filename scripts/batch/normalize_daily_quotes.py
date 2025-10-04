#!/usr/bin/env python
"""Normalize daily quotes data from bronze to silver layer."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta

from jqsys.data.ingest import normalize_daily_quotes
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.silver import SilverStorage

logger = logging.getLogger(__name__)


def get_default_date_range(bronze: BronzeStorage) -> tuple[date, date] | None:
    """Determine default date range for normalization.

    Logic:
    - If bronze data exists, use all available dates
    - If no data exists, return None

    Args:
        bronze: BronzeStorage instance to check for existing data

    Returns:
        Tuple of (from_date, to_date) or None if no data exists
    """
    # Check for existing data in bronze layer (returns datetime objects)
    existing_datetimes = bronze.list_available_dates("daily_quotes")

    if not existing_datetimes:
        logger.warning("No data found in bronze layer")
        return None

    # Convert datetime to date objects
    from_date = existing_datetimes[0].date()  # list_available_dates returns sorted list
    to_date = existing_datetimes[-1].date()

    logger.info(
        f"Found {len(existing_datetimes)} dates in bronze layer: "
        f"{from_date.isoformat()} to {to_date.isoformat()}"
    )

    return from_date, to_date


def main() -> int:
    """Main normalization workflow."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Normalize J-Quants daily quotes from bronze to silver"
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date for range normalization (YYYYMMDD). Defaults to earliest date in bronze layer.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date for range normalization (YYYYMMDD). Defaults to latest date in bronze layer.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-normalization even if data already exists",
    )

    args = parser.parse_args()

    bronze = BronzeStorage()
    silver = SilverStorage(bronze_storage=bronze)

    try:
        # Determine date range
        if args.from_date and args.to_date:
            # Both dates provided
            start_date = datetime.strptime(args.from_date, "%Y%m%d").date()
            end_date = datetime.strptime(args.to_date, "%Y%m%d").date()
        elif args.from_date:
            # Only from_date provided, use latest bronze date as end
            start_date = datetime.strptime(args.from_date, "%Y%m%d").date()
            default_range = get_default_date_range(bronze)
            if default_range:
                _, end_date = default_range
            else:
                logger.error("No data in bronze layer")
                return 1
        elif args.to_date:
            # Only to_date provided, use earliest bronze date as start
            end_date = datetime.strptime(args.to_date, "%Y%m%d").date()
            default_range = get_default_date_range(bronze)
            if default_range:
                start_date, _ = default_range
            else:
                logger.error("No data in bronze layer")
                return 1
        else:
            # No dates provided, use all dates from bronze layer
            default_range = get_default_date_range(bronze)
            if default_range:
                start_date, end_date = default_range
            else:
                logger.error("No data in bronze layer to normalize")
                return 1

        logger.info(f"Normalizing date range: {start_date.isoformat()} to {end_date.isoformat()}")

        # Build list of dates to process
        dates_to_process = []
        current_date = start_date
        while current_date <= end_date:
            dates_to_process.append(current_date)
            current_date += timedelta(days=1)

        # Call the normalization function
        successful = normalize_daily_quotes(
            silver=silver,
            dates=dates_to_process,
            force=args.force,
        )

        logger.info(f"Successfully normalized {successful} out of {len(dates_to_process)} dates")
        return 0

    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        return 1


if __name__ == "__main__":
    main()
