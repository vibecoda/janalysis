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


def main() -> int:
    """Main ingestion workflow."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Ingest J-Quants daily quotes data")
    parser.add_argument(
        "--date",
        type=str,
        help="Date to ingest (YYYYMMDD format). Defaults to yesterday.",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date for range ingestion (YYYYMMDD)",
    )
    parser.add_argument(
        "--to", dest="to_date", type=str, help="End date for range ingestion (YYYYMMDD)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion even if data already exists",
    )

    args = parser.parse_args()
    client = JQuantsClient.from_env()

    try:
        # Determine dates to process
        dates_to_process = []

        if args.from_date and args.to_date:
            # Date range
            start_date = datetime.strptime(args.from_date, "%Y%m%d")
            end_date = datetime.strptime(args.to_date, "%Y%m%d")

            current_date = start_date
            while current_date <= end_date:
                dates_to_process.append(current_date)
                current_date += timedelta(days=1)

        elif args.date:
            # Single date
            date = datetime.strptime(args.date, "%Y%m%d")
            dates_to_process.append(date)
        else:
            # Default to yesterday
            yesterday = datetime.now() - timedelta(days=1)
            dates_to_process.append(yesterday)

        # Call the ingestion function
        ingest_daily_quotes(
            client=client,
            bronze=BronzeStorage(),
            dates=dates_to_process,
            force=args.force,
        )

        return 0

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
