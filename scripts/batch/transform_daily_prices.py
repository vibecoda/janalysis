#!/usr/bin/env python
"""Transform daily prices from silver to gold layer (stock-centric format)."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime

from jqsys.data.ingest import transform_daily_prices
from jqsys.data.layers.gold import GoldStorage
from jqsys.data.layers.silver import SilverStorage

logger = logging.getLogger(__name__)


def get_default_date_range(silver: SilverStorage) -> tuple[date, date] | None:
    """Determine default date range for transformation.

    Logic:
    - If silver data exists, use all available dates
    - If no data exists, return None

    Args:
        silver: SilverStorage instance to check for existing data

    Returns:
        Tuple of (from_date, to_date) or None if no data exists
    """
    # Check for existing data in silver layer (returns date objects)
    existing_dates = silver.list_available_dates("daily_prices")

    if not existing_dates:
        logger.warning("No data found in silver layer")
        return None

    # Use all available dates
    from_date = existing_dates[0]  # list_available_dates returns sorted list
    to_date = existing_dates[-1]

    logger.info(
        f"Found {len(existing_dates)} dates in silver layer: "
        f"{from_date.isoformat()} to {to_date.isoformat()}"
    )

    return from_date, to_date


def main() -> int:
    """Main transformation workflow."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Transform J-Quants daily prices from silver to gold layer (stock-centric)"
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date for range transformation (YYYYMMDD). Defaults to earliest date in silver layer.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date for range transformation (YYYYMMDD). Defaults to latest date in silver layer.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-transformation even if data already exists",
    )

    args = parser.parse_args()

    silver = SilverStorage()
    gold = GoldStorage(silver_storage=silver)

    try:
        # Determine date range
        if args.from_date and args.to_date:
            # Both dates provided
            start_date = datetime.strptime(args.from_date, "%Y%m%d").date()
            end_date = datetime.strptime(args.to_date, "%Y%m%d").date()
        elif args.from_date:
            # Only from_date provided, use latest silver date as end
            start_date = datetime.strptime(args.from_date, "%Y%m%d").date()
            default_range = get_default_date_range(silver)
            if default_range:
                _, end_date = default_range
            else:
                logger.error("No data in silver layer")
                return 1
        elif args.to_date:
            # Only to_date provided, use earliest silver date as start
            end_date = datetime.strptime(args.to_date, "%Y%m%d").date()
            default_range = get_default_date_range(silver)
            if default_range:
                start_date, _ = default_range
            else:
                logger.error("No data in silver layer")
                return 1
        else:
            # No dates provided, use all dates from silver layer
            default_range = get_default_date_range(silver)
            if default_range:
                start_date, end_date = default_range
            else:
                logger.error("No data in silver layer to transform")
                return 1

        logger.info(f"Transforming date range: {start_date.isoformat()} to {end_date.isoformat()}")

        # Call the transformation function
        stats = transform_daily_prices(
            gold=gold,
            start_date=start_date,
            end_date=end_date,
            force=args.force,
        )

        logger.info(
            f"Successfully transformed {stats['dates_processed']} dates, "
            f"updated {stats['stocks_updated']} stocks, "
            f"wrote {stats['records_written']} records"
        )
        return 0

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        return 1


if __name__ == "__main__":
    main()
