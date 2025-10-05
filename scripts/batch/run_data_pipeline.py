#!/usr/bin/env python
"""Run the full J-Quants data pipeline for daily pricing data.

This orchestrates the following steps for a supplied (or default) date range:

1. Fetch the latest listed company information into the bronze layer.
2. Ingest raw daily quote data into the bronze layer.
3. Normalize the bronze data into the silver layer.
4. Transform the normalized data into the gold layer (stock-centric views).

Default date handling mirrors ``ingest_daily_quotes.py`` so users get the same
experience whether they run the individual scripts or this consolidated flow.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

from jqsys.data.client import JQuantsClient
from jqsys.data.ingest import (
    ingest_daily_quotes,
    ingest_listed_info,
    normalize_daily_quotes,
    transform_daily_prices,
)
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.gold import GoldStorage
from jqsys.data.layers.silver import SilverStorage
from scripts.batch.ingest_daily_quotes import (
    get_default_date_range as get_ingest_default_range,
)

logger = logging.getLogger(__name__)


def _build_date_sequence(start: datetime, end: datetime) -> list[datetime]:
    """Inclusive sequence of daily datetimes between start and end."""

    if start > end:
        raise ValueError("start date must be on or before end date")

    current = start
    dates: list[datetime] = []
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _determine_date_range(
    bronze: BronzeStorage,
    from_arg: str | None,
    to_arg: str | None,
) -> tuple[datetime, datetime]:
    """Resolve the ingest date window using the same rules as ingest_daily_quotes."""

    if from_arg and to_arg:
        start = datetime.strptime(from_arg, "%Y%m%d")
        end = datetime.strptime(to_arg, "%Y%m%d")
        return start, end

    if from_arg:
        start = datetime.strptime(from_arg, "%Y%m%d")
        end = start + timedelta(weeks=2)
        return start, end

    if to_arg:
        default_start, _ = get_ingest_default_range(bronze)
        end = datetime.strptime(to_arg, "%Y%m%d")
        return default_start, end

    return get_ingest_default_range(bronze)


def main() -> int:
    """Run the combined ingestion/normalization/transformation workflow."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Run the full J-Quants data pipeline (listed info, bronze, silver, gold)"
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        help="Start date for range ingestion (YYYYMMDD). Defaults mirror ingest_daily_quotes.",
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        help="End date for range ingestion (YYYYMMDD). Defaults mirror ingest_daily_quotes.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-processing for all steps even if data already exists.",
    )
    parser.add_argument(
        "--skip-listed-info",
        action="store_true",
        help="Skip refreshing listed company information (defaults to ingest).",
    )

    args = parser.parse_args()

    client = JQuantsClient.from_env()
    bronze = BronzeStorage()
    silver = SilverStorage(bronze_storage=bronze)
    gold = GoldStorage(silver_storage=silver)

    try:
        start_dt, end_dt = _determine_date_range(bronze, args.from_date, args.to_date)
        date_sequence = _build_date_sequence(start_dt, end_dt)
        date_sequence_dates = [dt.date() for dt in date_sequence]

        logger.info(
            "Pipeline date range: %s → %s (%d trading days)",
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
            len(date_sequence),
        )

        if not args.skip_listed_info:
            logger.info("Fetching latest listed company information…")
            ingest_listed_info(client=client, bronze=bronze, force=args.force)
        else:
            logger.info("Skipping listed company ingestion as requested")

        logger.info("Ingesting daily quotes into bronze layer…")
        ingest_daily_quotes(
            client=client,
            bronze=bronze,
            dates=date_sequence,
            force=args.force,
        )

        logger.info("Normalizing daily quotes into silver layer…")
        normalize_daily_quotes(
            silver=silver,
            dates=date_sequence_dates,
            force=args.force,
        )

        logger.info("Transforming silver data into gold layer…")
        stats = transform_daily_prices(
            gold=gold,
            start_date=date_sequence_dates[0],
            end_date=date_sequence_dates[-1],
            force=args.force,
        )

        logger.info(
            "Pipeline complete: %d dates processed, %d stocks updated, %d records written",
            stats["dates_processed"],
            stats["stocks_updated"],
            stats["records_written"],
        )

    except Exception as exc:  # pragma: no cover - script level error handling
        logger.error("Pipeline failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
