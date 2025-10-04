#!/usr/bin/env python
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from jqsys.data.client import JQuantsClient


def get_default_date() -> str:
    """Get a default date that is a weekday at least 12 weeks earlier.

    Returns:
        Date string in YYYYMMDD format
    """
    # Start from 12 weeks ago
    date = datetime.now() - timedelta(weeks=12)

    # If it's a weekend, move to previous Friday
    # Monday=0, Sunday=6
    while date.weekday() >= 5:  # Saturday=5, Sunday=6
        date -= timedelta(days=1)

    return date.strftime("%Y%m%d")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch daily stock prices (/v1/prices/daily_quotes)"
    )
    parser.add_argument("--code", help="Issue code (optional if date is provided)", default="")
    parser.add_argument(
        "--date",
        help="Date YYYYMMDD (defaults to a weekday at least 12 weeks ago)",
        default="",
    )
    parser.add_argument("--from", dest="from_", help="From date YYYYMMDD", default="")
    parser.add_argument("--to", help="To date YYYYMMDD", default="")
    parser.add_argument("--save", help="Path to save CSV/Parquet (by extension)", default="")
    parser.add_argument("--limit", type=int, default=20, help="Rows to display")
    args = parser.parse_args()

    client = JQuantsClient.from_env()
    params = {}
    if args.code:
        params["code"] = args.code

    # Use provided date or default to a weekday 12 weeks ago
    date = args.date if args.date else get_default_date()
    if date:
        params["date"] = date
        print(f"Fetching data for date: {date}")

    if args.from_:
        params["from"] = args.from_
    if args.to:
        params["to"] = args.to

    rows = client.get_paginated("/v1/prices/daily_quotes", data_key="daily_quotes", params=params)
    df = pd.DataFrame(rows)

    if args.save:
        out = Path(args.save)
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.suffix.lower() == ".parquet":
            df.to_parquet(out, index=False)
        else:
            df.to_csv(out, index=False)
        print(f"Saved {len(df)} rows to {out}")

    if args.limit:
        print(df.head(args.limit).to_string(index=False))
    else:
        print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
