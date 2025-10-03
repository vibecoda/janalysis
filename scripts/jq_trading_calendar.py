#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd

from jqsys.client import JQuantsClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch trading calendar (/v1/markets/trading_calendar)"
    )
    parser.add_argument(
        "--holidaydivision",
        choices=["0", "1", "2", "3"],
        default="",
        help="Holiday division (optional)",
    )
    parser.add_argument("--from", dest="from_", default="", help="From date YYYYMMDD")
    parser.add_argument("--to", default="", help="To date YYYYMMDD")
    parser.add_argument("--limit", type=int, default=20, help="Rows to display")
    args = parser.parse_args()

    client = JQuantsClient.from_env()
    params = {}
    if args.holidaydivision:
        params["holidaydivision"] = args.holidaydivision
    if args.from_:
        params["from"] = args.from_
    if args.to:
        params["to"] = args.to

    rows = client.get_paginated(
        "/v1/markets/trading_calendar", data_key="trading_calendar", params=params
    )
    df = pd.DataFrame(rows)
    if args.limit:
        print(df.head(args.limit).to_string(index=False))
    else:
        print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
