#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd


from jqsys.client import JQuantsClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch listed securities info (/v1/listed/info)")
    parser.add_argument("--code", help="Issue code (optional)", default="")
    parser.add_argument("--date", help="As of date YYYYMMDD (optional)", default="")
    parser.add_argument("--limit", type=int, default=20, help="Rows to display")
    args = parser.parse_args()

    client = JQuantsClient.from_env()
    params = {}
    if args.code:
        params["code"] = args.code
    if args.date:
        params["date"] = args.date

    rows = client.get_paginated("/v1/listed/info", data_key="info", params=params)
    df = pd.DataFrame(rows)
    if args.limit:
        print(df.head(args.limit).to_string(index=False))
    else:
        print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
