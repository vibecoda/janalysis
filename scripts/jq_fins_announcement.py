#!/usr/bin/env python
from __future__ import annotations

import argparse

import pandas as pd

from jqsys.client import JQuantsClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch earnings calendar (/v1/fins/announcement)")
    parser.add_argument("--limit", type=int, default=20, help="Rows to display")
    args = parser.parse_args()

    client = JQuantsClient.from_env()
    rows = client.get_paginated("/v1/fins/announcement", data_key="announcement", params={})
    df = pd.DataFrame(rows)
    if args.limit:
        print(df.head(args.limit).to_string(index=False))
    else:
        print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
