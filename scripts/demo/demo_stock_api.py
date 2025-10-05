#!/usr/bin/env python
"""Demonstrate the jqsys.fin.Stock API using local storages."""

from __future__ import annotations

import logging
from textwrap import dedent

from jqsys.core.utils.env import load_env_file_if_present
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.gold import GoldStorage
from jqsys.fin import Stock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def print_header(title: str) -> None:
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main() -> int:
    load_env_file_if_present()

    bronze = BronzeStorage()
    gold = GoldStorage()

    print_header("STOCK API DEMO")
    print(
        dedent(
            """
            This demo uses existing bronze listed_info snapshots and gold price data.
            Ensure you've ingested data (e.g. via scripts/batch) before running.
            """
        )
    )

    try:
        print_header("SEARCHING FOR SECURITIES")
        results = Stock.search(
            "CompanyNameEnglish",
            "Kyokuyo",
            bronze_storage=bronze,
            gold_storage=gold,
            match="icontains",
        )
        if not results:
            print("No matches found for 'Kyokuyo'. Try adjusting the search term.")
            return 0

        for stock in results:
            print(f"- {stock.code}: {stock.company_name}")

        primary = results[0]

        print_header(f"BASIC INFO - {primary.code}")
        info = primary.get_listed_info()
        for key in [
            "CompanyName",
            "CompanyNameEnglish",
            "MarketCodeName",
            "Sector17CodeName",
        ]:
            print(f"{key}: {info.get(key)}")

        print_header("PRICE HISTORY")
        history = primary.get_price_history(adjust="add")
        if history.is_empty():
            print("No price data found in gold storage for this stock.")
        else:
            print(history.sort("date").tail(5))
            latest = primary.get_latest_price()
            if latest:
                print("\nMost recent close:", latest.get("close"))

            print("\nAdjusted close series (latest 5):")
            print(primary.close_series().tail(5))

        return 0

    except Exception as exc:  # pragma: no cover - demo output only
        logging.exception("Stock demo failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
