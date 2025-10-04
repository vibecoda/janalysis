#!/usr/bin/env python
"""Ingest listed company info using the bronze storage system.

This script fetches the latest snapshot of listed company information from J-Quants.
The API does not support historical queries - it always returns the current state.
"""

from __future__ import annotations

import argparse
import logging

from jqsys.data.client import JQuantsClient
from jqsys.data.ingest import ingest_listed_info
from jqsys.data.layers.bronze import BronzeStorage

logger = logging.getLogger(__name__)


def main() -> int:
    """Main ingestion workflow."""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Ingest J-Quants listed company info (latest snapshot)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion even if data already exists for today",
    )

    args = parser.parse_args()
    client = JQuantsClient.from_env()
    bronze = BronzeStorage()

    try:
        # Fetch and store the latest listed company info
        record_count = ingest_listed_info(
            client=client,
            bronze=bronze,
            force=args.force,
        )

        print(f"\nSuccessfully ingested {record_count} listed company records")

    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
