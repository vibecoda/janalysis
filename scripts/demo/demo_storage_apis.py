#!/usr/bin/env python
"""Demonstration of Bronze and Silver storage APIs capabilities.

This script showcases the various features and capabilities of the storage layers
using data that has already been ingested into the system.
"""

from __future__ import annotations

import argparse
import logging
from datetime import timedelta

import polars as pl

from jqsys.core.utils.env import load_env_file_if_present
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.silver import SilverStorage

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def demo_bronze_storage():
    """Demonstrate Bronze storage layer capabilities."""
    print("\n" + "=" * 60)
    print("🥉 BRONZE STORAGE LAYER DEMONSTRATION")
    print("=" * 60)

    bronze = BronzeStorage()

    # 1. Show storage statistics
    print("\n📊 Storage Statistics:")
    stats = bronze.get_storage_stats()
    print(f"Total files: {stats['total_files']}")
    print(f"Total size: {stats['total_size_mb']:.2f} MB")

    for endpoint, endpoint_stats in stats["endpoints"].items():
        print(f"\n  {endpoint}:")
        print(f"    - Dates: {endpoint_stats['dates']}")
        print(f"    - Files: {endpoint_stats['files']}")
        print(f"    - Size: {endpoint_stats['size_mb']:.2f} MB")

    # 2. List available dates
    print("\n📅 Available Dates for daily_quotes:")
    available_dates = bronze.list_available_dates("daily_quotes")
    if available_dates:
        print(f"First date: {available_dates[0].strftime('%Y-%m-%d')}")
        print(f"Last date: {available_dates[-1].strftime('%Y-%m-%d')}")
        print(f"Total trading days: {len(available_dates)}")

        # Show first few and last few dates
        if len(available_dates) > 10:
            print(f"First 5 dates: {[d.strftime('%Y-%m-%d') for d in available_dates[:5]]}")
            print(f"Last 5 dates: {[d.strftime('%Y-%m-%d') for d in available_dates[-5:]]}")
        else:
            print(f"All dates: {[d.strftime('%Y-%m-%d') for d in available_dates]}")
    else:
        print("No data available")
        return

    # 3. Read raw data for a specific date
    print("\n📖 Reading Raw Data Sample:")
    sample_date = available_dates[0]  # Use first available date
    raw_data = bronze.read_raw_data("daily_quotes", date=sample_date)

    if not raw_data.is_empty():
        print(f"Sample date: {sample_date.strftime('%Y-%m-%d')}")
        print(f"Records: {len(raw_data)}")
        print(f"Columns: {raw_data.columns}")

        # Show sample records
        print("\nFirst 3 records:")
        sample_records = raw_data.select(
            ["Code", "Date", "Open", "High", "Low", "Close", "Volume"]
        ).head(3)
        print(sample_records)

        # Show metadata columns
        print("\nMetadata columns:")
        metadata_cols = [col for col in raw_data.columns if col.startswith("_")]
        if metadata_cols:
            metadata_sample = raw_data.select(metadata_cols).head(1)
            print(metadata_sample)

    # 4. Read data for a date range
    if len(available_dates) >= 3:
        print("\n📊 Reading Date Range Sample:")
        start_date = available_dates[0]
        end_date = available_dates[2]  # First 3 days

        range_data = bronze.read_raw_data("daily_quotes", date_range=(start_date, end_date))

        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Total records: {len(range_data)}")

        # Group by date to show daily counts
        daily_counts = range_data.group_by("Date").agg(pl.len().alias("record_count")).sort("Date")
        print("Daily record counts:")
        print(daily_counts)


def demo_silver_storage():
    """Demonstrate Silver storage layer capabilities."""
    print("\n" + "=" * 60)
    print("🥈 SILVER STORAGE LAYER DEMONSTRATION")
    print("=" * 60)

    bronze = BronzeStorage()
    silver = SilverStorage(bronze_storage=bronze)

    # Get available dates from bronze
    available_dates = bronze.list_available_dates("daily_quotes")
    if not available_dates:
        print("No bronze data available for silver layer demo")
        return

    # 1. Read normalized daily prices
    print("\n📈 Reading Normalized Daily Prices:")
    start_date = available_dates[0]
    end_date = min(available_dates[-1], start_date + timedelta(days=7))  # First week or all data

    daily_prices = silver.read_daily_prices(start_date, end_date)

    if not daily_prices.is_empty():
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Total records: {len(daily_prices)}")
        print(f"Unique stocks: {daily_prices['code'].n_unique()}")
        print(f"Columns: {daily_prices.columns}")

        # Show sample normalized data
        print("\nSample normalized records:")
        sample_data = daily_prices.select(
            ["code", "date", "open", "high", "low", "close", "volume", "adj_close"]
        ).head(5)
        print(sample_data)

        # Show data quality metrics
        print("\n🔍 Data Quality Metrics:")
        quality_stats = daily_prices.select(
            [
                pl.col("close").null_count().alias("null_closes"),
                pl.col("volume").null_count().alias("null_volumes"),
                pl.col("close").min().alias("min_close"),
                pl.col("close").max().alias("max_close"),
                pl.col("volume").sum().alias("total_volume"),
            ]
        )
        print(quality_stats)

        # Show top stocks by average price
        print("\n💰 Top 10 Stocks by Average Close Price:")
        top_stocks = (
            daily_prices.group_by("code")
            .agg(
                [
                    pl.col("close").mean().alias("avg_close"),
                    pl.col("volume").mean().alias("avg_volume"),
                    pl.len().alias("trading_days"),
                ]
            )
            .sort("avg_close", descending=True)
            .head(10)
        )
        print(top_stocks)

    # 2. Filter by specific stock codes
    if not daily_prices.is_empty():
        print("\n🎯 Filtering by Specific Stock Codes:")
        # Get some sample codes
        sample_codes = daily_prices["code"].unique().head(3).to_list()

        filtered_data = silver.read_daily_prices(start_date, end_date, codes=sample_codes)

        print(f"Sample codes: {sample_codes}")
        print(f"Filtered records: {len(filtered_data)}")

        # Show price trends for these stocks
        price_trends = (
            filtered_data.group_by("code")
            .agg(
                [
                    pl.col("close").first().alias("first_close"),
                    pl.col("close").last().alias("last_close"),
                    (
                        (pl.col("close").last() - pl.col("close").first())
                        / pl.col("close").first()
                        * 100
                    ).alias("return_pct"),
                ]
            )
            .sort("return_pct", descending=True)
        )
        print("Price trends (% return):")
        print(price_trends)


def demo_integration_example():
    """Demonstrate integration between all storage layers."""
    print("\n" + "=" * 60)
    print("🔗 INTEGRATION EXAMPLE")
    print("=" * 60)

    print("\n🎯 End-to-End Data Flow Example:")
    print("Bronze (Raw) → Silver (Normalized) → Gold (Stock-centric)")

    bronze = BronzeStorage()
    silver = SilverStorage(bronze_storage=bronze)

    # Get available data
    available_dates = bronze.list_available_dates("daily_quotes")
    if not available_dates:
        print("No data available for integration demo")
        return

    sample_date = available_dates[0]

    # 1. Bronze layer - raw data
    print(f"\n1️⃣ Bronze Layer - Raw data for {sample_date.strftime('%Y-%m-%d')}:")
    raw_data = bronze.read_raw_data("daily_quotes", date=sample_date)
    print(f"   Raw records: {len(raw_data)}")
    print(f"   Raw columns: {len(raw_data.columns)}")

    # 2. Silver layer - normalized data
    print("\n2️⃣ Silver Layer - Normalized data:")
    normalized_data = silver.read_daily_prices(sample_date, sample_date)
    print(f"   Normalized records: {len(normalized_data)}")
    print(f"   Data quality: {len(normalized_data)} valid records")

    # 3. Summary analytics with Polars
    print("\n3️⃣ Analytics with Polars:")
    try:
        summary = normalized_data.select(
            [
                pl.count("code").alias("unique_stocks"),
                pl.col("close").mean().alias("avg_close_price"),
                pl.col("volume").sum().alias("total_volume"),
                pl.col("close").max().alias("highest_price"),
                pl.col("close").min().alias("lowest_price"),
            ]
        )

        print(f"   Market summary for {sample_date.strftime('%Y-%m-%d')}:")
        print(summary)

    except Exception as e:
        print(f"   Analytics failed: {e}")

    print("\n✅ Integration flow completed successfully!")


def main() -> int:
    """Main demonstration workflow."""
    parser = argparse.ArgumentParser(description="Demonstrate storage APIs capabilities")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "integration", "all"],
        default="all",
        help="Which layer to demonstrate",
    )

    args = parser.parse_args()

    # Load environment
    load_env_file_if_present()

    print("🏗️ STORAGE APIS DEMONSTRATION")
    print("This demo showcases the capabilities of Bronze and Silver storage layers")
    print("using data that has already been ingested into the system.")

    try:
        if args.layer in ["bronze", "all"]:
            demo_bronze_storage()

        if args.layer in ["silver", "all"]:
            demo_silver_storage()

        if args.layer in ["integration", "all"]:
            demo_integration_example()

        print("\n" + "=" * 60)
        print("✅ DEMONSTRATION COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("\nKey takeaways:")
        print("• Bronze layer stores raw API responses with full lineage")
        print("• Silver layer provides normalized, validated data for analysis")
        print("• Gold layer provides stock-centric data access")
        print("• All layers use Polars for efficient data processing")

        return 0

    except Exception as e:
        logger.error(f"Demonstration failed: {e}")
        return 1


if __name__ == "__main__":
    main()
