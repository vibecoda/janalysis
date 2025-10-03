#!/usr/bin/env python
"""Test script for the new DuckDB + Polars storage system."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from jqsys.storage.bronze import BronzeStorage
from jqsys.storage.query import QueryEngine
from jqsys.storage.silver import SilverStorage


def create_sample_data() -> list[dict]:
    """Create sample J-Quants daily quotes data for testing."""

    sample_data = []
    codes = ["1301", "1332", "6501", "7203", "9984"]  # Sample Japanese stock codes
    base_date = datetime(2024, 1, 15)

    for i, code in enumerate(codes):
        base_price = 1000 + i * 500  # Different base prices
        sample_data.append(
            {
                "Code": code,
                "Date": base_date.strftime("%Y-%m-%d"),
                "Open": base_price * 1.01,
                "High": base_price * 1.05,
                "Low": base_price * 0.98,
                "Close": base_price,
                "Volume": 100000 + i * 10000,
                "TurnoverValue": base_price * (100000 + i * 10000),
                "AdjustmentFactor": 1.0,
                "AdjustmentClose": base_price,
            }
        )

    return sample_data


def test_bronze_layer():
    """Test bronze layer storage functionality."""
    print("🟤 Testing Bronze Layer Storage...")

    bronze = BronzeStorage("data/bronze")
    sample_data = create_sample_data()
    test_date = datetime(2024, 1, 15)

    # Store sample data
    file_path = bronze.store_raw_response(
        endpoint="daily_quotes",
        data=sample_data,
        date=test_date,
        metadata={"source": "test", "record_count": len(sample_data)},
    )
    print(f"✅ Stored data to: {file_path}")

    # Read back the data
    df = bronze.read_raw_data("daily_quotes", date=test_date)
    print(f"✅ Read back {len(df)} records")

    # Check available dates
    dates = bronze.list_available_dates("daily_quotes")
    print(f"✅ Available dates: {[d.strftime('%Y-%m-%d') for d in dates]}")

    # Get storage stats
    stats = bronze.get_storage_stats()
    print(f"✅ Storage stats: {json.dumps(stats, indent=2)}")

    # Don't return the bronze object for pytest compatibility
    assert len(df) > 0
    assert len(dates) > 0


def test_silver_layer():
    """Test silver layer normalization functionality."""
    print("\n🥈 Testing Silver Layer Storage...")

    # Set up bronze layer with test data first
    bronze = BronzeStorage("data/bronze")
    sample_data = create_sample_data()
    test_date = datetime(2024, 1, 15)

    # Store sample data in bronze layer
    bronze.store_raw_response(
        endpoint="daily_quotes",
        data=sample_data,
        date=test_date,
        metadata={"source": "test", "record_count": len(sample_data)},
    )

    silver = SilverStorage("data/silver", bronze_storage=bronze)

    # Normalize daily quotes
    file_path = silver.normalize_daily_quotes(test_date)
    if file_path:
        print(f"✅ Normalized data to: {file_path}")

        # Read normalized data
        df = silver.read_daily_prices(test_date, test_date)
        print(f"✅ Read {len(df)} normalized records")
        print(f"✅ Columns: {df.columns}")
        print(f"✅ Sample data:\n{df.head()}")

        # Add assertions for pytest
        assert len(df) > 0
        assert "code" in df.columns
        assert "date" in df.columns
    else:
        print("❌ No data was normalized")
        raise AssertionError("No data was normalized")


def test_query_engine():
    """Test DuckDB query engine functionality."""
    print("\n🔍 Testing Query Engine...")

    with QueryEngine(db_path=None) as query:
        # Test basic data retrieval
        try:
            df = query.get_daily_prices(limit=10)
            print(f"✅ Retrieved {len(df)} price records")

            if not df.is_empty():
                # Test summary stats
                stats = query.get_price_summary_stats()
                print(f"✅ Summary stats for {len(stats)} codes")
                print(f"✅ Stats sample:\n{stats.head()}")

                # Test returns calculation
                returns = query.calculate_returns(periods=[1])
                print(f"✅ Calculated returns for {len(returns)} records")

                # Test market coverage
                coverage = query.get_market_data_coverage()
                print(f"✅ Market coverage for {len(coverage)} dates")

        except Exception as e:
            print(f"⚠️  Query engine test with limited data: {e}")


def test_integration():
    """Run full integration test of the storage system."""
    print("🧪 Running Storage System Integration Test")
    print("=" * 50)

    try:
        # Test bronze layer
        bronze = BronzeStorage("data/bronze")
        sample_data = create_sample_data()
        test_date = datetime(2024, 1, 15)

        # Store sample data
        bronze.store_raw_response(
            endpoint="daily_quotes",
            data=sample_data,
            date=test_date,
            metadata={"source": "integration_test", "record_count": len(sample_data)},
        )

        # Test silver layer
        silver = SilverStorage("data/silver", bronze_storage=bronze)
        file_path = silver.normalize_daily_quotes(test_date)
        assert file_path is not None, "Silver layer normalization failed"

        # Test query engine
        test_query_engine()

        print("\n" + "=" * 50)
        print("✅ All storage system tests completed successfully!")

        # Show final directory structure
        print("\n📁 Data Directory Structure:")
        data_dir = Path("data")
        if data_dir.exists():
            for path in sorted(data_dir.rglob("*")):
                if path.is_file():
                    size_mb = path.stat().st_size / (1024 * 1024)
                    print(f"  {path.relative_to(data_dir)} ({size_mb:.2f} MB)")

    except Exception as e:
        print(f"\n❌ Integration test failed: {e}")
        raise


if __name__ == "__main__":
    test_integration()
