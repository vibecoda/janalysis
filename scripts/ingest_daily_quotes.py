#!/usr/bin/env python
"""Ingest daily quotes data using the new DuckDB + Polars storage system."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

from jqsys.auth import get_id_token, load_refresh_token
from jqsys.client import JQuantsClient
from jqsys.storage.bronze import BronzeStorage
from jqsys.storage.silver import SilverStorage
from jqsys.storage.query import QueryEngine
from jqsys.utils.env import load_env_file_if_present

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main ingestion workflow."""
    parser = argparse.ArgumentParser(description="Ingest J-Quants daily quotes data")
    parser.add_argument(
        "--date", 
        type=str,
        help="Date to ingest (YYYYMMDD format). Defaults to yesterday."
    )
    parser.add_argument(
        "--from", 
        dest="from_date",
        type=str,
        help="Start date for range ingestion (YYYYMMDD)"
    )
    parser.add_argument(
        "--to",
        dest="to_date", 
        type=str,
        help="End date for range ingestion (YYYYMMDD)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-ingestion even if data already exists"
    )
    parser.add_argument(
        "--query",
        action="store_true",
        help="Run sample queries after ingestion"
    )
    
    args = parser.parse_args()
    
    # Load environment and get authentication token
    load_env_file_if_present()
    refresh_token = load_refresh_token()
    id_token = get_id_token(refresh_token)
    
    # Initialize storage components
    bronze = BronzeStorage()
    silver = SilverStorage(bronze_storage=bronze)
    client = JQuantsClient(id_token)
    
    try:
        # Determine dates to process
        dates_to_process = []
        
        if args.from_date and args.to_date:
            # Date range
            start_date = datetime.strptime(args.from_date, "%Y%m%d")
            end_date = datetime.strptime(args.to_date, "%Y%m%d")
            
            current_date = start_date
            while current_date <= end_date:
                dates_to_process.append(current_date)
                current_date += timedelta(days=1)
                
        elif args.date:
            # Single date
            date = datetime.strptime(args.date, "%Y%m%d")
            dates_to_process.append(date)
        else:
            # Default to yesterday
            yesterday = datetime.now() - timedelta(days=1)
            dates_to_process.append(yesterday)
        
        logger.info(f"Processing {len(dates_to_process)} dates")
        
        # Process each date
        total_records = 0
        for date in dates_to_process:
            logger.info(f"Processing date: {date.strftime('%Y-%m-%d')}")
            
            # Check if already processed (unless force)
            if not args.force:
                existing_dates = bronze.list_available_dates("daily_quotes")
                if date in existing_dates:
                    logger.info(f"Data already exists for {date.strftime('%Y-%m-%d')}, skipping")
                    continue
            
            try:
                # Fetch data from J-Quants API
                logger.info("Fetching data from J-Quants API...")
                params = {"date": date.strftime("%Y%m%d")}  # J-Quants expects YYYYMMDD format
                data = client.get_paginated("/v1/prices/daily_quotes", data_key="daily_quotes", params=params)
                
                if not data:
                    logger.warning(f"No data returned for {date.strftime('%Y-%m-%d')}")
                    continue
                
                logger.info(f"Fetched {len(data)} records from API")
                
                # Store in bronze layer
                bronze_path = bronze.store_raw_response(
                    endpoint="daily_quotes",
                    data=data,
                    date=date,
                    metadata={
                        "api_call": "/v1/prices/daily_quotes", 
                        "date_param": date.strftime("%Y%m%d"),
                        "record_count": len(data)
                    }
                )
                logger.info(f"Stored raw data: {bronze_path}")
                
                # Normalize to silver layer
                silver_path = silver.normalize_daily_quotes(date, force_refresh=args.force)
                if silver_path:
                    logger.info(f"Normalized data: {silver_path}")
                    
                    # Count records
                    daily_prices = silver.read_daily_prices(date, date)
                    total_records += len(daily_prices)
                    logger.info(f"Processed {len(daily_prices)} daily price records")
                
            except Exception as e:
                logger.error(f"Failed to process {date.strftime('%Y-%m-%d')}: {e}")
                continue
        
        logger.info(f"Ingestion completed. Total records processed: {total_records}")
        
        # Run sample queries if requested
        if args.query and total_records > 0:
            logger.info("Running sample queries...")
            run_sample_queries()
        
        # Print storage statistics
        stats = bronze.get_storage_stats()
        logger.info(f"Storage statistics: {stats}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        return 1


def run_sample_queries():
    """Run sample analytical queries on the ingested data."""
    logger.info("🔍 Running sample queries...")
    
    with QueryEngine() as query:
        try:
            # Market coverage
            coverage = query.get_market_data_coverage()
            if len(coverage) > 0:
                logger.info(f"Market coverage: {len(coverage)} trading days")
                logger.info(f"Latest coverage:\n{coverage.head()}")
            
            # Price summary stats
            stats = query.get_price_summary_stats()
            if len(stats) > 0:
                logger.info(f"Price stats for {len(stats)} stocks")
                logger.info(f"Top 5 by average price:\n{stats.sort('avg_close', descending=True).head()}")
            
            # Sample returns calculation
            returns = query.calculate_returns(periods=[1])
            if len(returns) > 0:
                valid_returns = returns.filter(returns['return_1d'].is_not_null())
                if len(valid_returns) > 0:
                    logger.info(f"Calculated returns for {len(valid_returns)} stock-date pairs")
            
        except Exception as e:
            logger.warning(f"Sample queries failed: {e}")


if __name__ == "__main__":
    exit(main())