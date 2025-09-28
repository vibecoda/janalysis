"""Silver layer: Normalized timeseries data with data quality validation.

Transforms bronze layer raw data into clean, normalized timeseries tables
optimized for financial analysis and feature engineering.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

from .bronze import BronzeStorage

logger = logging.getLogger(__name__)


class SilverStorage:
    """Manages silver layer storage for normalized timeseries data."""
    
    def __init__(self, base_path: str | Path | None = None, bronze_storage: Optional[BronzeStorage] = None):
        """Initialize silver storage.
        
        Args:
            base_path: Base directory for silver layer storage. If None,
                      checks JQSYS_DATA_ROOT environment variable,
                      otherwise defaults to "data/silver"
            bronze_storage: Bronze storage instance for reading raw data
        """
        if base_path is None:
            # Check environment variable first, then default
            data_root = os.getenv("JQSYS_DATA_ROOT", "data")
            base_path = Path(data_root) / "silver"
        
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.bronze = bronze_storage or BronzeStorage()
        
    def normalize_daily_quotes(
        self,
        date: datetime,
        force_refresh: bool = False
    ) -> Optional[Path]:
        """Normalize daily quotes data from bronze to silver layer.
        
        Args:
            date: Date to process
            force_refresh: Whether to reprocess existing data
            
        Returns:
            Path to normalized file or None if no data
        """
        # Check if already processed
        output_path = self._get_silver_path("daily_prices", date)
        if output_path.exists() and not force_refresh:
            logger.info(f"Daily quotes already normalized for {date.strftime('%Y-%m-%d')}")
            return output_path
        
        try:
            # Read raw data from bronze
            raw_df = self.bronze.read_raw_data("daily_quotes", date=date)
            if raw_df.is_empty():
                logger.warning(f"No raw daily quotes data for {date.strftime('%Y-%m-%d')}")
                return None
            
            # Normalize the data
            normalized_df = self._normalize_daily_quotes_schema(raw_df, date)
            
            # Validate data quality
            self._validate_daily_quotes(normalized_df, date)
            
            # Store normalized data
            output_path.parent.mkdir(parents=True, exist_ok=True)
            normalized_df.write_parquet(
                output_path,
                compression="snappy",
                use_pyarrow=True
            )
            
            logger.info(f"Normalized {len(normalized_df)} daily quotes records for {date.strftime('%Y-%m-%d')}")
            return output_path
            
        except Exception as e:
            logger.error(f"Failed to normalize daily quotes for {date.strftime('%Y-%m-%d')}: {e}")
            raise
    
    def _normalize_daily_quotes_schema(self, raw_df: pl.DataFrame, date: datetime) -> pl.DataFrame:
        """Transform raw daily quotes to normalized schema.
        
        Args:
            raw_df: Raw DataFrame from bronze layer
            date: Processing date
            
        Returns:
            Normalized DataFrame
        """
        # Expected J-Quants daily quotes schema transformation
        # This is a basic normalization - adjust based on actual API response structure
        
        normalized = raw_df.select([
            # Core identification
            pl.col("Code").alias("code"),
            pl.col("Date").str.strptime(pl.Date, format="%Y-%m-%d").alias("date"),
            
            # OHLCV data  
            pl.col("Open").cast(pl.Float64).alias("open"),
            pl.col("High").cast(pl.Float64).alias("high"), 
            pl.col("Low").cast(pl.Float64).alias("low"),
            pl.col("Close").cast(pl.Float64).alias("close"),
            pl.col("Volume").cast(pl.Int64).alias("volume"),
            
            # Additional fields if available
            pl.col("TurnoverValue").cast(pl.Float64, strict=False).alias("turnover_value"),
            pl.col("AdjustmentFactor").cast(pl.Float64, strict=False).alias("adjustment_factor"),
            pl.col("AdjustmentClose").cast(pl.Float64, strict=False).alias("adj_close"),
            
            # Metadata
            pl.lit(datetime.now().isoformat()).alias("processed_at")
        ]).filter(
            # Remove any records with null core data
            pl.col("code").is_not_null() & 
            pl.col("date").is_not_null() &
            pl.col("close").is_not_null()
        )
        
        # Calculate adjusted close if not provided
        normalized = normalized.with_columns([
            pl.when(pl.col("adj_close").is_null())
            .then(pl.col("close") * pl.col("adjustment_factor").fill_null(1.0))
            .otherwise(pl.col("adj_close"))
            .alias("adj_close")
        ])
        
        return normalized.sort(["code", "date"])
    
    def _validate_daily_quotes(self, df: pl.DataFrame, date: datetime) -> None:
        """Validate daily quotes data quality.
        
        Args:
            df: Normalized DataFrame to validate
            date: Expected date
            
        Raises:
            ValueError: If data quality issues are found
        """
        # Check for required columns
        required_cols = ["code", "date", "open", "high", "low", "close", "volume"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Check for null values in critical columns
        null_counts = df.select([
            pl.col("code").null_count().alias("code_nulls"),
            pl.col("close").null_count().alias("close_nulls")
        ]).row(0)
        
        if null_counts[0] > 0:
            raise ValueError(f"Found {null_counts[0]} null codes")
        if null_counts[1] > 0:
            raise ValueError(f"Found {null_counts[1]} null close prices")
        
        # Check for reasonable price ranges (basic sanity check)
        price_stats = df.select([
            pl.col("close").min().alias("min_close"),
            pl.col("close").max().alias("max_close")
        ]).row(0)
        
        if price_stats[0] <= 0:
            raise ValueError(f"Found non-positive close prices: min={price_stats[0]}")
        if price_stats[1] > 1000000:  # 1M yen seems unreasonable for individual stock
            logger.warning(f"Found very high close price: max={price_stats[1]}")
        
        # Validate OHLC relationships
        invalid_ohlc = df.filter(
            (pl.col("high") < pl.col("low")) |
            (pl.col("high") < pl.col("open")) |
            (pl.col("high") < pl.col("close")) |
            (pl.col("low") > pl.col("open")) |
            (pl.col("low") > pl.col("close"))
        ).height
        
        if invalid_ohlc > 0:
            raise ValueError(f"Found {invalid_ohlc} records with invalid OHLC relationships")
        
        logger.info(f"Data quality validation passed for {len(df)} records")
    
    def _get_silver_path(self, table: str, date: datetime) -> Path:
        """Get path for silver layer table file.
        
        Args:
            table: Table name (e.g., 'daily_prices', 'fundamentals')
            date: Date for partitioning
            
        Returns:
            Path to silver layer file
        """
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")
        
        return self.base_path / table / f"year={year}" / f"month={month}" / f"day={day}.parquet"
    
    def read_daily_prices(
        self,
        start_date: datetime,
        end_date: datetime,
        codes: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Read normalized daily prices from silver layer.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive) 
            codes: Optional list of stock codes to filter
            
        Returns:
            DataFrame with daily prices
        """
        # Find all relevant files
        files_to_read = []
        current_date = start_date
        
        while current_date <= end_date:
            file_path = self._get_silver_path("daily_prices", current_date)
            if file_path.exists():
                files_to_read.append(str(file_path))
            current_date = current_date + timedelta(days=1)
        
        if not files_to_read:
            return pl.DataFrame()
        
        # Read and concatenate
        try:
            if len(files_to_read) == 1:
                df = pl.read_parquet(files_to_read[0])
            else:
                df = pl.concat([pl.read_parquet(f) for f in files_to_read])
            
            # Apply filters
            df = df.filter(
                (pl.col("date") >= start_date.date()) &
                (pl.col("date") <= end_date.date())
            )
            
            if codes:
                df = df.filter(pl.col("code").is_in(codes))
            
            return df.sort(["date", "code"])
            
        except Exception as e:
            logger.error(f"Failed to read daily prices: {e}")
            raise