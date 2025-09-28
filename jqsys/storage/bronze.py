"""Bronze layer: Raw data storage for J-Quants API responses.

Handles storage of raw API responses as partitioned Parquet files with 
minimal processing - primarily for data lineage and reprocessing capabilities.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import polars as pl

logger = logging.getLogger(__name__)


class BronzeStorage:
    """Manages bronze layer storage for raw J-Quants API data."""
    
    def __init__(self, base_path: str | Path | None = None):
        """Initialize bronze storage.
        
        Args:
            base_path: Base directory for bronze layer storage. If None, 
                      checks JQSYS_DATA_ROOT environment variable, 
                      otherwise defaults to "data/bronze"
        """
        if base_path is None:
            # Check environment variable first, then default
            data_root = os.getenv("JQSYS_DATA_ROOT", "data")
            base_path = Path(data_root) / "bronze"
        
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        
    def store_raw_response(
        self,
        endpoint: str,
        data: List[Dict[str, Any]],
        date: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Store raw API response data.
        
        Args:
            endpoint: J-Quants endpoint name (e.g., 'daily_quotes', 'listed_info') 
            data: Raw response data from API
            date: Date of the data (used for partitioning)
            metadata: Optional metadata about the request/response
            
        Returns:
            Path to stored file
        """
        # Create partitioned path: endpoint/date=YYYY-MM-DD/
        date_str = date.strftime("%Y-%m-%d")
        partition_path = self.base_path / endpoint / f"date={date_str}"
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Convert to Polars DataFrame for efficient storage
        if not data:
            logger.warning(f"No data to store for {endpoint} on {date_str}")
            return partition_path / "empty.parquet"
            
        try:
            df = pl.DataFrame(data)
            
            # Add metadata columns
            df = df.with_columns([
                pl.lit(endpoint).alias("_endpoint"),
                pl.lit(date_str).alias("_partition_date"),
                pl.lit(datetime.now().isoformat()).alias("_ingested_at")
            ])
            
            # Add optional metadata as JSON column
            if metadata:
                df = df.with_columns(
                    pl.lit(json.dumps(metadata)).alias("_metadata")
                )
            
            # Store as Parquet with compression
            file_path = partition_path / "data.parquet"
            df.write_parquet(
                file_path,
                compression="snappy",
                use_pyarrow=True
            )
            
            logger.info(f"Stored {len(df)} records to {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to store {endpoint} data for {date_str}: {e}")
            raise
    
    def read_raw_data(
        self,
        endpoint: str,
        date: Optional[datetime] = None,
        date_range: Optional[tuple[datetime, datetime]] = None
    ) -> pl.DataFrame:
        """Read raw data from bronze layer.
        
        Args:
            endpoint: J-Quants endpoint name
            date: Specific date to read (mutually exclusive with date_range)
            date_range: Tuple of (start_date, end_date) for range query
            
        Returns:
            Polars DataFrame with raw data
        """
        if date and date_range:
            raise ValueError("Cannot specify both date and date_range")
        
        endpoint_path = self.base_path / endpoint
        if not endpoint_path.exists():
            raise FileNotFoundError(f"No data found for endpoint: {endpoint}")
        
        # Build list of files to read
        files_to_read = []
        
        if date:
            # Single date
            date_str = date.strftime("%Y-%m-%d")
            date_path = endpoint_path / f"date={date_str}" / "data.parquet"
            if date_path.exists():
                files_to_read.append(str(date_path))
        elif date_range:
            # Date range
            start_date, end_date = date_range
            for date_dir in endpoint_path.iterdir():
                if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                    continue
                    
                try:
                    dir_date_str = date_dir.name.split("=")[1]
                    dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d")
                    
                    if start_date <= dir_date <= end_date:
                        data_file = date_dir / "data.parquet"
                        if data_file.exists():
                            files_to_read.append(str(data_file))
                            
                except (ValueError, IndexError):
                    logger.warning(f"Skipping invalid date directory: {date_dir}")
                    continue
        else:
            # All data for endpoint
            for date_dir in endpoint_path.iterdir():
                if date_dir.is_dir():
                    data_file = date_dir / "data.parquet"
                    if data_file.exists():
                        files_to_read.append(str(data_file))
        
        if not files_to_read:
            return pl.DataFrame()
        
        # Read and concatenate all files
        try:
            if len(files_to_read) == 1:
                return pl.read_parquet(files_to_read[0])
            else:
                return pl.concat([pl.read_parquet(f) for f in files_to_read])
                
        except Exception as e:
            logger.error(f"Failed to read {endpoint} data: {e}")
            raise
    
    def list_available_dates(self, endpoint: str) -> List[datetime]:
        """List all available dates for an endpoint.
        
        Args:
            endpoint: J-Quants endpoint name
            
        Returns:
            List of available dates, sorted ascending
        """
        endpoint_path = self.base_path / endpoint
        if not endpoint_path.exists():
            return []
        
        dates = []
        for date_dir in endpoint_path.iterdir():
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue
                
            try:
                date_str = date_dir.name.split("=")[1]
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                
                # Check if data file exists
                if (date_dir / "data.parquet").exists():
                    dates.append(date_obj)
                    
            except (ValueError, IndexError):
                continue
        
        return sorted(dates)
    
    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics for bronze layer.
        
        Returns:
            Dictionary with storage statistics
        """
        stats = {
            "endpoints": {},
            "total_files": 0,
            "total_size_mb": 0
        }
        
        for endpoint_dir in self.base_path.iterdir():
            if not endpoint_dir.is_dir():
                continue
                
            endpoint_stats = {
                "dates": 0,
                "files": 0,
                "size_mb": 0
            }
            
            for date_dir in endpoint_dir.iterdir():
                if not date_dir.is_dir():
                    continue
                    
                data_file = date_dir / "data.parquet"
                if data_file.exists():
                    endpoint_stats["dates"] += 1
                    endpoint_stats["files"] += 1
                    endpoint_stats["size_mb"] += data_file.stat().st_size / (1024 * 1024)
            
            if endpoint_stats["files"] > 0:
                stats["endpoints"][endpoint_dir.name] = endpoint_stats
                stats["total_files"] += endpoint_stats["files"]
                stats["total_size_mb"] += endpoint_stats["size_mb"]
        
        # Round size to 2 decimal places
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        for endpoint_stats in stats["endpoints"].values():
            endpoint_stats["size_mb"] = round(endpoint_stats["size_mb"], 2)
        
        return stats