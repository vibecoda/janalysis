"""Storage layer for J-Quants financial data.

This module provides a hybrid DuckDB + Polars storage solution optimized for
financial timeseries data with bronze/silver/gold data lake architecture.

Architecture:
- Bronze: Raw API responses stored as partitioned Parquet files
- Silver: Normalized timeseries tables with data quality validation  
- Gold: Feature-engineered datasets optimized for analytics

Key Components:
- bronze: Raw data ingestion and storage
- silver: Data normalization and cleaning
- gold: Feature engineering and analytical datasets
- query: DuckDB-based analytical query interface
"""

from .bronze import BronzeStorage
from .silver import SilverStorage
from .query import QueryEngine

__all__ = ["BronzeStorage", "SilverStorage", "QueryEngine", "bronze", "silver", "query"]