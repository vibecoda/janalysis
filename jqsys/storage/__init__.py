"""Storage layer for J-Quants financial data.

This module provides multiple storage abstractions:

1. Data Lake Architecture (DuckDB + Polars):
   - Bronze: Raw API responses stored as partitioned Parquet files
   - Silver: Normalized timeseries tables with data quality validation
   - Gold: Feature-engineered datasets optimized for analytics
   - Query: DuckDB-based analytical query interface

2. Blob Storage (MinIO, S3, etc.):
   - High-level abstraction for binary/file data
   - S3-compatible operations (put, get, list, delete)
   - Support for metadata, presigned URLs, streaming

3. Object Storage (MongoDB, PostgreSQL, etc.):
   - Document database abstraction
   - MongoDB-style query operations
   - Support for indexing, aggregation pipelines

Key Components:
- bronze: Raw data ingestion and storage
- silver: Data normalization and cleaning
- query: DuckDB-based analytical query interface
- blob: Blob storage interface (MinIO backend)
- object: Object/document storage interface (MongoDB backend)
"""

from .blob import BlobStorage, BlobStorageBackend
from .bronze import BronzeStorage
from .object import ObjectStorage, ObjectStorageBackend, SortOrder
from .query import QueryEngine
from .registry import (
    BackendConfigError,
    BackendNotFoundError,
    BlobBackendRegistry,
    get_blob_backend,
    get_default_registry,
)
from .silver import SilverStorage

__all__ = [
    "BronzeStorage",
    "SilverStorage",
    "QueryEngine",
    "BlobStorage",
    "BlobStorageBackend",
    "ObjectStorage",
    "ObjectStorageBackend",
    "SortOrder",
    "BlobBackendRegistry",
    "get_blob_backend",
    "get_default_registry",
    "BackendConfigError",
    "BackendNotFoundError",
]
