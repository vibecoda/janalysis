"""Blob storage backend configuration.

This module defines the CONFIGURATION dict which maps backend names to their
connection parameters. Users can customize this file or create their own
config module.

Example usage:
    from jqsys.storage import BlobStorage

    # Use named backend
    storage = BlobStorage.from_name("dev")

    # Use with namespace
    storage = BlobStorage.from_name("dev.images.thumbnails")
"""

from __future__ import annotations

import os
from pathlib import Path

# Default base path for filesystem backends
DEFAULT_BASE_PATH = Path.home() / ".jqsys" / "blob_storage"

CONFIGURATION = {
    # Local filesystem backend for development
    "dev": {
        "type": "filesystem",
        "base_path": str(DEFAULT_BASE_PATH / "dev"),
    },
    # Local MinIO for testing
    "minio-local": {
        "type": "minio",
        "endpoint": "localhost:9000",
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": "jqsys-dev",
        "secure": False,
    },
    # Example production MinIO/S3 configuration
    "prod": {
        "type": "minio",
        "endpoint": os.getenv("S3_ENDPOINT", "s3.amazonaws.com"),
        "access_key": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "bucket": os.getenv("S3_BUCKET", "jqsys-prod"),
        "secure": True,
    },
    # Temporary storage backend
    "tmp": {
        "type": "filesystem",
        "base_path": "/tmp/jqsys",
    },
}
