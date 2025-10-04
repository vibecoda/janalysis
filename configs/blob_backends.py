"""Blob storage backend configuration.

This module defines the CONFIGURATION dict which maps backend names to their
connection parameters. Users can customize this file or create their own
config module and load it using the config utilities.

Configuration location: configs/blob_backends.py

Example usage:
    from jqsys.core.storage import BlobStorage

    # Use named backend
    storage = BlobStorage.from_name("dev")

    # Use with namespace
    storage = BlobStorage.from_name("dev.images.thumbnails")

Configuration inheritance:
    # Configurations can inherit from other configurations to reduce repetition
    # Use the "__inherits__" key to specify the parent configuration

    "bronze": {
        "type": "minio",
        "endpoint": "localhost:9000",
        "bucket": "jq-data",
        # ... other settings
        "prefix": "bronze",
    },
    "silver": {
        "__inherits__": "bronze",  # Inherits all settings from bronze
        "prefix": "silver",         # Override only the prefix
    }

Custom configuration:
    # Create your own config module: myapp/custom_blob_config.py
    # Then pass it to the registry using load_config_from_module()
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
    "minio": {
        "type": "minio",
        "endpoint": "localhost:9000",
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": "jqsys-dev",
        "secure": False,
        "prefix": "dev",  # Optional: prefix for all keys
    },
    # Base MinIO configuration for J-Quants data
    "bronze": {
        "type": "minio",
        "endpoint": "localhost:9000",
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": "jq-data",
        "secure": False,
        "prefix": "bronze",
    },
    # Silver inherits from bronze, only overriding prefix
    "silver": {
        "__inherits__": "bronze",
        "prefix": "silver",
    },
    # Gold also inherits from bronze
    "gold": {
        "__inherits__": "bronze",
        "prefix": "gold",
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
