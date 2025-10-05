"""Blob storage backend configuration.

This module defines the CONFIGURATION dict which maps backend names to their
connection parameters. Users can customize this file or create their own
config module and load it using the config utilities.

Configuration location: configs/blob_backends.py

Example usage:
    from jqsys.core.storage import BlobStorage

    # Use named backend
    storage = BlobStorage.from_name("demo")

    # Use with namespace
    storage = BlobStorage.from_name("demo.bronze")

Environment overrides:
    # Switch demo namespace between filesystem and MinIO
    export JQSYS_DEMO_BACKEND=minio

Configuration inheritance:
    # Configurations can inherit from other configurations to reduce repetition
    # Use the "__inherits__" key to specify the parent configuration

    "remote": {
        "type": "minio",
        "endpoint": "localhost:9000",
        "bucket": "jq-data",
        # ... other settings
    },
    "remote.analytics": {
        "__inherits__": "remote",  # Inherits all settings from remote
        "prefix": "analytics",      # Override only the prefix
    }

Custom configuration:
    # Create your own config module: myapp/custom_blob_config.py
    # Then pass it to the registry using load_config_from_module()
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from jqsys.core.utils.env import load_env_file_if_present

load_env_file_if_present()  # Load .env file if present
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _resolve_default_base_path() -> Path:
    """Return the default filesystem storage root."""
    configured_path = os.environ.get("BLOB_STORAGE_PATH")
    if configured_path:
        return Path(configured_path).expanduser()

    return PROJECT_ROOT / "var" / "blob_storage"


DEFAULT_BASE_PATH = _resolve_default_base_path()


def _build_minio_config(prefix: str | None = None) -> dict[str, Any]:
    """Return a MinIO backend configuration."""
    secure_value = os.getenv("MINIO_SECURE")
    secure = False
    if secure_value is not None:
        secure = secure_value.strip().lower() in {"1", "true", "yes", "on"}

    config: dict[str, Any] = {
        "type": "minio",
        "endpoint": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "bucket": os.getenv("MINIO_BUCKET", "jq-data"),
        "secure": secure,
    }
    if prefix:
        config["prefix"] = prefix
    return config


def _build_demo_base_config() -> dict[str, Any]:
    """Determine the base configuration for the demo namespace."""
    backend_type = os.getenv("JQSYS_DEMO_BACKEND", "filesystem").strip().lower()
    if backend_type == "minio":
        return _build_minio_config()

    return {
        "type": "filesystem",
        "base_path": str(DEFAULT_BASE_PATH),
    }


CONFIGURATION = {
    # Primary namespace that powers Bronze/Silver/Gold storage by default
    "demo": _build_demo_base_config(),
    # Development filesystem directory (handy for adhoc experiments)
    "dev": {
        "type": "filesystem",
        "base_path": str(DEFAULT_BASE_PATH / "dev"),
    },
    # Standalone filesystem backend
    "filesystem": {
        "type": "filesystem",
        "base_path": str(DEFAULT_BASE_PATH),
    },
    # Standalone MinIO backend (handy for tests or scripts)
    "minio": _build_minio_config(),
    # Example production configuration (unchanged)
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
