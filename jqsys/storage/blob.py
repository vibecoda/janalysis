"""Blob storage abstraction for binary/file data.

Provides a high-level interface for blob storage operations with support for
various backends (MinIO, S3, Azure Blob, GCS, etc.).
"""

from __future__ import annotations

import builtins
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import BinaryIO


@dataclass
class BlobMetadata:
    """Metadata for a stored blob."""

    key: str
    size: int
    content_type: str | None
    last_modified: datetime
    etag: str | None
    custom_metadata: dict[str, str]


@dataclass
class BlobListResult:
    """Result from listing blobs."""

    blobs: list[BlobMetadata]
    prefixes: list[str]  # Common prefixes (directories)
    is_truncated: bool
    next_marker: str | None


class BlobStorageBackend(ABC):
    """Abstract base class for blob storage backends.

    This interface provides S3-like operations for storing and retrieving
    binary data (files, images, documents, etc.).
    """

    @abstractmethod
    def put(
        self,
        key: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store a blob.

        Args:
            key: Object key (path) for the blob
            data: Binary data or file-like object
            content_type: MIME type of the content
            metadata: Custom metadata key-value pairs

        Returns:
            ETag or version ID of the stored blob
        """
        pass

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Retrieve a blob.

        Args:
            key: Object key to retrieve

        Returns:
            Binary content of the blob

        Raises:
            BlobNotFoundError: If the blob doesn't exist
        """
        pass

    @abstractmethod
    def get_stream(self, key: str) -> BinaryIO:
        """Retrieve a blob as a stream.

        Args:
            key: Object key to retrieve

        Returns:
            File-like object for streaming the content

        Raises:
            BlobNotFoundError: If the blob doesn't exist
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a blob.

        Args:
            key: Object key to delete

        Raises:
            BlobNotFoundError: If the blob doesn't exist
        """
        pass

    @abstractmethod
    def delete_many(self, keys: list[str]) -> dict[str, bool]:
        """Delete multiple blobs.

        Args:
            keys: List of object keys to delete

        Returns:
            Dictionary mapping keys to success status
        """
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a blob exists.

        Args:
            key: Object key to check

        Returns:
            True if the blob exists, False otherwise
        """
        pass

    @abstractmethod
    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata for a blob without downloading it.

        Args:
            key: Object key

        Returns:
            Blob metadata

        Raises:
            BlobNotFoundError: If the blob doesn't exist
        """
        pass

    @abstractmethod
    def list_blobs(
        self,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int = 1000,
        marker: str | None = None,
    ) -> BlobListResult:
        """List blobs with optional prefix filtering.

        Args:
            prefix: Only list blobs with this prefix
            delimiter: Delimiter for grouping (e.g., '/' for directories)
            max_results: Maximum number of results to return
            marker: Continuation token for pagination

        Returns:
            List result with blobs and pagination info
        """
        pass

    @abstractmethod
    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate a presigned URL for temporary access.

        Args:
            key: Object key
            expiration: How long the URL should be valid
            method: HTTP method (GET, PUT, DELETE)

        Returns:
            Presigned URL string
        """
        pass

    @abstractmethod
    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy a blob to a new location.

        Args:
            source_key: Source object key
            dest_key: Destination object key
        """
        pass

    @abstractmethod
    def get_size(self, key: str) -> int:
        """Get the size of a blob in bytes.

        Args:
            key: Object key

        Returns:
            Size in bytes

        Raises:
            BlobNotFoundError: If the blob doesn't exist
        """
        pass


class BlobStorage:
    """High-level blob storage interface with pluggable backends."""

    def __init__(self, backend: BlobStorageBackend, bucket: str):
        """Initialize blob storage.

        Args:
            backend: Storage backend implementation
            bucket: Bucket/container name to use
        """
        self._backend = backend
        self._bucket = bucket

    def put(
        self,
        key: str,
        data: bytes | BinaryIO | Path,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store a blob.

        Args:
            key: Object key (path) for the blob
            data: Binary data, file-like object, or Path to file
            content_type: MIME type of the content
            metadata: Custom metadata key-value pairs

        Returns:
            ETag or version ID of the stored blob
        """
        if isinstance(data, Path):
            with open(data, "rb") as f:
                return self._backend.put(key, f, content_type, metadata)
        return self._backend.put(key, data, content_type, metadata)

    def get(self, key: str) -> bytes:
        """Retrieve a blob."""
        return self._backend.get(key)

    def get_stream(self, key: str) -> BinaryIO:
        """Retrieve a blob as a stream."""
        return self._backend.get_stream(key)

    def download_to_file(self, key: str, file_path: Path) -> None:
        """Download a blob to a file.

        Args:
            key: Object key to download
            file_path: Local file path to save to
        """
        with open(file_path, "wb") as f:
            stream = self.get_stream(key)
            f.write(stream.read())

    def delete(self, key: str) -> None:
        """Delete a blob."""
        self._backend.delete(key)

    def delete_many(self, keys: builtins.list[str]) -> dict[str, bool]:
        """Delete multiple blobs."""
        return self._backend.delete_many(keys)

    def exists(self, key: str) -> bool:
        """Check if a blob exists."""
        return self._backend.exists(key)

    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata for a blob."""
        return self._backend.get_metadata(key)

    def list(
        self, prefix: str | None = None, delimiter: str | None = None, max_results: int = 1000
    ) -> Iterator[BlobMetadata]:
        """Iterate over blobs with optional prefix filtering.

        Args:
            prefix: Only list blobs with this prefix
            delimiter: Delimiter for grouping (e.g., '/' for directories)
            max_results: Maximum results per page

        Yields:
            BlobMetadata for each blob
        """
        marker = None
        while True:
            result = self._backend.list_blobs(prefix, delimiter, max_results, marker)
            yield from result.blobs

            if not result.is_truncated:
                break
            marker = result.next_marker

    def list_prefixes(self, prefix: str | None = None, delimiter: str = "/") -> builtins.list[str]:
        """List common prefixes (directories).

        Args:
            prefix: Only list prefixes under this prefix
            delimiter: Delimiter for grouping

        Returns:
            List of prefixes
        """
        result = self._backend.list_blobs(prefix, delimiter, max_results=1)
        return result.prefixes

    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate a presigned URL for temporary access."""
        return self._backend.generate_presigned_url(key, expiration, method)

    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy a blob to a new location."""
        self._backend.copy(source_key, dest_key)

    def get_size(self, key: str) -> int:
        """Get the size of a blob in bytes."""
        return self._backend.get_size(key)


# Custom exceptions


class BlobStorageError(Exception):
    """Base exception for blob storage errors."""

    pass


class BlobNotFoundError(BlobStorageError):
    """Raised when a blob is not found."""

    pass


class BlobAlreadyExistsError(BlobStorageError):
    """Raised when trying to create a blob that already exists."""

    pass


class BlobStorageConnectionError(BlobStorageError):
    """Raised when connection to storage backend fails."""

    pass
