"""Internal prefix wrapper for backends.

This module is for internal use by the registry only and should not be imported directly.
"""

from __future__ import annotations

from datetime import timedelta
from typing import BinaryIO

from jqsys.core.storage.blob import BlobListResult, BlobMetadata, BlobStorageBackend


class PrefixedBlobBackend(BlobStorageBackend):
    """Wrapper that adds a prefix to all keys for any backend.

    This is an internal utility used by the registry to support hierarchical namespacing.
    Users should not instantiate this directly - use BlobStorage.from_name() instead.
    """

    def __init__(self, backend: BlobStorageBackend, prefix: str = ""):
        """Initialize prefixed backend wrapper.

        Args:
            backend: The underlying backend to wrap
            prefix: Prefix to add to all keys (e.g., "images/thumbnails")
        """
        self._backend = backend
        # Normalize prefix: ensure it ends with "/" if not empty
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""

    def _add_prefix(self, key: str) -> str:
        """Add prefix to a key."""
        return self._prefix + key if self._prefix else key

    def _remove_prefix(self, key: str) -> str:
        """Remove prefix from a key."""
        if self._prefix and key.startswith(self._prefix):
            return key[len(self._prefix) :]
        return key

    def put(
        self,
        key: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store a blob with prefixed key."""
        return self._backend.put(self._add_prefix(key), data, content_type, metadata)

    def get(self, key: str) -> bytes:
        """Retrieve a blob using prefixed key."""
        return self._backend.get(self._add_prefix(key))

    def get_stream(self, key: str) -> BinaryIO:
        """Retrieve a blob as stream using prefixed key."""
        return self._backend.get_stream(self._add_prefix(key))

    def delete(self, key: str) -> None:
        """Delete a blob using prefixed key."""
        self._backend.delete(self._add_prefix(key))

    def delete_many(self, keys: list[str]) -> dict[str, bool]:
        """Delete multiple blobs using prefixed keys."""
        prefixed_keys = [self._add_prefix(key) for key in keys]
        results = self._backend.delete_many(prefixed_keys)
        # Convert back to unprefixed keys in results
        return {self._remove_prefix(k): v for k, v in results.items()}

    def exists(self, key: str) -> bool:
        """Check if blob exists using prefixed key."""
        return self._backend.exists(self._add_prefix(key))

    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata using prefixed key, return with unprefixed key."""
        metadata = self._backend.get_metadata(self._add_prefix(key))
        # Return metadata with unprefixed key
        metadata.key = self._remove_prefix(metadata.key)
        return metadata

    def list_blobs(
        self,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int = 1000,
        marker: str | None = None,
    ) -> BlobListResult:
        """List blobs, adding our prefix to the search prefix."""
        # Combine our prefix with user's prefix
        full_prefix = self._add_prefix(prefix) if prefix else self._prefix or None

        result = self._backend.list_blobs(full_prefix, delimiter, max_results, marker)

        # Remove our prefix from all returned keys and prefixes
        unprefixed_blobs = []
        for blob in result.blobs:
            blob.key = self._remove_prefix(blob.key)
            unprefixed_blobs.append(blob)

        unprefixed_prefixes = [self._remove_prefix(p) for p in result.prefixes]

        return BlobListResult(
            blobs=unprefixed_blobs,
            prefixes=unprefixed_prefixes,
            is_truncated=result.is_truncated,
            next_marker=result.next_marker,
        )

    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate presigned URL using prefixed key."""
        return self._backend.generate_presigned_url(self._add_prefix(key), expiration, method)

    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy blob using prefixed keys."""
        self._backend.copy(self._add_prefix(source_key), self._add_prefix(dest_key))

    def get_size(self, key: str) -> int:
        """Get blob size using prefixed key."""
        return self._backend.get_size(self._add_prefix(key))
