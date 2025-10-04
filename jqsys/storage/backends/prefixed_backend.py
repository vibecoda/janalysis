"""Prefixed backend wrapper for adding namespace prefixes to blob operations."""

from __future__ import annotations

from datetime import timedelta
from typing import BinaryIO

from ..blob import BlobListResult, BlobMetadata, BlobStorageBackend


class PrefixedBlobBackend(BlobStorageBackend):
    """Wrapper that adds a prefix to all blob keys.

    This allows creating namespaces within a backend:
        backend = FilesystemBackend("/data")
        prefixed = PrefixedBlobBackend(backend, "images/thumbnails")
        prefixed.put("photo.jpg", data)  # Actually stored at "images/thumbnails/photo.jpg"
    """

    def __init__(self, backend: BlobStorageBackend, prefix: str = ""):
        """Initialize prefixed backend.

        Args:
            backend: The underlying backend to wrap
            prefix: Prefix to add to all keys (e.g., "images/thumbnails")
        """
        self._backend = backend
        # Normalize prefix - ensure it ends with / if not empty
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""

    def _add_prefix(self, key: str) -> str:
        """Add prefix to a key."""
        return self._prefix + key

    def _remove_prefix(self, key: str) -> str:
        """Remove prefix from a key."""
        if key.startswith(self._prefix):
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
        prefixed_keys = [self._add_prefix(k) for k in keys]
        results = self._backend.delete_many(prefixed_keys)
        # Map results back to unprefixed keys
        return {self._remove_prefix(k): v for k, v in results.items()}

    def exists(self, key: str) -> bool:
        """Check if a blob exists using prefixed key."""
        return self._backend.exists(self._add_prefix(key))

    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata for a blob using prefixed key."""
        metadata = self._backend.get_metadata(self._add_prefix(key))
        # Return metadata with unprefixed key
        return BlobMetadata(
            key=key,  # Use original unprefixed key
            size=metadata.size,
            content_type=metadata.content_type,
            last_modified=metadata.last_modified,
            etag=metadata.etag,
            custom_metadata=metadata.custom_metadata,
        )

    def list_blobs(
        self,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int = 1000,
        marker: str | None = None,
    ) -> BlobListResult:
        """List blobs with prefixed keys."""
        # Add our prefix to the user's prefix
        full_prefix = self._prefix + (prefix or "")

        # Add prefix to marker if provided
        full_marker = self._add_prefix(marker) if marker else None

        result = self._backend.list_blobs(full_prefix, delimiter, max_results, full_marker)

        # Remove our prefix from all returned keys
        unprefixed_blobs = []
        for blob in result.blobs:
            unprefixed_blobs.append(
                BlobMetadata(
                    key=self._remove_prefix(blob.key),
                    size=blob.size,
                    content_type=blob.content_type,
                    last_modified=blob.last_modified,
                    etag=blob.etag,
                    custom_metadata=blob.custom_metadata,
                )
            )

        # Remove prefix from prefixes list
        unprefixed_prefixes = [self._remove_prefix(p) for p in result.prefixes]

        # Remove prefix from next_marker
        next_marker = self._remove_prefix(result.next_marker) if result.next_marker else None

        return BlobListResult(
            blobs=unprefixed_blobs,
            prefixes=unprefixed_prefixes,
            is_truncated=result.is_truncated,
            next_marker=next_marker,
        )

    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate a presigned URL for a blob using prefixed key."""
        return self._backend.generate_presigned_url(self._add_prefix(key), expiration, method)

    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy a blob using prefixed keys."""
        self._backend.copy(self._add_prefix(source_key), self._add_prefix(dest_key))

    def get_size(self, key: str) -> int:
        """Get the size of a blob using prefixed key."""
        return self._backend.get_size(self._add_prefix(key))
