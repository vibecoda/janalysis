"""Filesystem backend implementation for blob storage."""

from __future__ import annotations

import json
import logging
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

from ..blob import (
    BlobListResult,
    BlobMetadata,
    BlobNotFoundError,
    BlobStorageBackend,
    BlobStorageError,
)

logger = logging.getLogger(__name__)


class FilesystemBackend(BlobStorageBackend):
    """Filesystem implementation of blob storage backend.

    Stores blobs as files on the local filesystem with metadata stored
    in accompanying JSON files.
    """

    def __init__(self, base_path: str | Path):
        """Initialize filesystem backend.

        Args:
            base_path: Base directory path for storing blobs
        """
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized filesystem backend at: {self._base_path}")

    def _get_blob_path(self, key: str) -> Path:
        """Get the full path for a blob file."""
        # Normalize key and ensure it's within base_path
        normalized_key = Path(key).as_posix()
        blob_path = self._base_path / normalized_key
        return blob_path

    def _get_metadata_path(self, key: str) -> Path:
        """Get the path for metadata file associated with a blob."""
        return self._get_blob_path(key).with_suffix(self._get_blob_path(key).suffix + ".meta")

    def _save_metadata(
        self,
        key: str,
        size: int,
        content_type: str | None = None,
        custom_metadata: dict[str, str] | None = None,
    ) -> None:
        """Save metadata for a blob."""
        metadata = {
            "key": key,
            "size": size,
            "content_type": content_type or "application/octet-stream",
            "last_modified": datetime.now(UTC).isoformat(),
            "custom_metadata": custom_metadata or {},
        }

        metadata_path = self._get_metadata_path(key)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self, key: str) -> dict:
        """Load metadata for a blob."""
        metadata_path = self._get_metadata_path(key)

        if not metadata_path.exists():
            # Return basic metadata if meta file doesn't exist
            blob_path = self._get_blob_path(key)
            if not blob_path.exists():
                raise BlobNotFoundError(f"Blob not found: {key}")

            stat = blob_path.stat()
            return {
                "key": key,
                "size": stat.st_size,
                "content_type": "application/octet-stream",
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "custom_metadata": {},
            }

        with open(metadata_path) as f:
            return json.load(f)

    def put(
        self,
        key: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store a blob in the filesystem."""
        try:
            blob_path = self._get_blob_path(key)
            blob_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the blob data
            if isinstance(data, bytes):
                blob_path.write_bytes(data)
                size = len(data)
            else:
                # Handle BinaryIO stream
                with open(blob_path, "wb") as f:
                    if hasattr(data, "read"):
                        size = 0
                        while chunk := data.read(8192):
                            f.write(chunk)
                            size += len(chunk)
                    else:
                        raise BlobStorageError(f"Invalid data type for key {key}")

            # Save metadata
            self._save_metadata(key, size, content_type, metadata)

            # Use file modification time as etag equivalent
            etag = str(int(blob_path.stat().st_mtime * 1000000))
            logger.info(f"Stored blob: {key} ({size} bytes)")
            return etag

        except Exception as e:
            raise BlobStorageError(f"Failed to store blob {key}: {e}")

    def get(self, key: str) -> bytes:
        """Retrieve a blob from the filesystem."""
        try:
            blob_path = self._get_blob_path(key)

            if not blob_path.exists():
                raise BlobNotFoundError(f"Blob not found: {key}")

            return blob_path.read_bytes()

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to retrieve blob {key}: {e}")

    def get_stream(self, key: str) -> BinaryIO:
        """Retrieve a blob as a stream from the filesystem."""
        try:
            blob_path = self._get_blob_path(key)

            if not blob_path.exists():
                raise BlobNotFoundError(f"Blob not found: {key}")

            return open(blob_path, "rb")

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to retrieve blob stream {key}: {e}")

    def delete(self, key: str) -> None:
        """Delete a blob from the filesystem."""
        try:
            blob_path = self._get_blob_path(key)
            metadata_path = self._get_metadata_path(key)

            if not blob_path.exists():
                raise BlobNotFoundError(f"Blob not found: {key}")

            # Delete blob and metadata
            blob_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()

            # Clean up empty parent directories
            self._cleanup_empty_dirs(blob_path.parent)

            logger.info(f"Deleted blob: {key}")

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to delete blob {key}: {e}")

    def _cleanup_empty_dirs(self, path: Path) -> None:
        """Remove empty parent directories up to base_path."""
        try:
            while path != self._base_path and path.exists():
                if not any(path.iterdir()):
                    path.rmdir()
                    path = path.parent
                else:
                    break
        except Exception:
            pass  # Ignore cleanup errors

    def delete_many(self, keys: list[str]) -> dict[str, bool]:
        """Delete multiple blobs from the filesystem."""
        results = {}

        for key in keys:
            try:
                self.delete(key)
                results[key] = True
            except Exception as e:
                logger.warning(f"Failed to delete {key}: {e}")
                results[key] = False

        successful = sum(results.values())
        logger.info(f"Deleted {successful} of {len(keys)} blobs")
        return results

    def exists(self, key: str) -> bool:
        """Check if a blob exists in the filesystem."""
        return self._get_blob_path(key).exists()

    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata for a blob in the filesystem."""
        try:
            metadata_dict = self._load_metadata(key)

            return BlobMetadata(
                key=metadata_dict["key"],
                size=metadata_dict["size"],
                content_type=metadata_dict.get("content_type"),
                last_modified=datetime.fromisoformat(metadata_dict["last_modified"]),
                etag=None,  # Filesystem doesn't have etags
                custom_metadata=metadata_dict.get("custom_metadata", {}),
            )

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to get metadata for {key}: {e}")

    def list_blobs(
        self,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int = 1000,
        marker: str | None = None,
    ) -> BlobListResult:
        """List blobs in the filesystem."""
        try:
            blobs = []
            prefixes = set()

            # Determine search path
            search_path = self._base_path
            if prefix:
                search_path = self._base_path / prefix

            if not search_path.exists():
                return BlobListResult(blobs=[], prefixes=[], is_truncated=False, next_marker=None)

            # Use recursive glob if no delimiter, otherwise list directory
            pattern = "**/*" if delimiter is None else "*"

            found_marker = marker is None
            count = 0

            for path in sorted(search_path.glob(pattern)):
                # Skip metadata files
                if path.suffix == ".meta":
                    continue

                # Skip directories
                if path.is_dir():
                    if delimiter:
                        rel_path = path.relative_to(self._base_path).as_posix()
                        if prefix:
                            # With prefix filter, only add if path starts with prefix
                            if rel_path.startswith(prefix):
                                prefixes.add(rel_path + delimiter)
                        else:
                            # No prefix filter, add all top-level directories
                            prefixes.add(rel_path + delimiter)
                    continue

                # Get relative key
                rel_path = path.relative_to(self._base_path).as_posix()

                # Handle marker (pagination)
                if not found_marker:
                    if rel_path == marker:
                        found_marker = True
                    continue

                # Check prefix filter
                if prefix and not rel_path.startswith(prefix):
                    continue

                # Check delimiter (simulate directory listing)
                if delimiter and prefix:
                    remaining = rel_path[len(prefix) :]
                    if delimiter in remaining:
                        # This is in a subdirectory
                        subdir = prefix + remaining.split(delimiter)[0] + delimiter
                        prefixes.add(subdir)
                        continue

                count += 1
                if count > max_results:
                    return BlobListResult(
                        blobs=blobs,
                        prefixes=list(prefixes),
                        is_truncated=True,
                        next_marker=blobs[-1].key if blobs else None,
                    )

                # Get metadata
                stat = path.stat()
                blobs.append(
                    BlobMetadata(
                        key=rel_path,
                        size=stat.st_size,
                        content_type=None,  # Would need to load from metadata file
                        last_modified=datetime.fromtimestamp(stat.st_mtime),
                        etag=None,
                        custom_metadata={},
                    )
                )

            return BlobListResult(
                blobs=blobs, prefixes=list(prefixes), is_truncated=False, next_marker=None
            )

        except Exception as e:
            raise BlobStorageError(f"Failed to list blobs: {e}")

    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate a presigned URL for the filesystem.

        Note: This returns a file:// URL since filesystem storage doesn't
        support HTTP-based presigned URLs. This is mainly for API compatibility.
        """
        blob_path = self._get_blob_path(key)

        if not blob_path.exists():
            raise BlobNotFoundError(f"Blob not found: {key}")

        # Return file:// URL (not really presigned, but maintains API compatibility)
        return f"file://{blob_path.absolute()}"

    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy a blob in the filesystem."""
        try:
            source_path = self._get_blob_path(source_key)
            dest_path = self._get_blob_path(dest_key)

            if not source_path.exists():
                raise BlobNotFoundError(f"Source blob not found: {source_key}")

            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy blob file
            shutil.copy2(source_path, dest_path)

            # Copy metadata if it exists
            source_meta = self._get_metadata_path(source_key)
            if source_meta.exists():
                dest_meta = self._get_metadata_path(dest_key)
                shutil.copy2(source_meta, dest_meta)

                # Update the key in the copied metadata
                with open(dest_meta) as f:
                    metadata = json.load(f)
                metadata["key"] = dest_key
                metadata["last_modified"] = datetime.now(UTC).isoformat()
                with open(dest_meta, "w") as f:
                    json.dump(metadata, f, indent=2)

            logger.info(f"Copied blob: {source_key} -> {dest_key}")

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to copy blob: {e}")

    def get_size(self, key: str) -> int:
        """Get the size of a blob in the filesystem."""
        try:
            blob_path = self._get_blob_path(key)

            if not blob_path.exists():
                raise BlobNotFoundError(f"Blob not found: {key}")

            return blob_path.stat().st_size

        except BlobNotFoundError:
            raise
        except Exception as e:
            raise BlobStorageError(f"Failed to get size for {key}: {e}")
