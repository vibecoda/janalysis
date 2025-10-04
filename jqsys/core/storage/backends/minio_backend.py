"""MinIO backend implementation for blob storage."""

from __future__ import annotations

import logging
from datetime import timedelta
from io import BytesIO
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error

from jqsys.core.storage.blob import (
    BlobListResult,
    BlobMetadata,
    BlobNotFoundError,
    BlobStorageBackend,
    BlobStorageConnectionError,
    BlobStorageError,
)

logger = logging.getLogger(__name__)


class MinIOBackend(BlobStorageBackend):
    """MinIO implementation of blob storage backend."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = True,
        region: str | None = None,
        prefix: str | None = None,
    ):
        """Initialize MinIO backend.

        Args:
            endpoint: MinIO server endpoint (e.g., 'localhost:9000')
            access_key: Access key (user ID)
            secret_key: Secret key (password)
            bucket: Bucket name to use
            secure: Use HTTPS if True
            region: Optional region name
            prefix: Optional prefix to prepend to all keys
        """
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""

        try:
            self._client = Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
                region=region,
            )

            # Ensure bucket exists
            if not self._client.bucket_exists(bucket):
                self._client.make_bucket(bucket, location=region)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Using existing bucket: {bucket}")

        except S3Error as e:
            raise BlobStorageConnectionError(f"Failed to connect to MinIO: {e}")

    def _full_key(self, key: str) -> str:
        """Prepend prefix to key."""
        return f"{self._prefix}{key}"

    def _strip_prefix(self, full_key: str) -> str:
        """Remove prefix from key."""
        if self._prefix and full_key.startswith(self._prefix):
            return full_key[len(self._prefix) :]
        return full_key

    def put(
        self,
        key: str,
        data: bytes | BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store a blob in MinIO."""
        try:
            full_key = self._full_key(key)

            # Convert bytes to BytesIO if needed
            if isinstance(data, bytes):
                stream = BytesIO(data)
                length = len(data)
            else:
                # Get current position and seek to end to get size
                start_pos = data.tell()
                data.seek(0, 2)  # Seek to end
                length = data.tell()
                data.seek(start_pos)  # Seek back to original position
                stream = data

            result = self._client.put_object(
                bucket_name=self._bucket,
                object_name=full_key,
                data=stream,
                length=length,
                content_type=content_type or "application/octet-stream",
                metadata=metadata,
            )

            logger.info(f"Stored blob: {key} (etag: {result.etag})")
            return result.etag

        except S3Error as e:
            raise BlobStorageError(f"Failed to store blob {key}: {e}")

    def get(self, key: str) -> bytes:
        """Retrieve a blob from MinIO."""
        try:
            full_key = self._full_key(key)
            response = self._client.get_object(self._bucket, full_key)
            data = response.read()
            response.close()
            response.release_conn()
            return data

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Blob not found: {key}")
            raise BlobStorageError(f"Failed to retrieve blob {key}: {e}")

    def get_stream(self, key: str) -> BinaryIO:
        """Retrieve a blob as a stream from MinIO."""
        try:
            full_key = self._full_key(key)
            response = self._client.get_object(self._bucket, full_key)
            return response

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Blob not found: {key}")
            raise BlobStorageError(f"Failed to retrieve blob stream {key}: {e}")

    def delete(self, key: str) -> None:
        """Delete a blob from MinIO."""
        try:
            full_key = self._full_key(key)
            self._client.remove_object(self._bucket, full_key)
            logger.info(f"Deleted blob: {key}")

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Blob not found: {key}")
            raise BlobStorageError(f"Failed to delete blob {key}: {e}")

    def delete_many(self, keys: list[str]) -> dict[str, bool]:
        """Delete multiple blobs from MinIO."""
        results = {}

        try:
            # MinIO's remove_objects returns an iterator of errors
            full_keys = [self._full_key(key) for key in keys]
            errors = self._client.remove_objects(self._bucket, full_keys)

            # Convert to dict - all keys succeed unless in error list
            error_keys = {err.object_name for err in errors}
            results = {key: self._full_key(key) not in error_keys for key in keys}

            logger.info(f"Deleted {sum(results.values())} of {len(keys)} blobs")
            return results

        except S3Error as e:
            raise BlobStorageError(f"Failed to delete multiple blobs: {e}")

    def exists(self, key: str) -> bool:
        """Check if a blob exists in MinIO."""
        try:
            full_key = self._full_key(key)
            self._client.stat_object(self._bucket, full_key)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise BlobStorageError(f"Failed to check blob existence {key}: {e}")

    def get_metadata(self, key: str) -> BlobMetadata:
        """Get metadata for a blob in MinIO."""
        try:
            full_key = self._full_key(key)
            stat = self._client.stat_object(self._bucket, full_key)

            return BlobMetadata(
                key=key,
                size=stat.size,
                content_type=stat.content_type,
                last_modified=stat.last_modified,
                etag=stat.etag,
                custom_metadata=stat.metadata or {},
            )

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Blob not found: {key}")
            raise BlobStorageError(f"Failed to get metadata for {key}: {e}")

    def list_blobs(
        self,
        prefix: str | None = None,
        delimiter: str | None = None,
        max_results: int = 1000,
        marker: str | None = None,
    ) -> BlobListResult:
        """List blobs in MinIO."""
        try:
            # Combine instance prefix with method prefix
            full_prefix = self._full_key(prefix) if prefix else self._prefix
            full_marker = self._full_key(marker) if marker else None

            objects = self._client.list_objects(
                bucket_name=self._bucket,
                prefix=full_prefix,
                recursive=(delimiter is None),
                start_after=full_marker,
            )

            blobs = []
            prefixes = set()
            count = 0

            for obj in objects:
                count += 1
                if count > max_results:
                    # MinIO doesn't support limit, so we break manually
                    return BlobListResult(
                        blobs=blobs,
                        prefixes=list(prefixes),
                        is_truncated=True,
                        next_marker=blobs[-1].key if blobs else None,
                    )

                # Check if it's a prefix (directory)
                if hasattr(obj, "is_dir") and obj.is_dir:
                    prefixes.add(self._strip_prefix(obj.object_name))
                else:
                    blobs.append(
                        BlobMetadata(
                            key=self._strip_prefix(obj.object_name),
                            size=obj.size,
                            content_type=None,  # Not available in list
                            last_modified=obj.last_modified,
                            etag=obj.etag,
                            custom_metadata={},
                        )
                    )

            return BlobListResult(
                blobs=blobs, prefixes=list(prefixes), is_truncated=False, next_marker=None
            )

        except S3Error as e:
            raise BlobStorageError(f"Failed to list blobs: {e}")

    def generate_presigned_url(
        self, key: str, expiration: timedelta = timedelta(hours=1), method: str = "GET"
    ) -> str:
        """Generate a presigned URL for MinIO."""
        try:
            full_key = self._full_key(key)
            url = self._client.presigned_get_object(
                bucket_name=self._bucket, object_name=full_key, expires=expiration
            )
            return url

        except S3Error as e:
            raise BlobStorageError(f"Failed to generate presigned URL for {key}: {e}")

    def copy(self, source_key: str, dest_key: str) -> None:
        """Copy a blob in MinIO."""
        try:
            from minio.commonconfig import CopySource

            full_source_key = self._full_key(source_key)
            full_dest_key = self._full_key(dest_key)

            self._client.copy_object(
                bucket_name=self._bucket,
                object_name=full_dest_key,
                source=CopySource(self._bucket, full_source_key),
            )

            logger.info(f"Copied blob: {source_key} -> {dest_key}")

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Source blob not found: {source_key}")
            raise BlobStorageError(f"Failed to copy blob: {e}")

    def get_size(self, key: str) -> int:
        """Get the size of a blob in MinIO."""
        try:
            full_key = self._full_key(key)
            stat = self._client.stat_object(self._bucket, full_key)
            return stat.size

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise BlobNotFoundError(f"Blob not found: {key}")
            raise BlobStorageError(f"Failed to get size for {key}: {e}")
