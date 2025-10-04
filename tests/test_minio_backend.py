"""Tests for MinIO backend implementation."""

from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import Mock, patch

import pytest

from jqsys.core.storage.backends.minio_backend import MinIOBackend
from jqsys.core.storage.blob import (
    BlobNotFoundError,
    BlobStorageConnectionError,
    BlobStorageError,
)


class TestMinIOBackendInit:
    """Test MinIO backend initialization."""

    @patch("jqsys.core.storage.backends.minio_backend.Minio")
    def test_init_creates_bucket_if_not_exists(self, mock_minio_class):
        """Test that initialization creates bucket if it doesn't exist."""
        mock_client = Mock()
        mock_client.bucket_exists.return_value = False
        mock_minio_class.return_value = mock_client

        backend = MinIOBackend(
            endpoint="localhost:9000",
            access_key="test_key",
            secret_key="test_secret",
            bucket="test-bucket",
            secure=False,
        )

        # Should check if bucket exists
        mock_client.bucket_exists.assert_called_once_with("test-bucket")

        # Should create bucket
        mock_client.make_bucket.assert_called_once_with("test-bucket", location=None)

        assert backend._bucket == "test-bucket"

    @patch("jqsys.core.storage.backends.minio_backend.Minio")
    def test_init_with_existing_bucket(self, mock_minio_class):
        """Test initialization with existing bucket."""
        mock_client = Mock()
        mock_client.bucket_exists.return_value = True
        mock_minio_class.return_value = mock_client

        MinIOBackend(
            endpoint="localhost:9000",
            access_key="test_key",
            secret_key="test_secret",
            bucket="existing-bucket",
        )

        # Should check if bucket exists
        mock_client.bucket_exists.assert_called_once_with("existing-bucket")

        # Should NOT create bucket
        mock_client.make_bucket.assert_not_called()

    @patch("jqsys.core.storage.backends.minio_backend.Minio")
    def test_init_with_region(self, mock_minio_class):
        """Test initialization with region parameter."""
        mock_client = Mock()
        mock_client.bucket_exists.return_value = False
        mock_minio_class.return_value = mock_client

        MinIOBackend(
            endpoint="s3.amazonaws.com",
            access_key="key",
            secret_key="secret",
            bucket="bucket",
            region="us-west-2",
        )

        # Should create bucket with region
        mock_client.make_bucket.assert_called_once_with("bucket", location="us-west-2")

    @patch("jqsys.core.storage.backends.minio_backend.Minio")
    def test_init_connection_error(self, mock_minio_class):
        """Test connection error during initialization."""
        from minio.error import S3Error

        mock_client = Mock()
        mock_client.bucket_exists.side_effect = S3Error(
            code="ConnectionError",
            message="Connection failed",
            resource="/",
            request_id="",
            host_id="",
            response=None,
        )
        mock_minio_class.return_value = mock_client

        with pytest.raises(BlobStorageConnectionError):
            MinIOBackend(
                endpoint="localhost:9000",
                access_key="key",
                secret_key="secret",
                bucket="bucket",
            )


class TestMinIOBackendOperations:
    """Test MinIO backend blob operations."""

    @pytest.fixture
    def mock_backend(self):
        """Create a MinIO backend with mocked client."""
        with patch("jqsys.core.storage.backends.minio_backend.Minio") as mock_minio_class:
            mock_client = Mock()
            mock_client.bucket_exists.return_value = True
            mock_minio_class.return_value = mock_client

            backend = MinIOBackend(
                endpoint="localhost:9000",
                access_key="test_key",
                secret_key="test_secret",
                bucket="test-bucket",
            )

            # Store mock client for access in tests
            backend._test_mock_client = mock_client

            yield backend

    def test_put_bytes(self, mock_backend):
        """Test storing bytes data."""
        mock_result = Mock()
        mock_result.etag = "abc123"
        mock_backend._test_mock_client.put_object.return_value = mock_result

        data = b"test data"
        etag = mock_backend.put("test.txt", data, content_type="text/plain")

        assert etag == "abc123"

        # Verify put_object was called correctly
        call_args = mock_backend._test_mock_client.put_object.call_args
        assert call_args.kwargs["bucket_name"] == "test-bucket"
        assert call_args.kwargs["object_name"] == "test.txt"
        assert call_args.kwargs["length"] == len(data)
        assert call_args.kwargs["content_type"] == "text/plain"

    def test_put_stream(self, mock_backend):
        """Test storing stream data."""
        mock_result = Mock()
        mock_result.etag = "def456"
        mock_backend._test_mock_client.put_object.return_value = mock_result

        data = b"stream data"
        stream = BytesIO(data)

        etag = mock_backend.put("stream.txt", stream)

        assert etag == "def456"

        call_args = mock_backend._test_mock_client.put_object.call_args
        assert call_args.kwargs["length"] == len(data)

    def test_put_with_metadata(self, mock_backend):
        """Test storing blob with custom metadata."""
        mock_result = Mock()
        mock_result.etag = "meta123"
        mock_backend._test_mock_client.put_object.return_value = mock_result

        metadata = {"key": "value", "author": "test"}
        mock_backend.put("test.txt", b"data", metadata=metadata)

        call_args = mock_backend._test_mock_client.put_object.call_args
        assert call_args.kwargs["metadata"] == metadata

    def test_put_error(self, mock_backend):
        """Test error handling during put."""
        from minio.error import S3Error

        mock_backend._test_mock_client.put_object.side_effect = S3Error(
            code="AccessDenied",
            message="Access denied",
            resource="/test.txt",
            request_id="",
            host_id="",
            response=None,
        )

        with pytest.raises(BlobStorageError):
            mock_backend.put("test.txt", b"data")

    def test_get(self, mock_backend):
        """Test retrieving blob data."""
        mock_response = Mock()
        mock_response.read.return_value = b"retrieved data"
        mock_backend._test_mock_client.get_object.return_value = mock_response

        data = mock_backend.get("test.txt")

        assert data == b"retrieved data"
        mock_backend._test_mock_client.get_object.assert_called_once_with("test-bucket", "test.txt")
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

    def test_get_not_found(self, mock_backend):
        """Test getting non-existent blob."""
        from minio.error import S3Error

        mock_backend._test_mock_client.get_object.side_effect = S3Error(
            code="NoSuchKey",
            message="Key not found",
            resource="/test.txt",
            request_id="",
            host_id="",
            response=None,
        )

        with pytest.raises(BlobNotFoundError):
            mock_backend.get("test.txt")

    def test_get_stream(self, mock_backend):
        """Test retrieving blob as stream."""
        mock_response = Mock()
        mock_backend._test_mock_client.get_object.return_value = mock_response

        stream = mock_backend.get_stream("test.txt")

        assert stream == mock_response
        mock_backend._test_mock_client.get_object.assert_called_once_with("test-bucket", "test.txt")

    def test_delete(self, mock_backend):
        """Test deleting a blob."""
        mock_backend.delete("test.txt")

        mock_backend._test_mock_client.remove_object.assert_called_once_with(
            "test-bucket", "test.txt"
        )

    def test_delete_not_found(self, mock_backend):
        """Test deleting non-existent blob."""
        from minio.error import S3Error

        mock_backend._test_mock_client.remove_object.side_effect = S3Error(
            code="NoSuchKey",
            message="Key not found",
            resource="/test.txt",
            request_id="",
            host_id="",
            response=None,
        )

        with pytest.raises(BlobNotFoundError):
            mock_backend.delete("test.txt")

    def test_delete_many(self, mock_backend):
        """Test batch deletion."""
        # Mock remove_objects to return no errors (all successful)
        mock_backend._test_mock_client.remove_objects.return_value = iter([])

        keys = ["file1.txt", "file2.txt", "file3.txt"]
        results = mock_backend.delete_many(keys)

        # All should succeed
        assert results == {"file1.txt": True, "file2.txt": True, "file3.txt": True}

    def test_delete_many_with_errors(self, mock_backend):
        """Test batch deletion with some failures."""
        # Mock remove_objects to return errors for some files
        error1 = Mock()
        error1.object_name = "file2.txt"

        mock_backend._test_mock_client.remove_objects.return_value = iter([error1])

        keys = ["file1.txt", "file2.txt", "file3.txt"]
        results = mock_backend.delete_many(keys)

        # file2.txt should fail, others succeed
        assert results == {"file1.txt": True, "file2.txt": False, "file3.txt": True}

    def test_exists_true(self, mock_backend):
        """Test checking blob existence when it exists."""
        mock_stat = Mock()
        mock_backend._test_mock_client.stat_object.return_value = mock_stat

        assert mock_backend.exists("test.txt") is True

    def test_exists_false(self, mock_backend):
        """Test checking blob existence when it doesn't exist."""
        from minio.error import S3Error

        mock_backend._test_mock_client.stat_object.side_effect = S3Error(
            code="NoSuchKey",
            message="Key not found",
            resource="/test.txt",
            request_id="",
            host_id="",
            response=None,
        )

        assert mock_backend.exists("test.txt") is False

    def test_get_metadata(self, mock_backend):
        """Test retrieving blob metadata."""
        mock_stat = Mock()
        mock_stat.size = 1024
        mock_stat.content_type = "text/plain"
        mock_stat.last_modified = datetime(2025, 10, 4, 12, 0, 0)
        mock_stat.etag = "abc123"
        mock_stat.metadata = {"key": "value"}

        mock_backend._test_mock_client.stat_object.return_value = mock_stat

        metadata = mock_backend.get_metadata("test.txt")

        assert metadata.key == "test.txt"
        assert metadata.size == 1024
        assert metadata.content_type == "text/plain"
        assert metadata.etag == "abc123"
        assert metadata.custom_metadata == {"key": "value"}

    def test_list_blobs(self, mock_backend):
        """Test listing blobs."""
        # Mock list_objects to return some objects
        mock_obj1 = Mock()
        mock_obj1.object_name = "file1.txt"
        mock_obj1.size = 100
        mock_obj1.last_modified = datetime(2025, 10, 4)
        mock_obj1.etag = "etag1"
        mock_obj1.is_dir = False

        mock_obj2 = Mock()
        mock_obj2.object_name = "file2.txt"
        mock_obj2.size = 200
        mock_obj2.last_modified = datetime(2025, 10, 4)
        mock_obj2.etag = "etag2"
        mock_obj2.is_dir = False

        mock_backend._test_mock_client.list_objects.return_value = iter([mock_obj1, mock_obj2])

        result = mock_backend.list_blobs()

        assert len(result.blobs) == 2
        assert result.blobs[0].key == "file1.txt"
        assert result.blobs[1].key == "file2.txt"
        assert result.is_truncated is False

    def test_list_blobs_with_prefix(self, mock_backend):
        """Test listing blobs with prefix."""
        mock_backend._test_mock_client.list_objects.return_value = iter([])

        mock_backend.list_blobs(prefix="docs/")

        call_args = mock_backend._test_mock_client.list_objects.call_args
        assert call_args.kwargs["prefix"] == "docs/"

    def test_list_blobs_truncated(self, mock_backend):
        """Test listing with truncation."""
        # Create more objects than max_results
        mock_objects = []
        for i in range(1001):  # More than default max_results of 1000
            obj = Mock()
            obj.object_name = f"file{i}.txt"
            obj.size = 100
            obj.last_modified = datetime(2025, 10, 4)
            obj.etag = f"etag{i}"
            obj.is_dir = False
            mock_objects.append(obj)

        mock_backend._test_mock_client.list_objects.return_value = iter(mock_objects)

        result = mock_backend.list_blobs(max_results=1000)

        assert len(result.blobs) == 1000
        assert result.is_truncated is True
        assert result.next_marker is not None

    def test_generate_presigned_url(self, mock_backend):
        """Test generating presigned URL."""
        mock_backend._test_mock_client.presigned_get_object.return_value = (
            "https://localhost:9000/test-bucket/test.txt?signature=xyz"
        )

        url = mock_backend.generate_presigned_url("test.txt", expiration=timedelta(hours=2))

        assert "signature=xyz" in url
        mock_backend._test_mock_client.presigned_get_object.assert_called_once()

    def test_copy(self, mock_backend):
        """Test copying a blob."""
        with patch("minio.commonconfig.CopySource") as mock_copy_source:
            mock_backend.copy("source.txt", "dest.txt")

            mock_copy_source.assert_called_once_with("test-bucket", "source.txt")
            mock_backend._test_mock_client.copy_object.assert_called_once()

    def test_copy_not_found(self, mock_backend):
        """Test copying non-existent blob."""
        from minio.error import S3Error

        with patch("minio.commonconfig.CopySource"):
            mock_backend._test_mock_client.copy_object.side_effect = S3Error(
                code="NoSuchKey",
                message="Source not found",
                resource="/source.txt",
                request_id="",
                host_id="",
                response=None,
            )

            with pytest.raises(BlobNotFoundError):
                mock_backend.copy("source.txt", "dest.txt")

    def test_get_size(self, mock_backend):
        """Test getting blob size."""
        mock_stat = Mock()
        mock_stat.size = 2048

        mock_backend._test_mock_client.stat_object.return_value = mock_stat

        size = mock_backend.get_size("test.txt")

        assert size == 2048


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
