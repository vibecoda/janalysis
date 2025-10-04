"""Tests for BlobStorage high-level API."""

from __future__ import annotations

from io import BytesIO

import pytest

from jqsys.core.storage import BlobStorage
from jqsys.core.storage.backends import FilesystemBackend
from jqsys.core.storage.blob import BlobNotFoundError


class TestBlobStorage:
    """Test suite for BlobStorage high-level API."""

    @pytest.fixture
    def temp_storage(self, tmp_path):
        """Create a temporary filesystem-backed blob storage."""
        backend = FilesystemBackend(base_path=tmp_path)
        storage = BlobStorage(backend)
        return storage

    def test_put_and_get_bytes(self, temp_storage):
        """Test storing and retrieving bytes."""
        data = b"Hello, blob storage!"
        etag = temp_storage.put("test.txt", data)

        assert etag is not None
        retrieved = temp_storage.get("test.txt")
        assert retrieved == data

    def test_put_and_get_with_path(self, temp_storage, tmp_path):
        """Test storing data from a file path."""
        # Create a test file
        test_file = tmp_path / "source.txt"
        test_file.write_bytes(b"File content")

        # Store from path
        etag = temp_storage.put("stored.txt", test_file)
        assert etag is not None

        # Retrieve and verify
        retrieved = temp_storage.get("stored.txt")
        assert retrieved == b"File content"

    def test_put_with_metadata(self, temp_storage):
        """Test storing blob with custom metadata."""
        data = b"test data"
        metadata = {"author": "test", "version": "1.0"}

        temp_storage.put("test.txt", data, metadata=metadata)

        # Get metadata and verify
        blob_meta = temp_storage.get_metadata("test.txt")
        assert blob_meta.custom_metadata == metadata

    def test_put_with_content_type(self, temp_storage):
        """Test storing blob with content type."""
        data = b'{"key": "value"}'
        temp_storage.put("data.json", data, content_type="application/json")

        metadata = temp_storage.get_metadata("data.json")
        assert metadata.content_type == "application/json"

    def test_get_stream(self, temp_storage):
        """Test retrieving blob as a stream."""
        data = b"Stream data content"
        temp_storage.put("stream.txt", data)

        with temp_storage.get_stream("stream.txt") as stream:
            retrieved = stream.read()
            assert retrieved == data

    def test_put_from_stream(self, temp_storage):
        """Test storing blob from a stream."""
        data = b"Stream input data"
        stream = BytesIO(data)

        temp_storage.put("from_stream.txt", stream)
        retrieved = temp_storage.get("from_stream.txt")
        assert retrieved == data

    def test_delete(self, temp_storage):
        """Test deleting a blob."""
        temp_storage.put("to_delete.txt", b"delete me")
        assert temp_storage.exists("to_delete.txt")

        temp_storage.delete("to_delete.txt")
        assert not temp_storage.exists("to_delete.txt")

    def test_delete_nonexistent(self, temp_storage):
        """Test deleting a non-existent blob raises error."""
        with pytest.raises(BlobNotFoundError):
            temp_storage.delete("nonexistent.txt")

    def test_delete_many(self, temp_storage):
        """Test batch deletion of blobs."""
        # Create multiple blobs
        for i in range(5):
            temp_storage.put(f"file{i}.txt", f"content {i}".encode())

        # Delete some
        keys = ["file0.txt", "file2.txt", "file4.txt", "nonexistent.txt"]
        results = temp_storage.delete_many(keys)

        # Check results
        assert results["file0.txt"] is True
        assert results["file2.txt"] is True
        assert results["file4.txt"] is True
        assert results["nonexistent.txt"] is False

        # Verify deletions
        assert not temp_storage.exists("file0.txt")
        assert temp_storage.exists("file1.txt")
        assert not temp_storage.exists("file2.txt")
        assert temp_storage.exists("file3.txt")
        assert not temp_storage.exists("file4.txt")

    def test_exists(self, temp_storage):
        """Test checking blob existence."""
        assert not temp_storage.exists("test.txt")

        temp_storage.put("test.txt", b"data")
        assert temp_storage.exists("test.txt")

        temp_storage.delete("test.txt")
        assert not temp_storage.exists("test.txt")

    def test_get_metadata(self, temp_storage):
        """Test retrieving blob metadata."""
        data = b"test data for metadata"
        metadata = {"key": "value"}

        temp_storage.put("test.txt", data, content_type="text/plain", metadata=metadata)

        blob_meta = temp_storage.get_metadata("test.txt")

        assert blob_meta.key == "test.txt"
        assert blob_meta.size == len(data)
        assert blob_meta.content_type == "text/plain"
        assert blob_meta.custom_metadata == metadata
        assert blob_meta.last_modified is not None

    def test_get_size(self, temp_storage):
        """Test getting blob size."""
        data = b"x" * 1024  # 1KB
        temp_storage.put("test.bin", data)

        size = temp_storage.get_size("test.bin")
        assert size == 1024

    def test_list_blobs(self, temp_storage):
        """Test listing blobs."""
        # Create test blobs
        temp_storage.put("file1.txt", b"data1")
        temp_storage.put("file2.txt", b"data2")
        temp_storage.put("file3.txt", b"data3")

        # List all
        blobs = list(temp_storage.list())
        assert len(blobs) == 3

        keys = {blob.key for blob in blobs}
        assert keys == {"file1.txt", "file2.txt", "file3.txt"}

    def test_list_with_prefix(self, temp_storage):
        """Test listing blobs with prefix filter."""
        # Create blobs with different prefixes
        temp_storage.put("docs/readme.txt", b"readme")
        temp_storage.put("docs/guide.txt", b"guide")
        temp_storage.put("images/photo.jpg", b"photo")
        temp_storage.put("root.txt", b"root")

        # List with prefix
        docs_blobs = list(temp_storage.list(prefix="docs/"))
        assert len(docs_blobs) == 2

        keys = {blob.key for blob in docs_blobs}
        assert keys == {"docs/readme.txt", "docs/guide.txt"}

    def test_list_prefixes(self, temp_storage):
        """Test listing prefixes (directory-like structure)."""
        # Create nested structure
        temp_storage.put("docs/readme.txt", b"readme")
        temp_storage.put("images/photo.jpg", b"photo")
        temp_storage.put("videos/clip.mp4", b"video")

        # List top-level prefixes
        prefixes = temp_storage.list_prefixes()

        assert set(prefixes) == {"docs/", "images/", "videos/"}

    def test_copy(self, temp_storage):
        """Test copying a blob."""
        data = b"original data"
        temp_storage.put("source.txt", data)

        temp_storage.copy("source.txt", "destination.txt")

        # Both should exist with same content
        assert temp_storage.exists("source.txt")
        assert temp_storage.exists("destination.txt")
        assert temp_storage.get("destination.txt") == data

    def test_download_to_file(self, temp_storage, tmp_path):
        """Test downloading blob to a file."""
        data = b"download this content"
        temp_storage.put("source.txt", data)

        download_path = tmp_path / "downloaded.txt"
        temp_storage.download_to_file("source.txt", download_path)

        assert download_path.exists()
        assert download_path.read_bytes() == data

    def test_get_nonexistent_blob(self, temp_storage):
        """Test getting non-existent blob raises error."""
        with pytest.raises(BlobNotFoundError):
            temp_storage.get("nonexistent.txt")

    def test_get_metadata_nonexistent(self, temp_storage):
        """Test getting metadata for non-existent blob raises error."""
        with pytest.raises(BlobNotFoundError):
            temp_storage.get_metadata("nonexistent.txt")

    def test_large_blob(self, temp_storage):
        """Test handling large blobs."""
        # Create 5MB blob
        large_data = b"x" * (5 * 1024 * 1024)
        temp_storage.put("large.bin", large_data)

        # Verify size
        size = temp_storage.get_size("large.bin")
        assert size == len(large_data)

        # Verify content
        retrieved = temp_storage.get("large.bin")
        assert len(retrieved) == len(large_data)

    def test_blob_with_special_characters(self, temp_storage):
        """Test blob keys with special characters."""
        keys = [
            "file with spaces.txt",
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.multiple.dots.txt",
        ]

        for key in keys:
            temp_storage.put(key, b"data")
            assert temp_storage.exists(key)
            assert temp_storage.get(key) == b"data"

    def test_nested_paths(self, temp_storage):
        """Test deeply nested blob paths."""
        key = "level1/level2/level3/level4/file.txt"
        data = b"nested data"

        temp_storage.put(key, data)
        assert temp_storage.get(key) == data

    def test_overwrite_blob(self, temp_storage):
        """Test overwriting an existing blob."""
        key = "overwrite.txt"

        # Write initial data
        temp_storage.put(key, b"original")
        assert temp_storage.get(key) == b"original"

        # Overwrite
        temp_storage.put(key, b"updated")
        assert temp_storage.get(key) == b"updated"

    def test_empty_blob(self, temp_storage):
        """Test storing empty blob."""
        temp_storage.put("empty.txt", b"")

        assert temp_storage.exists("empty.txt")
        assert temp_storage.get("empty.txt") == b""
        assert temp_storage.get_size("empty.txt") == 0


class TestBlobStorageFromName:
    """Test BlobStorage.from_name() class method."""

    def test_from_name_basic(self):
        """Test creating storage from named backend."""
        storage = BlobStorage.from_name("dev")

        # Should be able to use it
        storage.put("test.txt", b"data")
        assert storage.exists("test.txt")

        # Cleanup
        storage.delete("test.txt")

    def test_from_name_with_namespace(self):
        """Test creating storage with namespace."""
        storage = BlobStorage.from_name("dev.test_namespace")

        storage.put("file.txt", b"data")

        # Should exist in namespaced path
        base_storage = BlobStorage.from_name("dev")
        all_blobs = list(base_storage.list())

        # Find our file in the namespace
        keys = [blob.key for blob in all_blobs]
        assert "test_namespace/file.txt" in keys

        # Cleanup
        storage.delete("file.txt")

    def test_from_name_nonexistent_backend(self):
        """Test error when using non-existent backend."""
        from jqsys.core.storage import BackendNotFoundError

        with pytest.raises(BackendNotFoundError):
            BlobStorage.from_name("nonexistent_backend")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
