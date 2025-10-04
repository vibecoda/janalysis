"""Tests for backend registry and BlobStorage prefix functionality."""

from __future__ import annotations

import pytest

from jqsys.core.storage import (
    BackendConfigError,
    BackendNotFoundError,
    BlobBackendRegistry,
    BlobStorage,
)
from jqsys.core.storage.backends import FilesystemBackend


class TestBlobStorageWithPrefix:
    """Test suite for BlobStorage with prefixed backend."""

    @pytest.fixture
    def temp_backend(self, tmp_path):
        """Create a temporary filesystem backend."""
        return FilesystemBackend(base_path=tmp_path)

    def test_put_with_prefix(self, temp_backend):
        """Test that put adds prefix to key."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)

        storage.put("file.txt", b"data")

        # Should be stored at prefix/path/file.txt
        assert temp_backend.exists("prefix/path/file.txt")
        assert temp_backend.get("prefix/path/file.txt") == b"data"

    def test_get_with_prefix(self, temp_backend):
        """Test that get uses prefixed key."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file.txt", b"data")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        data = storage.get("file.txt")

        assert data == b"data"

    def test_delete_with_prefix(self, temp_backend):
        """Test that delete uses prefixed key."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file.txt", b"data")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        storage.delete("file.txt")

        assert not temp_backend.exists("prefix/path/file.txt")

    def test_exists_with_prefix(self, temp_backend):
        """Test that exists uses prefixed key."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file.txt", b"data")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)

        assert storage.exists("file.txt")
        assert not storage.exists("other.txt")

    def test_get_metadata_with_prefix(self, temp_backend):
        """Test that get_metadata uses prefixed key and returns unprefixed."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file.txt", b"data", metadata={"key": "value"})

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        metadata = storage.get_metadata("file.txt")

        assert metadata.key == "file.txt"  # Should be unprefixed
        assert metadata.custom_metadata == {"key": "value"}

    def test_list_blobs_with_prefix(self, temp_backend):
        """Test that list returns unprefixed keys."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file1.txt", b"data1")
        temp_backend.put("prefix/path/file2.txt", b"data2")
        temp_backend.put("other/file3.txt", b"data3")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        blobs = list(storage.list())

        keys = [blob.key for blob in blobs]
        assert set(keys) == {"file1.txt", "file2.txt"}

    def test_list_blobs_with_additional_prefix(self, temp_backend):
        """Test listing with additional user-provided prefix."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/docs/file1.txt", b"data1")
        temp_backend.put("prefix/path/docs/file2.txt", b"data2")
        temp_backend.put("prefix/path/images/file3.txt", b"data3")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        blobs = list(storage.list(prefix="docs/"))

        keys = [blob.key for blob in blobs]
        assert set(keys) == {"docs/file1.txt", "docs/file2.txt"}

    def test_delete_many_with_prefix(self, temp_backend):
        """Test batch deletion with prefixed keys."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/file1.txt", b"data1")
        temp_backend.put("prefix/path/file2.txt", b"data2")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        results = storage.delete_many(["file1.txt", "file2.txt"])

        assert results["file1.txt"] is True
        assert results["file2.txt"] is True
        assert not temp_backend.exists("prefix/path/file1.txt")

    def test_copy_with_prefix(self, temp_backend):
        """Test copying with prefixed keys."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        temp_backend.put("prefix/path/source.txt", b"data")

        prefixed = PrefixedBlobBackend(temp_backend, "prefix/path")
        storage = BlobStorage(prefixed)
        storage.copy("source.txt", "dest.txt")

        assert temp_backend.exists("prefix/path/dest.txt")
        assert temp_backend.get("prefix/path/dest.txt") == b"data"

    def test_empty_prefix(self, temp_backend):
        """Test that empty prefix works like no prefix."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        prefixed = PrefixedBlobBackend(temp_backend, "")
        storage = BlobStorage(prefixed)

        storage.put("file.txt", b"data")

        assert temp_backend.exists("file.txt")
        assert storage.get("file.txt") == b"data"

    def test_nested_prefix(self, temp_backend):
        """Test deeply nested prefix."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        prefixed = PrefixedBlobBackend(temp_backend, "level1/level2/level3")
        storage = BlobStorage(prefixed)

        storage.put("file.txt", b"data")

        assert temp_backend.exists("level1/level2/level3/file.txt")

    def test_prefix_normalization(self, temp_backend):
        """Test that prefix is normalized (trailing slash added)."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        prefixed1 = PrefixedBlobBackend(temp_backend, "prefix")
        prefixed2 = PrefixedBlobBackend(temp_backend, "prefix/")
        storage1 = BlobStorage(prefixed1)
        storage2 = BlobStorage(prefixed2)

        # Both should work the same way
        storage1.put("file.txt", b"data1")
        storage2.put("file.txt", b"data2")

        # Second put should overwrite first
        assert temp_backend.get("prefix/file.txt") == b"data2"


class TestBlobBackendRegistry:
    """Test suite for BlobBackendRegistry."""

    def test_parse_name_simple(self):
        """Test parsing simple backend name."""
        registry = BlobBackendRegistry({})

        base, prefix = registry.parse_name("dev")

        assert base == "dev"
        assert prefix == ""

    def test_parse_name_with_one_level(self):
        """Test parsing name with one level."""
        registry = BlobBackendRegistry({})

        base, prefix = registry.parse_name("dev.images")

        assert base == "dev"
        assert prefix == "images"

    def test_parse_name_with_multiple_levels(self):
        """Test parsing name with multiple levels."""
        registry = BlobBackendRegistry({})

        base, prefix = registry.parse_name("dev.images.thumbnails.small")

        assert base == "dev"
        assert prefix == "images/thumbnails/small"

    def test_create_filesystem_backend(self, tmp_path):
        """Test creating filesystem backend from config."""
        registry = BlobBackendRegistry({})

        config = {"type": "filesystem", "base_path": str(tmp_path)}

        backend = registry.create_backend(config)

        assert isinstance(backend, FilesystemBackend)

    def test_create_minio_backend(self):
        """Test creating MinIO backend from config."""
        from unittest.mock import patch

        registry = BlobBackendRegistry({})

        config = {
            "type": "minio",
            "endpoint": "localhost:9000",
            "access_key": "key",
            "secret_key": "secret",
            "bucket": "bucket",
        }

        with patch("jqsys.core.storage.registry.MinIOBackend") as mock_minio:
            registry.create_backend(config)

            mock_minio.assert_called_once()

    def test_create_backend_missing_type(self):
        """Test error when type is missing from config."""
        registry = BlobBackendRegistry({})

        with pytest.raises(BackendConfigError, match="must specify 'type'"):
            registry.create_backend({})

    def test_create_backend_unknown_type(self):
        """Test error with unknown backend type."""
        registry = BlobBackendRegistry({})

        with pytest.raises(BackendConfigError, match="Unknown backend type"):
            registry.create_backend({"type": "unknown"})

    def test_create_backend_missing_required_fields(self):
        """Test error when required fields are missing."""
        registry = BlobBackendRegistry({})

        # Filesystem without base_path
        with pytest.raises(BackendConfigError, match="requires 'base_path'"):
            registry.create_backend({"type": "filesystem"})

        # MinIO without required fields
        with pytest.raises(BackendConfigError, match="missing required fields"):
            registry.create_backend({"type": "minio", "endpoint": "localhost:9000"})

    def test_get_backend_simple(self, tmp_path):
        """Test getting backend by simple name."""
        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)
        backend = registry.get_backend("dev")

        assert isinstance(backend, FilesystemBackend)

    def test_get_backend_with_dotted_name(self, tmp_path):
        """Test getting backend with dotted name returns wrapped backend."""
        from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend

        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)
        backend1 = registry.get_backend("dev")
        backend2 = registry.get_backend("dev.images")

        # "dev" should be base backend, "dev.images" should be wrapped
        assert not isinstance(backend1, PrefixedBlobBackend)
        assert isinstance(backend2, PrefixedBlobBackend)

    def test_get_backend_not_found(self):
        """Test error when backend name not found."""
        registry = BlobBackendRegistry({})

        with pytest.raises(BackendNotFoundError, match="'nonexistent' not found"):
            registry.get_backend("nonexistent")

    def test_get_backend_caching(self, tmp_path):
        """Test that backends are cached."""
        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)

        backend1 = registry.get_backend("dev")
        backend2 = registry.get_backend("dev")

        assert backend1 is backend2

    def test_list_backends(self):
        """Test listing configured backends."""
        config = {"dev": {}, "prod": {}, "test": {}}

        registry = BlobBackendRegistry(config)
        backends = registry.list_backends()

        assert set(backends) == {"dev", "prod", "test"}

    def test_register_backend(self, tmp_path):
        """Test registering a new backend."""
        registry = BlobBackendRegistry({})

        config = {"type": "filesystem", "base_path": str(tmp_path)}
        registry.register("new_backend", config)

        assert "new_backend" in registry.list_backends()

        backend = registry.get_backend("new_backend")
        assert isinstance(backend, FilesystemBackend)

    def test_register_clears_cache(self, tmp_path):
        """Test that registering clears cache for that backend."""
        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)

        # Get backend to cache it
        backend1 = registry.get_backend("dev")

        # Register new config for same name
        new_path = tmp_path / "new"
        new_path.mkdir()
        registry.register("dev", {"type": "filesystem", "base_path": str(new_path)})

        # Should get new backend instance
        backend2 = registry.get_backend("dev")

        assert backend1 is not backend2

    def test_clear_cache(self, tmp_path):
        """Test clearing backend cache."""
        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)

        backend1 = registry.get_backend("dev")
        registry.clear_cache()
        backend2 = registry.get_backend("dev")

        assert backend1 is not backend2

    def test_loads_default_config(self):
        """Test that registry loads default config if none provided."""
        # This will load from blob_config.py
        registry = BlobBackendRegistry()

        # Should have some backends configured
        backends = registry.list_backends()
        assert len(backends) > 0

    def test_get_backend_with_use_cache_false(self, tmp_path):
        """Test getting backend without caching."""
        config = {"dev": {"type": "filesystem", "base_path": str(tmp_path)}}

        registry = BlobBackendRegistry(config)

        backend1 = registry.get_backend("dev", use_cache=False)
        backend2 = registry.get_backend("dev", use_cache=False)

        # Should be different instances
        assert backend1 is not backend2


class TestGlobalRegistryFunctions:
    """Test global registry utility functions."""

    def test_get_default_registry(self):
        """Test getting default global registry."""
        from jqsys.core.storage.registry import get_default_registry

        registry1 = get_default_registry()
        registry2 = get_default_registry()

        # Should return same instance
        assert registry1 is registry2

    def test_get_blob_backend(self):
        """Test get_blob_backend convenience function."""
        from jqsys.core.storage.registry import get_blob_backend

        backend = get_blob_backend("dev")

        assert backend is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
