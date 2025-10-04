"""Backend registry for named blob storage backends."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from jqsys.core.storage.backends.filesystem_backend import FilesystemBackend
from jqsys.core.storage.backends.minio_backend import MinIOBackend
from jqsys.core.storage.backends.prefixed_backend import PrefixedBlobBackend
from jqsys.core.storage.blob import BlobStorageBackend
from jqsys.core.utils.config import load_and_resolve_config

logger = logging.getLogger(__name__)


class BackendConfigError(Exception):
    """Raised when backend configuration is invalid."""

    pass


class BackendNotFoundError(Exception):
    """Raised when a named backend is not found in configuration."""

    pass


class BlobBackendRegistry:
    """Registry for managing named blob storage backends.

    The registry resolves backend names to configured backends, with support
    for hierarchical namespacing using dot notation.

    Examples:
        >>> registry = BlobBackendRegistry()
        >>> backend = registry.get_backend("dev")
        >>> backend = registry.get_backend("dev.images.thumbnails")
    """

    def __init__(self, configuration: dict[str, dict[str, Any]] | None = None):
        """Initialize the registry.

        Args:
            configuration: Backend configuration dict. If None, loads and resolves
                          configuration from configs/blob_backends.py with inheritance
        """
        if configuration is None:
            # Load and resolve configuration (including inheritance)
            configuration = load_and_resolve_config(
                "configs.blob_backends",
                config_name="CONFIGURATION",
                default={},
            )

        self._config = configuration
        self._backend_cache: dict[str, BlobStorageBackend] = {}

    def parse_name(self, name: str) -> tuple[str, str]:
        """Parse a backend name into base name and prefix.

        Args:
            name: Backend name (e.g., "dev", "dev.images", "dev.images.thumbnails")

        Returns:
            Tuple of (base_name, prefix) where prefix uses "/" separators

        Examples:
            >>> registry.parse_name("dev")
            ("dev", "")
            >>> registry.parse_name("dev.images.thumbnails")
            ("dev", "images/thumbnails")
        """
        parts = name.split(".")
        base_name = parts[0]
        prefix = "/".join(parts[1:]) if len(parts) > 1 else ""
        return base_name, prefix

    def create_backend(self, config: dict[str, Any]) -> BlobStorageBackend:
        """Create a backend instance from configuration.

        Args:
            config: Backend configuration dict with "type" and backend-specific params

        Returns:
            Instantiated backend

        Raises:
            BackendConfigError: If configuration is invalid
        """
        backend_type = config.get("type")

        if not backend_type:
            raise BackendConfigError("Backend configuration must specify 'type'")

        if backend_type == "filesystem":
            base_path = config.get("base_path")
            if not base_path:
                raise BackendConfigError("Filesystem backend requires 'base_path'")

            return FilesystemBackend(base_path=Path(base_path))

        elif backend_type == "minio":
            required_fields = ["endpoint", "access_key", "secret_key", "bucket"]
            missing = [f for f in required_fields if not config.get(f)]
            if missing:
                raise BackendConfigError(
                    f"MinIO backend missing required fields: {', '.join(missing)}"
                )

            return MinIOBackend(
                endpoint=config["endpoint"],
                access_key=config["access_key"],
                secret_key=config["secret_key"],
                bucket=config["bucket"],
                secure=config.get("secure", True),
                region=config.get("region"),
            )

        else:
            raise BackendConfigError(f"Unknown backend type: {backend_type}")

    def get_backend(self, name: str, use_cache: bool = True) -> BlobStorageBackend:
        """Get a backend instance by name.

        Supports hierarchical namespacing with dot notation. The backend returned
        will automatically handle prefixing for dotted names.

        Args:
            name: Backend name with optional namespace (e.g., "dev", "dev.images.thumbnails")
            use_cache: Whether to use cached backend instances

        Returns:
            Backend instance (wrapped with prefix if needed)

        Raises:
            BackendNotFoundError: If base name not found in configuration
            BackendConfigError: If backend configuration is invalid

        Examples:
            >>> registry = BlobBackendRegistry()
            >>> backend = registry.get_backend("dev")
            >>> prefixed = registry.get_backend("dev.images.thumbnails")
        """
        # Check cache first (cache the full name including prefix)
        if use_cache and name in self._backend_cache:
            return self._backend_cache[name]

        # Parse name into base and prefix
        base_name, prefix = self.parse_name(name)

        # Check if base backend exists in config
        if base_name not in self._config:
            available = ", ".join(self._config.keys())
            raise BackendNotFoundError(
                f"Backend '{base_name}' not found in configuration. "
                f"Available backends: {available or 'none'}"
            )

        # Get or create base backend
        if use_cache and base_name in self._backend_cache:
            base_backend = self._backend_cache[base_name]
        else:
            # Configuration is already resolved (inheritance handled by config loader)
            base_backend = self.create_backend(self._config[base_name])
            if use_cache:
                self._backend_cache[base_name] = base_backend

        # Wrap with prefix if needed
        backend = PrefixedBlobBackend(base_backend, prefix) if prefix else base_backend

        # Cache the final backend (including prefix wrapper)
        if use_cache:
            self._backend_cache[name] = backend

        logger.info(f"Created backend for '{name}' (base: {base_name}, prefix: {prefix or 'none'})")
        return backend

    def list_backends(self) -> list[str]:
        """List all configured backend names.

        Returns:
            List of backend names
        """
        return list(self._config.keys())

    def register(self, name: str, config: dict[str, Any]) -> None:
        """Register a new backend configuration.

        Args:
            name: Backend name
            config: Backend configuration dict
        """
        self._config[name] = config
        # Clear cache for this backend
        keys_to_remove = [k for k in self._backend_cache if k.startswith(name)]
        for key in keys_to_remove:
            del self._backend_cache[key]

    def clear_cache(self) -> None:
        """Clear the backend instance cache."""
        self._backend_cache.clear()


# Global registry instance
_default_registry: BlobBackendRegistry | None = None


def get_default_registry() -> BlobBackendRegistry:
    """Get the default global registry instance.

    Returns:
        Global BlobBackendRegistry instance
    """
    global _default_registry
    if _default_registry is None:
        _default_registry = BlobBackendRegistry()
    return _default_registry


def get_blob_backend(name: str) -> BlobStorageBackend:
    """Get a blob backend by name from the default registry.

    Convenience function for accessing backends without explicitly creating a registry.

    Args:
        name: Backend name (e.g., "dev", "dev.images.thumbnails")

    Returns:
        Backend instance

    Examples:
        >>> from jqsys.storage.registry import get_blob_backend
        >>> backend = get_blob_backend("dev.images")
    """
    return get_default_registry().get_backend(name)
