"""Configuration loading utilities using importlib.

This module provides utilities to dynamically load configuration from Python
modules using importlib, allowing flexible configuration management without
hardcoded imports.

Supports configuration inheritance using the "__inherits__" key.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when configuration loading or resolution fails."""

    pass


def load_config_from_module(
    module_path: str,
    config_name: str = "CONFIGURATION",
    default: Any | None = None,
) -> Any:
    """Load a configuration object from a Python module using importlib.

    This function dynamically imports a module and retrieves a named attribute,
    typically used for loading configuration dictionaries.

    Args:
        module_path: Dotted module path (e.g., "jqsys.core.storage.blob_config")
        config_name: Name of the configuration object to retrieve (default: "CONFIGURATION")
        default: Default value to return if loading fails

    Returns:
        The configuration object from the module, or default if loading fails

    Examples:
        >>> config = load_config_from_module("jqsys.core.storage.blob_config")
        >>> custom = load_config_from_module("myapp.custom_config", "SETTINGS")
    """
    try:
        # Dynamically import the module
        module = importlib.import_module(module_path)

        # Retrieve the configuration attribute
        if not hasattr(module, config_name):
            logger.warning(f"Module '{module_path}' does not have attribute '{config_name}'")
            return default

        config = getattr(module, config_name)
        logger.debug(f"Loaded configuration from {module_path}.{config_name}")
        return config

    except ImportError as e:
        logger.warning(f"Could not import module '{module_path}': {e}")
        return default

    except Exception as e:
        logger.error(f"Error loading configuration from '{module_path}': {e}")
        return default


def load_config_with_fallback(
    primary_module: str,
    fallback_modules: list[str] | None = None,
    config_name: str = "CONFIGURATION",
    default: Any | None = None,
) -> Any:
    """Load configuration with fallback to alternative modules.

    Attempts to load configuration from the primary module first, then tries
    fallback modules in order until one succeeds.

    Args:
        primary_module: Primary module path to try first
        fallback_modules: List of fallback module paths to try
        config_name: Name of the configuration object to retrieve
        default: Default value if all loading attempts fail

    Returns:
        Configuration object from first successful module, or default

    Examples:
        >>> config = load_config_with_fallback(
        ...     "myapp.config",
        ...     fallback_modules=["jqsys.core.storage.blob_config"]
        ... )
    """
    # Try primary module
    config = load_config_from_module(primary_module, config_name, default=None)
    if config is not None:
        return config

    # Try fallback modules
    if fallback_modules:
        for fallback in fallback_modules:
            config = load_config_from_module(fallback, config_name, default=None)
            if config is not None:
                logger.info(f"Using fallback configuration from '{fallback}'")
                return config

    # All attempts failed, return default
    logger.warning(f"Could not load configuration from any module, using default: {default}")
    return default


def resolve_config_inheritance(config_dict: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Resolve inheritance in a configuration dictionary.

    Configurations can inherit from other configurations using the "__inherits__" key.
    This function resolves all inheritance relationships and returns a fully resolved
    configuration dictionary where all values are expanded.

    Args:
        config_dict: Configuration dictionary with potential inheritance relationships

    Returns:
        Fully resolved configuration dictionary with all inheritance applied

    Raises:
        ConfigError: If circular inheritance detected or parent not found

    Examples:
        >>> config = {
        ...     "bronze": {"type": "minio", "endpoint": "localhost", "prefix": "bronze"},
        ...     "silver": {"__inherits__": "bronze", "prefix": "silver"}
        ... }
        >>> resolved = resolve_config_inheritance(config)
        >>> resolved["silver"]["endpoint"]  # Inherited from bronze
        'localhost'
        >>> resolved["silver"]["prefix"]  # Overridden
        'silver'
    """
    resolved_configs = {}

    def _resolve_single(name: str, config: dict[str, Any], visited: set[str]) -> dict[str, Any]:
        """Recursively resolve a single configuration with inheritance."""
        # Check for circular inheritance
        if name in visited:
            chain = " -> ".join(visited) + f" -> {name}"
            raise ConfigError(f"Circular inheritance detected: {chain}")

        # If already resolved, return cached result
        if name in resolved_configs:
            return resolved_configs[name]

        # If no inheritance, return config as-is
        if "__inherits__" not in config:
            resolved = config.copy()
            resolved_configs[name] = resolved
            return resolved

        # Get parent name
        parent_name = config["__inherits__"]

        # Check if parent exists
        if parent_name not in config_dict:
            raise ConfigError(
                f"Configuration '{name}' inherits from '{parent_name}', "
                f"but '{parent_name}' not found"
            )

        # Add current name to visited set
        new_visited = visited | {name}

        # Recursively resolve parent configuration
        parent_config = config_dict[parent_name]
        resolved_parent = _resolve_single(parent_name, parent_config, new_visited)

        # Merge: start with parent config, override with child config
        resolved = resolved_parent.copy()

        # Update with child config (excluding the __inherits__ key)
        for key, value in config.items():
            if key != "__inherits__":
                resolved[key] = value

        logger.debug(f"Resolved inheritance for '{name}' from '{parent_name}'")
        resolved_configs[name] = resolved
        return resolved

    # Resolve all configurations
    for name, config in config_dict.items():
        if name not in resolved_configs:
            _resolve_single(name, config, set())

    return resolved_configs


def load_and_resolve_config(
    module_path: str,
    config_name: str = "CONFIGURATION",
    default: Any | None = None,
) -> dict[str, dict[str, Any]]:
    """Load configuration from module and resolve all inheritance relationships.

    This is the main entry point for loading configurations. It handles both
    loading from the module and resolving any inheritance relationships.

    Args:
        module_path: Dotted module path (e.g., "configs.blob_backends")
        config_name: Name of the configuration object to retrieve
        default: Default value to return if loading fails

    Returns:
        Fully resolved configuration dictionary

    Examples:
        >>> config = load_and_resolve_config("configs.blob_backends")
        >>> # All inheritance is already resolved
        >>> config["silver"]["endpoint"]  # Has inherited endpoint from bronze
    """
    # Load raw config
    raw_config = load_config_from_module(module_path, config_name, default)

    if raw_config is None or not isinstance(raw_config, dict):
        logger.warning(f"Invalid configuration loaded from {module_path}, using default")
        return default or {}

    # Resolve inheritance
    try:
        resolved = resolve_config_inheritance(raw_config)
        logger.info(f"Loaded and resolved {len(resolved)} configurations from {module_path}")
        return resolved
    except ConfigError as e:
        logger.error(f"Failed to resolve configuration inheritance: {e}")
        raise
