"""Tests for configuration loading and inheritance resolution."""

from __future__ import annotations

import pytest

from jqsys.core.utils.config import ConfigError, resolve_config_inheritance


class TestConfigInheritance:
    """Test suite for configuration inheritance resolution."""

    def test_resolve_inheritance_basic(self):
        """Test basic configuration inheritance."""
        config = {
            "parent": {
                "type": "minio",
                "endpoint": "localhost:9000",
                "bucket": "test",
                "option1": "value1",
                "option2": "value2",
            },
            "child": {
                "__inherits__": "parent",
                "option2": "overridden",
            },
        }

        resolved = resolve_config_inheritance(config)

        # Parent should be unchanged
        assert resolved["parent"]["type"] == "minio"
        assert resolved["parent"]["option1"] == "value1"
        assert resolved["parent"]["option2"] == "value2"

        # Child should inherit and override
        assert resolved["child"]["type"] == "minio"
        assert resolved["child"]["endpoint"] == "localhost:9000"
        assert resolved["child"]["bucket"] == "test"
        assert resolved["child"]["option1"] == "value1"
        assert resolved["child"]["option2"] == "overridden"
        assert "__inherits__" not in resolved["child"]

    def test_resolve_inheritance_multi_level(self):
        """Test multi-level inheritance (grandchild -> child -> parent)."""
        config = {
            "parent": {
                "type": "minio",
                "option1": "parent_value",
                "option2": "parent_value",
                "option3": "parent_value",
            },
            "child": {
                "__inherits__": "parent",
                "option2": "child_value",
            },
            "grandchild": {
                "__inherits__": "child",
                "option3": "grandchild_value",
            },
        }

        resolved = resolve_config_inheritance(config)

        # Grandchild should have values from all levels
        assert resolved["grandchild"]["type"] == "minio"
        assert resolved["grandchild"]["option1"] == "parent_value"
        assert resolved["grandchild"]["option2"] == "child_value"
        assert resolved["grandchild"]["option3"] == "grandchild_value"
        assert "__inherits__" not in resolved["grandchild"]

    def test_resolve_inheritance_circular_detection(self):
        """Test that circular inheritance is detected."""
        config = {
            "a": {"__inherits__": "b"},
            "b": {"__inherits__": "a"},
        }

        with pytest.raises(ConfigError, match="Circular inheritance detected"):
            resolve_config_inheritance(config)

    def test_resolve_inheritance_self_reference(self):
        """Test that self-referencing inheritance is detected."""
        config = {
            "a": {"__inherits__": "a"},
        }

        with pytest.raises(ConfigError, match="Circular inheritance detected"):
            resolve_config_inheritance(config)

    def test_resolve_inheritance_missing_parent(self):
        """Test error when parent config doesn't exist."""
        config = {
            "child": {"__inherits__": "nonexistent"},
        }

        with pytest.raises(
            ConfigError, match="inherits from 'nonexistent', but 'nonexistent' not found"
        ):
            resolve_config_inheritance(config)

    def test_resolve_inheritance_no_inheritance(self):
        """Test that configs without inheritance are returned as-is."""
        config = {
            "standalone": {
                "type": "filesystem",
                "path": "/tmp/test",
            },
        }

        resolved = resolve_config_inheritance(config)

        assert resolved["standalone"]["type"] == "filesystem"
        assert resolved["standalone"]["path"] == "/tmp/test"
        assert "__inherits__" not in resolved["standalone"]

    def test_resolve_inheritance_multiple_children(self):
        """Test that multiple children can inherit from same parent."""
        config = {
            "bronze": {
                "type": "minio",
                "endpoint": "localhost:9000",
                "bucket": "jq-data",
                "prefix": "bronze",
            },
            "silver": {
                "__inherits__": "bronze",
                "prefix": "silver",
            },
            "gold": {
                "__inherits__": "bronze",
                "prefix": "gold",
            },
        }

        resolved = resolve_config_inheritance(config)

        # All should have same endpoint and bucket
        assert resolved["silver"]["endpoint"] == "localhost:9000"
        assert resolved["gold"]["endpoint"] == "localhost:9000"
        assert resolved["silver"]["bucket"] == "jq-data"
        assert resolved["gold"]["bucket"] == "jq-data"

        # But different prefixes
        assert resolved["bronze"]["prefix"] == "bronze"
        assert resolved["silver"]["prefix"] == "silver"
        assert resolved["gold"]["prefix"] == "gold"

    def test_resolve_inheritance_mixed_configs(self):
        """Test resolving a mix of inherited and standalone configs."""
        config = {
            "standalone1": {
                "type": "filesystem",
                "path": "/tmp/test1",
            },
            "parent": {
                "type": "minio",
                "endpoint": "localhost:9000",
            },
            "child": {
                "__inherits__": "parent",
                "bucket": "test",
            },
            "standalone2": {
                "type": "filesystem",
                "path": "/tmp/test2",
            },
        }

        resolved = resolve_config_inheritance(config)

        # Standalone configs unchanged
        assert resolved["standalone1"]["type"] == "filesystem"
        assert resolved["standalone2"]["type"] == "filesystem"

        # Child config resolved
        assert resolved["child"]["type"] == "minio"
        assert resolved["child"]["endpoint"] == "localhost:9000"
        assert resolved["child"]["bucket"] == "test"

    def test_resolve_inheritance_empty_config(self):
        """Test resolving empty configuration."""
        config = {}
        resolved = resolve_config_inheritance(config)
        assert resolved == {}

    def test_resolve_inheritance_chain(self):
        """Test a longer inheritance chain."""
        config = {
            "level0": {"type": "test", "val0": "0"},
            "level1": {"__inherits__": "level0", "val1": "1"},
            "level2": {"__inherits__": "level1", "val2": "2"},
            "level3": {"__inherits__": "level2", "val3": "3"},
        }

        resolved = resolve_config_inheritance(config)

        # level3 should have all values
        assert resolved["level3"]["type"] == "test"
        assert resolved["level3"]["val0"] == "0"
        assert resolved["level3"]["val1"] == "1"
        assert resolved["level3"]["val2"] == "2"
        assert resolved["level3"]["val3"] == "3"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
