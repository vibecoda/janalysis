from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from jqsys.utils.env import load_env_file_if_present


class TestLoadEnvFileIfPresent:
    def test_load_existing_env_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = """
# This is a comment
JQ_REFRESH_TOKEN=test_refresh_token_123
API_URL=https://custom.api.com
EMPTY_LINE=

# Another comment
QUOTED_VALUE="quoted_string"
SINGLE_QUOTED='single_quoted'
"""
        env_file.write_text(env_content)
        
        # Clear environment first
        original_env = os.environ.copy()
        for key in ["JQ_REFRESH_TOKEN", "API_URL", "QUOTED_VALUE", "SINGLE_QUOTED"]:
            os.environ.pop(key, None)
        
        try:
            result = load_env_file_if_present(env_file)
            
            expected = {
                "JQ_REFRESH_TOKEN": "test_refresh_token_123",
                "API_URL": "https://custom.api.com",
                "EMPTY_LINE": "",
                "QUOTED_VALUE": "quoted_string",
                "SINGLE_QUOTED": "single_quoted"
            }
            assert result == expected
            
            # Check that os.environ was updated
            assert os.environ["JQ_REFRESH_TOKEN"] == "test_refresh_token_123"
            assert os.environ["API_URL"] == "https://custom.api.com"
            assert os.environ["QUOTED_VALUE"] == "quoted_string"
            assert os.environ["SINGLE_QUOTED"] == "single_quoted"
            
        finally:
            # Restore original environment
            os.environ.clear()
            os.environ.update(original_env)

    def test_nonexistent_file_returns_empty_dict(self, tmp_path):
        nonexistent_file = tmp_path / "nonexistent.env"
        
        result = load_env_file_if_present(nonexistent_file)
        
        assert result == {}

    def test_default_dotenv_path(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_KEY=test_value")
        
        with patch("jqsys.utils.env.Path") as mock_path:
            mock_path.return_value = env_file
            
            result = load_env_file_if_present()
            
            assert result == {"TEST_KEY": "test_value"}

    def test_ignore_comments_and_empty_lines(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = """
# Full line comment
    # Indented comment
    
        
VALID_KEY=valid_value
# Another comment
ANOTHER_KEY=another_value

    # Final comment
"""
        env_file.write_text(env_content)
        
        result = load_env_file_if_present(env_file)
        
        expected = {
            "VALID_KEY": "valid_value", 
            "ANOTHER_KEY": "another_value"
        }
        assert result == expected

    def test_ignore_lines_without_equals(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = """
VALID_KEY=valid_value
INVALID_LINE_NO_EQUALS
ANOTHER_VALID=another_value
ALSO_INVALID
"""
        env_file.write_text(env_content)
        
        result = load_env_file_if_present(env_file)
        
        expected = {
            "VALID_KEY": "valid_value",
            "ANOTHER_VALID": "another_value"
        }
        assert result == expected

    def test_handle_equals_in_value(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = """
CONNECTION_STRING=postgresql://user:pass@host:5432/db?param=value
URL_WITH_QUERY=https://api.example.com/endpoint?key=value&another=test
"""
        env_file.write_text(env_content)
        
        result = load_env_file_if_present(env_file)
        
        expected = {
            "CONNECTION_STRING": "postgresql://user:pass@host:5432/db?param=value",
            "URL_WITH_QUERY": "https://api.example.com/endpoint?key=value&another=test"
        }
        assert result == expected

    def test_strip_whitespace_from_keys_and_values(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = """
  PADDED_KEY  =  padded_value  
ANOTHER_KEY=  another_value
  THIRD_KEY=third_value  
"""
        env_file.write_text(env_content)
        
        result = load_env_file_if_present(env_file)
        
        expected = {
            "PADDED_KEY": "padded_value",
            "ANOTHER_KEY": "another_value", 
            "THIRD_KEY": "third_value"
        }
        assert result == expected

    def test_respect_existing_environment_variables(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("EXISTING_VAR=from_file")
        
        # Set environment variable before loading
        os.environ["EXISTING_VAR"] = "from_env"
        
        try:
            result = load_env_file_if_present(env_file)
            
            # Should return the file value in the dict
            assert result == {"EXISTING_VAR": "from_file"}
            # But environment should keep existing value
            assert os.environ["EXISTING_VAR"] == "from_env"
            
        finally:
            os.environ.pop("EXISTING_VAR", None)

    def test_utf8_encoding(self, tmp_path):
        env_file = tmp_path / ".env"
        env_content = "UNICODE_KEY=héllo_wörld_🌍"
        env_file.write_text(env_content, encoding="utf-8")
        
        result = load_env_file_if_present(env_file)
        
        assert result == {"UNICODE_KEY": "héllo_wörld_🌍"}

    def test_pathlib_path_input(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("PATH_TEST=success")
        
        # Test with Path object instead of string
        result = load_env_file_if_present(Path(env_file))
        
        assert result == {"PATH_TEST": "success"}

    def test_string_path_input(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("STRING_TEST=success")
        
        # Test with string path
        result = load_env_file_if_present(str(env_file))
        
        assert result == {"STRING_TEST": "success"}