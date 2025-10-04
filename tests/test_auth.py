from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from jqsys.data.auth import AuthError, build_auth_headers, get_id_token, load_refresh_token


class TestLoadRefreshToken:
    def test_load_from_environment(self, monkeypatch):
        monkeypatch.setenv("JQ_REFRESH_TOKEN", "test_token_123")
        token = load_refresh_token(dotenv=False)
        assert token == "test_token_123"

    def test_load_from_dotenv_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("JQ_REFRESH_TOKEN=dotenv_token_456")

        with patch("jqsys.core.utils.env.Path") as mock_path:
            mock_path.return_value = env_file
            token = load_refresh_token()
            assert token == "dotenv_token_456"

    def test_missing_token_raises_error(self, monkeypatch):
        monkeypatch.delenv("JQ_REFRESH_TOKEN", raising=False)

        with pytest.raises(AuthError, match="Missing refresh token"):
            load_refresh_token(dotenv=False)

    def test_custom_env_key(self, monkeypatch):
        monkeypatch.setenv("CUSTOM_TOKEN", "custom_value")
        token = load_refresh_token(env_key="CUSTOM_TOKEN", dotenv=False)
        assert token == "custom_value"


class TestGetIdToken:
    @patch("jqsys.data.auth.requests.post")
    def test_successful_auth_refresh(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"idToken": "id_token_789"}
        mock_post.return_value = mock_response

        id_token = get_id_token("refresh_token_123")

        assert id_token == "id_token_789"
        mock_post.assert_called_once_with(
            "https://api.jquants.com/v1/token/auth_refresh?refreshtoken=refresh_token_123"
        )

    @patch("jqsys.data.auth.requests.post")
    def test_auth_refresh_failure_with_json_error(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "Invalid refresh token"}
        mock_post.return_value = mock_response

        with pytest.raises(AuthError, match="Auth refresh failed: 401"):
            get_id_token("invalid_token")

    @patch("jqsys.data.auth.requests.post")
    def test_auth_refresh_failure_with_text_error(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.json.side_effect = Exception("Not JSON")
        mock_response.text = "Internal Server Error"
        mock_post.return_value = mock_response

        with pytest.raises(AuthError, match="Auth refresh failed: 500 Internal Server Error"):
            get_id_token("some_token")

    @patch("jqsys.data.auth.requests.post")
    def test_missing_id_token_in_response(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Success but no idToken"}
        mock_post.return_value = mock_response

        with pytest.raises(AuthError, match="idToken missing in response"):
            get_id_token("some_token")

    @patch("jqsys.data.auth.load_refresh_token")
    @patch("jqsys.data.auth.requests.post")
    def test_uses_loaded_refresh_token_when_none_provided(self, mock_post, mock_load):
        mock_load.return_value = "loaded_token"
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"idToken": "loaded_id_token"}
        mock_post.return_value = mock_response

        id_token = get_id_token()

        assert id_token == "loaded_id_token"
        mock_load.assert_called_once()
        mock_post.assert_called_once_with(
            "https://api.jquants.com/v1/token/auth_refresh?refreshtoken=loaded_token"
        )

    @patch("jqsys.data.auth.requests.post")
    def test_custom_api_url(self, mock_post):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"idToken": "custom_id_token"}
        mock_post.return_value = mock_response

        custom_url = "https://custom-api.example.com"
        id_token = get_id_token("token", api_url=custom_url)

        assert id_token == "custom_id_token"
        mock_post.assert_called_once_with(f"{custom_url}/v1/token/auth_refresh?refreshtoken=token")


class TestBuildAuthHeaders:
    def test_builds_bearer_header(self):
        headers = build_auth_headers("test_id_token")
        expected = {"Authorization": "Bearer test_id_token"}
        assert headers == expected

    def test_handles_empty_token(self):
        headers = build_auth_headers("")
        expected = {"Authorization": "Bearer "}
        assert headers == expected
