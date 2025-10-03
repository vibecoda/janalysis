from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
import requests

from jqsys.client import JQuantsClient


class TestJQuantsClient:
    def test_init_with_id_token(self):
        client = JQuantsClient(id_token="test_token")

        assert client.id_token == "test_token"
        assert client.api_url == "https://api.jquants.com"
        assert client.timeout == 30.0
        assert "Authorization" in client.headers
        assert client.headers["Authorization"] == "Bearer test_token"

    def test_init_with_custom_params(self):
        client = JQuantsClient(
            id_token="custom_token", api_url="https://custom.api.com", timeout=60.0
        )

        assert client.id_token == "custom_token"
        assert client.api_url == "https://custom.api.com"
        assert client.timeout == 60.0

    @patch("jqsys.client.get_id_token")
    @patch("jqsys.client.load_refresh_token")
    def test_from_env_classmethod(self, mock_load_refresh, mock_get_id):
        mock_load_refresh.return_value = "refresh_token"
        mock_get_id.return_value = "id_token_from_env"

        client = JQuantsClient.from_env()

        assert client.id_token == "id_token_from_env"
        mock_load_refresh.assert_called_once()
        mock_get_id.assert_called_once_with("refresh_token")

    def test_get_request(self):
        client = JQuantsClient(id_token="test_token")

        with patch.object(client.session, "get") as mock_get:
            mock_response = Mock()
            mock_get.return_value = mock_response

            result = client.get("/test/path", params={"key": "value"})

            assert result == mock_response
            mock_get.assert_called_once_with(
                "https://api.jquants.com/test/path",
                params={"key": "value"},
                headers={"Authorization": "Bearer test_token"},
                timeout=30.0,
            )

    def test_get_request_no_params(self):
        client = JQuantsClient(id_token="test_token")

        with patch.object(client.session, "get") as mock_get:
            mock_response = Mock()
            mock_get.return_value = mock_response

            client.get("/test/path")

            mock_get.assert_called_once_with(
                "https://api.jquants.com/test/path",
                params={},
                headers={"Authorization": "Bearer test_token"},
                timeout=30.0,
            )


class TestGetPaginated:
    def test_single_page_response(self):
        client = JQuantsClient(id_token="test_token")

        mock_response = Mock()
        mock_response.json.return_value = {
            "daily_quotes": [{"code": "1234", "price": 100}, {"code": "5678", "price": 200}]
        }

        with patch.object(client, "get", return_value=mock_response):
            result = client.get_paginated("/test/path", "daily_quotes")

            expected = [{"code": "1234", "price": 100}, {"code": "5678", "price": 200}]
            assert result == expected

    def test_multi_page_response(self):
        client = JQuantsClient(id_token="test_token")

        # First page response
        first_response = Mock()
        first_response.json.return_value = {
            "daily_quotes": [{"code": "1234", "price": 100}],
            "pagination_key": "next_page_key",
        }

        # Second page response
        second_response = Mock()
        second_response.json.return_value = {"daily_quotes": [{"code": "5678", "price": 200}]}

        with patch.object(client, "get") as mock_get:
            mock_get.side_effect = [first_response, second_response]

            result = client.get_paginated("/test/path", "daily_quotes", {"param": "value"})

            expected = [{"code": "1234", "price": 100}, {"code": "5678", "price": 200}]
            assert result == expected

            # Verify the pagination logic
            assert mock_get.call_count == 2

            # First call
            first_call_args = mock_get.call_args_list[0]
            assert first_call_args[0] == ("/test/path",)
            assert first_call_args[1]["params"] == {"param": "value"}

            # Second call includes pagination_key
            second_call_args = mock_get.call_args_list[1]
            assert second_call_args[0] == ("/test/path",)
            assert second_call_args[1]["params"] == {
                "param": "value",
                "pagination_key": "next_page_key",
            }

    def test_empty_response(self):
        client = JQuantsClient(id_token="test_token")

        mock_response = Mock()
        mock_response.json.return_value = {}

        with patch.object(client, "get", return_value=mock_response):
            result = client.get_paginated("/test/path", "missing_key")
            assert result == []

    def test_http_error_raises_exception(self):
        client = JQuantsClient(id_token="test_token")

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "400 Bad Request"
        )

        with (
            patch.object(client, "get", return_value=mock_response),
            pytest.raises(requests.exceptions.HTTPError),
        ):
            client.get_paginated("/test/path", "data_key")

    def test_three_page_pagination(self):
        client = JQuantsClient(id_token="test_token")

        # Mock responses for 3 pages
        responses = [
            Mock(),  # Page 1
            Mock(),  # Page 2
            Mock(),  # Page 3
        ]

        responses[0].json.return_value = {"info": [{"id": 1}], "pagination_key": "page2"}

        responses[1].json.return_value = {"info": [{"id": 2}], "pagination_key": "page3"}

        responses[2].json.return_value = {"info": [{"id": 3}]}

        with patch.object(client, "get") as mock_get:
            mock_get.side_effect = responses

            result = client.get_paginated("/v1/listed/info", "info")

            expected = [{"id": 1}, {"id": 2}, {"id": 3}]
            assert result == expected
            assert mock_get.call_count == 3


class TestSessionWithRetries:
    def test_session_created_with_retries(self):
        from jqsys.client import _session_with_retries

        session = _session_with_retries()

        assert isinstance(session, requests.Session)
        # Test that adapters are mounted
        assert "https://" in session.adapters
        assert "http://" in session.adapters

    def test_custom_retry_params(self):
        from jqsys.client import _session_with_retries

        session = _session_with_retries(total=5, backoff=1.0)

        assert isinstance(session, requests.Session)
