from __future__ import annotations

from unittest.mock import Mock, patch

from jqsys.data.auth import get_id_token
from jqsys.data.client import JQuantsClient


class TestIntegration:
    """Integration tests that test the interaction between auth and client modules."""

    @patch("jqsys.data.auth.requests.post")
    def test_full_auth_and_client_flow(self, mock_post):
        # Mock the auth response
        mock_auth_response = Mock()
        mock_auth_response.status_code = 200
        mock_auth_response.json.return_value = {"idToken": "integration_test_token"}
        mock_post.return_value = mock_auth_response

        # Get ID token
        id_token = get_id_token("test_refresh_token")

        # Create client with the token
        client = JQuantsClient(id_token=id_token)

        # Mock API response for client call
        mock_api_response = Mock()
        mock_api_response.json.return_value = {
            "daily_quotes": [
                {
                    "Code": "13010",
                    "Date": "2024-01-15",
                    "Open": 100.0,
                    "High": 105.0,
                    "Low": 95.0,
                    "Close": 102.0,
                    "Volume": 1000,
                },
                {
                    "Code": "13020",
                    "Date": "2024-01-15",
                    "Open": 200.0,
                    "High": 210.0,
                    "Low": 190.0,
                    "Close": 205.0,
                    "Volume": 2000,
                },
            ]
        }

        with patch.object(client, "get", return_value=mock_api_response):
            # Test the full flow
            result = client.get_paginated("/v1/prices/daily_quotes", "daily_quotes")

            assert len(result) == 2
            assert result[0]["Code"] == "13010"
            assert result[1]["Code"] == "13020"
            assert client.headers["Authorization"] == "Bearer integration_test_token"

    @patch("jqsys.data.client.get_id_token")
    @patch("jqsys.data.client.load_refresh_token")
    def test_client_from_env_integration(self, mock_load_refresh, mock_get_id):
        """Test JQuantsClient.from_env() method integration."""
        mock_load_refresh.return_value = "env_refresh_token"
        mock_get_id.return_value = "env_id_token"

        client = JQuantsClient.from_env()

        assert client.id_token == "env_id_token"
        assert client.headers["Authorization"] == "Bearer env_id_token"
        mock_load_refresh.assert_called_once()
        mock_get_id.assert_called_once_with("env_refresh_token")

    def test_realistic_jquants_api_responses(self):
        """Test with realistic J-Quants API response structures."""
        client = JQuantsClient(id_token="test_token")

        # Mock realistic daily quotes response
        daily_quotes_response = Mock()
        daily_quotes_response.json.return_value = {
            "daily_quotes": [
                {
                    "Code": "86970",
                    "Date": "2024-01-15",
                    "Open": 2845.0,
                    "High": 2870.0,
                    "Low": 2835.0,
                    "Close": 2865.0,
                    "UpperLimit": None,
                    "LowerLimit": None,
                    "Volume": 1234567,
                    "TurnoverValue": 3.54e9,
                }
            ],
            "pagination_key": "next_page_key_123",
        }

        # Mock second page response
        second_page_response = Mock()
        second_page_response.json.return_value = {
            "daily_quotes": [
                {
                    "Code": "86971",
                    "Date": "2024-01-15",
                    "Open": 1234.0,
                    "High": 1250.0,
                    "Low": 1220.0,
                    "Close": 1245.0,
                    "UpperLimit": None,
                    "LowerLimit": None,
                    "Volume": 987654,
                    "TurnoverValue": 1.22e9,
                }
            ]
        }

        with patch.object(client, "get") as mock_get:
            mock_get.side_effect = [daily_quotes_response, second_page_response]

            result = client.get_paginated("/v1/prices/daily_quotes", "daily_quotes")

            assert len(result) == 2
            assert result[0]["Code"] == "86970"
            assert result[0]["Volume"] == 1234567
            assert result[1]["Code"] == "86971"
            assert result[1]["TurnoverValue"] == 1.22e9

    def test_realistic_listed_info_response(self):
        """Test with realistic listed info API response."""
        client = JQuantsClient(id_token="test_token")

        mock_response = Mock()
        mock_response.json.return_value = {
            "info": [
                {
                    "Code": "13010",
                    "CompanyName": "日本取引所グループ",
                    "CompanyNameEnglish": "Japan Exchange Group, Inc.",
                    "Sector17Code": "7050",
                    "Sector17CodeName": "その他金融業",
                    "Sector33Code": "7050",
                    "Sector33CodeName": "その他金融業",
                    "ScaleCategory": "TOPIX Large70",
                    "MarketCode": "0111",
                    "MarketCodeName": "プライム",
                }
            ]
        }

        with patch.object(client, "get", return_value=mock_response):
            result = client.get_paginated("/v1/listed/info", "info")

            assert len(result) == 1
            assert result[0]["Code"] == "13010"
            assert result[0]["CompanyName"] == "日本取引所グループ"
            assert result[0]["ScaleCategory"] == "TOPIX Large70"

    def test_realistic_trading_calendar_response(self):
        """Test with realistic trading calendar API response."""
        client = JQuantsClient(id_token="test_token")

        mock_response = Mock()
        mock_response.json.return_value = {
            "trading_calendar": [
                {"Date": "2024-01-04", "HolidayDivision": "0"},
                {"Date": "2024-01-05", "HolidayDivision": "0"},
                {"Date": "2024-01-08", "HolidayDivision": "1"},
            ]
        }

        with patch.object(client, "get", return_value=mock_response):
            result = client.get_paginated("/v1/markets/trading_calendar", "trading_calendar")

            assert len(result) == 3
            assert result[0]["Date"] == "2024-01-04"
            assert result[0]["HolidayDivision"] == "0"
            assert result[2]["HolidayDivision"] == "1"
