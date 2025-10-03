from __future__ import annotations

import os
from unittest.mock import Mock

import pytest


@pytest.fixture
def clean_env(monkeypatch):
    """Clean environment fixture to isolate tests."""
    # Store original values
    original_values = {}
    env_keys = ["JQ_REFRESH_TOKEN", "API_URL", "CUSTOM_TOKEN"]

    for key in env_keys:
        if key in os.environ:
            original_values[key] = os.environ[key]
        monkeypatch.delenv(key, raising=False)

    yield

    # Restore original values
    for key, value in original_values.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def mock_successful_auth_response():
    """Mock a successful J-Quants auth response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"idToken": "mock_id_token_12345"}
    return mock_response


@pytest.fixture
def mock_failed_auth_response():
    """Mock a failed J-Quants auth response."""
    mock_response = Mock()
    mock_response.status_code = 401
    mock_response.json.return_value = {"error": "Invalid refresh token"}
    return mock_response


@pytest.fixture
def sample_daily_quotes_data():
    """Sample daily quotes data matching J-Quants API structure."""
    return {
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
                "AdjustmentOpen": 2845.0,
                "AdjustmentHigh": 2870.0,
                "AdjustmentLow": 2835.0,
                "AdjustmentClose": 2865.0,
                "AdjustmentVolume": 1234567,
            },
            {
                "Code": "13010",
                "Date": "2024-01-15",
                "Open": 4200.0,
                "High": 4250.0,
                "Low": 4180.0,
                "Close": 4230.0,
                "UpperLimit": None,
                "LowerLimit": None,
                "Volume": 567890,
                "TurnoverValue": 2.4e9,
                "AdjustmentOpen": 4200.0,
                "AdjustmentHigh": 4250.0,
                "AdjustmentLow": 4180.0,
                "AdjustmentClose": 4230.0,
                "AdjustmentVolume": 567890,
            },
        ]
    }


@pytest.fixture
def sample_listed_info_data():
    """Sample listed info data matching J-Quants API structure."""
    return {
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
            },
            {
                "Code": "86970",
                "CompanyName": "日本プライムリアルティ投資法人",
                "CompanyNameEnglish": "Japan Prime Realty Investment Corporation",
                "Sector17Code": "8955",
                "Sector17CodeName": "不動産投信",
                "Sector33Code": "8955",
                "Sector33CodeName": "不動産投信",
                "ScaleCategory": "TOPIX Mid400",
                "MarketCode": "0111",
                "MarketCodeName": "プライム",
            },
        ]
    }


@pytest.fixture
def sample_trading_calendar_data():
    """Sample trading calendar data matching J-Quants API structure."""
    return {
        "trading_calendar": [
            {"Date": "2024-01-04", "HolidayDivision": "0"},
            {"Date": "2024-01-05", "HolidayDivision": "0"},
            {
                "Date": "2024-01-06",
                "HolidayDivision": "1",  # Holiday
            },
            {"Date": "2024-01-08", "HolidayDivision": "0"},
        ]
    }
