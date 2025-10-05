from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
from unittest.mock import MagicMock

import polars as pl
import pytest

from jqsys.core.storage.backends.filesystem_backend import FilesystemBackend
from jqsys.core.storage.blob import BlobStorage
from jqsys.data.layers.bronze import BronzeStorage
from jqsys.data.layers.gold import GoldStorage
from jqsys.data.layers.silver import SilverStorage
from jqsys.fin import Stock


@pytest.fixture
def bronze_storage(tmp_path) -> BronzeStorage:
    backend = FilesystemBackend(str(tmp_path / "bronze"))
    storage = BronzeStorage(storage=BlobStorage(backend))

    sample_data = [
        {
            "Code": "13010",
            "CompanyName": "Kyokuyo Co., Ltd.",
            "CompanyNameEnglish": "Kyokuyo Co., Ltd.",
            "Sector17Code": "1050",
            "Sector17CodeName": "Foods",
            "Sector33Code": "1050",
            "Sector33CodeName": "Foods",
            "MarketCode": "0111",
            "MarketCodeName": "Prime",
        },
        {
            "Code": "99990",
            "CompanyName": "Sample Holdings",
            "CompanyNameEnglish": "Sample Holdings Corp.",
            "Sector17Code": "9990",
            "Sector17CodeName": "Other",
            "Sector33Code": "9990",
            "Sector33CodeName": "Other",
            "MarketCode": "0112",
            "MarketCodeName": "Standard",
        },
    ]

    storage.store_raw_response(
        endpoint="listed_info",
        data=sample_data,
        date=datetime(2024, 1, 15),
        metadata={"record_count": len(sample_data)},
    )

    return storage


@pytest.fixture
def gold_storage(tmp_path) -> GoldStorage:
    backend = FilesystemBackend(str(tmp_path / "gold"))
    blob_storage = BlobStorage(backend)

    prices = pl.DataFrame(
        [
            {
                "code": "13010",
                "date": date(2024, 1, 15),
                "open": 101.0,
                "high": 105.0,
                "low": 99.0,
                "close": 103.0,
                "volume": 150000,
                "turnover_value": 12500000.0,
                "adjustment_factor": 0.5,
                "adj_close": 51.5,
                "processed_at": "2024-01-15T10:00:00",
            },
            {
                "code": "13010",
                "date": date(2024, 1, 16),
                "open": 102.0,
                "high": 106.0,
                "low": 101.0,
                "close": 104.5,
                "volume": 160000,
                "turnover_value": 16640000.0,
                "adjustment_factor": 1.0,
                "adj_close": 104.5,
                "processed_at": "2024-01-16T10:00:00",
            },
        ]
    )

    buffer = BytesIO()
    prices.write_parquet(buffer, compression="snappy", use_pyarrow=True)
    buffer.seek(0)
    blob_storage.put(
        "daily_prices/13010/data.parquet",
        buffer.read(),
        content_type="application/parquet",
    )

    silver_stub = MagicMock(spec=SilverStorage)
    return GoldStorage(storage=blob_storage, silver_storage=silver_stub)


def test_stock_listed_info_and_prices(bronze_storage, gold_storage):
    stock = Stock("1301", bronze_storage=bronze_storage, gold_storage=gold_storage)

    assert stock.code == "13010"
    assert stock.base_code == "1301"

    info = stock.get_listed_info()
    assert info["CompanyName"] == "Kyokuyo Co., Ltd."
    assert stock.company_name_english == "Kyokuyo Co., Ltd."

    history = stock.get_price_history()
    assert history.height == 2
    assert history.select("close").to_series().to_list() == [103.0, 104.5]

    latest = stock.get_latest_price()
    assert latest is not None
    assert latest["close"] == 104.5


def test_stock_search_exact(bronze_storage, gold_storage):
    results = Stock.search(
        "CompanyNameEnglish",
        "Kyokuyo Co., Ltd.",
        bronze_storage=bronze_storage,
        gold_storage=gold_storage,
    )

    assert len(results) == 1
    stock = results[0]
    assert stock.code == "13010"
    assert stock.company_name == "Kyokuyo Co., Ltd."


def test_stock_search_icontains(bronze_storage, gold_storage):
    results = Stock.search(
        "CompanyName",
        "sample",
        bronze_storage=bronze_storage,
        gold_storage=gold_storage,
        match="icontains",
    )

    assert {stock.code for stock in results} == {"99990"}


def test_stock_instantiation_with_five_digit_code(bronze_storage, gold_storage):
    stock = Stock("13010", bronze_storage=bronze_storage, gold_storage=gold_storage)

    info = stock.get_listed_info()
    assert info["Code"] == "13010"


def test_adjusted_price_history(bronze_storage, gold_storage):
    stock = Stock("1301", bronze_storage=bronze_storage, gold_storage=gold_storage)

    history = stock.get_price_history(adjust="add").sort("date")

    assert {"adj_open", "adj_high", "adj_low", "adj_close", "adj_volume"}.issubset(
        set(history.columns)
    )

    adjusted_opens = history.select("adj_open").to_series().to_list()
    assert adjusted_opens == [50.5, 102.0]

    adjusted_volume = history.select("adj_volume").to_series().to_list()
    assert adjusted_volume == [300000.0, 160000.0]


def test_convenience_series_methods(bronze_storage, gold_storage):
    stock = Stock("1301", bronze_storage=bronze_storage, gold_storage=gold_storage)

    closes = stock.close_series().to_list()
    assert closes == [51.5, 104.5]

    raw_closes = stock.close_series(adjusted=False).to_list()
    assert raw_closes == [103.0, 104.5]

    volumes = stock.volume_series().to_list()
    assert volumes == [300000.0, 160000.0]

    turnover = stock.turnover_series().to_list()
    assert turnover == [12500000.0, 16640000.0]

    factors = stock.adjustment_factor_series().to_list()
    assert factors == [0.5, 1.0]


def test_adjustment_events(bronze_storage, gold_storage):
    stock = Stock("1301", bronze_storage=bronze_storage, gold_storage=gold_storage)

    events = stock.adjustment_events()

    assert events.height == 1
    assert events.get_column("date").to_list()[0] == date(2024, 1, 15)
    assert events.get_column("adjustment_factor").to_list()[0] == 0.5


def test_stock_search_invalid_field(bronze_storage):
    with pytest.raises(ValueError):
        Stock.search("UnknownField", "value", bronze_storage=bronze_storage)
